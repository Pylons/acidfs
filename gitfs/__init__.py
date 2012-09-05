import contextlib
import io
import logging
import os
import subprocess
import transaction

log = logging.getLogger(__name__)

# TODO
#
# Can subprocesses be context managers?
# Implement locking
# Make sure all subprocess calls are checked
# disuse 'cd' context manager in favor of cwd argument to subprocess calls

class GitFS(object):
    session = None

    def __init__(self, path, ref=None):
        self.db = db = os.path.join(path, '.git')
        if not os.path.exists(db):
            # Bare repository
            self.db = db = path

        head = os.path.join(db, 'HEAD')
        if not os.path.exists(head):
            raise ValueError('No database found in %s' % path)

        if ref:
            self.ref = ref
            return

        headref = open(head).read().strip()
        if not headref.startswith('ref: '):
            raise ValueError('Cannot use detached HEAD state.')

        assert headref.startswith('ref: refs/heads/')
        self.ref = headref[16:]  # len('ref: refs/heads/') == 16

    def _session(self):
        """
        Make sure we're in a session.
        """
        if not self.session or self.session.closed:
            self.session = Session(self.db, self.ref)
        return self.session

    def open(self, path, mode='r'):
        session = self._session()
        if not isinstance(path, (list, tuple)):
            path = filter(None, path.split('/'))

        if mode == 'r':
            obj = session.find(path)
            if not obj:
                raise IOError(2, 'No such file or directory', '/'.join(path))
            if isinstance(obj, TreeNode):
                raise IOError(21, 'Is a directory', '/'.join(path))
            assert isinstance(obj, Blob)
            return obj.open()

        elif mode == 'w':
            name = path[-1]
            dirpath = path[:-1]
            obj = session.find(dirpath)
            if not obj:
                raise IOError(2, 'No such file or directory', '/'.join(path))
            if not isinstance(obj, TreeNode):
                raise IOError(20, 'Not a directory', '/'.join(path))
            prev = obj.get(name)
            if isinstance(prev, TreeNode):
                raise IOError(21, 'Is a directory', '/'.join(path))
            assert isinstance(prev, (Blob, type(None)))
            return obj.new_blob(name, prev)


class Session(object):
    closed = False
    joined = False

    def __init__(self, db, ref):
        self.db = db
        self.ref = ref

        reffile = os.path.join(db, 'refs', 'heads', ref)
        if not os.path.exists(reffile):
            # If no other heads exist, then we have a fresh repo.  Otherwise,
            # we probably have a typo.
            headsdir = os.path.join(db, 'refs', 'heads')
            if os.listdir(headsdir):
                raise ValueError("No such head: %s" % ref)
            else:
                log.info("New repository with no previous commits.")
                self.prev_commit = None
                self.tree = TreeNode(db)
                return
        else:
            self.prev_commit = open(reffile).read().strip()
            self.tree = TreeNode.read(db, '%s^{tree}' % self.prev_commit)

        transaction.get().join(self)

    def find(self, path):
        assert isinstance(path, (list, tuple))
        tree = self.tree
        if tree:
            return tree.find(path)

    ############################################################################
    # datamanager API methods, for use as datamanager for transaction package.
    #
    def abort(self, tx):
        """
        Abort transaction without attempting to commit.
        """
        self.closed = True

    def tpc_begin(self, tx):
        """
        Initiate two phase commit.
        """
        pass

    def commit(self, tx):
        """
        Prepare to save changes, but don't actually save them.
        """
        pass

    def tpc_vote(self, tx):
        """
        If we can't commit the transaction, raise an exception here.  If no
        exception is raised we damn well better be able to get through
        tpc_finish without any errors.  Last chance to bail is here.
        """
        if not self.tree.dirty:
            # Nothing to do
            return

        # Write tree to db
        tree_oid = self.tree.save()

        # Prepare metadata for commit
        message = tx.description
        if not message:
            message = 'GitFS transaction'
        gitenv = os.environ.copy()
        extension = tx._extension  # "Official" API despite underscore
        user = extension.get('user')
        if not user:
            user = tx.user
            if user:
                user = user.split(None, 1)[1] # strip Zope's "path"
        if user:
            gitenv['GIT_AUTHOR_NAME'] = gitenv['GIT_COMMITER_NAME'] = user
        email = extension.get('email')
        if email:
            gitenv['GIT_AUTHOR_EMAIL'] = gitenv['GIT_COMMITTER_EMAIL'] = \
                gitenv['EMAIL'] = email

        # Write commit to db
        args = ['git', 'commit-tree', tree_oid, '-m', message]
        if self.prev_commit:
            args.append('-p')
            args.append(self.prev_commit)
        self.commit_oid = subprocess.check_output(
            args, cwd=self.db, env=gitenv).strip()

        # Acquire an exclusive (aka write) lock for updating the branch ref
        # This would be a good place to do any necessary merging, but for now
        # we'll just bail if anything has gotten committed in the meantime.
        self.acquire_lock()
        reffile = os.path.join(self.db, 'refs', 'heads', self.ref)
        if not os.path.exists(reffile):
            assert not self.prev_commit # First commit
        else:
            cur_commit = open(reffile).read().strip()
            if cur_commit != self.prev_commit:
                raise ConflictError()

    def tpc_finish(self, tx):
        """
        Write data to disk, committing transaction.
        """
        if not self.tree.dirty:
            # Nothing to do
            return

        # Update branch to point to our commit
        reffile = os.path.join(self.db, 'refs', 'heads', self.ref)
        with open(reffile, 'w') as f:
            print >> f, self.commit_oid

        # Update working directory if appropriate
        if self.db.endswith('.git'):
            # Is not a bare repository
            headfile = os.path.join(self.db, 'HEAD')
            working_head = open(headfile).read().strip()
            assert working_head.startswith('ref: refs/heads/')
            working_head = working_head[16:]
            if working_head == self.ref:
                # And the working directory is tracking the branch we just
                # committed to.
                subprocess.check_output(['git', 'reset', 'HEAD', '--hard'],
                                        cwd=self.db[:-5])

        # Let other sessions commit
        self.release_lock()

    def tpc_abort(self, tx):
        """
        Clean up in the event that some data manager has vetoed the transaction.
        """
        self.closed = True

    def acquire_lock(self):
        pass # TODO

    def release_lock(self):
        pass # TODO


class TreeNode(object):
    parent = None
    name = None
    dirty = False

    @classmethod
    def read(cls, db, oid):
        node = cls(db)
        contents = node.contents
        with cd(db):
            lstree = subprocess.Popen(['git', 'ls-tree', oid],
                                      stdout=subprocess.PIPE)
            for line in lstree.stdout.readlines():
                mode, type, oid, name = line.split()
                contents[name] = (type, oid, None)

        return node

    def __init__(self, db):
        self.db = db
        self.contents = {}

    def get(self, name):
        contents = self.contents
        obj = contents.get(name)
        if not obj:
            return None
        type, oid, obj = obj
        assert type in ('tree', 'blob')
        if not obj:
            if type == 'tree':
                obj = TreeNode.read(self.db, oid)
            else:
                obj = Blob(self.db, oid)
            obj.parent = self
            obj.name = name
            contents[name] = (type, oid, obj)
        return obj

    def find(self, path):
        if not path:
            return self
        obj = self.get(path[0])
        if obj:
            return obj.find(path[1:])

    def new_blob(self, name, prev):
        obj = NewBlob(self.db, prev)
        obj.parent = self
        obj.name = name
        self.contents[name] = ('blob', None, obj)
        return obj

    def set_dirty(self):
        node = self
        while node and not node.dirty:
            node.dirty = True
            node = node.parent

    def save(self):
        # Recursively save children, first
        for name, (type, oid, obj) in list(self.contents.items()):
            if not obj:
                continue # Nothing to do
            if type == 'blob' and not oid:
                raise ValueError(
                    'Open file: %s.  Must close files before commit.' %
                    object_path(obj))
            elif type == 'tree' and obj.dirty:
                new_oid = obj.save()
                self.contents[name] = ('tree', new_oid, None)

        # Save tree object out to database
        proc = subprocess.Popen(['git', 'mktree'], cwd=self.db,
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE)
        for name, (type, oid, obj) in self.contents.items():
            mode = '100644' if type == 'blob' else '040000'
            print >> proc.stdin, '%s %s %s\t%s' % (mode, type, oid, name)
        proc.stdin.close()
        oid = proc.stdout.read().strip()
        proc.stdout.close()
        return oid


class Blob(object):

    def __init__(self, db, oid):
        self.db = db
        self.oid = oid

    def open(self):
        db = self.db
        with cd(db):
            # Documentation and internet scuttlebutt on pipe IO is generally
            # very confusing and contradictory.  This may not be the best way
            # to do it or the best way may be different depending on Python
            # version.
            proc = subprocess.Popen(['git', 'cat-file', 'blob', self.oid],
                                    stdout=subprocess.PIPE)
        return proc.stdout

    def find(self, path):
        if not path:
            return self


class NewBlob(io.RawIOBase):

    def __init__(self, db, prev):
        self.db = db
        self.prev = prev

        proc = subprocess.Popen(
            ['git', 'hash-object', '-w', '--stdin'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, cwd=db)
        self.sink = proc.stdin
        self.source = proc.stdout

    def write(self, b):
        return self.sink.write(b)

    def close(self):
        super(NewBlob, self).close()
        self.sink.close()
        oid = self.source.read().strip()
        self.source.close()
        self.parent.contents[self.name] = ('blob', oid, None)
        self.parent.set_dirty()

    def writable(self):
        return True

    def open(self):
        if self.prev:
            return self.prev.open()
        raise IOError(2, 'No such file or directory', object_path(self))


def object_path(obj):
    path = []
    node = obj
    while node:
        path.insert(0, node.name)
        node = node.parent
    return '/'.join(path)


@contextlib.contextmanager
def cd(path):
    prev = os.getcwd()
    os.chdir(path)
    yield
    os.chdir(prev)


class ConflictError(Exception):
    pass
