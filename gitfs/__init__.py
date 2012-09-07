import contextlib
import io
import logging
import os
import subprocess
import transaction

log = logging.getLogger(__name__)


class GitFS(object):
    session = None

    def __init__(self, path, branch=None, create=True, bare=False):
        db = os.path.join(path, '.git')
        if not os.path.exists(db):
            # Bare repository
            db = path

        head = os.path.join(db, 'HEAD')
        if not os.path.exists(head):
            if create:
                if bare:
                    subprocess.check_output(['git', 'init', '--bare', db])
                else:
                    subprocess.check_output(['git', 'init', path])
                    db = os.path.join(path, '.git')
                    head = os.path.join(db, 'HEAD')
            else:
                raise ValueError('No database found in %s' % path)

        self.db = db
        if branch:
            self.ref = branch
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
        path = mkpath(path)

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

        raise ValueError("Bad mode: %s" % mode)

    def mkdir(self, path):
        session = self._session()
        path = mkpath(path)
        name = path[-1]

        parent = session.find(path[:-1])
        if not parent:
            raise IOError(2, 'No such file or directory', '/'.join(path))
        if not isinstance(parent, TreeNode):
            raise IOError(20, 'Not a directory', '/'.join(path))
        if name in parent.contents:
            raise IOError(17, 'File exists', '/'.join(path))

        parent.new_tree(name)


class Session(object):
    closed = False
    joined = False

    def __init__(self, db, ref):
        self.db = db
        self.ref = ref
        transaction.get().join(self)

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


    def find(self, path):
        assert isinstance(path, (list, tuple))
        tree = self.tree
        if tree:
            return tree.find(path)

    def abort(self, tx):
        """
        Abort transaction without attempting to commit.
        """
        self.close()

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
        if os.path.split(self.db)[1] == '.git':
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

        self.close()

    def tpc_abort(self, tx):
        """
        Clean up in the event that some data manager has vetoed the transaction.
        """
        self.close()

    def close(self):
        self.closed = True
        self.release_lock()

    def acquire_lock(self):
        pass # TODO

    def release_lock(self):
        pass # TODO


class TreeNode(object):
    parent = None
    name = None
    dirty = True

    @classmethod
    def read(cls, db, oid):
        node = cls(db)
        contents = node.contents
        with popen(['git', 'ls-tree', oid],
                   stdout=subprocess.PIPE, cwd=db) as lstree:
            for line in lstree.stdout.readlines():
                mode, type, oid, name = line.split()
                contents[name] = (type, oid, None)

        node.dirty = False
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
        self.set_dirty()
        return obj

    def new_tree(self, name):
        node = TreeNode(self.db)
        node.parent = self
        node.name = name
        self.contents[name] = ('tree', None, node)
        self.set_dirty()

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
            if isinstance(obj, NewBlob):
                obj.close()
            elif type == 'tree' and obj.dirty:
                new_oid = obj.save()
                self.contents[name] = ('tree', new_oid, None)

        # Save tree object out to database
        with popen(['git', 'mktree'], cwd=self.db,
                   stdin=subprocess.PIPE, stdout=subprocess.PIPE) as proc:
            for name, (type, oid, obj) in self.contents.items():
                mode = '100644' if type == 'blob' else '040000'
                print >> proc.stdin, '%s %s %s\t%s' % (mode, type, oid, name)
            proc.stdin.close()
            oid = proc.stdout.read().strip()
        return oid


class Blob(object):

    def __init__(self, db, oid):
        self.db = db
        self.oid = oid

    def open(self):
        return BlobStream(self.db, self.oid)

    def find(self, path):
        if not path:
            return self


class NewBlob(io.RawIOBase):

    def __init__(self, db, prev):
        self.db = db
        self.prev = prev

        self.proc = subprocess.Popen(
            ['git', 'hash-object', '-w', '--stdin'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, cwd=db)

    def write(self, b):
        return self.proc.stdin.write(b)

    def close(self):
        if not self.closed:
            super(NewBlob, self).close()
            self.proc.stdin.close()
            oid = self.proc.stdout.read().strip()
            self.proc.stdout.close()
            self.parent.contents[self.name] = ('blob', oid, None)
            retcode = self.proc.wait()
            if retcode != 0:
                raise subprocess.CalledProcessError(
                    retcode, 'git hash-object -w --stdin')

    def writable(self):
        return True

    def open(self):
        if self.prev:
            return self.prev.open()
        raise IOError(2, 'No such file or directory', object_path(self))


class BlobStream(io.RawIOBase):

    def __init__(self, db, oid):
        # XXX buffer?
        self.proc = subprocess.Popen(
            ['git', 'cat-file', 'blob', oid],
            stdout=subprocess.PIPE, cwd=db)
        self.oid = oid

    def readable(self):
        return True

    def read(self, n=-1):
        return self.proc.stdout.read(n)

    def close(self):
        if not self.closed:
            super(BlobStream, self).close()
            self.proc.stdout.close()
            retcode = self.proc.wait()
            if retcode != 0:
                raise subprocess.CalledProcessError(
                    retcode, 'git cat-file blob %s' % self.oid)


def object_path(obj):
    path = []
    node = obj
    while node.parent:
        path.insert(0, node.name)
        node = node.parent
    return '/'.join(path)


class ConflictError(Exception):
    pass


@contextlib.contextmanager
def popen(args, **kw):
    proc = subprocess.Popen(args, **kw)
    yield proc
    for stream in (proc.stdin, proc.stdout, proc.stderr):
        if stream is not None:
            stream.close()
    retcode = proc.wait()
    if  retcode != 0:
        raise subprocess.CalledProcessError(retcode, repr(args))


def mkpath(path):
    if not isinstance(path, (list, tuple)):
        path = filter(None, path.split('/'))
    return path
