import contextlib
import fcntl
import io
import logging
import os
import shutil
import subprocess
import transaction
import weakref

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
            self.session = _Session(self.db, self.ref)
        return self.session

    def open(self, path, mode='r'):
        session = self._session()
        parsed = _mkpath(path)

        mode = mode.replace('b', '')
        if mode == 'a':
            mode = 'w'
            append = True
        else:
            append = False

        if mode == 'r':
            obj = session.find(parsed)
            if not obj:
                raise _NoSuchFileOrDirectory(path)
            if isinstance(obj, _TreeNode):
                raise _IsADirectory(path)
            return obj.open()

        elif mode == 'w':
            if not parsed:
                raise _IsADirectory(path)
            name = parsed[-1]
            dirpath = parsed[:-1]
            obj = session.find(dirpath)
            if not obj:
                raise _NoSuchFileOrDirectory(path)
            if not isinstance(obj, _TreeNode):
                raise _NotADirectory(path)
            prev = obj.get(name)
            if isinstance(prev, _TreeNode):
                raise _IsADirectory(path)
            blob = obj.new_blob(name, prev)
            if append and prev:
                shutil.copyfileobj(prev.open(), blob)
            return blob

        raise ValueError("Bad mode: %s" % mode)

    def listdir(self, path=''):
        session = self._session()
        obj = session.find(_mkpath(path))
        if not obj:
            raise _NoSuchFileOrDirectory(path)
        if not isinstance(obj, _TreeNode):
            raise _NotADirectory(path)
        return list(obj.contents.keys())

    def mkdir(self, path):
        session = self._session()
        parsed = _mkpath(path)
        name = parsed[-1]

        parent = session.find(parsed[:-1])
        if not parent:
            raise _NoSuchFileOrDirectory(path)
        if not isinstance(parent, _TreeNode):
            raise _NotADirectory(path)
        if name in parent.contents:
            raise _FileExists(path)

        parent.new_tree(name)

    def mkdirs(self, path):
        session = self._session()
        parsed = _mkpath(path)
        node = session.tree
        for name in parsed:
            next_node = node.get(name)
            if not next_node:
                next_node = node.new_tree(name)
            elif not isinstance(next_node, _TreeNode):
                raise _NotADirectory(path)
            node = next_node

    def rm(self, path):
        session = self._session()
        parsed = _mkpath(path)

        obj = session.find(parsed)
        if not obj:
            raise _NoSuchFileOrDirectory(path)
        if isinstance(obj, _TreeNode):
            raise _IsADirectory(path)
        obj.parent.remove(obj.name)

    def rmdir(self, path):
        session = self._session()
        parsed = _mkpath(path)

        obj = session.find(parsed)
        if not obj:
            raise _NoSuchFileOrDirectory(path)
        if not isinstance(obj, _TreeNode):
            raise _NotADirectory(path)
        if not obj.empty():
            raise _DirectoryNotEmpty(path)

        obj.parent.remove(obj.name)

    def rmtree(self, path):
        session = self._session()
        parsed = _mkpath(path)

        obj = session.find(parsed)
        if not obj:
            raise _NoSuchFileOrDirectory(path)
        if not isinstance(obj, _TreeNode):
            raise _NotADirectory(path)

        obj.parent.remove(obj.name)

    def mv(self, src, dst):
        session = self._session()
        spath = _mkpath(src)
        if not spath:
            raise _NoSuchFileOrDirectory(src)
        sname = spath[-1]
        sfolder = session.find(spath[:-1])
        if not sfolder or not sname in sfolder:
            raise _NoSuchFileOrDirectory(src)

        dpath = _mkpath(dst)
        dobj = session.find(dpath)
        if not dobj:
            if dpath:
                dname = dpath[-1]
                dfolder = session.find(dpath[:-1])
                if dfolder:
                    dfolder.set(dname, sfolder.remove(sname))
                    return
            raise _NoSuchFileOrDirectory(dst)
        if isinstance(dobj, _TreeNode):
            dobj.set(sname, sfolder.remove(sname))
        else:
            dobj.parent.set(dobj.name, sfolder.remove(sname))

    def exists(self, path):
        session = self._session()
        return bool(session.find(_mkpath(path)))

    def isdir(self, path):
        session = self._session()
        return isinstance(session.find(_mkpath(path)), _TreeNode)

    def empty(self, path):
        session = self._session()
        obj = session.find(_mkpath(path))
        if not obj:
            raise _NoSuchFileOrDirectory(path)
        if not isinstance(obj, _TreeNode):
            raise _NotADirectory(path)
        return obj.empty()


class ConflictError(Exception):
    pass


class _Session(object):
    closed = False
    joined = False
    lockfd = None

    def __init__(self, db, ref):
        self.db = db
        self.ref = ref
        self.lock_file = os.path.join(db, 'gitfs.lock')
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
                self.tree = _TreeNode(db)
                return
        else:
            self.prev_commit = open(reffile).read().strip()
            self.tree = _TreeNode.read(db, '%s^{tree}' % self.prev_commit)


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
        assert not self.lockfd
        self.lockfd = fd = os.open(self.lock_file, os.O_WRONLY | os.O_CREAT)
        fcntl.lockf(fd, fcntl.LOCK_EX)

    def release_lock(self):
        fd = self.lockfd
        if fd is not None:
            fcntl.lockf(fd, fcntl.LOCK_UN)
            os.close(fd)
            self.lockfd = None


class _TreeNode(object):
    parent = None
    name = None
    dirty = True

    @classmethod
    def read(cls, db, oid):
        node = cls(db)
        contents = node.contents
        with _popen(['git', 'ls-tree', oid],
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
                obj = _TreeNode.read(self.db, oid)
            else:
                obj = _Blob(self.db, oid)
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
        obj = _NewBlob(self.db, prev)
        obj.parent = self
        obj.name = name
        self.contents[name] = ('blob', None, weakref.proxy(obj))
        self.set_dirty()
        return obj

    def new_tree(self, name):
        node = _TreeNode(self.db)
        node.parent = self
        node.name = name
        self.contents[name] = ('tree', None, node)
        self.set_dirty()
        return node

    def remove(self, name):
        entry = self.contents.pop(name)
        self.set_dirty()
        return entry

    def set(self, name, entry):
        self.contents[name] = entry
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
            if isinstance(obj, _NewBlob):
                raise ValueError("Cannot commit transaction with open files.")
            elif type == 'tree' and obj.dirty:
                new_oid = obj.save()
                self.contents[name] = ('tree', new_oid, None)

        # Save tree object out to database
        with _popen(['git', 'mktree'], cwd=self.db,
                   stdin=subprocess.PIPE, stdout=subprocess.PIPE) as proc:
            for name, (type, oid, obj) in self.contents.items():
                mode = '100644' if type == 'blob' else '040000'
                print >> proc.stdin, '%s %s %s\t%s' % (mode, type, oid, name)
            proc.stdin.close()
            oid = proc.stdout.read().strip()
        return oid

    def empty(self):
        return not self.contents

    def __contains__(self, name):
        return name in self.contents


class _Blob(object):

    def __init__(self, db, oid):
        self.db = db
        self.oid = oid

    def open(self):
        return _BlobStream(self.db, self.oid)

    def find(self, path):
        if not path:
            return self


class _NewBlob(io.RawIOBase):

    def __init__(self, db, prev):
        self.db = db
        self.prev = prev

        self.proc = subprocess.Popen(
            ['git', 'hash-object', '-w', '--stdin'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, cwd=db)

    def write(self, b):
        return self.proc.stdin.write(b)

    def close(self):
        if not self.closed:
            super(_NewBlob, self).close()
            self.proc.stdin.close()
            oid = self.proc.stdout.read().strip()
            self.proc.stdout.close()
            retcode = self.proc.wait()
            if retcode != 0:
                raise subprocess.CalledProcessError(
                    retcode, 'git hash-object -w --stdin')
            self.parent.contents[self.name] = ('blob', oid, None)

    def writable(self):
        return True

    def open(self):
        if self.prev:
            return self.prev.open()
        raise _NoSuchFileOrDirectory(_object_path(self))

    def find(self, path):
        if not path:
            return self


class _BlobStream(io.RawIOBase):

    def __init__(self, db, oid):
        # XXX buffer?
        self.proc = subprocess.Popen(
            ['git', 'cat-file', 'blob', oid],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=db)
        self.oid = oid

    def readable(self):
        return True

    def read(self, n=-1):
        return self.proc.stdout.read(n)

    def close(self):
        if not self.closed:
            super(_BlobStream, self).close()
            self.proc.stdout.close()
            self.proc.stderr.close()
            retcode = self.proc.wait()
            if retcode != 0:
                raise subprocess.CalledProcessError(
                    retcode, 'git cat-file blob %s' % self.oid)


def _object_path(obj):
    path = []
    node = obj
    while node.parent:
        path.insert(0, node.name)
        node = node.parent
    return '/'.join(path)


@contextlib.contextmanager
def _popen(args, **kw):
    proc = subprocess.Popen(args, **kw)
    yield proc
    for stream in (proc.stdin, proc.stdout, proc.stderr):
        if stream is not None:
            stream.close()
    retcode = proc.wait()
    if retcode != 0:
        raise subprocess.CalledProcessError(retcode, repr(args))


def _mkpath(path):
    if not isinstance(path, (list, tuple)):
        if path == '.':
            path = []
        else:
            path = filter(None, path.split('/'))
    return path


def _NoSuchFileOrDirectory(path):
    return IOError(2, 'No such file or directory', path)


def _IsADirectory(path):
    return IOError(21, 'Is a directory', path)


def _NotADirectory(path):
    return IOError(20, 'Not a directory', path)


def _FileExists(path):
    return IOError(17, 'File exists', path)


def _DirectoryNotEmpty(path):
    return IOError(39, 'Directory not empty', path)
