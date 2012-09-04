import contextlib
import logging
import os
import subprocess

from acidfs.datamanager import AcidFSDataManagerMixin

log = logging.getLogger(__name__)


class AcidFS(object):
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
            assert isinstance(obj, (Blob, type(None)))
            return obj.new_blob(name, prev)


class Session(AcidFSDataManagerMixin):

    def __init__(self, db, ref):
        super(Session, self).__init__()
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
            self.tree = TreeNode.read(db, '%s^{tree}' % ref)

    def find(self, path):
        assert isinstance(path, (list, tuple))
        tree = self.tree
        if tree:
            return tree.find(path)


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
        raise NotImplementedError()


class Blob(object):

    def __init__(self, db, oid):
        self.db = db
        self.oid = oid

    def open(self):
        db = self.db
        with cd(db):
            # Documentation and internet scuttlebutt on pipe IO is generally very
            # confusing and contradictory.  This may not be the best way to do it
            # or the best way may be different depending on Python version.
            proc = subprocess.Popen(['git', 'cat-file', 'blob', self.oid],
                                    stdout=subprocess.PIPE)
        return proc.stdout

    def find(self, path):
        if not path:
            return self


@contextlib.contextmanager
def cd(path):
    prev = os.getcwd()
    os.chdir(path)
    yield
    os.chdir(prev)
