import contextlib
import fcntl
import io
import logging
import os
import shutil
import subprocess
import tempfile
import traceback
import transaction
import weakref

log = logging.getLogger(__name__)


class AcidFS(object):
    """
    An instance of `AcidFS` exposes a transactional filesystem view of a `Git`
    repository.  Instances of `AcidFS` are not threadsafe and should not be
    shared across threads, greenlets, etc.

    **Paths**

    Many methods take a `path` as an argument.  All paths use forward slash `/`
    as a separator, regardless of the path separator of the
    underlying operating system.  The path `/` represents the root folder of
    the repository.  Paths may be relative or absolute: paths beginning with a
    `/` are absolute with respect to the repository root, paths not beginning
    with a `/` are relative to the current working directory.  The current
    working directory always starts at the root of the repository.  The current
    working directory can be changed using the :meth:`chdir` and
    :meth:`cd` methods.

    **Constructor Arguments**

    ``repo``

       The path to the repository in the real, local filesystem.

    ``head``

       The name of a branch to use as the head for this transaction.  Changes
       made using this instance will be merged to the given head.  The default,
       if omitted, is to use the repository's current head.

    ``create``

       If there is not a Git repository in the indicated directory, should one
       be created?  The default is `True`.

    ``bare``

       If the Git repository is to be created, create it as a bare repository.
       If the repository is already created or `create` is False, this argument
       has no effect.

    ``user_name``

       If the Git repository is to be created, set the user name for the
       repository to this value.  This is the same as creating the repository
       and running `git config user.name "<user_name>"`.

    ``user_email``

       If the Git repository is to be created, set the user email for the
       repository to this value.  This is the same as creating the repository
       and running `git config user.email "<user_email>"`.

    ``name``

       Name to be used as a sort key when ordering the various databases
       (datamanagers in the parlance of the transaction package) during a
       commit.  It is exceedingly rare that you would need to use anything other
       than the default, here.

    ``path_encoding``

       Encode paths with this encoding. The default is `ascii`.
    """

    session = None
    _cwd = ()

    def __init__(
        self,
        repo,
        head="HEAD",
        create=True,
        bare=False,
        user_name=None,
        user_email=None,
        name="AcidFS",
        path_encoding="ascii",
    ):
        wdpath = repo
        dbpath = os.path.join(repo, ".git")
        if not os.path.exists(dbpath):
            wdpath = None
            dbpath = repo
            if create:
                args = ["git", "init", repo]
                if bare:
                    args.append("--bare")
                else:
                    wdpath = repo
                    dbpath = os.path.join(repo, ".git")
                _check_output(args)
                if user_name:
                    args = ["git", "config", "user.name", user_name]
                    _check_output(args, cwd=dbpath)
                if user_email:
                    args = ["git", "config", "user.email", user_email]
                    _check_output(args, cwd=dbpath)
                args = ["git", "config", "core.quotepath", "false"]
                _check_output(args, cwd=dbpath)
            else:
                raise ValueError("No database found in %s" % dbpath)

        self.wd = wdpath
        self.db = dbpath
        self.head = head
        self.name = name
        self.path_encoding = path_encoding

    def _session(self):
        """
        Make sure we're in a session.
        """
        if not self.session or self.session.closed:
            self.session = _Session(
                self.wd, self.db, self.head, self.name, self.path_encoding
            )
        return self.session

    def _mkpath(self, path):
        if path == ".":
            parsed = []
        else:
            parsed = list(filter(None, path.split("/")))
        if not path.startswith("/"):
            parsed = list(self._cwd) + parsed
        return parsed

    def get_base(self):
        """
        Returns the id of the commit that is the current base for the
        transaction.
        """
        session = self._session()
        return session.prev_commit

    def set_base(self, commit):
        """
        Sets the base commit for the current transaction.  The `commit`
        argument may be the SHA1 of a commit or the name of a reference (eg.
        branch or tag).  The current transaction must be clean.  If any changes
        have been made in the transaction, a ConflictError will be raised.
        """
        session = self._session()
        session.set_base(commit)

    def cwd(self):
        """
        Returns the path to the current working directory in the repository.
        """
        return "/" + "/".join(self._cwd)

    def chdir(self, path):
        """
        Change the current working directory in repository.
        """
        session = self._session()
        parsed = self._mkpath(path)
        obj = session.find(parsed)
        if not obj:
            raise _NoSuchFileOrDirectory(path)
        if not isinstance(obj, _TreeNode):
            raise _NotADirectory(path)
        self._cwd = parsed

    @contextlib.contextmanager
    def cd(self, path):
        """
        A context manager that changes the current working directory only in
        the scope of the 'with' context.  Eg::

            import acidfs

            fs = acidfs.AcidFS('myrepo')
            with fs.cd('some/folder'):
                fs.open('a/file')   # relative to /some/folder
            fs.open('another/file') # relative to /
        """
        prev = self._cwd
        self.chdir(path)
        yield
        self._cwd = prev

    def open(
        self, path, mode="r", buffering=-1, encoding=None, errors=None, newline=None
    ):
        """
        Open a file for reading or writing.

        Implements the semantics of the `open` function in Python's `io module
        <http://docs.python.org/library/io.html#io.open>`_, which is the
        default implementation `in Python 3
        <http://docs.python.org/py3k/library/functions.html#open>`_. Opening a
        file in text mode will return a file-like object which reads or writes
        unicode strings, while opening a file in binary mode will return a
        file-like object which reads or writes raw bytes.

        Because the underlying implementation uses a pipe to a `Git` plumbing
        command, opening for update (read and write) is not supported, nor is
        seeking.
        """
        session = self._session()
        parsed = self._mkpath(path)

        if "b" in mode:
            text = False
            if "t" in mode:
                raise ValueError("can't have text and binary mode at once")
        else:
            if not buffering:
                raise ValueError("can't have unbuffered text I/O")
            text = True

        if "+" in mode:
            raise ValueError("Read/write mode is not supported")

        mode = mode.replace("b", "")
        mode = mode.replace("t", "")
        if mode == "a":
            mode = "w"
            append = True
        else:
            append = False
        if mode == "x":
            mode = "w"
            exclusive = True
        else:
            exclusive = False

        if buffering < 0:
            buffer_size = io.DEFAULT_BUFFER_SIZE
            line_buffering = False
        elif buffering == 1:
            buffer_size = io.DEFAULT_BUFFER_SIZE
            line_buffering = True
        else:
            buffer_size = buffering
            line_buffering = False

        if mode == "r":
            obj = session.find(parsed)
            if not obj:
                raise _NoSuchFileOrDirectory(path)
            if isinstance(obj, _TreeNode):
                raise _IsADirectory(path)
            stream = obj.open()
            if buffering:
                stream = io.BufferedReader(stream, buffer_size)
            if text:
                stream = io.TextIOWrapper(
                    stream, encoding, errors, newline, line_buffering
                )
            return stream

        elif mode == "w":
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
            if prev and exclusive:
                raise _FileExists(path)
            blob = obj.new_blob(name, prev)
            if append and prev:
                shutil.copyfileobj(prev.open(), blob)
            if buffering:
                blob = io.BufferedWriter(blob, buffer_size)
            if text:
                blob = io.TextIOWrapper(blob, encoding, errors, newline, line_buffering)
            return blob

        raise ValueError("Bad mode: %s" % mode)

    def hash(self, path=""):
        """
        Returns the sha1 hash of the object referred to by `path`.  If `path` is
        omitted the current working directory is used.
        """
        session = self._session()
        obj = session.find(self._mkpath(path))
        if not obj:
            raise _NoSuchFileOrDirectory(path)
        return obj.hash()

    def listdir(self, path=""):
        """
        Return list of files in indicated directory.  If `path` is omitted, the
        current working directory is used.
        """
        session = self._session()
        obj = session.find(self._mkpath(path))
        if not obj:
            raise _NoSuchFileOrDirectory(path)
        if not isinstance(obj, _TreeNode):
            raise _NotADirectory(path)
        return list(obj.contents.keys())

    def mkdir(self, path):
        """
        Create a new directory.  The parent of the new directory must already
        exist.
        """
        session = self._session()
        parsed = self._mkpath(path)
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
        """
        Create a new directory, including any ancestors which need to be created
        in order to create the directory with the given `path`.
        """
        session = self._session()
        parsed = self._mkpath(path)
        node = session.tree
        for name in parsed:
            next_node = node.get(name)
            if not next_node:
                next_node = node.new_tree(name)
            elif not isinstance(next_node, _TreeNode):
                raise _NotADirectory(path)
            node = next_node

    def rm(self, path):
        """
        Remove a single file.
        """
        session = self._session()
        parsed = self._mkpath(path)

        obj = session.find(parsed)
        if not obj:
            raise _NoSuchFileOrDirectory(path)
        if isinstance(obj, _TreeNode):
            raise _IsADirectory(path)
        obj.parent.remove(obj.name)

    def rmdir(self, path):
        """
        Remove a single directory.  The directory must be empty.
        """
        session = self._session()
        parsed = self._mkpath(path)

        if not parsed:
            raise ValueError("Can't remove root directory.")

        obj = session.find(parsed)
        if not obj:
            raise _NoSuchFileOrDirectory(path)
        if not isinstance(obj, _TreeNode):
            raise _NotADirectory(path)
        if not obj.empty():
            raise _DirectoryNotEmpty(path)

        obj.parent.remove(obj.name)

    def rmtree(self, path):
        """
        Remove a directory and any of its contents.
        """
        session = self._session()
        parsed = self._mkpath(path)

        if not parsed:
            raise ValueError("Can't remove root directory.")

        obj = session.find(parsed)
        if not obj:
            raise _NoSuchFileOrDirectory(path)
        if not isinstance(obj, _TreeNode):
            raise _NotADirectory(path)

        obj.parent.remove(obj.name)

    def mv(self, src, dst):
        """
        Move a file or directory from `src` path to `dst` path.
        """
        session = self._session()
        spath = self._mkpath(src)
        if not spath:
            raise _NoSuchFileOrDirectory(src)
        sname = spath[-1]
        sfolder = session.find(spath[:-1])
        if not sfolder or sname not in sfolder:
            raise _NoSuchFileOrDirectory(src)

        dpath = self._mkpath(dst)
        dobj = session.find(dpath)
        if not dobj:
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
        """
        Returns boolean indicating whether a file or directory exists at the
        given `path`.
        """
        session = self._session()
        return bool(session.find(self._mkpath(path)))

    def isdir(self, path):
        """
        Returns boolean indicating whether the given `path` is a directory.
        """
        session = self._session()
        return isinstance(session.find(self._mkpath(path)), _TreeNode)

    def empty(self, path):
        """
        Returns boolean indicating whether the directory indicated by `path` is
        empty.
        """
        session = self._session()
        obj = session.find(self._mkpath(path))
        if not obj:
            raise _NoSuchFileOrDirectory(path)
        if not isinstance(obj, _TreeNode):
            raise _NotADirectory(path)
        return obj.empty()


class ConflictError(Exception):
    def __init__(self, msg="Unable to merge changes to repository."):
        super(ConflictError, self).__init__(msg)


class _Session(object):
    closed = False
    lockfd = None

    def __init__(self, wd, db, head, name, path_encoding):
        self.wd = wd
        self.db = db
        self.name = name
        self.path_encoding = path_encoding
        self.lock_file = os.path.join(db, "acidfs.lock")
        transaction.get().join(self)

        curhead = open(os.path.join(db, "HEAD")).read().strip()[16:]
        if head == curhead:
            head = "HEAD"
        if head == "HEAD":
            self.headref = os.path.join(db, "refs", "heads", curhead)
        else:
            self.headref = os.path.join(db, "refs", "heads", head)
        self.head = head

        if os.path.exists(self.headref):
            # Existing head, get head revision
            self.prev_commit = _check_output(
                ["git", "rev-list", "--max-count=1", head], cwd=db
            ).strip()
            tree = _check_output(
                [
                    "git",
                    "rev-parse",
                    f"{self.prev_commit.decode('ascii')}^{{tree}}",
                ],
                cwd=self.db,
            ).strip()
            self.tree = _TreeNode.read(db, tree, path_encoding)
        else:
            # New head, no commits yet
            self.tree = _TreeNode(db, path_encoding)  # empty tree
            self.prev_commit = None

    def set_base(self, ref):
        if self.tree.dirty:
            raise ConflictError(
                "Cannot set base when changes already made in transaction."
            )
        self.prev_commit = _check_output(
            ["git", "rev-list", "--max-count=1", ref], cwd=self.db
        ).strip()
        self.tree = _TreeNode.read(self.db, self.prev_commit, self.path_encoding)

    def find(self, path):
        assert isinstance(path, (list, tuple))
        return self.tree.find(path)

    def abort(self, tx):
        """
        Part of datamanager API.
        """
        self.close()

    def tpc_begin(self, tx):
        """
        Part of datamanager API.
        """

    def commit(self, tx):
        """
        Part of datamanager API.
        """

    def tpc_vote(self, tx):
        """
        Part of datamanager API.
        """
        if not self.tree.dirty:
            # Nothing to do
            return

        # Write tree to db
        tree_oid = self.tree.save()
        if self.tree.committed_oid == tree_oid:
            # Nothing actually changed
            self.tree.dirty = False
            return

        if self.prev_commit:
            parents = [self.prev_commit]
        else:
            parents = []
        commit_oid = self.mkcommit(tx, tree_oid, parents)

        # Acquire an exclusive (aka write) lock for merge.
        self.acquire_lock()

        # If this is initial commit, there's not really anything to merge
        if not self.prev_commit:
            # Make sure there haven't been other commits
            if os.path.exists(self.headref):
                # This was to be the initial commit, but somebody got to it
                # first No idea how to try to resolve that one.  Luckily it
                # will be very rare.
                raise ConflictError()

            # New commit is new head
            self.next_commit = commit_oid
            return

        # Find the merge base
        current = _check_output(
            ["git", "rev-list", "--max-count=1", "HEAD"], cwd=self.db
        ).strip()
        merge_base = _check_output(
            ["git", "merge-base", current, commit_oid], cwd=self.db
        ).strip()

        # If the merge base is the current commit, it means there have been no
        # intervening changes and we can just fast forward to the new commit.
        # This is the most common case.
        if merge_base == current:
            self.next_commit = commit_oid
            return

        # Darn it, now we have to actually try to merge
        self.merge(merge_base, current, tree_oid)
        self.next_commit = self.mkcommit(
            tx, self.tree.save(), [current, commit_oid], "Merge"
        )

    def tpc_finish(self, tx):
        """
        Part of datamanager API.
        """
        if not self.tree.dirty:
            # Nothing to do
            return

        # Make our commit the new head
        if self.head == "HEAD":
            # Use git reset to update current head
            args = ["git", "reset", self.next_commit]
            if self.wd:
                args.append("--hard")
                cwd = self.wd
            else:
                args.append("--soft")
                cwd = self.db
            _check_output(args, cwd=cwd)

        else:
            # If not updating current head, just write the commit to the ref
            # file directly.
            reffile = os.path.join(self.db, "refs", "heads", self.head)
            with open(reffile, "wb") as f:
                f.write(self.next_commit)
                f.write(b"\n")

        self.close()

    def tpc_abort(self, tx):
        """
        Part of datamanager API.
        """
        self.close()

    def sortKey(self):
        return self.name

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

    def mkcommit(self, tx, tree_oid, parents, message=None):
        # Prepare metadata for commit
        if not message:
            message = tx.description
        if not message:
            message = "AcidFS transaction"
        gitenv = os.environ.copy()
        extension = tx._extension  # "Official" API despite underscore
        user = extension.get("acidfs_user")
        if not user:
            user = extension.get("user")
            if not user:
                user = tx.user
                if user:
                    if user.startswith(" "):
                        user = user[1:]
                    else:
                        # strip Zope's "path"
                        user = user.split(None, 1)
                        if len(user) == 2:
                            user = user[1]
                        else:
                            user = user[0]
        if user:
            gitenv["GIT_AUTHOR_NAME"] = gitenv["GIT_COMMITER_NAME"] = user

        email = extension.get("acidfs_email")
        if not email:
            email = extension.get("email")
        if email:
            gitenv["GIT_AUTHOR_EMAIL"] = gitenv["GIT_COMMITTER_EMAIL"] = gitenv[
                "EMAIL"
            ] = email

        # Write commit to db
        args = ["git", "commit-tree", tree_oid, "-m", message]
        for parent in parents:
            args.append("-p")
            args.append(parent)
        return _check_output(args, cwd=self.db, env=gitenv).strip()

    def merge(self, base_oid, current, tree_oid):
        """
        This attempts to interpret the output of 'git merge-tree', given the
        current head, the tree we're currently working on, and the nearest
        common ancestor commit (base_oid).

        I haven't found any documentation on the format of the output of
        'git merge-tree' so this is basically reverse engineered from studying
        its output in different situations.  I try to be as conservative as
        possible here and bail as soon as I hit anything I'm not 100% sure
        about.  It is far preferable to raise a ConflictError than incorrectly
        merge.  As such, the code below is peppered with assertions using the
        'expect' function, which will raise a ConflictError if any of our
        expectations aren't met.  I also attempt to log as much useful debug
        information as possible in the case of an unmet expectation, so I can go
        back and take into account more cases as they are encountered.

        The basic algorithm here is a finite state machine operating on the
        output of 'git merge-tree' one line at a time.  This should be fairly
        memory efficient for even large changesets, with the caveat there may
        have been added a large binary file which contains few or no line break
        characters, which could cause a buffer to get large while scanning
        through the merge data.

        One might ask, why not use the porcelain 'git merge' command?  One
        reason is, in the context of the two phase commit protocol, we'd rather
        do pretty much everything we possibly can in the voting stage, leaving
        ourselves with nothing to do in the finish phase except updating the
        head to the commit we just created, and possibly updating the working
        directory--operations that are guaranteed to work.  Since 'git merge'
        will update the head, we'd prefer to do it during the final phase of the
        commit, but we can't guarantee it will work.  There is not a convenient
        way to do a merge dry run during the voting phase.  Although I can
        conceive of ways to do the merge during the voting phase and roll back
        to the previous head if we need to, that feels a little riskier.  Doing
        the merge ourselves, here, also frees us from having to work with a
        working directory, required by the porcelain 'git merge' command.  This
        means we can use bare repositories and/or have transactions that use
        a head other than the repositories 'current' head.

        In general, tranactions will be short and will not have much a of a
        chance to get very far behind the head, so merges will tend not to be
        terribly complicated.  We should be able to handle the vast majority of
        cases here, even if there are some rare corner cases the porcelain
        command might be able to handle that we can't.  I think that's a
        reasonable trade off for the flexibility this approach provides.

        Some dead/unreachable branches are left in here, just in case we haven't
        entirely characterized the behavior of 'git merge-tree'. These are marked with
        'pragma NO COVER' and are easily recognized.
        """
        with _popen(
            ["git", "merge-tree", base_oid, tree_oid, current],
            cwd=self.db,
            stdout=subprocess.PIPE,
        ) as proc:
            # Messy finite state machine
            state = None
            extra_state = None
            stream = proc.stdout
            line = stream.readline()

            def expect(expectation, *msg):
                if not expectation:  # pragma no cover
                    log.debug("Unmet expectation during merge.")
                    log.debug("".join(traceback.format_stack()))
                    if msg:
                        log.debug(msg[0], *msg[1:])
                    if extra_state:
                        log.debug("Extra state: %s", extra_state)
                    raise ConflictError()

            while line:
                if state is None:  # default, scanning for start of a change
                    if line[0:1].isalpha():
                        # If first column is a letter, then we have the first
                        # line of a change, which describes the change.
                        line = line.strip()
                        if line in (
                            b"added in local",
                            b"removed in local",
                            b"removed in both",
                        ):  # pragma NO COVER
                            # This doesn't seem to come up in practice
                            # We don't care about changes to our current tree.
                            # We already know about those.
                            pass

                        elif line == b"added in remote":
                            # The head got a new file, we should grab it
                            state = _MERGE_ADDED_IN_REMOTE
                            extra_state = []

                        elif line == b"removed in remote":
                            # File got deleted from head, remove it
                            state = _MERGE_REMOVED_IN_REMOTE
                            extra_state = []

                        elif line == b"changed in both":
                            # File was edited in both branches, see if we can
                            # patch
                            state = _MERGE_CHANGED_IN_BOTH
                            extra_state = []

                        elif line == b"added in both":
                            state = _MERGE_ADDED_IN_BOTH
                            extra_state = []

                        else:  # pragma NO COVER
                            log.debug("Don't know how to merge: %s", line)
                            raise ConflictError()

                elif state is _MERGE_ADDED_IN_REMOTE:
                    if line[0:1].isalpha() or line.startswith(b"@"):
                        # Done collecting tree lines, only expecting one
                        expect(len(extra_state) == 1, "Wrong number of lines")
                        whose, mode, oid, path = _parsetree(extra_state[0])
                        expect(whose == b"their", "Unexpected whose: %s", whose)
                        expect(mode == b"100644", "Unexpected mode: %s", mode)
                        parsed = path.decode("ascii").split("/")
                        folder = self.find(parsed[:-1])
                        expect(isinstance(folder, _TreeNode), "Not a folder: %s", path)
                        folder.set(parsed[-1], (b"blob", oid, None))
                        state = extra_state = None
                        continue

                    else:
                        extra_state.append(line)

                elif state is _MERGE_REMOVED_IN_REMOTE:
                    if line[0:1].isalpha() or line.startswith(b"@"):
                        # Done collecting tree lines, expect two, one for base,
                        # one for our copy, whose sha1s should match
                        expect(len(extra_state) == 2, "Wrong number of lines")
                        whose, mode, oid, path = _parsetree(extra_state[0])
                        expect(
                            whose in (b"our", b"base"), "Unexpected whose: %s", whose
                        )
                        expect(mode == b"100644", "Unexpected mode: %s", mode)
                        whose, mode, oid2, path2 = _parsetree(extra_state[1])
                        expect(
                            whose in (b"our", b"base"), "Unexpected whose: %s", whose
                        )
                        expect(mode == b"100644", "Unexpected mode: %s", mode)
                        expect(oid == oid2, "SHA1s don't match")
                        expect(path == path2, "Paths don't match")
                        path = path.decode("ascii").split("/")
                        folder = self.find(path[:-1])
                        expect(isinstance(folder, _TreeNode), "Not a folder")
                        folder.remove(path[-1])
                        state = extra_state = None
                        continue

                    else:
                        extra_state.append(line)

                elif state is _MERGE_CHANGED_IN_BOTH:
                    if line.startswith(b"@"):
                        # Done collecting tree lines, expect three, one for base
                        # and one for each copy
                        expect(len(extra_state) == 3, "Wrong number of lines")
                        whose, mode, oid, path = _parsetree(extra_state[0])
                        expect(
                            whose in (b"base", b"our", b"their"),
                            "Unexpected whose: %s",
                            whose,
                        )
                        expect(mode == b"100644", "Unexpected mode: %s", mode)
                        for extra_line in extra_state[1:]:
                            whose, mode, oid2, path2 = _parsetree(extra_line)
                            expect(
                                whose in (b"base", b"our", b"their"),
                                "Unexpected whose: %s",
                                whose,
                            )
                            expect(mode == b"100644", "Unexpected mode: %s", mode)
                            expect(path == path2, "Paths don't match")
                        parsed = path.decode("ascii").split("/")
                        folder = self.find(parsed[:-1])
                        expect(isinstance(folder, _TreeNode), "Not a folder")
                        name = parsed[-1]
                        blob = folder.get(name)
                        expect(isinstance(blob, _Blob), "Not a blob")
                        with _tempfile() as tmp:
                            shutil.copyfileobj(blob.open(), open(tmp, "wb"))
                            with _popen(
                                ["patch", "-s", tmp, "-"],
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                            ) as p:
                                f = p.stdin
                                while line and not line[0:1].isalpha():
                                    if line[1:9] == b"<<<<<<< ":
                                        raise ConflictError()
                                    f.write(line)
                                    line = stream.readline()
                            newblob = folder.new_blob(name, blob)
                            shutil.copyfileobj(open(tmp, "rb"), newblob)

                        state = extra_state = None
                        continue

                    else:
                        extra_state.append(line)

                elif state is _MERGE_ADDED_IN_BOTH:  # pragma NO BRANCH
                    # NO BRANCH pragma added to workaround what seems to be a bug in
                    # coverage. This if..elif structure handles every case that's thrown
                    # at it, but coverage seems concerned that there isn't a case that
                    # doesn't get handled.
                    if line[0:1].isalpha() or line.startswith(b"@"):
                        # Done collecting tree lines, expect two, one for base,
                        # one for our copy, whose sha1s should match
                        expect(len(extra_state) == 2, "Wrong number of lines")
                        whose, mode, oid, path = _parsetree(extra_state[0])
                        expect(
                            whose in (b"our", b"their"), "Unexpected whose: %s", whose
                        )
                        expect(mode == b"100644", "Unexpected mode: %s", mode)
                        whose, mode, oid2, path2 = _parsetree(extra_state[1])
                        expect(
                            whose in (b"our", b"their"), "Unexpected whose: %s", whose
                        )
                        expect(mode == b"100644", "Unexpected mode: %s", mode)
                        expect(path == path2, "Paths don't match")
                        # Either it's the same file or a different file.
                        if oid != oid2:
                            # Different files, can't merge
                            raise ConflictError()

                        else:  # pragma NO COVER
                            # Seems to not come up. Probably merge-tree detects this and
                            # doesn't bother us about it.
                            # Same file, nothing to do
                            state = extra_state = None
                            continue

                    else:
                        extra_state.append(line)

                line = stream.readline()


class _TreeNode(object):
    parent = None
    name = None
    dirty = False
    oid = None
    committed_oid = None

    @classmethod
    def read(cls, db, oid, path_encoding):
        node = cls(db, path_encoding)
        node.committed_oid = node.oid = oid
        contents = node.contents
        with _popen(["git", "ls-tree", oid], stdout=subprocess.PIPE, cwd=db) as lstree:
            for line in lstree.stdout.readlines():
                mode, type, oid, name = _parsetree(line)
                name = name.decode(path_encoding)
                contents[name] = (type, oid, None)

        return node

    def __init__(self, db, path_encoding):
        self.db = db
        self.path_encoding = path_encoding
        self.contents = {}

    def get(self, name):
        contents = self.contents
        obj = contents.get(name)
        if not obj:
            return None
        type, oid, obj = obj
        assert type in (b"tree", b"blob")
        if not obj:
            if type == b"tree":
                obj = _TreeNode.read(self.db, oid, self.path_encoding)
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
        self.contents[name] = (b"blob", None, weakref.proxy(obj))
        self.set_dirty()
        return obj

    def new_tree(self, name):
        node = _TreeNode(self.db, self.path_encoding)
        node.parent = self
        node.name = name
        self.contents[name] = (b"tree", None, node)
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
            node.oid = None
            node.dirty = True
            node = node.parent

    def save(self):
        # Recursively save children, first
        for name, (type, oid, obj) in list(self.contents.items()):
            if not obj:
                continue  # Nothing to do
            if isinstance(obj, _NewBlob):
                raise ValueError("Cannot commit transaction with open files.")
            elif type == b"tree" and (obj.dirty or not oid):
                new_oid = obj.save()
                self.contents[name] = (b"tree", new_oid, None)

        # Save tree object out to database
        with _popen(
            ["git", "mktree"],
            cwd=self.db,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        ) as proc:
            for name, (type, oid, obj) in self.contents.items():
                proc.stdin.write(b"100644" if type == b"blob" else b"040000")
                proc.stdin.write(b" ")
                proc.stdin.write(type)
                proc.stdin.write(b" ")
                proc.stdin.write(oid)
                proc.stdin.write(b"\t")
                proc.stdin.write(name.encode(self.path_encoding))
                proc.stdin.write(b"\n")
            proc.stdin.close()
            oid = proc.stdout.read().strip()
        self.oid = oid
        return oid

    def empty(self):
        return not self.contents

    def __contains__(self, name):
        return name in self.contents

    def hash(self):
        if not self.oid:
            self.save()
        return self.oid


def _parsetree(line):
    return line.strip().split(None, 3)


class _Blob(object):
    def __init__(self, db, oid):
        self.db = db
        self.oid = oid

    def open(self):
        return _BlobStream(self.db, self.oid)

    def find(self, path):
        if not path:
            return self

    def hash(self):
        return self.oid


class _NewBlob(io.RawIOBase):
    def __init__(self, db, prev):
        self.db = db
        self.prev = prev

        self.proc = subprocess.Popen(
            ["git", "hash-object", "-w", "--stdin"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=db,
        )

    def write(self, b):
        self.proc.stdin.write(b)
        return len(b)

    def close(self):
        super(_NewBlob, self).close()
        self.proc.stdin.close()
        oid = self.proc.stdout.read().strip()
        self.proc.stdout.close()
        retcode = self.proc.wait()
        if retcode != 0:
            raise subprocess.CalledProcessError(retcode, "git hash-object -w --stdin")
        self.parent.contents[self.name] = (b"blob", oid, None)

    def writable(self):
        return True

    def open(self):
        if self.prev:
            return self.prev.open()
        raise _NoSuchFileOrDirectory(_object_path(self))

    def hash(self):
        if self.prev:
            return self.prev.hash()
        raise _NoSuchFileOrDirectory(_object_path(self))

    def find(self, path):
        if not path:
            return self


class _BlobStream(io.RawIOBase):
    def __init__(self, db, oid):
        self.proc = subprocess.Popen(
            ["git", "cat-file", "blob", oid],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=db,
        )
        self.oid = oid

    def readable(self):
        return True

    def read(self, n=-1):
        return self.proc.stdout.read(n)

    def readinto(self, b):
        """
        Although the documentation asserts a default implementation of this
        method can be found in io.RawIOBase, there actually isn't one::

            http://docs.python.org/py3k/library/io.html#io.RawIOBase

        See::

            http://bugs.python.org/issue9858
        """
        return self.proc.stdout.readinto(b)

    def close(self):
        super(_BlobStream, self).close()
        self.proc.stdout.close()
        self.proc.stderr.close()
        retcode = self.proc.wait()
        if retcode != 0:
            raise subprocess.CalledProcessError(
                retcode, "git cat-file blob %s" % self.oid
            )


def _object_path(obj):
    path = []
    node = obj
    while node.parent:
        path.insert(0, node.name)
        node = node.parent
    return "/".join(path)


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


@contextlib.contextmanager
def _tempfile():
    fd, tmp = tempfile.mkstemp(".acidfs-merge")
    os.close(fd)
    yield tmp
    os.remove(tmp)


def _NoSuchFileOrDirectory(path):
    return IOError(2, "No such file or directory", path)


def _IsADirectory(path):
    return IOError(21, "Is a directory", path)


def _NotADirectory(path):
    return IOError(20, "Not a directory", path)


def _FileExists(path):
    return IOError(17, "File exists", path)


def _DirectoryNotEmpty(path):
    return IOError(39, "Directory not empty", path)


_MERGE_ADDED_IN_REMOTE = object()
_MERGE_REMOVED_IN_REMOTE = object()
_MERGE_CHANGED_IN_BOTH = object()
_MERGE_ADDED_IN_BOTH = object()

_check_output = subprocess.check_output
