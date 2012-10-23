try: #pragma no cover
    import unittest2 as unittest
    unittest # stfu pyflakes
except ImportError: # pragma NO COVER
    import unittest

import contextlib
import io
import mock
import os
import shutil
import subprocess
import tempfile
import transaction

from acidfs import _check_output


try:
    # Python 2
    u = __builtins__.unicode
except AttributeError:
    # Python 3
    def u(buf, enc='utf8'):
        assert isinstance(buf, bytes)
        return buf.decode(enc)


class FunctionalTests(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp('.acidfs-test')

    def tearDown(self):
        shutil.rmtree(self.tmp)
        transaction.abort()

    def make_one(self, *args, **kw):
        from acidfs import AcidFS as test_class
        return test_class(self.tmp, *args, **kw)

    def test_new_repo_w_working_directory(self):
        self.make_one()
        self.assertTrue(os.path.exists(os.path.join(self.tmp, '.git')))

    def test_no_repo_dont_create(self):
        with self.assertRaises(ValueError) as cm:
            self.make_one(create=False)
        self.assertTrue(str(cm.exception).startswith('No database found'))

    @contextlib.contextmanager
    def assertNoSuchFileOrDirectory(self, path):
        try:
            yield
            raise AssertionError('IOError not raised') # pragma no cover
        except IOError as e:
            self.assertEqual(e.errno, 2)
            self.assertEqual(e.strerror, 'No such file or directory')
            self.assertEqual(e.filename, path)

    @contextlib.contextmanager
    def assertIsADirectory(self, path):
        try:
            yield
            raise AssertionError('IOError not raised') # pragma no cover
        except IOError as e:
            self.assertEqual(e.errno, 21)
            self.assertEqual(e.strerror, 'Is a directory')
            self.assertEqual(e.filename, path)

    @contextlib.contextmanager
    def assertNotADirectory(self, path):
        try:
            yield
            raise AssertionError('IOError not raised') # pragma no cover
        except IOError as e:
            self.assertEqual(e.errno, 20)
            self.assertEqual(e.strerror, 'Not a directory')
            self.assertEqual(e.filename, path)

    @contextlib.contextmanager
    def assertFileExists(self, path):
        try:
            yield
            raise AssertionError('IOError not raised') # pragma no cover
        except IOError as e:
            self.assertEqual(e.errno, 17)
            self.assertEqual(e.strerror, 'File exists')
            self.assertEqual(e.filename, path)

    @contextlib.contextmanager
    def assertDirectoryNotEmpty(self, path):
        try:
            yield
            raise AssertionError('IOError not raised') # pragma no cover
        except IOError as e:
            self.assertEqual(e.errno, 39)
            self.assertEqual(e.strerror, 'Directory not empty')
            self.assertEqual(e.filename, path)

    def test_read_write_file(self):
        fs = self.make_one()
        with fs.open('foo', 'wb') as f:
            self.assertTrue(f.writable())
            fprint(f, b'Hello')
            with self.assertNoSuchFileOrDirectory('foo'):
                fs.open('foo', 'rb')
        self.assertEqual(fs.open('foo', 'rb').read(), b'Hello\n')
        actual_file = os.path.join(self.tmp, 'foo')
        self.assertFalse(os.path.exists(actual_file))
        transaction.commit()
        with fs.open('foo', 'rb', buffering=80) as f:
            self.assertTrue(f.readable())
            self.assertEqual(f.read(), b'Hello\n')
        with open(actual_file, 'rb') as f:
            self.assertEqual(f.read(), b'Hello\n')
        transaction.commit() # Nothing to commit

    def test_read_write_text_file(self):
        fs = self.make_one()
        with fs.open('foo', 'wt') as f:
            self.assertTrue(f.writable())
            fprint(f, u(b'Hell\xc3\xb2'))
            with self.assertNoSuchFileOrDirectory('foo'):
                fs.open('foo', 'r')
        self.assertEqual(fs.open('foo', 'r').read(), u(b'Hell\xc3\xb2\n'))
        actual_file = os.path.join(self.tmp, 'foo')
        self.assertFalse(os.path.exists(actual_file))
        transaction.commit()
        with fs.open('foo', 'r', buffering=1) as f:
            self.assertTrue(f.readable())
            self.assertEqual(f.read(), u(b'Hell\xc3\xb2\n'))
        with open(actual_file, 'rb') as f:
            self.assertEqual(f.read(), b'Hell\xc3\xb2\n')
        transaction.commit() # Nothing to commit

    def test_read_write_file_in_subfolder(self):
        fs = self.make_one()
        self.assertFalse(fs.isdir('foo'))
        fs.mkdir('foo')
        self.assertTrue(fs.isdir('foo'))
        with fs.open('foo/bar', 'wb') as f:
            fprint(f, b'Hello')
        with fs.open('foo/bar', 'rb') as f:
            self.assertEqual(f.read(), b'Hello\n')
        actual_file = os.path.join(self.tmp, 'foo', 'bar')
        self.assertFalse(os.path.exists(actual_file))
        transaction.commit()
        self.assertTrue(fs.isdir('foo'))
        self.assertFalse(fs.isdir('foo/bar'))
        with fs.open('foo/bar', 'rb') as f:
            self.assertEqual(f.read(), b'Hello\n')
        with open(actual_file, 'rb') as f:
            self.assertEqual(f.read(), b'Hello\n')

    def test_read_write_file_in_subfolder_bare_repo(self):
        fs = self.make_one(bare=True)
        self.assertFalse(fs.isdir('foo'))
        fs.mkdir('foo')
        self.assertTrue(fs.isdir('foo'))
        with fs.open('foo/bar', 'wb') as f:
            fprint(f, b'Hello')
        with fs.open('foo/bar', 'rb') as f:
            self.assertEqual(f.read(), b'Hello\n')
        transaction.commit()
        self.assertTrue(fs.isdir('foo'))
        self.assertFalse(fs.isdir('foo/bar'))
        with fs.open('foo/bar', 'rb') as f:
            self.assertEqual(f.read(), b'Hello\n')

    def test_open_edge_cases(self):
        fs = self.make_one()

        with self.assertNoSuchFileOrDirectory('foo'):
            fs.open('foo', 'rb')

        with self.assertNoSuchFileOrDirectory('foo/bar'):
            fs.open('foo/bar', 'wb')

        with self.assertIsADirectory('.'):
            fs.open('.', 'rb')

        with self.assertIsADirectory('.'):
            fs.open('.', 'wb')

        fs.mkdir('foo')

        with self.assertIsADirectory('foo'):
            fs.open('foo', 'wb')

        fs.open('bar', 'wb').write(b'Howdy')

        with self.assertNotADirectory('bar/foo'):
            fs.open('bar/foo', 'wb')

        with self.assertRaises(ValueError):
            fs.open('foo', 'wtf')

        with self.assertRaises(ValueError):
            fs.open('foo', 'wbt')

        with self.assertRaises(ValueError):
            fs.open('foo', 'w+')

        with self.assertRaises(ValueError):
            fs.open('foo', 'r', buffering=0)

        with fs.open('bar', 'wb') as f:
            fprint(f, b'Howdy!')
            with self.assertRaises(ValueError) as cm:
                transaction.commit()
            self.assertEqual(str(cm.exception),
                             "Cannot commit transaction with open files.")
        transaction.abort()

        fs.open('bar', 'xb').write(b'Hello!')
        with self.assertFileExists('bar'):
            fs.open('bar', 'xb')

    def test_mkdir_edge_cases(self):
        fs = self.make_one()

        with self.assertNoSuchFileOrDirectory('foo/bar'):
            fs.mkdir('foo/bar')

        fs.open('foo', 'wb').write(b'Howdy!')

        with self.assertNotADirectory('foo/bar'):
            fs.mkdir('foo/bar')

        fs.mkdir('bar')
        with self.assertFileExists('bar'):
            fs.mkdir('bar')

    def test_commit_metadata(self):
        fs = self.make_one()
        tx = transaction.get()
        tx.note("A test commit.")
        tx.setUser('Fred Flintstone')
        tx.setExtendedInfo('email', 'fred@bed.rock')
        fs.open('foo', 'wb').write(b'Howdy!')
        transaction.commit()

        output = _check_output(['git', 'log'], cwd=self.tmp)
        self.assertIn(b'Author: Fred Flintstone <fred@bed.rock>', output)
        self.assertIn(b'A test commit.', output)

    def test_commit_metadata_extended_info_for_user(self):
        fs = self.make_one()
        tx = transaction.get()
        tx.note("A test commit.")
        tx.setExtendedInfo('user', 'Fred Flintstone')
        tx.setExtendedInfo('email', 'fred@bed.rock')
        fs.open('foo', 'wb').write(b'Howdy!')
        transaction.commit()

        output = _check_output(['git', 'log'], cwd=self.tmp)
        self.assertIn(b'Author: Fred Flintstone <fred@bed.rock>', output)
        self.assertIn(b'A test commit.', output)

    def test_modify_file(self):
        fs = self.make_one()
        with fs.open('foo', 'wb') as f:
            fprint(f, b"Howdy!")
        transaction.commit()

        path = os.path.join(self.tmp, 'foo')
        with fs.open('foo', 'wb') as f:
            fprint(f, b"Hello!")
            self.assertEqual(fs.open('foo', 'rb').read(), b'Howdy!\n')
        self.assertEqual(fs.open('foo', 'rb').read(), b'Hello!\n')
        self.assertEqual(open(path, 'rb').read(), b'Howdy!\n')
        transaction.commit()

        self.assertEqual(open(path, 'rb').read(), b'Hello!\n')

    def test_error_writing_blob(self):
        fs = self.make_one()
        with self.assertRaises((IOError, subprocess.CalledProcessError)):
            with fs.open('foo', 'wb') as f:
                wait = f.raw.proc.wait
                def dummy_wait():
                    wait()
                    return 1
                f.raw.proc.wait = dummy_wait
                fprint(f, b'Howdy!')

    def test_error_reading_blob(self):
        fs = self.make_one()
        fs.open('foo', 'wb').write(b'a' * 10000)
        with self.assertRaises(subprocess.CalledProcessError):
            with fs.open('foo', 'rb') as f:
                wait = f.raw.proc.wait
                def dummy_wait():
                    wait()
                    return 1
                f.raw.proc.wait = dummy_wait
                f.read()

    def test_append(self):
        fs = self.make_one()
        fs.open('foo', 'wb').write(b'Hello!\n')
        transaction.commit()

        path = os.path.join(self.tmp, 'foo')
        with fs.open('foo', 'ab') as f:
            fprint(f, b'Daddy!')
            self.assertEqual(fs.open('foo', 'rb').read(), b'Hello!\n')
            self.assertEqual(open(path, 'rb').read(), b'Hello!\n')
        self.assertEqual(fs.open('foo', 'rb').read(), b'Hello!\nDaddy!\n')
        self.assertEqual(open(path, 'rb').read(), b'Hello!\n')

        transaction.commit()
        self.assertEqual(fs.open('foo', 'rb').read(), b'Hello!\nDaddy!\n')
        self.assertEqual(open(path, 'rb').read(), b'Hello!\nDaddy!\n')

    def test_rm(self):
        fs = self.make_one()
        fs.open('foo', 'wb').write(b'Hello\n')
        transaction.commit()

        path = os.path.join(self.tmp, 'foo')
        self.assertTrue(fs.exists('foo'))
        fs.rm('foo')
        self.assertFalse(fs.exists('foo'))
        self.assertTrue(os.path.exists(path))

        transaction.commit()
        self.assertFalse(fs.exists('foo'))
        self.assertFalse(os.path.exists(path))

        with self.assertNoSuchFileOrDirectory('foo'):
            fs.rm('foo')
        with self.assertIsADirectory('.'):
            fs.rm('.')

    def test_rmdir(self):
        fs = self.make_one()
        fs.mkdir('foo')
        fs.open('foo/bar', 'wb').write(b'Hello\n')
        transaction.commit()

        path = os.path.join(self.tmp, 'foo')
        with self.assertNotADirectory('foo/bar'):
            fs.rmdir('foo/bar')
        with self.assertDirectoryNotEmpty('foo'):
            fs.rmdir('foo')
        with self.assertNoSuchFileOrDirectory('bar'):
            fs.rmdir('bar')
        fs.rm('foo/bar')
        fs.rmdir('foo')
        self.assertFalse(fs.exists('foo'))
        self.assertTrue(os.path.exists(path))

        transaction.commit()
        self.assertFalse(fs.exists('foo'))
        self.assertFalse(os.path.exists(path))

    def test_rmtree(self):
        fs = self.make_one()
        fs.mkdirs('foo/bar')
        fs.open('foo/bar/baz', 'wb').write(b'Hello\n')
        with self.assertNotADirectory('foo/bar/baz/boz'):
            fs.mkdirs('foo/bar/baz/boz')
        transaction.commit()

        path = os.path.join(self.tmp, 'foo', 'bar', 'baz')
        with self.assertNotADirectory('foo/bar/baz'):
            fs.rmtree('foo/bar/baz')
        with self.assertNoSuchFileOrDirectory('bar'):
            fs.rmtree('bar')
        self.assertTrue(fs.exists('foo/bar'))
        self.assertTrue(fs.exists('foo'))
        self.assertFalse(fs.empty('/'))
        fs.rmtree('foo')
        self.assertFalse(fs.exists('foo/bar'))
        self.assertFalse(fs.exists('foo'))
        self.assertTrue(fs.empty('/'))
        self.assertTrue(os.path.exists(path))

        transaction.commit()
        self.assertFalse(os.path.exists(path))

    def test_empty(self):
        fs = self.make_one()
        self.assertTrue(fs.empty('/'))
        fs.open('foo', 'wb').write(b'Hello!')
        self.assertFalse(fs.empty('/'))
        with self.assertNotADirectory('foo'):
            fs.empty('foo')
        with self.assertNoSuchFileOrDirectory('foo/bar'):
            fs.empty('foo/bar')

    def test_mv(self):
        fs = self.make_one()
        fs.mkdirs('one/a')
        fs.mkdirs('one/b')
        fs.open('one/a/foo', 'wb').write(b'Hello!')
        fs.open('one/b/foo', 'wb').write(b'Howdy!')
        transaction.commit()

        with self.assertNoSuchFileOrDirectory('/'):
            fs.mv('/', 'one')
        with self.assertNoSuchFileOrDirectory('bar'):
            fs.mv('bar', 'one')
        with self.assertNoSuchFileOrDirectory('bar/baz'):
            fs.mv('one', 'bar/baz')

        pexists = os.path.exists
        j = os.path.join
        fs.mv('one/a/foo', 'one/a/bar')
        self.assertFalse(fs.exists('one/a/foo'))
        self.assertTrue(fs.exists('one/a/bar'))
        self.assertTrue(pexists(j(self.tmp, 'one', 'a', 'foo')))
        self.assertFalse(pexists(j(self.tmp, 'one', 'a', 'bar')))

        transaction.commit()
        self.assertFalse(fs.exists('one/a/foo'))
        self.assertTrue(fs.exists('one/a/bar'))
        self.assertFalse(pexists(j(self.tmp, 'one', 'a', 'foo')))
        self.assertTrue(pexists(j(self.tmp, 'one', 'a', 'bar')))

        fs.mv('one/b/foo', 'one/a/bar')
        self.assertFalse(fs.exists('one/b/foo'))
        self.assertEqual(fs.open('one/a/bar', 'rb').read(), b'Howdy!')
        self.assertTrue(pexists(j(self.tmp, 'one', 'b', 'foo')))
        self.assertEqual(open(j(self.tmp, 'one', 'a', 'bar'), 'rb').read(),
                         b'Hello!')

        transaction.commit()
        self.assertFalse(fs.exists('one/b/foo'))
        self.assertEqual(fs.open('one/a/bar', 'rb').read(), b'Howdy!')
        self.assertFalse(pexists(j(self.tmp, 'one', 'b', 'foo')))
        self.assertEqual(open(j(self.tmp, 'one', 'a', 'bar'), 'rb').read(),
                         b'Howdy!')

        fs.mv('one/a', 'one/b')
        self.assertFalse(fs.exists('one/a'))
        self.assertTrue(fs.exists('one/b/a'))
        self.assertTrue(pexists(j(self.tmp, 'one', 'a')))
        self.assertFalse(pexists(j(self.tmp, 'one', 'b', 'a')))

        transaction.commit()
        self.assertFalse(fs.exists('one/a'))
        self.assertTrue(fs.exists('one/b/a'))
        self.assertFalse(pexists(j(self.tmp, 'one', 'a')))
        self.assertTrue(pexists(j(self.tmp, 'one', 'b', 'a')))

    def test_listdir(self):
        fs = self.make_one()
        fs.mkdirs('one/a')
        fs.mkdir('two')
        fs.open('three', 'wb').write(b'Hello!')

        with self.assertNoSuchFileOrDirectory('bar'):
            fs.listdir('bar')
        with self.assertNotADirectory('three'):
            fs.listdir('three')
        self.assertEqual(sorted(fs.listdir()), ['one', 'three', 'two'])
        self.assertEqual(fs.listdir('/one'), ['a'])

        transaction.commit()
        self.assertEqual(sorted(fs.listdir()), ['one', 'three', 'two'])
        self.assertEqual(fs.listdir('/one'), ['a'])

    def test_chdir(self):
        fs = self.make_one()

        fs.mkdirs('one/a')
        fs.mkdir('two')
        fs.open('three', 'wb').write(b'Hello!')
        fs.open('two/three', 'wb').write(b'Haha!')

        self.assertEqual(fs.cwd(), '/')
        self.assertEqual(sorted(fs.listdir()), ['one', 'three', 'two'])

        with self.assertNoSuchFileOrDirectory('foo'):
            fs.chdir('foo')

        fs.chdir('one')
        self.assertEqual(fs.cwd(), '/one')
        self.assertEqual(fs.listdir(), ['a'])

        with self.assertNotADirectory('/three'):
            with fs.cd('/three'):
                pass # pragma no cover

        with fs.cd('/two'):
            self.assertEqual(fs.cwd(), '/two')
            self.assertEqual(fs.listdir(), ['three'])
            self.assertEqual(fs.open('three', 'rb').read(), b'Haha!')
            self.assertEqual(fs.open('/three', 'rb').read(), b'Hello!')

        self.assertEqual(fs.cwd(), '/one')
        self.assertEqual(fs.listdir(), ['a'])

    def test_conflict_error_on_first_commit(self):
        from acidfs import ConflictError
        fs = self.make_one()
        fs.open('foo', 'wb').write(b'Hello!')
        open(os.path.join(self.tmp, 'foo'), 'wb').write(b'Howdy!')
        _check_output(['git', 'add', '.'], cwd=self.tmp)
        _check_output(['git', 'commit', '-m', 'Haha!  First!'], cwd=self.tmp)
        with self.assertRaises(ConflictError):
            transaction.commit()

    def test_unable_to_merge_file(self):
        from acidfs import ConflictError
        fs = self.make_one()
        fs.open('foo', 'wb').write(b'Hello!')
        transaction.commit()
        fs.open('foo', 'wb').write(b'Party!')
        open(os.path.join(self.tmp, 'foo'), 'wb').write(b'Howdy!')
        _check_output(['git', 'add', '.'], cwd=self.tmp)
        _check_output(['git', 'commit', '-m', 'Haha!  First!'], cwd=self.tmp)
        with self.assertRaises(ConflictError):
            transaction.commit()

    def test_merge_add_file(self):
        fs = self.make_one()
        fs.open('foo', 'wb').write(b'Hello!\n')
        transaction.commit()

        fs.open('bar', 'wb').write(b'Howdy!\n')
        open(os.path.join(self.tmp, 'baz'), 'wb').write(b'Ciao!\n')
        _check_output(['git', 'add', 'baz'], cwd=self.tmp)
        _check_output(['git', 'commit', '-m', 'haha'], cwd=self.tmp)
        transaction.commit()

        self.assertTrue(fs.exists('foo'))
        self.assertTrue(fs.exists('bar'))
        self.assertTrue(fs.exists('baz'))

    def test_merge_rm_file(self):
        fs = self.make_one(head='master')
        fs.open('foo', 'wb').write(b'Hello\n')
        fs.open('bar', 'wb').write(b'Grazie\n')
        fs.open('baz', 'wb').write(b'Prego\n')
        transaction.commit()

        fs.rm('foo')
        _check_output(['git', 'rm', 'baz'], cwd=self.tmp)
        _check_output(['git', 'commit', '-m', 'gotcha'], cwd=self.tmp)
        transaction.commit()

        self.assertFalse(fs.exists('foo'))
        self.assertTrue(fs.exists('bar'))
        self.assertFalse(fs.exists('baz'))

    def test_merge_rm_same_file(self):
        fs = self.make_one(head='master')
        fs.open('foo', 'wb').write(b'Hello\n')
        fs.open('bar', 'wb').write(b'Grazie\n')
        transaction.commit()

        base = fs.get_base()
        fs.rm('foo')
        transaction.commit()

        fs.set_base(base)
        fs.rm('foo')
        # Do something else besides, so commit has different sha1
        fs.open('baz', 'wb').write(b'Prego\n')
        transaction.commit()

        self.assertFalse(fs.exists('foo'))
        self.assertTrue(fs.exists('bar'))

    def test_merge_add_same_file(self):
        fs = self.make_one(head='master')
        fs.open('foo', 'wb').write(b'Hello\n')
        transaction.commit()

        base = fs.get_base()
        fs.open('bar', 'wb').write(b'Grazie\n')
        transaction.commit()

        fs.set_base(base)
        fs.open('bar', 'wb').write(b'Grazie\n')
        # Do something else besides, so commit has different sha1
        fs.open('baz', 'wb').write(b'Prego\n')
        transaction.commit()

        self.assertEqual(fs.open('bar', 'rb').read(), b'Grazie\n')

    def test_merge_add_different_file_same_path(self):
        from acidfs import ConflictError
        fs = self.make_one(head='master')
        fs.open('foo', 'wb').write(b'Hello\n')
        transaction.commit()

        base = fs.get_base()
        fs.open('bar', 'wb').write(b'Grazie\n')
        transaction.commit()

        fs.set_base(base)
        fs.open('bar', 'wb').write(b'Prego\n')
        with self.assertRaises(ConflictError):
            transaction.commit()

    def test_merge_file(self):
        fs = self.make_one()
        with fs.open('foo', 'wb') as f:
            fprint(f, b'One')
            fprint(f, b'Two')
            fprint(f, b'Three')
            fprint(f, b'Four')
            fprint(f, b'Five')
        transaction.commit()

        base = fs.get_base()
        with fs.open('foo', 'wb') as f:
            fprint(f, b'One')
            fprint(f, b'Dos')
            fprint(f, b'Three')
            fprint(f, b'Four')
            fprint(f, b'Five')
        transaction.commit()

        fs.set_base(base)
        with fs.open('foo', 'ab') as f:
            fprint(f, b'Sei')
        transaction.commit()

        self.assertEqual(list(fs.open('foo', 'rb').readlines()), [
            b'One\n',
            b'Dos\n',
            b'Three\n',
            b'Four\n',
            b'Five\n',
            b'Sei\n'])

    def test_set_base(self):
        from acidfs import ConflictError
        fs = self.make_one()
        fs.open('foo', 'wb').write(b'Hello\n')
        transaction.commit()

        base = fs.get_base()
        fs.open('bar', 'wb').write(b'Grazie\n')
        with self.assertRaises(ConflictError):
            fs.set_base('whatever')
        transaction.commit()

        fs.set_base(base)
        self.assertTrue(fs.exists('foo'))
        self.assertFalse(fs.exists('bar'))
        fs.open('baz', 'wb').write(b'Prego\n')
        transaction.commit()

        self.assertTrue(fs.exists('foo'))
        self.assertTrue(fs.exists('bar'))
        self.assertTrue(fs.exists('baz'))

    def test_use_other_branch(self):
        fs = self.make_one(head='foo')
        fs.open('foo', 'wb').write(b'Hello\n')
        transaction.commit()

        fs2 = self.make_one()
        fs2.open('foo', 'wb').write(b'Howdy!\n')
        transaction.commit()

        self.assertEqual(fs.open('foo', 'rb').read(), b'Hello\n')
        self.assertEqual(fs2.open('foo', 'rb').read(), b'Howdy!\n')

    def test_branch_and_then_merge(self):
        fs = self.make_one()
        fs.open('foo', 'wb').write(b'Hello')
        transaction.commit()

        fs2 = self.make_one(head='abranch')
        fs2.set_base(fs.get_base())
        fs2.open('bar', 'wb').write(b'Ciao')
        fs.open('baz', 'wb').write(b'Hola')
        transaction.commit()

        fs.set_base('abranch')
        fs.open('beez', 'wb').write(b'buzz')
        transaction.commit()

        self.assertTrue(fs.exists('foo'))
        self.assertTrue(fs.exists('bar'))
        self.assertTrue(fs.exists('baz'))
        self.assertTrue(fs.exists('beez'))
        self.assertTrue(fs2.exists('foo'))
        self.assertTrue(fs2.exists('bar'))
        self.assertFalse(fs2.exists('baz'))
        self.assertFalse(fs2.exists('beez'))

        # Expecting two parents for commit since it's a merge
        commit = _check_output(
            ['git', 'cat-file', '-p', 'HEAD^{commit}'],
            cwd=self.tmp).decode('ascii').split('\n')
        self.assertTrue(commit[1].startswith('parent'))
        self.assertTrue(commit[2].startswith('parent'))


class PopenTests(unittest.TestCase):

    @mock.patch('acidfs.subprocess.Popen')
    def test_called_process_error(self, Popen):
        from acidfs import _popen
        Popen.return_value.return_value.wait.return_value = 1
        with self.assertRaises(subprocess.CalledProcessError):
            with _popen(['what', 'ever']):
                pass


def fprint(f, s):
    f.write(s)
    if isinstance(f, io.TextIOWrapper):
        f.write(u(b'\n'))
    else:
        f.write(b'\n')
