try: #pragma no cover
    import unittest2 as unittest
    unittest # stfu pyflakes
except ImportError:
    import unittest

import contextlib
import mock
import os
import shutil
import subprocess
import tempfile
import transaction


class InitializationTests(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp('.gitstore-test')

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def make_one(self, *args, **kw):
        from acidfs import AcidFS as test_class
        return test_class(self.tmp, *args, **kw)

    def test_new_repo_w_working_directory(self):
        self.make_one()
        self.assertTrue(os.path.exists(os.path.join(self.tmp, '.git')))

    def test_new_bare_repo(self):
        self.make_one(bare=True)
        self.assertTrue(os.path.exists(os.path.join(self.tmp, 'HEAD')))

    def test_no_repo_dont_create(self):
        with self.assertRaises(ValueError) as cm:
            self.make_one(create=False)
        self.assertTrue(str(cm.exception).startswith('No database found'))

    def test_detached_head(self):
        fs = self.make_one()
        fs.open('foo', 'w').write('bar')
        transaction.commit()

        os.chdir(self.tmp)
        reffile = os.path.join(self.tmp, '.git', 'refs', 'heads', 'master')
        commit = open(reffile).read().strip()
        subprocess.check_output(['git', 'checkout', commit],
                                stderr=subprocess.STDOUT)
        with self.assertRaises(ValueError) as cm:
            fs = self.make_one()
        self.assertEqual(str(cm.exception), 'Cannot use detached HEAD state.')

    def test_branch(self):
        fs = self.make_one(branch='foo')
        fs.open('foo', 'w').write('bar')
        transaction.commit()

        reffile = os.path.join(self.tmp, '.git', 'refs', 'heads', 'foo')
        self.assertTrue(os.path.exists(reffile))

    def test_no_such_branch(self):
        fs = self.make_one()
        fs.open('foo', 'w').write('bar')
        transaction.commit()

        with self.assertRaises(ValueError):
            fs = self.make_one(branch='foo')
            fs.open('foo')


class OperationalTests(unittest.TestCase):

    def setUp(self):
        from acidfs import AcidFS as test_class
        self.tmp = tempfile.mkdtemp('.gitstore-test')
        self.fs = test_class(self.tmp)
        transaction.abort()

    def tearDown(self):
        shutil.rmtree(self.tmp)
        transaction.abort()

    @contextlib.contextmanager
    def assertNoSuchFileOrDirectory(self, path):
        try:
            yield
            raise AssertionError('IOError not raised') # pragma no cover
        except IOError, e:
            self.assertEqual(e.errno, 2)
            self.assertEqual(e.strerror, 'No such file or directory')
            self.assertEqual(e.filename, path)

    @contextlib.contextmanager
    def assertIsADirectory(self, path):
        try:
            yield
            raise AssertionError('IOError not raised') # pragma no cover
        except IOError, e:
            self.assertEqual(e.errno, 21)
            self.assertEqual(e.strerror, 'Is a directory')
            self.assertEqual(e.filename, path)

    @contextlib.contextmanager
    def assertNotADirectory(self, path):
        try:
            yield
            raise AssertionError('IOError not raised') # pragma no cover
        except IOError, e:
            self.assertEqual(e.errno, 20)
            self.assertEqual(e.strerror, 'Not a directory')
            self.assertEqual(e.filename, path)

    @contextlib.contextmanager
    def assertFileExists(self, path):
        try:
            yield
            raise AssertionError('IOError not raised') # pragma no cover
        except IOError, e:
            self.assertEqual(e.errno, 17)
            self.assertEqual(e.strerror, 'File exists')
            self.assertEqual(e.filename, path)

    @contextlib.contextmanager
    def assertDirectoryNotEmpty(self, path):
        try:
            yield
            raise AssertionError('IOError not raised') # pragma no cover
        except IOError, e:
            self.assertEqual(e.errno, 39)
            self.assertEqual(e.strerror, 'Directory not empty')
            self.assertEqual(e.filename, path)

    def test_read_write_file(self):
        fs = self.fs
        with fs.open('foo', 'w') as f:
            self.assertTrue(f.writable())
            print >> f, 'Hello'
            with self.assertNoSuchFileOrDirectory('foo'):
                fs.open('foo')
        self.assertEqual(fs.open('foo').read(), 'Hello\n')
        actual_file = os.path.join(self.tmp, 'foo')
        self.assertFalse(os.path.exists(actual_file))
        transaction.commit()
        with fs.open('foo') as f:
            self.assertTrue(f.readable())
            self.assertEqual(f.read(), 'Hello\n')
        with open(actual_file) as f:
            self.assertEqual(f.read(), 'Hello\n')
        transaction.commit() # Nothing to commit

    def test_read_write_file_in_subfolder(self):
        fs = self.fs
        self.assertFalse(fs.isdir('foo'))
        fs.mkdir('foo')
        self.assertTrue(fs.isdir('foo'))
        with fs.open('foo/bar', 'w') as f:
            print >> f, 'Hello'
        with fs.open('foo/bar') as f:
            self.assertEqual(f.read(), 'Hello\n')
        actual_file = os.path.join(self.tmp, 'foo', 'bar')
        self.assertFalse(os.path.exists(actual_file))
        transaction.commit()
        self.assertTrue(fs.isdir('foo'))
        self.assertFalse(fs.isdir('foo/bar'))
        with fs.open('foo/bar') as f:
            self.assertEqual(f.read(), 'Hello\n')
        with open(actual_file) as f:
            self.assertEqual(f.read(), 'Hello\n')

    def test_open_edge_cases(self):
        fs = self.fs

        with self.assertNoSuchFileOrDirectory('foo'):
            fs.open('foo')

        with self.assertNoSuchFileOrDirectory('foo/bar'):
            fs.open('foo/bar', 'w')

        with self.assertIsADirectory('.'):
            fs.open('.')

        with self.assertIsADirectory('.'):
            fs.open('.', 'w')

        fs.mkdir('foo')

        with self.assertIsADirectory('foo'):
            fs.open('foo', 'w')

        fs.open('bar', 'w').write('Howdy')

        with self.assertNotADirectory('bar/foo'):
            fs.open('bar/foo', 'w')

        with self.assertRaises(ValueError):
            fs.open('foo', 'wtf')

        with fs.open('bar', 'w') as f:
            print >> f, 'Howdy!'
            with self.assertRaises(ValueError) as cm:
                transaction.commit()
            self.assertEqual(str(cm.exception),
                             "Cannot commit transaction with open files.")

    def test_mkdir_edge_cases(self):
        fs = self.fs

        with self.assertNoSuchFileOrDirectory('foo/bar'):
            fs.mkdir('foo/bar')

        fs.open('foo', 'w').write('Howdy!')

        with self.assertNotADirectory('foo/bar'):
            fs.mkdir('foo/bar')

        fs.mkdir('bar')
        with self.assertFileExists('bar'):
            fs.mkdir('bar')

    def test_commit_metadata(self):
        tx = transaction.get()
        tx.note("A test commit.")
        tx.setUser('Fred Flintstone')
        tx.setExtendedInfo('email', 'fred@bed.rock')
        self.fs.open('foo', 'w').write('Howdy!')
        transaction.commit()

        output = subprocess.check_output(['git', 'log'], cwd=self.tmp)
        self.assertIn('Author: Fred Flintstone <fred@bed.rock>', output)
        self.assertIn('A test commit.', output)

    def test_commit_metadata_extended_info_for_user(self):
        tx = transaction.get()
        tx.note("A test commit.")
        tx.setExtendedInfo('user', 'Fred Flintstone')
        tx.setExtendedInfo('email', 'fred@bed.rock')
        self.fs.open('foo', 'w').write('Howdy!')
        transaction.commit()

        output = subprocess.check_output(['git', 'log'], cwd=self.tmp)
        self.assertIn('Author: Fred Flintstone <fred@bed.rock>', output)
        self.assertIn('A test commit.', output)

    def test_modify_file(self):
        fs = self.fs
        with fs.open('foo', 'w') as f:
            print >> f, "Howdy!"
        transaction.commit()

        path = os.path.join(self.tmp, 'foo')
        with fs.open('foo', 'w') as f:
            print >> f, "Hello!"
            self.assertEqual(fs.open('foo').read(), 'Howdy!\n')
        self.assertEqual(fs.open('foo').read(), 'Hello!\n')
        self.assertEqual(open(path).read(), 'Howdy!\n')
        transaction.commit()

        self.assertEqual(open(path).read(), 'Hello!\n')

    def test_error_writing_blob(self):
        with self.assertRaises(subprocess.CalledProcessError):
            with self.fs.open('foo', 'w') as f:
                shutil.rmtree(os.path.join(self.tmp, '.git'))
                print >> f, 'Howdy!'

    def test_error_reading_blob(self):
        self.fs.open('foo', 'w').write('a' * 1000)
        with self.assertRaises(subprocess.CalledProcessError):
            with self.fs.open('foo', 'r') as f:
                shutil.rmtree(os.path.join(self.tmp, '.git'))
                f.read()

    def test_conflict_error(self):
        from acidfs import ConflictError
        self.fs.open('foo', 'w').write('Hello!')
        open(os.path.join(self.tmp, 'foo'), 'w').write('Howdy!')
        subprocess.check_output(['git', 'add', '.'], cwd=self.tmp)
        subprocess.check_output(['git', 'commit', '-m', 'Haha!  First!'],
                                cwd=self.tmp)
        with self.assertRaises(ConflictError):
            transaction.commit()

    def test_append(self):
        fs = self.fs
        fs.open('foo', 'w').write('Hello!\n')
        transaction.commit()

        path = os.path.join(self.tmp, 'foo')
        with fs.open('foo', 'a') as f:
            print >> f, 'Daddy!'
            self.assertEqual(fs.open('foo', 'rb').read(), 'Hello!\n')
            self.assertEqual(open(path).read(), 'Hello!\n')
        self.assertEqual(fs.open('foo', 'rb').read(), 'Hello!\nDaddy!\n')
        self.assertEqual(open(path).read(), 'Hello!\n')

        transaction.commit()
        self.assertEqual(fs.open('foo', 'rb').read(), 'Hello!\nDaddy!\n')
        self.assertEqual(open(path).read(), 'Hello!\nDaddy!\n')

    def test_rm(self):
        fs = self.fs
        fs.open('foo', 'w').write('Hello\n')
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
        fs = self.fs
        fs.mkdir('foo')
        fs.open('foo/bar', 'w').write('Hello\n')
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
        fs = self.fs
        fs.mkdirs('foo/bar')
        fs.open('foo/bar/baz', 'w').write('Hello\n')
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
        fs = self.fs
        self.assertTrue(fs.empty('/'))
        fs.open('foo', 'w').write('Hello!')
        self.assertFalse(fs.empty('/'))
        with self.assertNotADirectory('foo'):
            fs.empty('foo')
        with self.assertNoSuchFileOrDirectory('foo/bar'):
            fs.empty('foo/bar')

    def test_mv(self):
        fs = self.fs
        fs.mkdirs('one/a')
        fs.mkdirs('one/b')
        fs.open('one/a/foo', 'w').write('Hello!')
        fs.open('one/b/foo', 'w').write('Howdy!')
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
        self.assertEqual(fs.open('one/a/bar').read(), 'Howdy!')
        self.assertTrue(pexists(j(self.tmp, 'one', 'b', 'foo')))
        self.assertEqual(open(j(self.tmp, 'one', 'a', 'bar')).read(), 'Hello!')

        transaction.commit()
        self.assertFalse(fs.exists('one/b/foo'))
        self.assertEqual(fs.open('one/a/bar').read(), 'Howdy!')
        self.assertFalse(pexists(j(self.tmp, 'one', 'b', 'foo')))
        self.assertEqual(open(j(self.tmp, 'one', 'a', 'bar')).read(), 'Howdy!')

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
        fs = self.fs
        fs.mkdirs('one/a')
        fs.mkdir('two')
        fs.open('three', 'w').write('Hello!')

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
        fs = self.fs

        fs.mkdirs('one/a')
        fs.mkdir('two')
        fs.open('three', 'w').write('Hello!')
        fs.open('two/three', 'w').write('Haha!')

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
            self.assertEqual(fs.open('three').read(), 'Haha!')
            self.assertEqual(fs.open('/three').read(), 'Hello!')

        self.assertEqual(fs.cwd(), '/one')
        self.assertEqual(fs.listdir(), ['a'])


class PopenTests(unittest.TestCase):

    @mock.patch('acidfs.subprocess.Popen')
    def test_called_process_error(self, Popen):
        from acidfs import _popen
        Popen.return_value.return_value.wait.return_value = 1
        with self.assertRaises(subprocess.CalledProcessError):
            with _popen(['what', 'ever']):
                pass
