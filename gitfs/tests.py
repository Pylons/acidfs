try: #pragma no cover
    import unittest2 as unittest
    unittest # stfu pyflakes
except ImportError:
    import unittest

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
        from gitfs import GitFS as test_class
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


class OperationalTests(unittest.TestCase):

    def setUp(self):
        from gitfs import GitFS as test_class
        self.tmp = tempfile.mkdtemp('.gitstore-test')
        self.fs = test_class(self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_read_write_file(self):
        fs = self.fs
        with fs.open('foo', 'w') as f:
            print >> f, 'Hello'
        with fs.open('foo') as f:
            self.assertEqual(f.read(), 'Hello\n')
        actual_file = os.path.join(self.tmp, 'foo')
        self.assertFalse(os.path.exists(actual_file))
        transaction.commit()
        with fs.open('foo') as f:
            self.assertEqual(f.read(), 'Hello\n')
        with open(actual_file) as f:
            self.assertEqual(f.read(), 'Hello\n')

    def test_read_write_file_in_subfolder(self):
        fs = self.fs
        fs.mkdir('foo')
        with fs.open('foo/bar', 'w') as f:
            print >> f, 'Hello'
        with fs.open('foo/bar') as f:
            self.assertEqual(f.read(), 'Hello\n')
        actual_file = os.path.join(self.tmp, 'foo', 'bar')
        self.assertFalse(os.path.exists(actual_file))
        transaction.commit()
        with fs.open('foo/bar') as f:
            self.assertEqual(f.read(), 'Hello\n')
        with open(actual_file) as f:
            self.assertEqual(f.read(), 'Hello\n')

