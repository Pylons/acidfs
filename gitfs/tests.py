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


class FunctionalTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp('.gitstore-test')

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_it(self):
        from gitfs import GitFS

        # Repo not initialized yet
        with self.assertRaises(ValueError) as cm:
            GitFS(self.tmp)
        self.assertTrue(str(cm.exception).startswith('No database found'))

        os.chdir(self.tmp)
        subprocess.check_output(['git',  'init', '.'], stderr=subprocess.STDOUT)

        # Add a file to working directory but don't commit yet
        with open('foo', 'w') as f:
            print >> f, 'bar'

        fs = GitFS(self.tmp)

        # No such file
        with self.assertRaises(IOError) as cm:
            fs.open('foo')
        e = cm.exception
        self.assertEqual(e.errno, 2)
        self.assertEqual(e.strerror, 'No such file or directory')
        self.assertEqual(e.filename, 'foo')

        # Is a directory
        with self.assertRaises(IOError) as cm:
            fs.open('')
        e = cm.exception
        self.assertEqual(e.errno, 21)
        self.assertEqual(e.strerror, 'Is a directory')
        self.assertEqual(e.filename, '')

        # Commit working directory
        subprocess.check_output(['git', 'add', '.'], stderr=subprocess.STDOUT)
        subprocess.check_output(['git', 'commit', '-m', 'foo'],
                                stderr=subprocess.STDOUT)

        fs = GitFS(self.tmp, 'master')
        self.assertEqual(fs.open('foo').read(), 'bar\n')

        # Test detached head state
        commit = open('.git/refs/heads/master').read().strip()
        subprocess.check_output(['git', 'checkout', commit],
                                stderr=subprocess.STDOUT)
        with self.assertRaises(ValueError) as cm:
            fs = GitFS(self.tmp)
        subprocess.check_output(['git', 'checkout', 'master'],
                                stderr=subprocess.STDOUT)

        # Bad head
        with self.assertRaises(ValueError) as cm:
            fs = GitFS(self.tmp, 'foo')
            fs.open('foo')
        self.assertEqual(str(cm.exception), 'No such head: foo')

        # Nest a dir
        os.mkdir('somedir')
        with open('somedir/foo', 'w') as f:
            print >> f, 'Howdy!'
        subprocess.check_output(['git', 'add', '.'], stderr=subprocess.STDOUT)
        subprocess.check_output(['git', 'commit', '-m', 'foo'],
                                stderr=subprocess.STDOUT)

        fs = GitFS(self.tmp)
        self.assertEqual(fs.open('somedir/foo').read(), 'Howdy!\n')

        # No such folder
        with self.assertRaises(IOError) as cm:
            fs.open('wunder/bar', 'w')
        e = cm.exception
        self.assertEqual(e.errno, 2)
        self.assertEqual(e.strerror, 'No such file or directory')
        self.assertEqual(e.filename, 'wunder/bar')

        # Isn't a folder
        with self.assertRaises(IOError) as cm:
            fs.open('foo/bar', 'w')
        e = cm.exception
        self.assertEqual(e.errno, 20)
        self.assertEqual(e.strerror, 'Not a directory')
        self.assertEqual(e.filename, 'foo/bar')

        # Is a folder
        with self.assertRaises(IOError) as cm:
            fs.open('somedir', 'w')
        e = cm.exception
        self.assertEqual(e.errno, 21)
        self.assertEqual(e.strerror, 'Is a directory')
        self.assertEqual(e.filename, 'somedir')

        # New file
        with fs.open('hello', 'w') as f:
            print >> f, 'Hi Mom!'
        self.assertEqual(fs.open('hello').read(), 'Hi Mom!\n')

        transaction.abort()

        with self.assertRaises(IOError) as cm:
            fs.open('hello')
        e = cm.exception
        self.assertEqual(e.errno, 2)
        self.assertEqual(e.strerror, 'No such file or directory')
        self.assertEqual(e.filename, 'hello')

        with fs.open('hello', 'w') as f:
            print >> f, 'Hi Mom!'
        self.assertEqual(fs.open('hello').read(), 'Hi Mom!\n')

        transaction.commit()
        self.assertEqual(fs.open('hello').read(), 'Hi Mom!\n')
        fs = GitFS(self.tmp)
        self.assertEqual(fs.open('hello').read(), 'Hi Mom!\n')
        transaction.commit() # nothing to commit
        self.assertEqual(fs.open('hello').read(), 'Hi Mom!\n')

