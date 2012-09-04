try: #pragma no cover
    import unittest2 as unittest
    unittest # stfu pyflakes
except ImportError:
    import unittest

import os
import shutil
import subprocess
import tempfile


class FunctionalTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp('.gitstore-test')

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_it(self):
        from acidfs import AcidFS

        # Repo not initialized yet
        with self.assertRaises(ValueError) as cm:
            AcidFS(self.tmp)
        self.assertTrue(str(cm.exception).startswith('No database found'))

        os.chdir(self.tmp)
        subprocess.check_call(['git',  'init', '.'])

        # Add a file to working directory but don't commit yet
        with open('foo', 'w') as f:
            print >> f, 'bar'

        fs = AcidFS(self.tmp)

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
        subprocess.check_call(['git', 'add', '.'])
        subprocess.check_call(['git', 'commit', '-m', 'foo'])

        fs = AcidFS(self.tmp, 'master')
        self.assertEqual(fs.open('foo').read(), 'bar\n')

        # Test detached head state
        commit = open('.git/refs/heads/master').read().strip()
        subprocess.check_call(['git', 'checkout', commit])
        with self.assertRaises(ValueError) as cm:
            fs = AcidFS(self.tmp)
        subprocess.check_call(['git', 'checkout', 'master'])

        # Bad head
        with self.assertRaises(ValueError) as cm:
            fs = AcidFS(self.tmp, 'foo')
            fs.open('foo')
        self.assertEqual(str(cm.exception), 'No such head: foo')

        # Nest a dir
        os.mkdir('somedir')
        with open('somedir/foo', 'w') as f:
            print >> f, 'Howdy!'
        subprocess.check_call(['git', 'add', '.'])
        subprocess.check_call(['git', 'commit', '-m', 'foo'])

        fs = AcidFS(self.tmp)
        self.assertEqual(fs.open('somedir/foo').read(), 'Howdy!\n')
