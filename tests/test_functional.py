import contextlib
import io
import os
import pytest
import shutil
import subprocess
import tempfile
import transaction

from acidfs import AcidFS, _check_output


@pytest.fixture
def tmp(request):
    tmp = tempfile.mkdtemp()
    return tmp


@pytest.fixture
def factory(request, tmp):
    cwd = os.getcwd()

    def mkstore(*args, **kw):
        store = AcidFS(tmp, *args, **kw)

        tx = transaction.get()
        tx.setUser('Test User')
        tx.setExtendedInfo('email', 'test@example.com')

        os.chdir(tmp)
        subprocess.check_call(
            ['git', 'config', 'user.name', 'Test User'])
        subprocess.check_call(
            ['git', 'config', 'user.email', 'test@example.com'])
        os.chdir(cwd)

        return store

    def cleanup():
        transaction.abort()
        shutil.rmtree(tmp)

    request.addfinalizer(cleanup)
    return mkstore


def test_new_repo_w_working_directory(factory, tmp):
    factory()
    assert os.path.exists(os.path.join(tmp, '.git'))


def test_no_repo_dont_create(factory):
    with pytest.raises(ValueError) as cm:
        factory(create=False)
    assert str(cm.exconly()).startswith('ValueError: No database found')


@contextlib.contextmanager
def assert_no_such_file_or_directory(path):
    try:
        yield
        raise AssertionError('IOError not raised')  # pragma no cover
    except IOError as e:
        assert e.errno == 2
        assert e.strerror == 'No such file or directory'
        assert e.filename == path


@contextlib.contextmanager
def assert_is_a_directory(path):
    try:
        yield
        raise AssertionError('IOError not raised')  # pragma no cover
    except IOError as e:
        assert e.errno == 21
        assert e.strerror == 'Is a directory'
        assert e.filename == path


@contextlib.contextmanager
def assert_not_a_directory(path):
    try:
        yield
        raise AssertionError('IOError not raised')  # pragma no cover
    except IOError as e:
        assert e.errno == 20
        assert e.strerror == 'Not a directory'
        assert e.filename == path


@contextlib.contextmanager
def assert_file_exists(path):
    try:
        yield
        raise AssertionError('IOError not raised')  # pragma no cover
    except IOError as e:
        assert e.errno == 17
        assert e.strerror == 'File exists'
        assert e.filename == path


@contextlib.contextmanager
def assert_directory_not_empty(path):
    try:
        yield
        raise AssertionError('IOError not raised')  # pragma no cover
    except IOError as e:
        assert e.errno == 39
        assert e.strerror == 'Directory not empty'
        assert e.filename == path


def test_read_write_file(factory, tmp):
    fs = factory()
    assert fs.hash() == '4b825dc642cb6eb9a060e54bf8d69288fbee4904'
    with assert_no_such_file_or_directory('foo'):
        fs.hash('foo')
    with fs.open('foo', 'wb') as f:
        assert f.writable()
        fprint(f, b'Hello')
        with assert_no_such_file_or_directory('foo'):
            fs.open('foo', 'rb')
        with assert_no_such_file_or_directory('foo'):
            fs.hash('foo')
    assert fs.open('foo', 'rb').read() == b'Hello\n'
    assert fs.hash('foo') == 'e965047ad7c57865823c7d992b1d046ea66edf78'
    actual_file = os.path.join(tmp, 'foo')
    assert not os.path.exists(actual_file)
    transaction.commit()
    with fs.open('foo', 'rb', buffering=80) as f:
        assert f.readable()
        assert f.read() == b'Hello\n'
    with open(actual_file, 'rb') as f:
        assert f.read() == b'Hello\n'
    transaction.commit()  # Nothing to commit


def test_read_write_nonascii_name(factory):
    fs = factory(path_encoding='utf-8')
    filename = b'Hell\xc3\xb2'.decode('utf-8')
    with fs.open(filename, 'wb') as f:
        fprint(f, b'Hello')
    transaction.commit()
    assert fs.listdir(), [filename]


def test_read_write_text_file(factory, tmp):
    fs = factory()
    with fs.open('foo', 'wt') as f:
        assert f.writable()
        fprint(f, u'Hell\xf2')
        with assert_no_such_file_or_directory('foo'):
            fs.open('foo', 'r')
    assert fs.open('foo', 'r').read() == u'Hell\xf2\n'
    actual_file = os.path.join(tmp, 'foo')
    assert not os.path.exists(actual_file)
    transaction.commit()
    with fs.open('foo', 'r', buffering=1) as f:
        assert f.readable()
        assert f.read() == u'Hell\xf2\n'
    with open(actual_file, 'rb') as f:
        assert f.read() == b'Hell\xc3\xb2\n'
    transaction.commit()  # Nothing to commit


def test_read_write_file_in_subfolder(factory, tmp):
    fs = factory()
    assert not fs.isdir('foo')
    fs.mkdir('foo')
    assert fs.isdir('foo')
    assert fs.hash('foo') == '4b825dc642cb6eb9a060e54bf8d69288fbee4904'
    with fs.open('foo/bar', 'wb') as f:
        fprint(f, b'Hello')
    with fs.open('foo/bar', 'rb') as f:
        assert f.read() == b'Hello\n'
    assert fs.hash('foo') == 'c57c02051dae8e2e4803530c217ac38121f393d3'
    actual_file = os.path.join(tmp, 'foo', 'bar')
    assert not os.path.exists(actual_file)
    transaction.commit()
    assert fs.isdir('foo')
    assert not fs.isdir('foo/bar')
    with fs.open('foo/bar', 'rb') as f:
        assert f.read() == b'Hello\n'
    with open(actual_file, 'rb') as f:
        assert f.read() == b'Hello\n'


def test_read_write_file_in_subfolder_bare_repo(factory):
    fs = factory(bare=True)
    assert not fs.isdir('foo')
    fs.mkdir('foo')
    assert fs.isdir('foo')
    with fs.open('foo/bar', 'wb') as f:
        fprint(f, b'Hello')
    with fs.open('foo/bar', 'rb') as f:
        assert f.read() == b'Hello\n'
    transaction.commit()
    assert fs.isdir('foo')
    assert not fs.isdir('foo/bar')
    with fs.open('foo/bar', 'rb') as f:
        assert f.read() == b'Hello\n'


def test_append_twice_to_same_file(factory):
    fs = factory()
    with fs.open('foo', 'a') as f:
        fprint(f, u'One')
    with fs.open('foo', 'a') as f:
        fprint(f, u'Two')
    with fs.open('foo') as f:
        assert f.read() == (
            "One\n"
            "Two\n"
        )
    transaction.commit()
    with fs.open('foo') as f:
        assert f.read() == (
            "One\n"
            "Two\n"
        )


def test_open_edge_cases(factory):
    fs = factory()

    with assert_no_such_file_or_directory('foo'):
        fs.open('foo', 'rb')

    with assert_no_such_file_or_directory('foo/bar'):
        fs.open('foo/bar', 'wb')

    with assert_is_a_directory('.'):
        fs.open('.', 'rb')

    with assert_is_a_directory('.'):
        fs.open('.', 'wb')

    fs.mkdir('foo')

    with assert_is_a_directory('foo'):
        fs.open('foo', 'wb')

    fs.open('bar', 'wb').write(b'Howdy')

    with assert_not_a_directory('bar/foo'):
        fs.open('bar/foo', 'wb')

    with pytest.raises(ValueError):
        fs.open('foo', 'wtf')

    with pytest.raises(ValueError):
        fs.open('foo', 'wbt')

    with pytest.raises(ValueError):
        fs.open('foo', 'w+')

    with pytest.raises(ValueError):
        fs.open('foo', 'r', buffering=0)

    with fs.open('bar', 'wb') as f:
        fprint(f, b'Howdy!')
        with pytest.raises(ValueError) as cm:
            transaction.commit()
        assert (str(cm.exconly()) ==
                "ValueError: Cannot commit transaction with open files.")
    transaction.abort()

    fs.open('bar', 'xb').write(b'Hello!')
    with assert_file_exists('bar'):
        fs.open('bar', 'xb')


def test_mkdir_edge_cases(factory):
    fs = factory()

    with assert_no_such_file_or_directory('foo/bar'):
        fs.mkdir('foo/bar')

    fs.open('foo', 'wb').write(b'Howdy!')

    with assert_not_a_directory('foo/bar'):
        fs.mkdir('foo/bar')

    fs.mkdir('bar')
    with assert_file_exists('bar'):
        fs.mkdir('bar')


def test_commit_metadata(factory, tmp):
    fs = factory()
    tx = transaction.get()
    tx.note("A test commit.")
    tx.setUser('Fred Flintstone')
    tx.setExtendedInfo('email', 'fred@bed.rock')
    fs.open('foo', 'wb').write(b'Howdy!')
    transaction.commit()

    output = _check_output(['git', 'log'], cwd=tmp)
    assert b'Author: Fred Flintstone <fred@bed.rock>' in output
    assert b'A test commit.' in output


def test_commit_metadata_user_path_is_blank(factory, tmp):
    # pyramid_tm calls setUser with '' for path
    fs = factory()
    tx = transaction.get()
    tx.note("A test commit.")
    tx.setUser('Fred', '')
    tx.setExtendedInfo('email', 'fred@bed.rock')
    fs.open('foo', 'wb').write(b'Howdy!')
    transaction.commit()

    output = _check_output(['git', 'log'], cwd=tmp)
    assert b'Author: Fred <fred@bed.rock>' in output
    assert b'A test commit.' in output


def test_commit_metadata_extended_info_for_user(factory, tmp):
    fs = factory()
    tx = transaction.get()
    tx.note("A test commit.")
    tx.setExtendedInfo('user', 'Fred Flintstone')
    tx.setExtendedInfo('email', 'fred@bed.rock')
    fs.open('foo', 'wb').write(b'Howdy!')
    transaction.commit()

    output = _check_output(['git', 'log'], cwd=tmp)
    assert b'Author: Fred Flintstone <fred@bed.rock>' in output
    assert b'A test commit.' in output


def test_modify_file(factory, tmp):
    fs = factory()
    with fs.open('foo', 'wb') as f:
        fprint(f, b"Howdy!")
    transaction.commit()

    path = os.path.join(tmp, 'foo')
    with fs.open('foo', 'wb') as f:
        fprint(f, b"Hello!")
        assert fs.open('foo', 'rb').read() == b'Howdy!\n'
        assert fs.hash('foo') == 'c564dac563c1974addaa0ac0ae028fc92b2370f1'
    assert fs.open('foo', 'rb').read() == b'Hello!\n'
    assert fs.hash('foo') == '10ddd6d257e01349d514541981aeecea6b2e741d'
    assert open(path, 'rb').read() == b'Howdy!\n'
    transaction.commit()

    assert open(path, 'rb').read() == b'Hello!\n'


def test_error_writing_blob(factory):
    fs = factory()
    with pytest.raises((IOError, subprocess.CalledProcessError)):
        with fs.open('foo', 'wb') as f:
            wait = f.raw.proc.wait

            def dummy_wait():
                wait()
                return 1

            f.raw.proc.wait = dummy_wait
            fprint(f, b'Howdy!')


def test_error_reading_blob(factory):
    fs = factory()
    fs.open('foo', 'wb').write(b'a' * 10000)
    with pytest.raises(subprocess.CalledProcessError):
        with fs.open('foo', 'rb') as f:
            wait = f.raw.proc.wait

            def dummy_wait():
                wait()
                return 1

            f.raw.proc.wait = dummy_wait
            f.read()


def test_append(factory, tmp):
    fs = factory()
    fs.open('foo', 'wb').write(b'Hello!\n')
    transaction.commit()

    path = os.path.join(tmp, 'foo')
    with fs.open('foo', 'ab') as f:
        fprint(f, b'Daddy!')
        assert fs.open('foo', 'rb').read() == b'Hello!\n'
        assert open(path, 'rb').read() == b'Hello!\n'
    assert fs.open('foo', 'rb').read() == b'Hello!\nDaddy!\n'
    assert open(path, 'rb').read() == b'Hello!\n'

    transaction.commit()
    assert fs.open('foo', 'rb').read() == b'Hello!\nDaddy!\n'
    assert open(path, 'rb').read() == b'Hello!\nDaddy!\n'


def test_rm(factory, tmp):
    fs = factory()
    fs.open('foo', 'wb').write(b'Hello\n')
    transaction.commit()

    path = os.path.join(tmp, 'foo')
    assert fs.exists('foo')
    fs.rm('foo')
    assert not fs.exists('foo')
    assert os.path.exists(path)

    transaction.commit()
    assert not fs.exists('foo')
    assert not os.path.exists(path)

    with assert_no_such_file_or_directory('foo'):
        fs.rm('foo')
    with assert_is_a_directory('.'):
        fs.rm('.')


def test_rmdir(factory, tmp):
    fs = factory()
    fs.mkdir('foo')
    fs.open('foo/bar', 'wb').write(b'Hello\n')
    transaction.commit()

    path = os.path.join(tmp, 'foo')
    with assert_not_a_directory('foo/bar'):
        fs.rmdir('foo/bar')
    with assert_directory_not_empty('foo'):
        fs.rmdir('foo')
    with assert_no_such_file_or_directory('bar'):
        fs.rmdir('bar')
    fs.rm('foo/bar')
    fs.rmdir('foo')
    assert not fs.exists('foo')
    assert os.path.exists(path)

    transaction.commit()
    assert not fs.exists('foo')
    assert not os.path.exists(path)


def test_rmtree(factory, tmp):
    fs = factory()
    fs.mkdirs('foo/bar')
    fs.open('foo/bar/baz', 'wb').write(b'Hello\n')
    with assert_not_a_directory('foo/bar/baz/boz'):
        fs.mkdirs('foo/bar/baz/boz')
    transaction.commit()

    path = os.path.join(tmp, 'foo', 'bar', 'baz')
    with assert_not_a_directory('foo/bar/baz'):
        fs.rmtree('foo/bar/baz')
    with assert_no_such_file_or_directory('bar'):
        fs.rmtree('bar')
    assert fs.exists('foo/bar')
    assert fs.exists('foo')
    assert not fs.empty('/')
    fs.rmtree('foo')
    assert not fs.exists('foo/bar')
    assert not fs.exists('foo')
    assert fs.empty('/')
    assert os.path.exists(path)

    transaction.commit()
    assert not os.path.exists(path)


def test_cant_remove_root_dir(factory):
    fs = factory()
    with pytest.raises(ValueError):
        fs.rmdir('/')
    with pytest.raises(ValueError):
        fs.rmtree('/')


def test_empty(factory):
    fs = factory()
    assert fs.empty('/')
    fs.open('foo', 'wb').write(b'Hello!')
    assert not fs.empty('/')
    with assert_not_a_directory('foo'):
        fs.empty('foo')
    with assert_no_such_file_or_directory('foo/bar'):
        fs.empty('foo/bar')


def test_mv(factory, tmp):
    fs = factory()
    fs.mkdirs('one/a')
    fs.mkdirs('one/b')
    fs.open('one/a/foo', 'wb').write(b'Hello!')
    fs.open('one/b/foo', 'wb').write(b'Howdy!')
    transaction.commit()

    with assert_no_such_file_or_directory('/'):
        fs.mv('/', 'one')
    with assert_no_such_file_or_directory('bar'):
        fs.mv('bar', 'one')
    with assert_no_such_file_or_directory('bar/baz'):
        fs.mv('one', 'bar/baz')

    pexists = os.path.exists
    j = os.path.join
    fs.mv('one/a/foo', 'one/a/bar')
    assert not fs.exists('one/a/foo')
    assert fs.exists('one/a/bar')
    assert pexists(j(tmp, 'one', 'a', 'foo'))
    assert not pexists(j(tmp, 'one', 'a', 'bar'))

    transaction.commit()
    assert not fs.exists('one/a/foo')
    assert fs.exists('one/a/bar')
    assert not pexists(j(tmp, 'one', 'a', 'foo'))
    assert pexists(j(tmp, 'one', 'a', 'bar'))

    fs.mv('one/b/foo', 'one/a/bar')
    assert not fs.exists('one/b/foo')
    assert fs.open('one/a/bar', 'rb').read() == b'Howdy!'
    assert pexists(j(tmp, 'one', 'b', 'foo'))
    assert open(j(tmp, 'one', 'a', 'bar'), 'rb').read() == b'Hello!'

    transaction.commit()
    assert not fs.exists('one/b/foo')
    assert fs.open('one/a/bar', 'rb').read() == b'Howdy!'
    assert not pexists(j(tmp, 'one', 'b', 'foo'))
    assert open(j(tmp, 'one', 'a', 'bar'), 'rb').read() == b'Howdy!'

    fs.mv('one/a', 'one/b')
    assert not fs.exists('one/a')
    assert fs.exists('one/b/a')
    assert pexists(j(tmp, 'one', 'a'))
    assert not pexists(j(tmp, 'one', 'b', 'a'))

    transaction.commit()
    assert not fs.exists('one/a')
    assert fs.exists('one/b/a')
    assert not pexists(j(tmp, 'one', 'a'))
    assert pexists(j(tmp, 'one', 'b', 'a'))


def test_mv_noop(factory):
    """
    Tests an error report from Hasan Karahan.
    """
    fs = factory()
    fs.open('foo', 'wb').write(b'Hello!')
    fs.mv('foo', 'foo')
    assert fs.open('foo', 'rb').read() == b'Hello!'
    transaction.commit()
    assert fs.open('foo', 'rb').read() == b'Hello!'

    fs.mv('foo', 'foo')
    assert fs.open('foo', 'rb').read() == b'Hello!'
    transaction.commit()
    assert fs.open('foo', 'rb').read() == b'Hello!'


def test_listdir(factory):
    fs = factory()
    fs.mkdirs('one/a')
    fs.mkdir('two')
    fs.open('three', 'wb').write(b'Hello!')

    with assert_no_such_file_or_directory('bar'):
        fs.listdir('bar')
    with assert_not_a_directory('three'):
        fs.listdir('three')
    assert sorted(fs.listdir()) == ['one', 'three', 'two']
    assert fs.listdir('/one') == ['a']

    transaction.commit()
    assert sorted(fs.listdir()) == ['one', 'three', 'two']
    assert fs.listdir('/one') == ['a']


def test_chdir(factory):
    fs = factory()

    fs.mkdirs('one/a')
    fs.mkdir('two')
    fs.open('three', 'wb').write(b'Hello!')
    fs.open('two/three', 'wb').write(b'Haha!')

    assert fs.cwd() == '/'
    assert sorted(fs.listdir()) == ['one', 'three', 'two']

    with assert_no_such_file_or_directory('foo'):
        fs.chdir('foo')

    fs.chdir('one')
    assert fs.cwd() == '/one'
    assert fs.listdir() == ['a']

    with assert_not_a_directory('/three'):
        with fs.cd('/three'):
            pass  # pragma no cover

    with fs.cd('/two'):
        assert fs.cwd() == '/two'
        assert fs.listdir() == ['three']
        assert fs.open('three', 'rb').read() == b'Haha!'
        assert fs.open('/three', 'rb').read() == b'Hello!'

    assert fs.cwd() == '/one'
    assert fs.listdir() == ['a']


'''
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


def test_directory_name_with_spaces(self):
    fs = self.make_one()
    fs.mkdir("foo bar")
    with fs.cd("foo bar"):
        fs.open("foo", "wb").write(b"bar")
    transaction.commit()

    with fs.cd("foo bar"):
        self.assertTrue(fs.open("foo", "rb").read(), b"bar")


@mock.patch('acidfs.subprocess.Popen')
def test_called_process_error(Popen):
    from acidfs import _popen
    Popen.return_value.return_value.wait.return_value = 1
    with self.assertRaises(subprocess.CalledProcessError):
        with _popen(['what', 'ever']):
            pass
'''


def fprint(f, s):
    f.write(s)
    if isinstance(f, io.TextIOWrapper):
        f.write(u'\n')
    else:
        f.write(b'\n')
