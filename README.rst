======
AcidFS
======

----------------------
The filesystem on ACID
----------------------

`AcidFS` allows interaction with the filesystem using transactions with ACID 
semantics.  `Git` is used as a back end, and `AcidFS` integrates with the 
`transaction <http://pypi.python.org/pypi/transaction>`_ package allowing use of
multiple databases in a single transaction.

Features
========

+ Changes to the filesystem will only be persisted when a transaction is 
  committed and if the transaction succeeds.  

+ Within the scope of a transaction, your application will only see a view of 
  the filesystem consistent with that filesystem's state at the beginning of the
  transaction.  Concurrent writes do not affect the current context.

+ A full history of all changes is available, since files are stored in a 
  backing `Git` repository.  The standard `Git` toolchain can be used to recall
  past states, roll back particular changes, replicate the repository remotely,
  etc.

+ Changes to a `AcidFS` filesystem are synced automatically with any other 
  database making use of the `transaction` package and its two phase commit
  protocol, eg. `ZODB` or `SQLAlchemy`.

Motivation
==========

The motivation for this package is the fact that it often is convenient for 
certain very simple problems to simply write and read data from a fileystem, 
but often a database of some sort winds up being used simply because of the 
power and safety available with a system which uses transactions and ACID 
semantics.  For example, you wouldn't want a web application with any amount of
concurrency at all to be writing directly to the filesystem, since it would be
easy for two threads or processes to both attempt to write to the same file at
the same time, with the result that one change is clobbered by another, or even
worse, the application is left in an inconsistent, corrupted state.  After 
thinking about various ways to attack this problem and looking at `Git's` 
datastore and plumbing commands, it was determined that `Git` was a very good fit,
allowing a graceful solution to this problem.

Limitations
===========

In a nutshell:

+ Only platforms where `fcntl` is available are supported.  This excludes 
  Microsoft Windows and probably the JVM as well.

+ Kernel level locking is used to manage concurrency.  This means `AcidFS` 
  cannot handle multiple application servers writing to a shared network drive.

+ The type of locking used only synchronizes other instances of `AcidFS`.  Other
  processes manipulating the `Git` repository without using `AcidFS` could cause a
  race condition.  A repository used by `AcidFS` should only be written to by 
  `AcidFS` in order to avoid unpleasant race conditions.
  
For the most part, during a transaction, nothing special needs to be done to
manage concurrency since `Git's` storage model makes management of multiple,
parallel trees trivially easy.  At commit time, however, the current head has
to be updated with the new commit and any concurrent commits that have come in
since the current transaction began, need to be merged.  This last step should
be synchronized such that only one instance of `AcidFS` is attempting this at a
time.  The mechanism, currently, for doing this is use of the `fcntl` module
which takes advantage of an advisory locking mechanism available in Unix
kernels.

Usage
=====

`AcidFS` is easy to use.  Just create an instance of `acidfs.AcidFS` and start 
using the filesystem::

    import acidfs

    fs = acidfs.AcidFS('path/to/my/repo')
    fs.mkdir('foo')
    with fs.open('/foo/bar', 'w') as f:
        print >> f, 'Hello!'

If there is not already a `Git` repository at the path specified, one is created.  
An instance of `AcidFS` is not thread safe.  The same `AcidFS` instance should
not be shared across threads or greenlets, etc.  

The `transaction <http://pypi.python.org/pypi/transaction>`_ package is used to
commit and abort transactions::

    import transaction

    transaction.commit()
    # If no exception has been thrown, then changes are saved!  Yeah!

Protip: If you're using `Pyramid <http://www.pylonsproject.org/>`_, you should
use `pyramid_tm <http://pypi.python.org/pypi/pyramid_tm>`_.

For methods which accept a `path` argument, if the path begins with a `/`, that
path is construed to be absolute, starting at the root of the `Git` repository. 
Paths which do not beging with a `/`, are construed as being relative to the 
current working directory.  The current working directory always begins as the
root of the repository, but may be changed at any time using 
`acidfs.AcidFS.chdir()`::

    fs.chdir('foo')
    print fs.open('bar').read()

The current working directory can also be changed only for a particular scope 
using the `acidfs.AcidFS.cd()` context manager with Python's `with` statement::

    with fs.cd('/foo'):
        print fs.open('bar').read()

Path separators are always `/` regardless of the path separator used by the 
underlying filesystem.  

API
===

The only object exposed publicly by `AcidFS` is the class, `acidfs.AcidFS`.  All
interaction with `AcidFS` is performed by using instances of `acidfs.AcidFS`.

acidfs.AcidFS
-----------

def __init__(path, branch=None, create=True, bare=False)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Arguments

    ``path``

       The path in the real, local fileystem of the repository.

    ``branch``

       Name of the branch in the `Git` repository to use.  If omitted, the
       current HEAD is used.  If omitted, the repository cannot be in a
       detached HEAD state.

    ``create``

       If there is not `Git` repository in the indicated directory, should one
       be created?  The defaul is `True`.

    ``bare``

       If the `Git` repository has to be created, should it be created as a bare
       repository?  The default is `False`.  This argument is only used at the
       time of repository creation.  When connecting to existing repositories,
       `AcidFS` detects whether the repository is bare or not and behaves
       accordingly.
 
def cwd()
~~~~~~~~~

Returns the path to the current working directory in the repository.

def chdir(path)
~~~~~~~~~~~~~~~

Change the current working directory in repository.
 

def cd(path)
~~~~~~~~~~~~

A context manager that changes the current working directory only in
the scope of the 'with' context.  Eg::

    import acidfs

    fs = acidfs.AcidFS('myrepo')
    with fs.cd('some/folder'):
        fs.open('a/file')   # relative to /some/folder
    fs.open('another/file') # relative to /

def open(path, mode='r')
~~~~~~~~~~~~~~~~~~~~~~~~

Open a file for reading or writing.  Supported modes are::

    + 'r', file is opened for reading
    + 'w', file opened for writing
    + 'a', file is opened for writing in append mode

'b' may appear in any mode but is ignored.  Effectively all files are
opened in binary mode, which should have no impact for platforms other
than Windows, which is not supported by this library anyway.

Files are not seekable as they are attached via pipes to subprocesses
that are reading or writing to the git database via git plumbing
commands.
 
def listdir(path='')
~~~~~~~~~~~~~~~~~~~~

Return list of files in directory indicated py `path`.  If `path` is
omitted, the current working directory is used.

def mkdir(path)
~~~~~~~~~~~~~~~

Create a new directory.  The parent of the new directory must already
exist.

def mkdirs(path)
~~~~~~~~~~~~~~~~

Create a new directory, including any ancestors which need to be created
in order to create the directory with the given `path`.

def rm(path)
~~~~~~~~~~~~

Remove a single file.
 
def rmdir(path)
~~~~~~~~~~~~~~~

Remove a single directory.  The directory must be empty.


def rmtree(path)
~~~~~~~~~~~~~~~~

Remove a directory and any of its contents.


def mv(src, dst)
~~~~~~~~~~~~~~~~

Move a file or directory from `src` path to `dst` path.

def exists(path)
~~~~~~~~~~~~~~~~

Returns boolean indicating whether a file or directory exists at the
given `path`.

def isdir(path)
~~~~~~~~~~~~~~~

Returns boolean indicating whether the given `path` is a directory.


def empty(path)
~~~~~~~~~~~~~~~

Returns boolean indicating whether the directory indicated by `path` is
empty.

Roadmap to 1.0 beta
===================

+ Implement merging at commit time.  Currently if a concurrent commit has 
  occured during another transaction, the transaction which commits second will
  always raise a `acidfs.ConflictError`.  It is expected we'll at least try to 
  merge first.

+ Get a tox going and test under Python 2.6 and 2.7.  

+ See if it's feasible to make it compatible with Python 3.2 with a shared code
  base.

+ Determine whether the Pylons project will have us.
