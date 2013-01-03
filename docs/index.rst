.. AcidFS documentation master file, created by
   sphinx-quickstart on Tue Sep 11 21:26:36 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

======
AcidFS
======

**The filesystem on ACID**

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

+ Most common concurrent changes can be merged.  There's even a decent chance 
  concurrent modifications to the same text file can be merged.

+ Transactions can be started from an arbitrary commit point, allowing, for 
  example, a web application to apply the results of a form submission to the
  state of your data at the time the form was rendered, making concurrent edits
  to the same resource less risky and effectively giving you transactions that
  can span request boundaries.

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

+ The type of locking used only synchronizes other instances of `AcidFS`.
  Other processes manipulating the `Git` repository without using `AcidFS`
  could cause a race condition.  A repository used by `AcidFS` should only be
  written to by `AcidFS` in order to avoid unpleasant race conditions.
  
All of the above limitations are a result of the locking used to synchronize
commits.  For the most part, during a transaction, nothing special needs to
be done to manage concurrency since `Git's` storage model makes management of
multiple, parallel trees trivially easy.  At commit time, however, any new data
has to be merged with the current head which may have changed since the
transaction began.  This last step should be synchronized such that only one
instance of `AcidFS` is attempting this at a time.  The mechanism, currently,
for doing this is use of the `fcntl` module which takes advantage of an
advisory locking mechanism available in Unix kernels.

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

.. note::

    If you're using `Pyramid <http://www.pylonsproject.org/>`_, you should use
    `pyramid_tm <http://pypi.python.org/pypi/pyramid_tm>`_.  For other WSGI
    frameworks there is also `repoze.tm2
    <http://pypi.python.org/pypi/repoze.tm2>`_.

Commit Metadata
===============

The `transaction <http://pypi.python.org/pypi/transaction>`_ package has built
in support for providing metadata about a particular transaction.  This
metadata is used to set the commit data for the underlying git commit for a 
transaction.  Use of these hooks is optional but recommended to provide 
meaningful audit information in the history of your repository.  An example is
the best illustration::

    import transaction

    current = transaction.get()
    current.note('Added blog entry: "Bedrock Bro Culture: Yabba Dabba Dude!"')
    current.setUser('Fred Flintstone')
    current.setExtendedInfo('email', 'fred@bed.rock')

A users's name may also be set by using the ``setExtendedInfo`` method::

    current.setExtendedInfo('user', 'Fred Flintstone')

The transaction might look something like this in the git log::

    commit 3aa61073ea755f2c642ef7e258abe77215fe54a2
    Author: Fred Flintstone <fred@bed.rock>
    Date:   Sun Sep 16 22:08:08 2012 -0400

        Added blog entry: "Bedrock Bro Culture: Yabba Dabba Dude!"

API
===

.. automodule:: acidfs

  .. autoclass:: AcidFS

     .. automethod:: open
     .. automethod:: cwd
     .. automethod:: chdir
     .. automethod:: cd(path)
     .. automethod:: listdir
     .. automethod:: mkdir
     .. automethod:: mkdirs
     .. automethod:: rm
     .. automethod:: rmdir
     .. automethod:: rmtree
     .. automethod:: mv
     .. automethod:: exists
     .. automethod:: isdir
     .. automethod:: empty
     .. automethod:: get_base
     .. automethod:: set_base
     .. automethod:: hash

.. toctree::
   :maxdepth: 2
