AcidFS allows interaction with the filesystem using transactions with ACID 
semantics.  `Git` is used as a back end, and `AcidFS` integrates with the 
`transaction <http://pypi.python.org/pypi/transaction>`_ package allowing use of
multiple databases in a single transaction.  AcidFS makes concurrent persistent
to the filesystem  safe and reliable.

Full documentation is available at `Read the Docs 
<http://acidfs.readthedocs.org/>`_.

This beta is considered a release candidate.  If sufficient time passes without
an issue arising, this code will become the final 1.0 release.
