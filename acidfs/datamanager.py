import transaction


class AcidFSDataManagerMixin(object):
    closed = False

    def __init__(self):
        transaction.get().join(self)

    def abort(self, tx):
        """
        Abort transaction without attempting to commit.
        """

    def tpc_begin(self, tx):
        """
        Initiate two phase commit.
        """

    def commit(self, tx):
        """
        Prepare to save changes, but don't actually save them.
        """

    def tpc_vote(self, tx):
        """
        If we can't commit the transaction, raise an exception here.  If no
        exception is raised we damn well better be able to get through
        tpc_finish without any errors.  Last chance to bail is here.
        """

    def tpc_finish(self, tx):
        """
        Write data to disk, committing transaction.
        """

    def tpc_abort(self, tx):
        """
        Clean up in the event that some data manager has vetoed the transaction.
        """

