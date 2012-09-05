import transaction


class GitFSDataManagerMixin(object):
    closed = False

    def __init__(self):
        transaction.get().join(self)


