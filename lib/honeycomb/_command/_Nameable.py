class _Nameable(object):

    def __init__(self, name):
        self._name = name

    @property
    def name(self) -> str:
        return self._name
