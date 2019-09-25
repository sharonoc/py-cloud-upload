class SizeComparableFile:
    """"
    Comparable class for sorting files by size
    """
    def __init__(self, size, filename, header):
        self.size = size
        self.filename = filename
        if hasattr(header, 'encode'):
            self.header = header.encode('utf-8')
        else:
            self.header = header

    def __new__(cls, size=None, filename=None, header=None):
        instance = object.__new__(cls)
        instance.__init__(size=size, filename=filename, header=header)
        return instance

    # comparable methods
    def __eq__(self, other):
        return self.size == other.size

    def __lt__(self, other):
        return self.size < other.size

    def __gt__(self, other):
        return self.size > other.size

    # required methods for 'pickling'
    def __getstate__(self):
        state = self.__dict__.copy()
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
