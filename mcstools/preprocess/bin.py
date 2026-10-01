import numpy as np

class Bins():
    def __init__(self, start, stop, size, name=None):
        self.start = start
        self.stop = stop
        self.size = size
        self.name = name
        self.bin_array = np.arange(self.start, self.stop+self.size, self.size)
    
    def make_bins(self):
        return [Bin(bs, bs+self.size) for bs in self.bin_array[:-1]]

    @property
    def bins(self):
        return self.make_bins()

    @property
    def midpoints(self):
        return np.array([b.midpoint for b in self.bins])

    def find_bin_from_value(self, value):
        return self.bins[np.digitize(value, self.bins)-1]

class Bin():
    def __init__(self, start, stop):
        self.start = start
        self.stop = stop
    
    @property
    def midpoint(self):
        return (self.start + self.stop)/2