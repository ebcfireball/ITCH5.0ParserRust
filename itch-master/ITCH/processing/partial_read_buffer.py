# Filename: partial_read_buffer.py
# Author: Roy Roth (roykroth@gmail.com)
import gzip


class PartialReadBuffer(object):
    """
    This object allows us to read in from a buffer in increments while keeping track of 
    all of the necessary information.
    """
    def __init__(self, filen, chunk_size=5000):
        self._chunk_size = int(chunk_size)
        self.buffer = gzip.open(filen, 'r')
        self.data = self.buffer.read(self._chunk_size)
        self._position = 0
        # Track how many bytes read from entire file, allows for quick seeking to position
        # in case we need to debug
        self._counter = 0
        # Track the number of bytes from the previous reads to we can seek to a couple of positions
        # before this current read in case we need to debug
        self._previous_Ns = [None, None, None]

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.buffer.close()

    def counter(self):
        """
        Helper function to return read counter, in case we want to quickly seek to a message
        in the event of an Exception
        :return: int, Number of bytes read from entire file
        """
        return self._counter

    def prev_reads(self):
        """
        Helper function to return the relative byte positions of the previous file reads
        before the current one. This allows for seeking to a read or two before the current
        one to see the state of the buffer and parser.
        :return: List of previous read sizes
        """
        return self._previous_Ns

    def read(self, n):
        """
        Parameters:
            n: number of bytes to "read" in and and return
        Returns:
            dat: the desired number of bytes in binary format
        """
        dat = self.data[self._position:self._position + n]
        self._position += n
        self._counter += n
        done = len(dat) == n
        # Store the last couple values of n for debugging
        self._previous_Ns = [n, self._previous_Ns[0], self._previous_Ns[1]]
        while not done:
            append = self.buffer.read(self._chunk_size)
            if len(append) < self._chunk_size:
                done = True
            self.data = dat + append
            self._position = 0
            dat = self.data[self._position:self._position + n]
            self._position += n
            if len(dat) == n:
                done = True
        return dat
