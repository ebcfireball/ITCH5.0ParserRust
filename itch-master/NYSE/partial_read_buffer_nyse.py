# Filename: partial_read_buffer.py
# Author: Roy Roth (roykroth@gmail.com)
import zipfile

class PartialReadBuffer(object):
    '''

    '''
    def __init__(self, filen, chunk_size = 6900):
        self._chunk_size = int(chunk_size)
        self.buffer = open(filen, 'r')
        self.data = self.buffer.read(self._chunk_size)
        self._position = 0

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.buffer.close()

    def read_message(self, n=69):
        """
        Parameters:
            n: number of bytes to "read" in and and returned
        Returns:
            dat: the desired number of bytes in binary format
        """
        dat = self.data[self._position:self._position + n]
        self._position += n
        done = len(dat) == n
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

    def read_all(self):
        dat = self.data[self._position:]
        return dat
