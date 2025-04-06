"""
This is a test script to compare the performance of decoding the raw binary files into
a data structure of some sort against that of a Rust itch parser
(github.com/seanlane/itchy-rust). Initial results show Rust to be about 93% faster
"""

from ITCH.processing.decode import decode_next
from ITCH.processing.partial_read_buffer import PartialReadBuffer

import struct
import sys
from pathlib import Path

data_file = str(Path(sys.argv[1]).expanduser())

with PartialReadBuffer(data_file) as data:
    t, mess = decode_next(data)
    counter = 1
    while not (t == b'S' and mess[3] == b'C'):
        try:
            t, mess = decode_next(data)
            counter += 1
            if counter % 10000000 == 0:
                print("# of messages: {}".format(counter))
        except ValueError:
            break
        except struct.error:
            break
    print("Total # of messages: {}".format(counter))
