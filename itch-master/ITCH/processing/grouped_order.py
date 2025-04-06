from ITCH.processing.partial_read_buffer import PartialReadBuffer
from ITCH import locations
from ITCH.utils import warning
from ITCH.processing.decode import decode_next
import os
import gzip
import struct
import time

"""
Modified for version 5.0 by Evan Argyle.
MAJOR MODIFICATION:
As we no longer have time messages coming in, we no longer record time. Instead, the time comes 
directly with each message, usually as the third element, and can be written to the csv with the 
rest of the messages info.
TWEAKED by Christopher Hair to fix the "C" message bug.
"""


class OrderGroup(object):
    """
    This class is an object of orders added and all other subsequent orders
    that modify the initial message in some way.

    Parameters
    ----------
    message_type: str
        'A', 'F', 'P', or 'U', depending on the message.
    mess: An "A", "F", "P", or special "U" message from the ITCH Data. 
        See the ITCH documentation to determine the format
    """
    def __init__(self, message_type, mess):
        self.message_type = message_type.strip()
        self.log = []  # The log lists each order and what it did.

        self.orn = mess[3]  # int
        if type(mess[4]) is bytes:
            self.side = mess[4].strip().decode()  # str
        elif type(mess[4]) is str:
            self.side = mess[4].strip()
        else:
            print('Side data type is neither bytes nor str, stock: {}'.format(mess[4]))
            self.side = str(mess[4])
        self.shares = mess[5]  # int
        if type(mess[6]) is bytes:
            self.stock = mess[6].strip().decode()  # str
        elif type(mess[6]) is str:
            self.stock = mess[6].strip()
        else:
            print('Stock data type is neither bytes nor str, stock: {}'.format(mess[6]))
            self.stock = str(mess[6])
        self.price = mess[7] * 1e-4  # convert to dollars (float)
        if self.message_type in 'FU':
            self.MPID = mess[8]  # String
        else:
            self.MPID = ''

        if self.message_type == 'P':
            self._log_append(self.message_type, mess[2], self.shares, self.price, '0')
        elif self.message_type == 'J':
            self._log_append(self.message_type, mess[2], mess[4], mess[5], mess[6])
            try:
                self.orn = mess[3].strip() + str(round(mess[2]*1.e-9, 7))
            except TypeError:
                self.orn = mess[3].strip().decode() + str(round(mess[2]*1.e-9, 7))
            self.side = ''
            self.stock = mess[3].strip().decode()
        else:
            self._log_append(self.message_type, mess[2], self.shares, self.price, self.shares)

    def __repr__(self):
        return "OrderGroup object {}".format(self.orn)

    def _log_append(self, msg_type, msg_time, msg_shares, msg_price, msg_shares_remaining):
        try:
            msg_time = str(round(msg_time * 1.e-9, 7))
            msg_shares = str(msg_shares)
            if not isinstance(msg_price, str):
                msg_price = str(round(msg_price, 4))
            msg_shares_remaining = str(msg_shares_remaining)
            self.log.append([msg_type, msg_time, msg_shares, msg_price, msg_shares_remaining])
        except TypeError as e:
            print('_log_append error; message:')
            print('msg_type: ', msg_type)
            print('msg_time: ', msg_time)
            print('msg_shares: ', msg_shares)
            print('msg_price: ', msg_price)
            print('msg_shares_remaining: ', msg_shares_remaining)
            raise e

    def _process_E(self, mess):
        """
        This adds a new entry to the log indicating that the order has 
        been executed in full or in part.  It then updates the number of 
        shares.

        Parameters
        ----------

        mess: list
            This is the information contained in an 'E' message.

        Returns
        ------
        loc: int
            The index of the new log entry
        """
        shares = mess[4]
        self.shares -= shares
        self._log_append('E', mess[2], shares, self.price, self.shares)
        return len(self.log) - 1

    def _process_C(self, mess):
        """
        This adds a new entry to the log indicating that the order has 
        been executed in full or in part, at a different price than
        originally listed.  It then updates the number of shares.

        Parameters
        ----------
        mess: list
            This is the information contained in an 'C' message.

        Returns
        ------
        loc: int
            The index of the new log entry
        """
        # orn = mess[3]
        shares = mess[4]
        price = float(mess[7])*1.e-4  # This is sketchy.
        self.shares -= shares
        self._log_append('C', mess[2], shares, price, self.shares)
        return len(self.log) - 1

    def _process_X(self, mess):
        """
        This adds a new entry to the log indicating that the order has 
        been partially cancelled.  The number of shares is reduced and
        the message is recorded in the log.

        Parameters
        ---------- 
        mess: list
            This is the information contained in an 'X' message.
        """
        # orn = mess[3]
        shares = mess[4]
        self.shares -= shares
        self._log_append('X', mess[2], shares, self.price, self.shares)

    def _process_D(self, mess):
        """
        This adds a new entry to the log indicating that the order has 
        been cancelled.  The number of shares is reduced and
        the message is recorded in the log.

        Parameters
        ----------      
        mess: list
            This is the information contained in an 'D' message.
        """
        self._log_append('D', mess[2], self.shares, self.price, '0')
        self.shares = 0

    def _process_U(self, mess):
        """
        This adds a new entry to the log indicating that the order has 
        been cancel-replaced.  The number of shares is reduced and
        the message is recorded in the log.  A new order group should also
        be created, but is not done so here.

        Parameters
        ---------
        mess: list
            This is the information contained in an 'U' message.
        """
        self._log_append('U', mess[2], self.shares, self.price, '0')
        self.shares = 0

    def _make_special_U_message(self, mess):
        #TODO
        """
        This makes a message in the same format as an 'F' message so that
        a new OrderGroup object can be made.

        Parameters
        ---------
        mess: list
            This is a 'U' message.

        Returns
        -------
        mess: list
            This is a ITCH message in the same format as an 'F' message.
        """
        time = mess[2]
        new_orn = mess[4]
        shares = mess[5]
        price = mess[6]
        new_mess = [None, None, time, new_orn, self.side, shares, self.stock, price, self.MPID]
        return new_mess

    def _process_B(self, loc, mess):
        """
        B messages are special - they represent a broken trade.  The line
        of the log for the trade broken has the message type changed
        to end with '-B', and a new log entry is made with the time that 
        the order was broken.  Additionally, the stock volume must be
        adjusted to undo the order.

        Parameters
        ----------
        time: int
            current time up to the nearest second
        mess: list
            ITCH 'B' message
        loc: int
            Index of the log for the order that was broken.
        """
        # TODO check that the number of shares is correct.
        warning('B message executed')
        warning('Ticker: {}'.format(self.stock))
        warning('loc: {}'.format(loc))
        warning('mess: {}'.format(mess))
        warning('self.log:\n{}'.format('\n'.join([','.join(x) for x in self.log])))
        self.log[loc][0] += '-B'
        self.shares += int(self.log[loc][2])
        self._log_append('B', mess[2], self.log[loc][2], self.log[loc][3], self.shares)

    def to_csv_format(self):
        strings = ["{}, {}, {}, {}, {}, {}, {}\n".format(
                entry[0], entry[1], str(self.orn), self.side, entry[2], entry[3], entry[4])
            for entry in self.log]
        return ''.join(strings)


class ParallelDay(object):
    """
    This class is a day of OrderGroup objects for a group of tickers.
    
    Parameters
    ----------
    date: str
        The date in MMDDYY format.
    size: int
        The total number of processers being used
    rank: int
        The rank of this processor
    write_cache_max: int, optional
        The maximum number of orders to keep in a ticker's cache
    """
    def __init__(self, date, size, rank, locations, write_cache_max=1000):
        self.date = date
        self.size = size
        self.rank = rank
        self.locations = locations
        self.groups = {}
        self.match_no = {}
        self.no_hidden = 0  # we need this because hidden have the same ORN.
        self.tickers = set()
        self.no_tickers = 0
        self.no_erroredCs = 0
        self.no_erroredDs = 0
        self.no_erroredUs = 0
        self.write_cache = {}  # filename, list of ORN to be written
        self.write_cache_len = {}  # How many orders are in each entry
        self.write_cache_max = write_cache_max
        self.mess_types = {
            'T': self._process_T, 'A': self._process_A, 'F': self._process_F, 'P': self._process_P,
            'X': self._process_X, 'D': self._process_D, 'E': self._process_E, 'C': self._process_C,
            'U': self._process_U, 'B': self._process_B, 'R': self._process_R, 'S': self._process_S,
            'Q': self._process_Q, 'H': self._process_H_Y_L, 'Y': self._process_H_Y_L,
            'L': self._process_H_Y_L, 'I': self._process_I_N, 'N': self._process_I_N,
            'V': self._process_V, 'W': self._process_W, 'K': self._process_K, 'J': self._process_J
        }
        self.keep_processing = True
        self.counter = 0
        self.R_message_counter = 0  # Test counter for repeating R messages
        self.allTickers = set()

    def __repr__(self):
        return "ParallelDay object of OrderGroups for {}.".format(self.date)

    def process_next(self, buf):
        """
        This processes the next message in the buffer.
        """
        try:
            t, mess = decode_next(buf)
        except struct.error:
            print("Something went wrong reading length in")
            print("buf byte counter: {}".format(buf.counter))
            print("buf previous n values: {}".format(buf.prev_reads()))
            print("filename: {}".format(buf.buffer.filename))
            raise ValueError("Something went wrong reading length")
        mess = list(mess)  # convert the tuple to a list
        opt = self.mess_types[t]
        try:
            opt(mess)
        except KeyError as e:
            print('KeyError at grouped_order.py:process_next')
            print('Exception e: ', e)
            pass
        except ValueError:
            print('ValueError')
            print('PRB counter: {}'.format(buf.counter()))
            print('PRB previous reads: {}'.format(buf.prev_reads()))
            raise ValueError

        self.counter += 1
        if self.counter % 1000000 == 0:
            print('{}: {} million messages read on rank {}'.format(
                time.time(), self.counter/1000000, self.rank))

    def _process_T(self, mess):
        self.time = mess[0]

    def _process_A(self, mess):
        if mess[6].strip().decode() in self.tickers:
            self.groups[mess[3]] = OrderGroup('A', mess)

    def _process_F(self, mess):
        if mess[6].strip().decode() in self.tickers:
            self.groups[mess[3]] = OrderGroup('F', mess)

    def _process_P(self, mess):
        self.no_hidden += 1
        if mess[6].strip().decode() in self.tickers:
            new_group = OrderGroup('P', mess)
            # TODO confirm this is right.
            self.groups[-1*self.no_hidden] = new_group
            self.remove_order(-1*self.no_hidden)

    def _process_X(self, mess):
        try:
            self.groups[mess[3]]._process_X(mess)
        except KeyError:
            pass

    def _process_D(self, mess):
        orn = mess[3]
        try:
            self.groups[orn]._process_D(mess)
            self.remove_order(orn)
        except KeyError:
            pass

    def _process_E(self, mess):
        orn = mess[3]
        loc = .1
        try:
            loc = self.groups[orn]._process_E(mess)
            # Could be broken later.
            if self.groups[orn].shares == 0:
                self.remove_order(orn)
        except KeyError:
            pass
        else:
            self.match_no[mess[5]] = (orn, loc)

    def _process_C(self, mess):
        orn = mess[3]
        loc = .1
        try:
            loc = self.groups[orn]._process_C(mess)
            if self.groups[orn].shares == 0:
                self.remove_order(orn)
        except KeyError:
            pass
        else:
            self.match_no[mess[5]] = (orn, loc)

    def _process_U(self, mess):
        orn = mess[3] #original ORN
        try:
            self.groups[orn]._process_U(mess)
            new_mess = self.groups[orn]._make_special_U_message(mess)
            self.groups[new_mess[3]] = OrderGroup('U', new_mess)
            self.remove_order(orn)
        except KeyError:
            pass

    def _process_B(self, mess):
        match_num = mess[3] - 1
        try:
            orn, loc = self.match_no[match_num]
            print("Match num: {}".format(match_num))
            print("ORN: {}".format(orn))
            print("loc: {}".format(loc))
            print("Mess(age): {}.".format(str(mess)))
            print("self.match_no: {}".format(str(self.match_no)))
            self.groups[orn]._process_B(loc, mess)
        except KeyError:
            pass
        except AttributeError:  # This happens if the order is in the buffer
            warning('Order {} was broken.'.format(orn))

    def _process_R(self, mess):
        ticker = mess[3].strip().decode()
        """
        #The following if statement was to test if there are multiple R messages for a single ticker
        if ticker == 'AAWW':
            self.R_message_counter += 1
            print('Number of R messages for AAWW: {}'.format(self.R_message_counter))
        """
        if self.no_tickers % self.size == self.rank:
            if ticker in self.allTickers:
                print("{} already assigned, not original R message".format(ticker))
            else:
                self.tickers.add(ticker)
                self.make_header(ticker)

        self.no_tickers += 1
        self.allTickers.add(ticker)

    def _process_S(self, mess):
        if mess[3] == b'C':
            self.keep_processing = False
        else:
            # Q Means we are in market hours
            # M Means we are not in market hours
            pass

    def _process_Q(self, mess):
        pass  # I think we just leave these alone.

    def _process_H_Y_L(self, mess):
        pass  # Do not change the order book.

    def _process_I_N(self, mess):
        pass  # Do not change the order book.

    def _process_W(self, mess):
        pass  # Does not change order book

    def _process_V(self, mess):
        pass  # Does not change order book

    # New message type for ITCH 5.0 added by Christopher Hair
    def _process_K(self, mess):
        pass  # Does not change order book

    def _process_J(self, mess):
        pass  # We used to process these, but they're ignored in process_orderbooks, so passing here

    def remove_order(self, orn):
        """
        This writes the order to file and deletes it. This keeps hidden, 
        cancel-replaced, and deleted orders from staying in memory.
        """
        ticker = self.groups[orn].stock
        file = 'OrderGroups_{}_{}.csv'.format(self.date, ticker)
        self.groups[orn] = self.groups[orn].to_csv_format()
        try:
            self.write_cache[file].append(orn)
            self.write_cache_len[file] += 1
        except KeyError:  # If we haven't started this cache yet
            self.write_cache[file] = [orn]
            self.write_cache_len[file] = 1
        else:
            if self.write_cache_len[file] >= self.write_cache_max:
                self.clear_cache(file)

    @locations.grouped_data
    def clear_cache(self, file):
        """
        This will empty that file's cache and write it to the file.
        """
        year = '20' + self.date[4:]
        folder = os.path.join(year, self.date)
        os.chdir(folder)
        print('Cache to ', os.path.join(folder, file))
        with gzip.open(file + '.gz', 'at') as buf:
            for orn in self.write_cache[file]:
                try:
                    buf.write(self.groups[orn])
                except OSError as e:
                    print('Catching OSError e: {}', e)
                    print('Trying again')
                    buf.write(self.groups[orn])
                del self.groups[orn]
        del self.write_cache[file]

    @locations.grouped_data
    def to_csv(self):
        year = '20' + self.date[4:]
        folder = os.path.join(year, self.date)
        os.chdir(folder)
        for file in list(self.write_cache.keys()):
            self.clear_cache(file)  # empty everything in the cache

        # This is used for the leftovers.
        grouped = self._group_by_ticker()
        for ticker in grouped.keys():
            file = 'OrderGroups_{}_{}.csv.gz'.format(self.date, ticker)
            print('Write to ', os.path.join(folder, file))
            with gzip.open(file, 'at') as f:
                for orn in grouped[ticker].keys():
                    f.write(self.groups[orn].to_csv_format())

    @locations.grouped_data
    def make_header(self, ticker):
        year = '20' + self.date[4:]
        folder = os.path.join(year, self.date)

        # Make the Directories we need
        os.makedirs(folder, exist_ok=True)
        os.chdir(folder)
        file = 'OrderGroups_{}_{}.csv.gz'.format(self.date, ticker)
        if os.path.exists(file):
            os.remove(file)
        print('Write header for {}'.format(os.path.join(folder, file)))
        with gzip.open('OrderGroups_{}_{}.csv.gz'.format(self.date, ticker), 'wt') as f:
            header = 'type, seconds, orn, side, shares, price, shares_remaining\n'
            f.write(header)

    def _group_by_ticker(self):
        """
        This will sort through all the orders and make dictionaries for each
        ticker with the corresponding orders.
        Returns
        -------
        grouped: dict
            Dictionary with keys as tickers.  Elements are dictionaries of
            OrderGroups with the ORN as the key.
        """
        grouped = {ticker: {} for ticker in self.tickers}
        for og in self.groups.keys():
            grouped[self.groups[og].stock][og] = self.groups[og]
        return grouped


@locations.binary_data
def process_parallelday(date, size, rank, write_cache_max=1000):
    """
    Parameters
    ----------
    date: str
        Date in MMDDYY format.
    size: int
        Number of processors in use.
    rank: int
        The rank of the processor this is running on.
    write_cache_max: int, optional
        The maximum number of orders to keep in a ticker's cache
    Returns
    -------
    Day: Day object
        Day object processed
    """
    print('Grouped order {}/{}'.format(rank, size))
    that_day = ParallelDay(date, size, rank, write_cache_max)
    year = '20' + date[4:]
    data_file = '{}/S{}-v50.txt.gz'.format(year, date)
    with PartialReadBuffer(data_file) as data:
        while that_day.keep_processing:
            that_day.process_next(data)
        that_day.to_csv()
    return that_day
