from ITCH.utils import warning
import numpy as np


class BookStatus(object):
    """
    This class is to keep track of the current active orders in order
    to calculate the best bid and ask.

    Parameters
    ----------
    date_in: str
        The ticker date (MMDDYY)
    ticker: str
        The ticker name.
    """
    def __init__(self, date_in, ticker):
        self.date = date_in
        self.ticker = ticker
        self.buy_orders = {}
        self.sell_orders = {}
        self.best_bid = None
        self.best_ask = None
        self.ask_depth = 0
        self.bid_depth = 0
        self.depth = 0

    def process_line(self, line):
        """
        This reads in one line of data and updates the object accordingly.

        Parameters
        ----------
        line: tuple
            Line of ITCH data read in as a tuple of strings
            (correct data types not required)

        Returns
        -------
        best_bid, best_ask: floats
            Best bid and ask after this message has taken effect.
        """
        mess_type, _, orn, side, shares_changed, price, remaining = line
        side = side.strip()
        price = float(price)
        remaining = float(remaining)
        orn = orn.strip()
        if side == 'B':
            if remaining == 0:  # Deleted
                if mess_type != 'P':
                    self.bid_depth -= int(shares_changed)
                    try:
                        del[self.buy_orders[orn]]
                    except KeyError as e:
                        print(e)
                        # These should not happen often, if at all.
                        warning('KeyError for {}: {}.'.format(self.ticker, orn),
                                mess_type, side, shares_changed, remaining)
                if price == self.best_bid or mess_type == 'C':
                    # We calculate this if the order was at the spread.
                    try:
                        self.best_bid = np.array(list(self.buy_orders.values())).max()
                    except ValueError:
                        self.best_bid = None
            else:
                if mess_type in ['E', 'X', 'C']:
                    self.bid_depth -= int(shares_changed)
                elif mess_type != 'C':
                    # We can't let C messages with a different price be included
                    self.bid_depth += int(shares_changed)
                    self.buy_orders[orn] = price
                    if self.best_bid is None or price > self.best_bid:
                        self.best_bid = price
        if side == 'S':
            if remaining == 0:  # delete
                if mess_type != 'P':
                    self.ask_depth -= int(shares_changed)
                    try:
                        del[self.sell_orders[orn]]
                    except KeyError as e:
                        print(e)
                        warning('KeyError for {}: {}.'.format(self.ticker, orn),
                                mess_type, side, shares_changed, remaining)
                if price == self.best_ask or mess_type == 'C':
                    # We calculate this if the order was at the spread.
                    try:
                        self.best_ask = np.array(list(self.sell_orders.values())).min()
                    except ValueError:
                        self.best_ask = None
            else:
                if mess_type in ['E', 'X', 'C']:
                    self.ask_depth -= int(shares_changed)
                elif mess_type != 'C':
                    # We can't let C messages with a different price be included
                    self.sell_orders[orn] = price
                    self.ask_depth += int(shares_changed)
                    if self.best_ask is None or price < self.best_ask:
                        self.best_ask = price
        self.depth = self.bid_depth + self.ask_depth
        return self.best_bid, self.best_ask
