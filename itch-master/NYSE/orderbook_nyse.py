import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import csv
import os
import sys
import gzip

class NYSEOrderbook(object):

    def __init__(self, date='', ticker='', fh='', stop_time = 86399, volume_range = [],):
        '''

        Parameters:
            date = date in format MMDDYY
            ticker = ticker for desired stock
            stop_time = the time (in seconds after midnight) at which we want to
            stop processing the orderbook (default time is one second before midnight)
        '''
        self.bid = {}
        self.sorted_bid = []
        self.ask = {}
        self.sorted_ask = []
        self.date = date
        self.ticker = fh[0:-16].replace('_','-')
        path = '/panfs/pan.fsl.byu.edu/scr/grp/fslg_market_data/NYSE/ProcessedData/20{}/{}/'.format(year,date)
        self.fh = path + fh + '_' + date + '_NYSE.csv.gz'
        #/panfs/pan.fsl.byu.edu/scr/grp/fslg_market_data/NYSE/ProcessedData/{0}/{1}_{2}_NYSE.csv
        self.df = pd.read_csv(self.fh)
        self.stop_time = stop_time
        self.best_ask = 0
        self.best_bid = 0
        self.spread = 0
        self.ask_depth = 0
        self.bid_depth = 0
        self.depth = 0
        self.volume_range = volume_range
        self.header = ['type', ' seconds', ' orn', ' side', ' shares', ' price', 'shares_remaining', 'num_orders_at_price', 'trading_status', 'current bid', 'current ask', 'spread', 'ask_depth', 'bid_depth', 'depth']
        # shares_remaining corresponds to Total_Volume from NYSE (not directly, but for the purpose of the analysis), and shares corresponds to Order_Quantity
        self.write_cache = [self.header]
        self.write_cache_max = 50000
        #self.processed_location = "C:\\Users\\Alex\\Documents\\{0}_{1}.csv".format(date,ticker)
        self.processed_location = path + date + "_" + fh + ".csv.gz"


    def get_volume(self, price_range):
        '''
        Parameters:
            price_range: the maximum distance from best_ask/best_bid to sum total volume

        Returns: a tuple containing the bid side and ask side volume, respectively
        '''
        self.sorted_ask = sorted(list(self.ask.items()))
        self.sorted_bid = sorted(list(self.bid.items()), reverse = True)
        ask_max = self.best_ask + price_range
        ask_volume = 0
        for i in range(len(self.sorted_ask)): # - self.best_ask_position):
            if self.sorted_ask[i][0] <= ask_max: # + self.best_ask_position][0] <= ask_max:
                ask_volume += self.sorted_ask[i][1] # + self.best_ask_position][1]
            else:
                break
        bid_min = self.best_bid - price_range
        bid_volume = 0
        for i in range(len(self.sorted_bid)): # - self.best_bid_position):
            if self.sorted_bid[i][0]>= bid_min: # + self.best_bid_position][0] >= bid_min:
                bid_volume += self.sorted_bid[i][1] # + self.best_bid_position][1]
            else:
                break
        return (bid_volume, ask_volume)


    def put_in_cache(self, message):
        '''
        '''
        self.write_cache.append(message)
        if len(self.write_cache) >= self.write_cache_max:
            # print('writing to file')
            with gzip.open(self.processed_location, 'a') as buf:
                writer = csv.writer(buf)
                writer.writerows(self.write_cache)
            self.write_cache = []


    def update_next(self, message):
        '''
        Reads in a message and updates the bid/ask dictionary price:volume pair

        Parameters: message - a d
        '''
        if message['Side'] == 'B':
            if message['Total_Volume'] == 0:
                try:
                    del(self.bid[message['Price']])
                except KeyError as err:
                    print(err)
                    print(message)
            else:
                self.bid[message['Price']] = message['Total_Volume']
            try:
                self.best_bid = np.array(list(self.bid.keys())).max()
            except ValueError as ve:
                # print(ve)
                self.best_bid = 0
            if message['Type'] == "A":
                self.bid_depth += int(message['Order_Quantity'])
            elif message['Type'] in ['D', 'E']:
                self.bid_depth -= int(message['Order_Quantity'])
        if message['Side'] == 'S':
            if message['Total_Volume'] == 0:
                try:
                    del(self.ask[message['Price']])
                except KeyError as err:
                    print(err)
                    print(message)
            else:
                self.ask[message['Price']] = message['Total_Volume']
            try:
                self.best_ask = np.array(list(self.ask.keys())).min()
            except ValueError as ve:
                # print(ve)
                self.best_ask = 0
            if message['Type'] == "A":
                self.ask_depth += int(message['Order_Quantity'])
            elif message['Type'] in ['D', 'E']:
                self.ask_depth -= int(message['Order_Quantity'])
        self.depth = self.bid_depth + self.ask_depth
        self.spread = self.best_ask - self.best_bid
        # volume = []
        # for x in self.volume_range:
        #     volume.append(self.get_volume(x))
        order_book_line = [message['Type'], message['Seconds'], message['Message_Sequence_Number'], message['Side'], message['Order_Quantity'], message['Price'], message['Total_Volume'], message['Num_Orders_at_Price'], message['Trading_Status'], self.best_bid, self.best_ask, self.spread, self.ask_depth, self.bid_depth, self.depth]
        self.put_in_cache(order_book_line)


    def update(self):
        print('Creating orderbook for {0} and writing to file.'.format(self.ticker))
        try:
            self.df.replace(to_replace='O', value='A', inplace=True)
            self.df.replace(to_replace='C', value='D', inplace=True)
            self.df.sort_values(by=['Price'], inplace=True)
            self.df.loc[self.df.Price == self.df.Price.shift(1), 'Change'] = self.df['Order_Quantity'] - self.df['Order_Quantity'].shift(1)
            self.df.loc[self.df.Price != self.df.Price.shift(1), 'Change'] = self.df['Order_Quantity']
            self.df.loc[(self.df.Change > 0) & (self.df.Type=='X'), 'Type'] = 'A'
            self.df.loc[(self.df.Change < 0) & (self.df.Type=='X'), 'Type'] = 'D'
            self.df.loc[(self.df.Change == 0) & (self.df.Type=='X'), 'Type'] = np.nan
            self.df.sort_values(by=['Seconds'], inplace=True)
        except Exception as e:
            print('ERROR: File for {0}'.format(self.ticker))
            print(e)
            print('\n')
            pass
        for i in range(len(self.df)):
            self.update_next(self.df.iloc[i])
            if self.df.iloc[i,1] >= self.stop_time:
                break
        with gzip.open(self.processed_location, 'a') as buf:
            writer = csv.writer(buf)
            writer.writerows(self.write_cache)
        self.write_cache = []
        print('Finished creating orderbook for {0}.'.format(self.ticker))

    def print_to_screen(self):
        print('\nBids:\nPrice\tVolume')
        for key in sorted(self.bid):
            print('$' + str(key), '\t' + str(self.bid[key]))
        print('\nAsks:\nPrice\tVolume')
        for key in sorted(self.ask):
            print('$' + str(key), '\t' + str(self.ask[key]))


if __name__ == "__main__":
    day = str(sys.argv[1])
    rank = int(sys.argv[2])
    size = int(sys.argv[3])
    year = sys.argv[4]
    print('\nProcessing Tickers for:{0}'.format(day))
    print("Rank = {}".format(rank))
    path = '/panfs/pan.fsl.byu.edu/scr/grp/fslg_market_data/NYSE/ProcessedData/20{}/{}'.format(year, day)
    all = os.listdir(path)
    tickers = [x[:-19] for x in all if "NYSE" in x] # Get a list of all tickers
                                                    # for the given day
    tickers = tickers[rank::size]
    for ticker in tickers:
        if "{}_{}.csv.gz".format(day, tickers) in all:
            pass # This check is for robustness when only part of a day has
                 # successfully processed
        else:
            if "TEST" not in ticker:
                print(ticker)
                I = NYSEOrderbook(fh=ticker, date=day)
                I.update()
    # Delete stage 1 files now that stage 2 processing had been competed
    delete = [f for f in os.listdir(path) if "NYSE" in f and f[:-19] in tickers]
    for ticker in delete:
        os.remove(path + "/" + ticker)
    print("Finished!")
