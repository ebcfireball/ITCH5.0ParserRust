# This program takes in a date and creates a new file from
# a previously processed data file by sorting according to
# time and adding in the spread.
# Author: Robert Buss (rbuss@byu.net)

import csv
import gzip
import operator
import os
import pandas as pd
import sys
import time

from ITCH import locations
from ITCH.processing import BookStatus
from ITCH.utils import grouped_data_tickers, warning


@locations.grouped_data
def get_parsed_file(date_in, ticker):
    year = '20' + date[4:]
    folder = os.path.join(year, date)

    try:
        os.chdir(folder)
    except OSError as e:
        warning('Parsed folder does not exist: {}'.format(os.path.join(os.getcwd(), folder)))
        raise e
    path = os.path.realpath('OrderGroups_{}_{}.csv.gz'.format(date_in, ticker))
    return path


@locations.processed_data
def new_investigator(date, ticker):
    """
    Takes a processed data file and adds in the spread.
    Parameters
    ----------
    date: str
        Date of interest in DDMMYY format
    ticker: str
        Ticker of interest.

    Returns
    -------
    elapsed: float
        Elapsed time to process the given file
    It also creates the file Processed_DATE_TICKER.csv that is sorted by time.
    """
    start = time.time()
    year = '20' + date[4:]
    folder = os.path.join(year, date)

    try:
        os.chdir(folder)
    except OSError as e:
        warning('Folder does not exist: {}'.format(folder))
        raise e
    parsed_file = get_parsed_file(date, ticker)
    new_file = os.path.realpath('{}_{}.csv'.format(date, ticker))
    # we first sort and remove lines as needed.
    with gzip.open(parsed_file, 'rt') as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        sortedlist = sorted(reader, key=operator.itemgetter(1, -1), reverse=False)

    columns = ['type', 'seconds', 'orn', 'side', 'shares', 'price', 'shares_remaining',
               'current bid', 'current ask', 'spread', 'ask depth', 'bid depth', 'depth']

    # Initial write of file headers
    headers = pd.DataFrame(columns=columns)
    if os.path.exists(new_file):
        os.remove(new_file)
    headers.to_csv(new_file, index=False)

    status = BookStatus(date, ticker)
    updated_lines = []

    end_line = -1 if sortedlist and (sortedlist[-1][0].strip().lower() == 'type') else None
    for line in sortedlist[:end_line]:
        mess_type, seconds, orn, side, shares_changed, price, shares_remaining = line
        if mess_type.strip() == 'J':
            pass
        else:
            bid, ask = status.process_line(line)
            b_depth = status.bid_depth
            a_depth = status.ask_depth
            depth = status.depth
            try:
                spread = str(round(ask - bid, 4))
            except TypeError:
                spread = None
            line_result = {
                'type': mess_type,
                'seconds': seconds,
                'orn': orn,
                'side': side,
                'shares': shares_changed,
                'price': price,
                'shares_remaining': shares_remaining,
                'current bid': bid,
                'current ask': ask,
                'spread': spread,
                'ask depth': a_depth,
                'bid depth': b_depth,
                'depth': depth
            }
            updated_lines.append(line_result)

        if len(updated_lines) % 5000 == 0:
            df = pd.DataFrame(updated_lines, columns=columns)
            df.to_csv(new_file, mode='a', index=False, na_rep='None', header=False)
            updated_lines = []

    # Make final append to CSV file once we're at the end of sortedlist
    df = pd.DataFrame(updated_lines, columns=columns)
    df.to_csv(new_file, mode='a', index=False, na_rep='None', header=False)

    # Compress file and delete temp file
    compressed_file = new_file + '.gz'
    if os.path.exists(compressed_file):
        os.remove(compressed_file)
    with open(new_file, 'rb') as src, gzip.open(compressed_file, 'wb') as dst:
        dst.writelines(src)
    os.remove(new_file)

    time_elapsed = time.time() - start
    return time_elapsed


if __name__ == '__main__':
    date = sys.argv[1]
    size = int(sys.argv[2])
    rank = int(sys.argv[3])

    @locations.processed_data
    def create_dir(date_in):
        """
        Creates the directories needed if they don't exist.
        Not going to bother with deleting existing files first, since anything that's
        already there will be overwritten by Pandas. Trying to delete first can
        cause problems if the 0 rank process is still deleting (can take 75+ seconds)
        when the other threads begin writing. That could be fixed by switching this
        to use a thread pool, but not going to worry about that for the time being.
        """
        year = '20' + date_in[4:]
        folder = os.path.join(year, date_in)

        # Make the Directories we need
        if os.path.exists(folder):
            print('Folder path already exists, a good sanity check would be to verify that both',
                  'the grouped_data and processed_data have the same number of files')
        os.makedirs(folder, exist_ok=True)


    create_dir(date)
    print('Rank {} of {} processing on date: {}'.format(rank, size, date))
    tickers_and_size = grouped_data_tickers(date)
    # Get a list of tickers, sorted by file size, to help distribute the tickers among the cpus
    tickers = [ticker for (ticker, size) in sorted(
        tickers_and_size, key=lambda x: x[1], reverse=False)]

    tickers = tickers[rank::size]
    print("Time Elapsed\t-\tTicker No. / Total No. Tickers:\tTicker Processed")
    print('================================================================')
    start_time = time.time()
    for i in range(len(tickers)):
        ticker = tickers[i]
        elapsed = new_investigator(date, ticker)
        print('{:>8.3f}\t-\t{:>{width}}/{}:\t\t{}'.format(
            elapsed, i+1, len(tickers), ticker, width=len(str(len(tickers)))))

    elapsed_time = time.time() - start_time
    print('================================================================')
    print('Completed working on {}'.format(date))
    m, s = divmod(elapsed_time, 60)
    h, m = divmod(m, 60)
    print('Elapsed time for rank {}: {}:{}:{:.3f} ({:.3f} seconds)'.format(
        rank, int(h), int(m), s, elapsed_time))
