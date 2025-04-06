"""
This script was used to compare sizes of files in compressed HDF5 files vs compressed CSVs
"""

# import dask.dataframe as dd
import numpy as np
import pandas as pd
import tables
import time
import warnings

from functools import partial
from pathlib import Path


def strip(text):
    return text.strip()


warnings.filterwarnings('ignore', category=tables.NaturalNameWarning)
file_path = Path('~/Code/Work/AHT/itch_project/data/processed_data/2017/112417.pq').expanduser()
# store = pd.HDFStore(file_path)
data_path = Path('~/Code/Work/AHT/itch_project/data/processed_data/2017/112417/').expanduser()
dd_path = str(data_path / '*.csv.gz')

dtypes = {
    'seconds': np.float, 'orn': np.int, 'shares': np.int, 'price': np.float,
    'shares_remaining': np.int, 'current bid': np.float, 'current ask': np.float,
    'spread': np.float, 'ask depth': np.int, 'bid depth': np.int, 'depth': np.int
}
col_names = ['type', 'seconds', 'orn', 'side', 'shares', 'price', 'shares_remaining',
             'current bid', 'current ask', 'spread', 'ask depth', 'bid depth', 'depth']

# Make a partial function to have one place to change common parameters
read_csv = partial(pd.read_csv, engine='c', float_precision='high', dtype=dtypes, names=col_names,
                   na_values=['None', ' None'], converters={'type': strip, 'side': strip}, header=0)


def process_file(f_in):
    file_time = time.time()
    ticker = f_in.name.split('_')[1].split('.')[0]
    try:
        df = read_csv(f_in)
        # Downcast categories
        df.type = df.type.astype('category')
        df.side = df.side.astype('category')
        df['ticker'] = ticker
        df.ticker = df.ticker.astype('category')

        # Downcast floats
        df.seconds = pd.to_numeric(df.seconds, downcast='float')
        df.price = pd.to_numeric(df.price, downcast='float')
        df['current bid'] = pd.to_numeric(df['current bid'], downcast='float')
        df['current ask'] = pd.to_numeric(df['current ask'], downcast='float')
        df['spread'] = pd.to_numeric(df['spread'], downcast='float')

        # Downcast ints
        df['orn'] = pd.to_numeric(df['orn'], downcast='unsigned')
        df['shares'] = pd.to_numeric(df['shares'], downcast='unsigned')
        df['shares_remaining'] = pd.to_numeric(df['shares_remaining'], downcast='unsigned')
        df['ask depth'] = pd.to_numeric(df['ask depth'], downcast='unsigned')
        df['bid depth'] = pd.to_numeric(df['bid depth'], downcast='unsigned')
        df['depth'] = pd.to_numeric(df['depth'], downcast='unsigned')

        # df.to_hdf(store, key=ticker, format="table", complib="zlib", compression=3, append=True)
        print('{:>6.2f} - {}'.format(time.time() - file_time, ticker))
        return df
    except ValueError as e:
        print(ticker)
        raise e


start_time = time.time()

files = data_path.glob('*')
result = process_file(next(files))
for file in files:
    result = pd.concat([result, process_file(file)], axis=0)
result.to_parquet(file_path, compression='gzip')
print('Total time: {:.2f}'.format(time.time() - start_time))



