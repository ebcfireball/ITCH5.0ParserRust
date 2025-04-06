"""
This was used to tested the size of Parquet files vs compressed CSVs
"""

import numpy as np
import pandas as pd
import time

from functools import partial
from multiprocessing import Pool
from pathlib import Path


def strip(text):
    return text.strip()


pq_path = Path('~/Code/Work/AHT/itch_project/data/processed_data/2017/parquet').expanduser()
data_path = Path('~/Code/Work/AHT/itch_project/data/processed_data/2017/112417/').expanduser()

dtypes = {
    'type': 'category', 'seconds': np.float, 'orn': np.int32, 'side': 'category', 'shares': np.int32,
    'price': np.float, 'shares_remaining': np.int32, 'current bid': np.float, 'current ask': np.float,
    'spread': np.float, 'ask depth': np.int32, 'bid depth': np.int32, 'depth': np.int32
}
col_names = ['type', 'seconds', 'orn', 'side', 'shares', 'price', 'shares_remaining',
             'current bid', 'current ask', 'spread', 'ask depth', 'bid depth', 'depth']

# Make a partial function to have one place to change common parameters
read_csv = partial(pd.read_csv, engine='c', float_precision='high', dtype=dtypes,
                   na_values=['None', ' None'], names=col_names, header=0)

start_time = time.time()


def process_file(file):
    file_time = time.time()
    ticker = file.name.split('_')[1].split('.')[0]
    df = read_csv(file)
    file_name = pq_path / (ticker + '.pq')
    df.to_parquet(file_name, engine='pyarrow', compression='brotli')
    print('{:>6.2f} - {}'.format(time.time() - file_time, ticker))


files = data_path.glob('*')
p = Pool()
p.map(process_file, files)
print('Total time: {:.2f}'.format(time.time() - start_time))



