"""
There is some doubt as to whether the processed order books are truly in order. This script will
attempt to verify that the messages for a given day are actually in order within the file
"""
from functools import reduce, partial
from pathlib import Path

import argparse
import numpy as np
import pandas as pd
import pathos.multiprocessing as mp


def check_file_sort(file_path, verbose=False):
    file_path = Path(file_path)
    assert file_path.is_file(), "Invalid path"

    # Get the first line to figure out if it has headers
    df = pd.read_csv(file_path, header=None, nrows=2)

    # Get the needed columns and corresponding dtypes
    col_names = ['type', 'seconds', 'orn', 'side', 'shares', 'price', 'shares_remaining',
                 'current bid', 'current ask', 'spread', 'ask depth', 'bid depth', 'depth']
    col_names = col_names[:df.columns.size]
    dtypes = {
        'seconds': np.float, 'orn': np.int, 'shares': np.int, 'price': np.float,
        'shares_remaining': np.int, 'current bid': np.float, 'current ask': np.float,
        'spread': np.float, 'ask depth': np.int, 'bid depth': np.int, 'depth': np.int
    }
    dtypes = {x: dtypes[x] for x in dtypes.keys() if x in col_names}

    # If the header is present the file, skip that line
    skip = 1 if df[0][0].lower() == 'type' else 0

    # Check two situations:
    # 1. Either file has zero lines, and is essentially an empty file, or
    # 2. In one of the files, the first line is the header and there are no transactions
    if df.shape[0] == 0 or (skip and df.shape[0] == 1):
        print('File contains no transactions, sorted by default')
        return True

    df = pd.read_csv(file_path, header=None, names=col_names, engine='c', float_precision='high',
                     dtype=dtypes, na_values=['None', ' None'], skiprows=skip,
                     converters={'type': str.strip, 'side': str.strip})

    seconds = df['seconds'].values
    sorted_series = np.sort(seconds)
    if not np.logical_and.reduce(np.equal(seconds, sorted_series)):
        print("Values don't appear to be sorted in {}".format(file_path))
        return False
    if verbose:
        print('Sorted: {}'.format(file_path.stem))
    return True


def check_folder(dir_path, verbose=False):
    directory = Path(dir_path)
    assert directory.is_dir(), "Invalid path"
    files = directory.glob('*.csv.gz')
    worker = partial(check_file_sort, verbose=verbose)

    with mp.Pool() as pool:
        file_result = pool.map(worker, files)
    if file_result:
        method_result = reduce(lambda x, y: x and y, file_result)
        return method_result
    else:
        print("No files were compared, so returning True")
        return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Check that an orderbook file is sorted')
    parser.add_argument('path', type=str, help='Path to verify (file or folder)')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()
    path = Path(args.path).expanduser().resolve()

    if path.is_dir():
        result = check_folder(path, args.verbose)
    else:
        result = check_file_sort(path, args.verbose)

    if not result:
        "Not sorted"
