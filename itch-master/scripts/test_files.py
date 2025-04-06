#!/usr/bin/env python3

"""
Compare two versions of the same orderbook for a given day and ticker. Useful when trying to verify
that changes to the code haven't altered expected behavior.

Note that both paths must be either files or folders, no mixing.
If the files have a .csv.gz extensions, we'll attempt to open and compare them line by line,
using the numerical values so minor formatting issues like one having a column header line or
trailing zeros aren't a factor.

Otherwise, for files a file hash will be calculated and compared.

For folders, we'll list the files in each folder, and attempt to compare files of the same
name as indicate above. If one folder is missing a file, a message will be printed indicating
such, and then we'll continue with the comparisons.
"""
from functools import reduce, partial

import argparse
import hashlib
import numpy as np
import os
import pandas as pd
import pathos.multiprocessing as mp
import sys
import time

LOG_DIR = 'log_out'


def get_file_hash(file, buf_size=65536, hash_alg=hashlib.sha1):
    # buf_size is arbitrary, change as needed. Let's read stuff in 64kb chunks!
    hash_buffer = hash_alg()
    with open(file, 'rb') as f:
        while True:
            data = f.read(buf_size)
            if not data:
                break
            hash_buffer.update(data)
    return hash_buffer.hexdigest()


def compare_csv_files(file1, file2, max_diff=None):
    def strip(text):
        return text.strip()

    def get_seconds_skip_count(lesser, greater):
        count = 0
        chunk_greater = next(greater).iterrows()
        index_great, line_greater = next(chunk_greater)

        for chunk_lesser in lesser:
            chunk_lesser_iter = chunk_lesser.iterrows()
            for index_lesser, line_lesser in chunk_lesser_iter:
                if line_lesser['seconds'] < line_greater['seconds']:
                    count += 1
                else:
                    return count
                if count % 1000 == 0:
                    print('Count: ', count)

        # Never found a matching line
        return -1

    # Get the first line of each file to figure out the headers
    df1 = pd.read_csv(file1, header=None, nrows=2)
    df2 = pd.read_csv(file2, header=None, nrows=2)

    if df1.columns.size != df2.columns.size:
        print('Files do not have the same number of columns: {} vs {}'.format(
            df1.columns.size, df2.columns.size))
        return False

    col_names = ['type', 'seconds', 'orn', 'side', 'shares', 'price', 'shares_remaining']
    # If comparing the finished order book, add the new column headers
    if df1.columns.size == 13:
        col_names.extend(['current bid', 'current ask', 'spread',
                          'ask depth', 'bid depth', 'depth'])

    # If the header is present the file, skip that line
    skip1 = 1 if df1[0][0].lower() == 'type' else 0
    skip2 = 1 if df2[0][0].lower() == 'type' else 0
    chunk_size = 2500

    # Check two situations:
    # 1. Either file has zero lines, and is essentially an empty file, or
    # 2. In one of the files, the first line is the header and there are no transactions
    if df1.shape[0] == 0 or df2.shape[0] == 0 or \
            (skip1 and df1.shape[0] == 1) or (skip2 and df2.shape[0] == 1):
        if not (df1.shape[0] - skip1) and not (df2.shape[0] - skip2):
            print('Boths files are empty')
        elif df1.shape[0] - skip1 == 0:
            print('File1 (left) contains no transactions')
        else:
            print('File2 (right) contains no transactions')
        return (df1.shape[0] - skip1) == (df2.shape[0] - skip2)

    try:
        left_time = float(df1[1][1])
        right_time = float(df2[1][1])
    except KeyError as e:
        msg = ('Error trying to read file\n' +
               'file1: {}\n'.format(file1) +
               'file2: {}'.format(file2))
        raise Exception(msg, e)

    dtypes = {
        'seconds': np.float64, 'orn': np.int64, 'shares': np.int64, 'price': np.float64,
        'shares_remaining': np.int64, 'current bid': np.float64, 'current ask': np.float64,
        'spread': np.float64, 'ask depth': np.int64, 'bid depth': np.int64, 'depth': np.int64
    }
    dtypes = {x: dtypes[x] for x in dtypes.keys() if x in col_names}
    # Make a partial function to have one place to change common parameters
    read_csv = partial(pd.read_csv, header=None, names=col_names, engine='c',
                       float_precision='high', chunksize=chunk_size, dtype=dtypes,
                       converters={'type': strip, 'side': strip}, na_values=['None', ' None'])

    if abs(left_time - right_time) > 10:
        print('Files appear to have different start times')
        print('Attempting to align them')

        df1 = read_csv(file1, skiprows=skip1)
        df2 = read_csv(file2, skiprows=skip2)

        if left_time < right_time:
            skip_count = get_seconds_skip_count(df1, df2)
        else:
            skip_count = get_seconds_skip_count(df2, df1)

        if skip_count == -1:
            print("Didn't find any lines to match")
            return False

        if left_time < right_time:
            skip1 += skip_count
        else:
            skip2 += skip_count

    df1 = read_csv(file1, skiprows=skip1, float_precision='high')
    df2 = read_csv(file2, skiprows=skip2, float_precision='high')

    chunk_counter = 0
    diff_line_counter = 0
    file_different_columns = set()
    files_equivalent = True
    for chunk1, chunk2 in zip(df1, df2):
        if diff_line_counter > 1025:
            print('Found over 1000 different lines, closing this file')
            break

        # Since NaN values are not comparable, we'll fillna values in the following columns as
        # -1 so we can confirm they have the same values
        if 'current bid' in chunk1.columns:
            values = {'current bid': -1, 'current ask': -1, 'spread': -1}
            chunk1.fillna(value=values, inplace=True)
            chunk2.fillna(value=values, inplace=True)

        # If different, print out the lines that are different
        if not chunk1.equals(chunk2):
            # Returns a series (dateframe of one column) of boolean values of whether or not there
            # is a difference in a given row between chunk1 and chunk2
            diff_rows = ~chunk1.eq(chunk2).apply(
                lambda x: np.logical_and.reduce(x), axis=1, raw=True)

            diff1 = chunk1.loc[diff_rows]
            diff2 = chunk2.loc[diff_rows]

            if max_diff:
                chunk_abs_diff = (diff1.drop(['type', 'side'], axis=1) -
                                  diff2.drop(['type', 'side'], axis=1)).abs().max(axis=1)
                chunk_str_eq = diff1[['type', 'side']].eq(
                    diff2[['type', 'side']]).aggregate(all, axis=1)
                chunk_within_max_diff = (chunk_abs_diff < max_diff) & chunk_str_eq
                diff1 = diff1.loc[~chunk_within_max_diff]
                diff2 = diff2.loc[~chunk_within_max_diff]

            diff1iter, diff2iter = diff1.iterrows(), diff2.iterrows()

            for (index1, line1), (index2, line2) in zip(diff1iter, diff2iter):
                files_equivalent = False
                diff_line_counter += 1
                diff_cols = line1.loc[~line1.eq(line2)].index.values
                file_different_columns.update(diff_cols)
                diff_cols = ', '.join(diff_cols)
                line1csv = line1.to_frame().T.to_csv(
                    header=None, index=None, float_format='{:.4f}', na_rep='None').replace(
                    ',', ',\t').replace(',\t-1.0,', ',\tNone,')
                line2csv = line2.to_frame().T.to_csv(
                    header=None, index=None, float_format='{:.4f}', na_rep='None').replace(
                    ',', ',\t').replace(',\t-1.0,', ',\tNone,')

                print('Line {} differences in columns: {}'.format(index1 + 1, diff_cols))
                print(''.join(['- ', line1csv, '+ ', line2csv]))

            # We've hopefully finished with all of the lines in each chunk, so both of the
            # following should raise Exceptions if there are any lines left
            try:
                _ = next(diff1iter)
                print('Chunk {} in File 1 has lines remaining'.format(chunk_counter+1))
                files_equivalent = False
            except StopIteration:
                # This is expected behavior
                pass

            try:
                _ = next(diff2iter)
                print('Chunk {} in File 2 has lines remaining'.format(chunk_counter+1))
                files_equivalent = False
            except StopIteration:
                # This is expected behavior
                pass

        chunk_counter += 1

    if not files_equivalent:
        print('Differences found in columns: ', ', '.join(
            [x for x in col_names if x in file_different_columns]))
    # We've hopefully finished with all of the chunks, so both of the following should raise
    # Exceptions if there are any chunks left
    try:
        _ = next(df1)
        print('File 1 has lines remaining')
        files_equivalent = False
    except StopIteration:
        # This is expected behavior
        pass

    try:
        _ = next(df2)
        print('File 2 has lines remaining')
        files_equivalent = False
    except StopIteration:
        # This is expected behavior
        pass

    # We've read through all of the chunks and compared them. We've already returned if the number
    # of lines aren't
    return files_equivalent


def compare_files(file1, file2, max_diff=None):
    # If both files appear to be compressed CSVs, then compare line by line
    if ('csv' in os.path.basename(file1).split(sep='.', maxsplit=1)[1] and
            'csv' in os.path.basename(file2).split(sep='.', maxsplit=1)[1]):
        return compare_csv_files(file1, file2, max_diff=max_diff)
    else:
        hash1 = get_file_hash(file1)
        hash2 = get_file_hash(file2)
        return hash1 == hash2


def compare_folders(dir1, dir2, verbose=True, max_diff=None):
    job_hash = hashlib.sha1(str(int(time.time())).encode('UTF-8')).hexdigest()[:10]
    current_log_dir = os.path.join(LOG_DIR, job_hash)
    os.makedirs(current_log_dir, exist_ok=True)
    files1 = {x.name: x for x in os.scandir(dir1) if x.is_file()}
    files2 = {x.name: x for x in os.scandir(dir2) if x.is_file()}

    # Find if there are files in one directory, but not the other
    file_set1, file_set2 = set(files1.keys()), set(files2.keys())
    if file_set1 != file_set2:
        print('Some files not found in both directories:')
        file_set1_orphans = file_set1 - file_set2
        if file_set1_orphans:
            print('# unique of files in {}: {}'.format(dir1, len(file_set1_orphans)))
        file_set2_orphans = file_set2 - file_set1
        if file_set2_orphans:
            print('# unique of files in {}: {}'.format(dir2, len(file_set2_orphans)))

    # Find the intersection of files in both directories by name, and then compare
    files = [(files1[x].path, files2[x].path) for x in (file_set1 & file_set2)]
    print('# of files being compared: {}'.format(len(files)))

    def worker(file_tuple):
        file_name = os.path.basename(file_tuple[0])
        log_path = os.path.join(current_log_dir, str(file_name) + ".out")
        sys.stdout = open(log_path, "a")
        if verbose:
            print('Checking {}'.format(file_name))
        wrk_result = compare_files(file_tuple[0], file_tuple[1], max_diff=max_diff)
        sys.stdout = sys.__stdout__
        if verbose:
            if not wrk_result:
                print('File is different: {}'.format(file_name))
            else:
                print('File is the same: {}'.format(file_name))
                os.remove(log_path)
        return wrk_result

    with mp.Pool(processes=os.cpu_count()) as pool:
        file_result = pool.map(worker, files)
    if file_result:
        method_result = reduce(lambda x, y: x and y, file_result)
        return method_result
    else:
        print("No files were compared, so returning True")
        return True


def get_path(path_str):
    return os.path.realpath(os.path.expanduser(path_str))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Test two processed ITCH files or directories')
    parser.add_argument('--max-diff', dest='max_diff', action='store', type=float,
                        help='Max acceptable difference between numeric values, '
                             'otherwise values must be exactly the same')
    parser.add_argument('left_path', type=str, help='First path to verify')
    parser.add_argument('right_path', type=str, help='Second path to verify')
    args = parser.parse_args()

    left_path, right_path = get_path(args.left_path), get_path(args.right_path)

    print(' Left Path: ', left_path)
    print('Right Path: ', right_path)

    # Check invalid scenarios
    if not os.path.exists(left_path):
        print('Left Path is not a valid path')
        sys.exit(0)

    if not os.path.exists(right_path):
        print('Right Path is not a valid path')
        sys.exit(0)

    if not os.path.isdir(left_path) == os.path.isdir(right_path):
        print('Cannot mix a folder with a file')
        sys.exit(0)

    # Everything is valid, now get to work
    if os.path.isdir(left_path):
        print('# of processes in used to compare folders: ', os.cpu_count())
        result = compare_folders(left_path, right_path, max_diff=args.max_diff)
        if result:
            print('The compared files within the folders appear to be ' +
                  'the same based on hash or CSV contents')
        else:
            print('One or more of the compared files within these directories are different')
            print('Check the {} directory for detailed output'.format(LOG_DIR))
    else:
        result = compare_files(left_path, right_path, max_diff=args.max_diff)
        if result:
            print('Files appear to be the same based on hash or CSV contents')
        else:
            print('Files are different')
