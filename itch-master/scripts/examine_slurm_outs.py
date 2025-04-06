#!/usr/bin/env python3

"""
This script is to compare the run times for process_raw on the FSL, using the slurm out files.
Folder1 should be the path of a folder containing the .out files for one slurm process_raw run,
while Folder2 is another (or the same, but the differences will all be zeros, of course.
"""

import glob
import numpy as np
import os
import sys

usage = "Usage: {} folder1 folder2".format(__name__)


def get_path(path_str):
    return os.path.realpath(os.path.expanduser(path_str))


def process_file(f):
    with open(f) as f_in:
        lines = f_in.readlines()
        rank = int(lines[3].split()[-1])
        seconds = float(lines[-1].split()[-2][1:])
        return rank, seconds


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(usage)
        sys.exit(0)

    path1, path2 = get_path(sys.argv[1]), get_path(sys.argv[2])
    print('Path 1: ', path1)
    print('Path 2: ', path2)
    if not os.path.exists(path1):
        print('Path 1 is not a valid path')
        sys.exit(0)

    if not os.path.exists(path1):
        print('Path 2 is not a valid path')
        sys.exit(0)

    if not os.path.isdir(path1) == os.path.isdir(path2):
        print('Cannot mix a folder with a file')
        sys.exit(0)

    files_left = glob.glob(os.path.join(path1, '*.out'))
    files_right = glob.glob(os.path.join(path2, '*.out'))

    left_perf = {}
    right_perf = {}

    for file in files_left:
        r, s = process_file(file)
        left_perf[r] = s

    for file in files_right:
        r, s = process_file(file)
        right_perf[r] = s

    left_values = [left_perf[x] for x in sorted(left_perf.keys())]
    right_values = [right_perf[x] for x in sorted(right_perf.keys())]

    left_sum, right_sum = np.sum(left_values), np.sum(right_values)
    left_avg, right_avg = np.average(left_values), np.average(right_values)
    left_dev, right_dev = np.std(left_values), np.std(right_values)
    left_max, right_max = np.max(left_values), np.max(right_values)

    diff = np.subtract(left_values, right_values)
    diff_mean, diff_std = np.average(diff), np.std(diff)

    print('==================================================================')
    print(' Left: {}'.format(path1))
    print('Right: {}'.format(path2))
    print('==================================================================')
    print("{:>15s}\t{:>10s}\t\t\t{:>10s}\t\t\t{:>10s}".format('', 'Left', 'Right', 'Difference'))
    print("{:>15s}\t{:>10}\t\t\t{:>10}\t\t\t{:>10}".format(
        '# of procs:', len(left_values), len(right_values), len(left_values) - len(right_values)))
    print("{:>15s}\t{:>10.2f}\t\t\t{:>10.2f}\t\t\t{:>10.2f}".format(
        'Total (s):', left_sum, right_sum, left_sum - right_sum))
    print("{:>15s}\t{:>10.2f}\t\t\t{:>10.2f}\t\t\t{:>10.2f}".format(
        'Avg. (s):', left_avg, right_avg, left_avg - right_avg))
    print("{:>15s}\t{:>10.2f}\t\t\t{:>10.2f}\t\t\t{:>10.2f}".format(
        'Std. Dev. (s):', left_dev, right_dev, left_dev - right_dev))
    print("{:>15s}\t{:>10.2f}\t\t\t{:>10.2f}\t\t\t{:>10.2f}".format(
        'Max (s):', left_max, right_max, left_max - right_max))
    print('==================================================================')
    print("Diff. Avg: {:.3f}\t\t\tDiff. Std. Dev: {:.3f}".format(diff_mean, diff_std))

