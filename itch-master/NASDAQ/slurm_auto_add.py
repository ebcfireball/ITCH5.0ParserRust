import argparse
import os
import pwd
import re
import sys

from ITCH import utils
from ITCH.processing import slurm

"""
This script will check what jobs are currently running on Slurm or already exist as processed
data, and then check what raw, binary data is available, and then automatically add new
whole jobs based on what needs to be run, within the constraint of the maximum number of jobs
allowed on the FSL.
"""

FSL_MAX_NUM_JOBS = 5000


def get_username():
    return pwd.getpwuid(os.getuid()).pw_name


if __name__ == '__main__':
    def check_year(s, pat=re.compile(r"^20[0-9]{2}$")):
        if not pat.match(s):
            raise argparse.ArgumentTypeError('Year has invalid format, should be 20YY')
        return s

    parser = argparse.ArgumentParser(description='Auto add jobs to slurm')
    parser.add_argument('--size', type=int, default=256,
                        help='The number of nodes to use on the FSL, default is %(default)s')
    parser.add_argument('--year', type=check_year, help='The year to process (20YY)',
                        default=None)
    parser.add_argument('--version', choices=['50', '41'], help='ITCH version', default='50')
    parser.add_argument('--username', help='Manually specify a different FSL user to check')
    parser.add_argument('--debug', action='store_true',
                        help="Don't actually run, just print all commands to STDOUT")
    parser.add_argument('--show', action='store_true',
                        help="Show dates that will run and exit, a dry run if you will.")
    parser.add_argument('--exclude', action='append',
                        help='''Dates that you want to exclude (MMDDYY). This option can be 
                        repeated to exclude multiple dates.''')

    args = parser.parse_args()
    username = args.username if args.username else get_username()
    slurm_job_dates = slurm.get_slurm_job_dates(username)
    binary_dates = utils.get_available_binary_dates(year=args.year, version=args.version)
    processed_dates = utils.get_processed_dates(year=args.year)

    available_dates = binary_dates - slurm_job_dates.union(processed_dates)
    
    # Remove any dates that should be excluded
    if args.exclude:
        for x in args.exclude:
            available_dates.discard(x)
    
    sorted_dates = sorted(list(available_dates),
                          key=lambda x: (x[4:6], x[0:2], x[2:4]), reverse=True)
    
    if args.show:
        print(sorted_dates)
        sys.exit(0)

    for date in sorted_dates:
        cur_num = slurm.get_slurm_job_count(username)
        print("Current job count: {}".format(cur_num))

        # If we're going to go over the max number of jobs, break out
        if cur_num + args.size*2 > FSL_MAX_NUM_JOBS:
            print('Reached maximum number of jobs')
            print('Current amount: {} (MAX: {})'.format(cur_num, FSL_MAX_NUM_JOBS))
            break

        raw_ids = [slurm.raw_array(date, args.size, debug=args.debug)]
        slurm.orb_array(date, args.size, raw_ids, debug=args.debug)
        print("================== END {} ===================".format(date))
