"""
A helper script to cancel SLURM jobs by Job ID based on the date being processed
"""

import argparse
import os
import pwd
import re
import subprocess

from ITCH.processing import slurm


def get_username():
    return pwd.getpwuid(os.getuid()).pw_name


if __name__ == '__main__':
    def check_date_type(s, pat=re.compile(r"^[0-9]{6}$")):
        if not pat.match(s):
            raise argparse.ArgumentTypeError('Date has invalid format, should be MMDDYY')
        return s

    parser = argparse.ArgumentParser(description='Cancel processing jobs on FSL by date')
    parser.add_argument('type', choices=['raw', 'orderbook', 'both'],
                        help='Cancel only raw jobs, orderbook jobs, or both types')
    parser.add_argument('date', type=check_date_type, help='The date to process (MMDDYY)')
    parser.add_argument('--username', help='Specify FSL user of the jobs')
    parser.add_argument('--debug', action='store_true',
                        help="Don't actually cancel, just print command to STDOUT")

    args = parser.parse_args()
    username = args.username if args.username else get_username()
    job_ids = slurm.get_slurm_job_ids(args.date, args.type, username)

    if not job_ids:
        print('No jobs available to cancel')
    else:
        cancel_args = ['scancel', '-u', username]
        cancel_args.extend([str(x) for x in job_ids])
        if args.debug:
            print(cancel_args)
        else:
            out = subprocess.check_output(cancel_args, encoding='utf-8')
            print(out)
