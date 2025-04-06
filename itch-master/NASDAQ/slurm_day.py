import argparse
import re
import textwrap

from ITCH.processing.slurm import raw_array, orb_array

"""
This file combines the `slurm_day_raw` and `slurm_day_orderbooks` scripts into one. We first add
the raw jobs, and then instruct slurm to not allow any of the orderbook jobs to commence until
all of the raw jobs for that day have finished successfully.

Note that if any of the raw jobs fail, the orderbook jobs will still be pending and will need to
be cancelled manually.
"""


if __name__ == '__main__':
    def check_date_type(s, pat=re.compile(r"^[0-9]{6}$")):
        if not pat.match(s):
            raise argparse.ArgumentTypeError('Date has invalid format, should be MMDDYY')
        return s

    parser = argparse.ArgumentParser(description='Submit processing jobs on the supercomputer',
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('scope', choices=['raw', 'orderbook', 'both'],
                        help=textwrap.dedent('''\
                        Choose which part of the processing process to perform:
                        
                        "raw": Converts the binary file for the given date into the grouped 
                        order files.
                        
                        "orderbook": Takes the grouped order files from the "raw" step and
                        finishes processing them into the finished order book files with the 
                        spread, current ask, current bid, etc.
                        
                        "both": This first submits the raw jobs, and then adds the orderbook
                        jobs with the raw job IDs as dependencies, so they will only run 
                        after all raw jobs have sucessfully finished'''))
    parser.add_argument('date', type=check_date_type, help='The date to process (MMDDYY)')
    parser.add_argument('--size', type=int, default=256,
                        help='The number of nodes to use on the FSL, default is %(default)s')
    parser.add_argument('--debug', action='store_true',
                        help="Don't submit jobs, just print the args to STDOUT")

    args = parser.parse_args()
    if args.scope == 'raw':
        raw_array(args.date, args.size, debug=args.debug)
    elif args.scope == 'orderbook':
        orb_array(args.date, args.size, debug=args.debug)
    elif args.scope == 'both':
        raw_ids = [raw_array(args.date, args.size, debug=args.debug)]
        orb_array(args.date, args.size, raw_ids, debug=args.debug)
        print("================== END {} ===================".format(args.date))
