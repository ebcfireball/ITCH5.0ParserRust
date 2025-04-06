"""
This program processes raw data for one day.  From that it creates 
a csv file containing the processed data.
"""

from ITCH.processing import grouped_order
from ITCH import locations

import os
import sys
import time

date = str(sys.argv[1])
size = int(sys.argv[2])
rank = int(sys.argv[3])

print('Starting raw processing of date: {}'.format(date))
print('# of nodes on job: {} - My rank is {}'.format(size, rank))
print("Start time: " + str(time.time()))
assert size > 0, "Invalid size provided"


@locations.grouped_data
def clean_out(date_in):
    """
    Cleans out the folder containing the info for that date if it exists
    """
    year = '20' + date_in[4:]
    folder = os.path.join(year, date_in)

    # Make the Directories we need
    os.makedirs(folder, exist_ok=True)


# Create the destination folder if necessary
clean_out(date)
print('Working on rank {}'.format(rank))

start = time.time()
day_obj = grouped_order.process_parallelday(date, size, rank, write_cache_max=1000)
elapsed_time = time.time() - start

print('Completed working on {}'.format(date))
m, s = divmod(elapsed_time, 60)
h, m = divmod(m, 60)
print('Elapsed time for rank {}: {}:{}:{:.3f} ({:.3f} seconds)'.format(
    rank, int(h), int(m), s, elapsed_time))
