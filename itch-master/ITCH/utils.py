# This contains various utility functions useful while working with the ITCH library

import glob
import os
from pathlib import Path
import re
import sys

from datetime import datetime
from itertools import groupby, count
from ITCH import locations


def warning(*objs):
    """This is to print to stderr for error messages."""
    print("WARNING: ", *objs, file=sys.stderr)


def to_datetime(date):
    """Converts a MMDDYY date to a datetime object"""
    return datetime(2000+int(date[4:]), int(date[:2]), int(date[2:4]))


@locations.binary_data
def raw_data_list():  # ? is the bash single character wildcard.
    """Returns a list of days with raw ITCH data"""
    return [file[1:7] for file in glob.glob('S??????-v41.txt.gz')]


@locations.grouped_data
def grouped_data_tickers(date):
    """
    This returns a list of tuples of all tickers on a given date, and the file size to allow for
    sorting by size
    (Determined by .csv files)
    Parameters
    ----------
    date: str
        MMDDYY date
    Returns
    -------
    tickers: list
        List of all tickers for that day.
    """
    year = '20' + date[4:]
    folder = os.path.join(year, date)

    try:
        os.chdir(folder)
    except OSError:
        warning("Directory for {} cannot be found: {}".format(date, folder))
        return None
    else:
        files = glob.glob('OrderGroups_*')
        grouped_results = []
        for file in files:
            ticker = file[19:-7]
            size = os.path.getsize(file)
            grouped_results.append((ticker, size))

    return grouped_results


@locations.binary_data
def get_days_from_month(month, year='2016', version='50'):
    """Determines which days are in the month and return as a set."""
    days = set()
    year = year[2:]
    for i in range(1, 32):
        date = '{}{:0>2}{}'.format(month, str(i), year)
        if os.path.isfile('20{}/S{}-v{}.txt.gz'.format(year, date, version)):
            days.add(date)
    return days


@locations.binary_data
def get_available_binary_dates(year='2016', version='50'):
    dates = set()
    if not year:
        years = glob.glob('*')
    else:
        years = [year]
    for y in years:
        for i in range(1, 13):
            dates.update(get_days_from_month('{:0>2}'.format(i), year=y, version=version))
    return dates


@locations.processed_data
def get_processed_dates(year=None):  # ? is the bash single character wildcard.
    """
    Return a set of dates of processed data
    :param year: Year to check, maybe none
    :return:
    """
    if not year:
        years = [x for x in glob.glob('20*') if Path(x).is_dir()]
    else:
        years = [year]
    pat = re.compile(r"^[0-9]{6}$")
    dates = set()
    for y in years:
        if Path(y).exists():
            os.chdir(y)
            dates.update({x for x in glob.glob('*') if pat.match(x)})
            os.chdir('../')
    return dates


# Source:
# codereview.stackexchange.com/questions/5196/grouping-consecutive-numbers-into-ranges-in-python-3-2
def get_range_string(num_list):
    def as_range(iterable):  # not sure how to do this part elegantly
        converted_list = list(iterable)
        if len(converted_list) > 1:
            return '{0}-{1}'.format(converted_list[0], converted_list[-1])
        else:
            return '{0}'.format(converted_list[0])

    if num_list and type(num_list[0]) is not int:
        num_list = [int(x) for x in num_list]
    return ','.join(as_range(g) for _, g in groupby(num_list, key=lambda n, c=count(): n-next(c)))
