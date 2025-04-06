# This file will have decorators that can be used to move to different directories.  If properly
# implemented, changing this file to match the local filesystem will allow the user to use their
# own file structure.

from ITCH import ROOT_DIR
from functools import partial, wraps
from pathlib import Path

import configparser
import os


# If base_path not explicitly defined, check environment vars
if 'ITCH_BASE_PATH' in os.environ:
    BASE_PATH = os.environ['ITCH_BASE_PATH']
elif Path(ROOT_DIR / 'itch.conf').exists():
    config = configparser.ConfigParser()
    config_path = Path(ROOT_DIR / 'itch.conf')
    config.read(config_path)
    if not config['Default']['ITCH_BASE_PATH']:
        raise ValueError('"ITCH_BASE_PATH" not defined in {}'.format(config_path.resolve()))
    else:
        BASE_PATH = config['Default']['ITCH_BASE_PATH']
else:
    raise ValueError('base_path not provided and "ITCH_BASE_PATH" not in env')

BASE = Path(BASE_PATH).expanduser().resolve()
assert BASE.is_dir(), '"ITCH_BASE_PATH" provided is not a valid directory'


# ======== Base Function =========
def base_wrapper(function, path_name):
    """
    This is the base wrapper, which we then use as the foundation for a partial function
    to create the rest of the wrappers

    Parameters
    ----------
    function: python function
        This will be the function run in the directory specified above.
    path_name: str or pathlib Path object
        This is the directory that the wrapper will chdir to, perform the function that is the
        previous parameter, and then will return to the original directory

    Returns
    ------
    decorated: python function
        This returns a wrapper (`decorated`) which will cause the provided function in the
         arguments to be executed in the directory specified by `path_name`, and then return.
    """
    @wraps(function)
    def decorated(*args, **kwargs):
        cwd = Path.cwd()
        os.chdir(BASE / path_name)
        stuff = function(*args, **kwargs)
        os.chdir(cwd)
        return stuff
    decorated.__doc__ = function.__doc__
    return decorated


"""
Since all of the following functions have the following parameter and return values, we'll 
summarize here. The remainder of the comments are descriptions of the anticipated usaged
for each directory location.

Parameters
----------
function: python function
    This will be the function run in the directory specified by `path_name`.

Returns
------
function: python function
    This function will now run in the directory specified by `path_name`.
"""

# ======== Main wrappers for handling data locations ========

"""
This is to be used as a decorator to run the given function while in  the binary_data directory.
The binary_data directory contains the raw, binary ITCH data from NASDAQ.
"""
binary_data = partial(base_wrapper, path_name='binary_data')

"""
This is to be used as a decorator to run the given function while in the grouped_data directory.
The grouped_data directory contains csv files that were generated using the program
process_raw.py or its derivatives. These files contain the parsed and grouped messages (grouped
by order number) for the corresponding raw files.
"""
grouped_data = partial(base_wrapper, path_name='grouped_data')

"""
This is to be used as a decorator to run the given function while in the processed_data
directory. The processed_data directory contains csv files that were generated using the program
process_orderbooks or its derivatives, which takes grouped data and forms the final edition of
the order books, including the spread and best ask/bid prices. 
"""
processed_data = partial(base_wrapper, path_name='processed_data')

# ======== Wrappers for Test location ========
"""
This is to be used as a decorator to run the given function while in
the Results directory.  The Results directory contains subdirectories
for various forms of processed data. 
"""
results = partial(base_wrapper, path_name='Test/Results')

"""
This is to be used as a decorator to run the given function while in
the scripts directory.  The scripts directory contains scripts that use
the ITCHy library for various puproses
"""
scripts = partial(base_wrapper, path_name='scripts')

"""
This is to be used as a decorator to run the given function while in
the saved_stderr directory.  The saved_stderr directory contains files
with everything printed to stderr when the data was processed.  The files
should be of the form MMDDYY.err
"""
saved_stderr = partial(base_wrapper, path_name='Scripts/Output')

"""
This is to be used as a decorator to run the given function while in
the saved_stdout directory.  The saved_stdout directory contains files
with everything printed to stdout when the data was processed.  The files
should be of the form MMDDYY.out
"""
saved_stdout = partial(base_wrapper, path_name='Scripts/Output')

"""
This is to be used as a decorator to run the given function while in
the standard_data directory.  The standard_data directory contains
subdirectories for all the days of standard processed data.
"""
standard_data = partial(base_wrapper, path_name='Test/Results/StandardData')

"""
This is to be used as a decorator to run the given function while in
the summary_stats directory.  The summary_stats directory contains various
summary statistics generated from the data.
"""
summary_stats = partial(base_wrapper, path_name='Test/Results/Summary')

"""
This is to be used as a decorator to run the given function while in
the summary_stats_spread directory.  The summary_stats directory
contains various summary statistics generated from the data.  It has its
own directory because of the large number of files cloggs up the regular
summary_stats directory.
"""
summary_stats_spread = partial(base_wrapper, path_name='Test/Results/Summary/Spread')

"""
This is to be used as a decorator to run the given function while in
the graphs directory.  The graphs directory contains graphs generated
from the data.
"""

# Decorator template
"""def ***(sef, function):
   
    This is to be used as a decorator to run the given function while in 
    the *** directory.  The *** directory contains ***
    After the function is run, the program returns to its initial directory.

    Parameters
    ----------
    function: python function
        This will be the function run in the directory specified above.

    Returns
    ------
    function: python function
        This function will now run in the directory specified above.
   
    @wraps(sef, function)
    def decorated(*args, **kwargs):
        cwd = os.getcwd()
        os.chdir(self.base / '***')
        stuff = function(*args, **kwargs)
        os.chdir(cwd)
        return stuff
    decorated.__doc__ = function.__doc__
    return decorated
"""
