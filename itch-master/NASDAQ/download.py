#!/usr/bin/env python3

# This is not locations-compatible.  It was written by Roy, and appears to be bug-free, thus I've
# not ever had a need to change it. The most useful function in it is try_download.  It can be used
# to # download a specific date.

from functools import partial
from tqdm import tqdm
from ITCH import locations

import argparse
import ftplib
import netrc
import os
import re

HOST = 'itchdata.nasdaq.com'
credentials = netrc.netrc()
# This requires a netrc file with the username and password for the ITCH server.
user, account, password = credentials.authenticators(HOST)

versions = {
    '50': {
        'locations': ['itchfiles_v5/'],
        'file_string': 'v50'
    },
    '41': {
        'locations': ['itchfilesarchive_v41/', 'itchfiles_v41/'],
        'file_string': 'v41'
    }
}


def download_block(block, temp_file=None, pbar=None):
    temp_file.write(block)
    if pbar:
        pbar.update(len(block))


def try_download(date: str, dest_path: str = None, ver_string: str = '50',
                 display_bar: bool = False, debug: bool = False) -> bool:
    """
    Attempt to download NASDAQ ITCH data for the given data and version

    :param date: String in the form of 'mmddyy' to download
    :param dest_path: Path to download the data. If none given, attempt to use default: binary_data
    :param ver_string: Which version of NASDAQ ITCH data to download
    :param display_bar: Display a download bar if true
    :param debug: Don't actually download, just print the file name if found
    :return: Boolean value of successful download
    """

    if not dest_path:
        year = '20' + date[4:]
        dest_path = locations.BASE / 'binary_data' / year

    assert ver_string in versions.keys(), 'Invalid version provided'
    assert os.path.isdir(dest_path), 'Provided path is not a valid directory:\n{}'.format(dest_path)
    base_path = os.path.expanduser(dest_path)
    version = versions[ver_string]

    file_name = 'S{}-{}.txt.gz'.format(date, version['file_string'])
    file_path = os.path.join(base_path, file_name)
    temp = file_path + '.tmp'  # Use this until the file is fully downloaded
    print('Temp path: ', temp)
    try:
        os.remove(temp)
    except OSError:
        pass
    print('Checking for {}'.format(file_name))
    if os.path.isfile(file_path):
        print('{} already exists! Skipping'.format(file_name))
        return True
    else:
        print('{} does not found, attempting to download it from {}'.format(file_name, HOST))
        try:
            ftp = ftplib.FTP(HOST)
        except TimeoutError:
            # Try again
            ftp = ftplib.FTP(HOST)
        ftp.login(user=user, passwd=password)
        for loc in version['locations']:
            print('Attempting to locate file in {}'.format(loc))
            files = ftp.nlst(loc)

            if file_name in files:
                if debug:
                    print('Found {} on server, would download to: {}'.format(file_name, file_path))
                    return True

                print('Found {} on server, downloading'.format(file_name))
                size = ftp.size('{}/{}'.format(loc, file_name))

                if display_bar:
                    pbar = tqdm(total=size, unit='bytes')
                else:
                    pbar = None

                with open(temp, 'wb') as temp_file:
                    callback = partial(download_block, temp_file=temp_file, pbar=pbar)
                    try:
                        ftp.retrbinary('RETR {}/{}'.format(loc, file_name), callback)
                    except TimeoutError:
                        # Try again
                        ftp.retrbinary('RETR {}/{}'.format(loc, file_name), callback)
                pbar.close()
                os.rename(temp, file_path)
                print('Succesfully downloaded {}'.format(file_name))
                return True
            else:
                print('{} not found in {}'.format(file_name, loc))

        # If we reach this point, the file was not found.
        print('{} not found on server'.format(file_name))
        return False


if __name__ == '__main__':
    def check_date(s, pat=re.compile(r"^[0-9]{6}$")):
        if not pat.match(s):
            raise argparse.ArgumentTypeError('Date has invalid format, should be MMDDYY')
        return s

    def check_month(s, pat=re.compile(r"^[0-1][0-9]-20[0-9]{2}$")):
        if not pat.match(s):
            raise argparse.ArgumentTypeError('Date has invalid format, should be MM-20YY')
        return s


    parser = argparse.ArgumentParser(description='Download raw binary files')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--date', action='store', type=check_date,
                       help='Try to download a specific date (MMDDYY)')
    group.add_argument('--month', action='store', type=check_month,
                       help='Try to download a specific month (MM-20YY)')
    parser.add_argument('--dest', action='store', help='Download to this directory')
    parser.add_argument('--bar', action='store_true', help='Show a download bar')
    parser.add_argument('--version', action='store', default='50',
                        help='ITCH version, default: %(default)s')
    parser.add_argument('--debug', action='store_true',
                        help="Don't actually download, just print the filenames to STDOUT")
    args = parser.parse_args()

    if args.date:
        try_download(args.date, args.dest, args.version, args.bar, args.debug)

    if args.month:
        mm, yyyy = args.month.split('-')
        for day in range(1, 32):
            date = '{}{:0>2}{}'.format(mm, day, yyyy[2:])
            try_download(date, args.dest, args.version, args.bar, args.debug)

    # Example for ITCH version 4.1:
    # try_download(date_in, path_in, '41')
