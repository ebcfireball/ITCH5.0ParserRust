#! /usr/bin/env python3

"""
A script to rename files on the box.byu.edu service that weren't correctly named
"""

import subprocess

for d in ['grouped', 'processed']:
    out = subprocess.check_output(['rclone', 'ls', 'byu-box:ITCH_DATA/{}_data/2018'.format(d)],
                                  encoding='utf-8').split('\n')
    out = [x.split()[1] for x in out if x and d not in x.split()[1]]
    for x in out:
            name = x.split('.')[0]
            args = ['rclone',
                    'moveto',
                    'byu-box:ITCH_DATA/{}_data/2018/{}'.format(d, x),
                    'byu-box:ITCH_DATA/{}_data/2018/{}_{}.tar'.format(d, name, d)]
            output = subprocess.check_output(args, encoding='utf-8')
            print(args)
            print(output)

