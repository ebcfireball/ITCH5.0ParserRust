"""
Helper script to get the number of jobs running on the FSL at the moment
"""

from ITCH.processing import slurm

import os
import pwd


def get_username():
    return pwd.getpwuid(os.getuid()).pw_name


username = get_username()
print(slurm.get_slurm_job_count(username))
