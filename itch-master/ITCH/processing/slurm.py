import hashlib
import pandas as pd
import subprocess
import time
import warnings

from ITCH.utils import get_range_string


def raw_day(raw_date, raw_size, raw_dependencies=None, debug=False):
    if raw_dependencies:
        dependencies = '--dependency=afterok:{}'.format(
            ':'.join([str(x) for x in raw_dependencies]))
    else:
        dependencies = None
    raw_job_hash = hashlib.sha1(str(int(time.time())).encode('UTF-8')).hexdigest()[:8]
    raw_job_ids = []
    print("Raw Day Start: {}".format(raw_date))
    for RANK in range(raw_size):
        raw_name = '%j-{}-{}-{:0>3}-raw'.format(raw_job_hash, raw_date, RANK)
        raw_args = ['sbatch', '--output=slurm_files/{}.out'.format(raw_name),
                    '--error=slurm_files/{}.err'.format(raw_name),
                    '--job-name=raw-{}-{:0>3}'.format(raw_date, RANK),
                    'process_raw.sbatch', raw_date, str(raw_size), str(RANK)]

        if dependencies:
            raw_args.insert(1, dependencies)

        if debug:
            print(' '.join(raw_args))
            raw_job_ids.append('{}'.format(RANK))
        else:
            raw_output = subprocess.check_output(raw_args, encoding='utf-8')
            raw_job_ids.append(int(raw_output.strip().split()[-1]))

        if (RANK + 1) % 10 == 0:
            print("Submitted {}/{} jobs".format(RANK+1, raw_size))

    print("Raw Day Complete: {}".format(raw_date))
    print("Job IDs: {}".format(get_range_string(raw_job_ids)))
    return raw_job_ids


def orderbook_day(orb_date, orb_size, ob_dependencies=None, debug=False):
    if ob_dependencies:
        dependencies = '--dependency=afterok:{}'.format(
            ':'.join([str(x) for x in ob_dependencies]))
    else:
        dependencies = None
    orb_job_hash = hashlib.sha1(str(int(time.time())).encode('UTF-8')).hexdigest()[:8]
    orb_job_ids = []
    print("Orderbook Day Start: {}".format(orb_date))
    for RANK in range(orb_size):
        orb_name = '%j-{}-{}-{:0>3}-orb'.format(orb_job_hash, orb_date, RANK)

        orb_args = ['sbatch', '--output=slurm_files/{}.out'.format(orb_name),
                    '--error=slurm_files/{}.err'.format(orb_name),
                    '--job-name=orb-{}-{:0>3}'.format(orb_date, RANK),
                    'process_orderbooks.sbatch', orb_date, str(orb_size), str(RANK)]

        if dependencies:
            orb_args.insert(1, dependencies)

        if debug:
            print(' '.join(orb_args))
            orb_job_ids.append('{}'.format(RANK))
        else:
            orb_output = subprocess.check_output(orb_args, encoding='utf-8')
            orb_job_ids.append(int(orb_output.strip().split()[-1]))

        if (RANK + 1) % 10 == 0:
            print("Submitted {}/{} jobs".format(RANK + 1, orb_size))

    print("Orderbook Day Complete: {}".format(orb_date))
    print("Job IDs: {}".format(get_range_string(orb_job_ids)))
    return orb_job_ids


def raw_array(raw_date, raw_size, raw_dependencies=None, debug=False):
    if raw_dependencies:
        dependencies = '--dependency=afterok:{}'.format(
            ':'.join([str(x) for x in raw_dependencies]))
    else:
        dependencies = None
    print("Raw Day Start: {}".format(raw_date))

    raw_name = '%A-{}-%a-raw'.format(raw_date)
    raw_args = ['sbatch',
                '--array=0-{}'.format(raw_size - 1),
                '--output=slurm_files/{}.out'.format(raw_name),
                '--error=slurm_files/{}.err'.format(raw_name),
                '--job-name=raw-{}'.format(raw_date),
                'slurm_array_raw.sbatch',
                raw_date]

    if dependencies:
        raw_args.insert(1, dependencies)

    if debug:
        print(' '.join(raw_args))
        raw_job_id = 0
    else:
        raw_output = subprocess.check_output(raw_args, encoding='utf-8')
        raw_job_id = int(raw_output.strip().split()[-1])

    print("Raw Day Complete: {}".format(raw_date))
    print("Job IDs: {}".format(raw_job_id))
    return raw_job_id


def orb_array(orb_date, orb_size, ob_dependencies=None, debug=False):
    if ob_dependencies:
        dependencies = '--dependency=afterok:{}'.format(
            ':'.join([str(x) for x in ob_dependencies]))
    else:
        dependencies = None

    print("Orderbook Day Start: {}".format(orb_date))

    orb_name = '%A-{}-%a-orb'.format(orb_date)

    orb_args = ['sbatch',
                '--array=0-{}'.format(orb_size - 1),
                '--output=slurm_files/{}.out'.format(orb_name),
                '--error=slurm_files/{}.err'.format(orb_name),
                '--job-name=orb-{}'.format(orb_date),
                'slurm_array_orb.sbatch',
                orb_date]

    if dependencies:
        orb_args.insert(1, dependencies)

    if debug:
        print(' '.join(orb_args))
        orb_job_id = 0
    else:
        orb_output = subprocess.check_output(orb_args, encoding='utf-8')
        orb_job_id = int(orb_output.strip().split()[-1])

    print("Orderbook Day Complete: {}".format(orb_date))
    print("Job ID: {}".format(orb_job_id))
    return orb_job_id


def get_slurm_job_ids(date, job_type, username):
    """
    Get the slurm job IDs for currently queued jobs, filtered by job_type and username
    :param date: Date to match with (MMDDYY) or None to match all dates
    :param job_type: 'raw', 'orderbook', or 'both'
    :param username: FSL username
    :return: list of all job IDs that matched
    """
    id_args = ['squeue', '-u', username, '--format="%.18i %.16j"']
    id_output = subprocess.check_output(id_args, encoding='utf-8').splitlines()
    id_output = [x.replace('"', '').strip().split() for x in id_output]
    df = pd.DataFrame(id_output[1:], columns=id_output[0])

    if not date:
        date = '[0-9]{6}'
    assert job_type in ['raw', 'orderbook', 'both'], "Job type not as expected"

    if job_type == 'raw':
        reg_string = '^raw-{}'.format(date)
    elif job_type == 'orderbook':
        reg_string = '^orb-{}'.format(date)
    else:  # job_type == 'both'
        warnings.filterwarnings("ignore", 'This pattern has match groups')
        reg_string = '^(raw|orb)-{}'.format(date)

    return list(df.JOBID.loc[df.NAME.str.contains(reg_string)].values)


def get_slurm_job_count(username):
    """
    Get a count of how many jobs are on the FSL queue. Note that we skip the first line, since it
    is the squeue header.

    :param username:
    :return: int
    """

    id_args = ['squeue', '-u', username, '--format="%.18i %.16j"']
    id_output = subprocess.check_output(id_args, encoding='utf-8').splitlines()
    id_output = [x.replace('"', '').strip().split() for x in id_output[1:]]

    job_count = len([x for x in id_output if '[' not in x[0]])
    arrays = [x for x in id_output if '[' in x[0]]
    for arr in arrays:
        # Each job id here should look like [0-9]+_\[[0-9]+\-[0-9]+\], e.g. 28744805_[0-255]
        vals = arr[0].split('_')[1][1:-1].split('-')
        high, low = int(vals[1]), int(vals[0])
        job_count += high - low + 1

    return job_count


def get_slurm_job_dates(username):
    """
    Return the set of dates for jobs currently queued on slurm
    :param username: Slurm user for the job
    :return: set of dates (MMDDYY) for running jobs
    """
    date_args = ['squeue', '-u', username, '--format="%.18i %.16j"']
    date_output = subprocess.check_output(date_args, encoding='utf-8').splitlines()
    date_output = [x.replace('"', '').strip().split() for x in date_output]
    df = pd.DataFrame(date_output[1:], columns=date_output[0])

    warnings.filterwarnings("ignore", 'This pattern has match groups')
    reg_string = "^(raw|orb)-[0-9]{6}"
    df = df.loc[df.NAME.str.contains(reg_string)]

    return set(df.NAME.apply(lambda x: x.split('-')[1]).values)
