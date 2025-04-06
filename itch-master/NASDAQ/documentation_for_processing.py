"""
This program attempts to fully document how to process itch data. It should be followed to the
letter with the specified version of Python, various packages, and the repositories.

This program can't run unless you have the modules file. for now, go back to terminal and type

```
$ module load python/2.7.7
```

Then come back and read through this file, changing variables as noted before running this program.

First, in your home directory on fsl, make a .modules file. This will load up Python and most of
the packages you will need.
"""

import os
import sys
import time
from subprocess import call
from subprocess import check_output

# We will need some files from a repo.  I'll explain these later.
# sys.path.append('/panfs/pan.fsl.byu.edu/scr/grp/fslg_market_data/compute/NASDAQ/')
from . import download

"""
This program expects to be run in your home directory. If you aren't sure where that is, in 
terminal enter

```
$ cd
$ pwd
```

This filepath is your home directory.
"""
home = os.getcwd() + '/'
"""
if len(sys.argv) > 1:
    date_file = str(sys.argv[1])
    dates = []

    with open(date_file, 'r') as csv_file:
        reader = csv.reader(csv_file, delimiter= ',')
        for row in reader:
            dates.append(int(row[0]))
"""
if len(sys.argv) > 1:
    dates = [sys.argv[1]]
else:
    dates = []
    for i in range(1, 31):
        if i < 10:
            dates.append('040'+str(i)+'16')
        else:
            dates.append('04'+str(i)+'16')
    print (dates)

# check to see if you have a .modules file.
# if os.path.isfile(home+'.modules'):
#    pass
# else:
#    print('Making  a .modules file.')
#    # This only needs to be done once.
#    with open(home+'.modules', 'w') as modules:
#        modules.write("#%Module")
#        modules.write("module load defaultenv")
#        modules.write("module load python/2.7.7")
#

"""
Now that you have Python, it's good to check that your packages are the same version I used to make 
sure you don't have any compatibility issues.

Here is my list of packages:
   arrow (0.4.4)
   arvados-python-client (0.1.20141118191609.b1f65fc)
   backports.ssl-match-hostname (3.4.0.2)
   bcbio-nextgen (0.8.3)
   bioblend (0.5.2)
   biopython (1.64)
   boto (2.34.0)
   chanjo (2.2.1)
   click (3.3)
   CNVkit (0.2.5)
   cutadapt (1.4.2)
   *Cython (0.20.2)
   Fabric (1.10.0)
   gffutils (0.8.2)
   google-api-python-client (1.3)
   *h5py (2.3.1)
   HTSeq (0.6.1p1)
   *ipython (2.1.0)
   ipython-cluster-helper (0.3.6)
   *ITCHy (0.2)
   Warning: cannot find svn location for joblib==0.8.3-r1
   joblib (0.8.3-r1)
   Logbook (0.8.0)
   lxml (3.4.0)
   *matplotlib (1.3.1)
   *mpi4py (1.3.1)
   msgpack-python (0.4.2)
   nose (1.3.3)
   numexpr (2.4)
   *numpy (1.9.1)
   *openpyxl (1.8.6)
   *pandas (0.14.0)
   path.py (7.0)
   *patsy (0.3.0)
   *pip (1.5.6)
   prettyplotlib (0.1.7)
   psutil (2.1.3)
   pybedtools (0.6.8)
   pyinotify (0.9.4)
   pyparsing (2.0.2)
   pysam (0.8.0)
   python-dateutil (2.2)
   pythonpy (0.3.6)
   pytz (2014.4)
   PyYAML (3.11)
   pyzmq (14.4.1)
   *scipy (0.14.0)
   setuptools (0.6c11)
   six (1.7.3)
   *statsmodels (0.6.1)
   tables (3.1.1)
   toolz (0.7.0)
   tornado (3.2.2)
   when-changed (0.2.1)
   wsgiref (0.1.2)

To help you with any problems, I've put an astrisk before the names of packages I recall us using.  
There may be other packages we use.

To get a specific version of a package on your account, run the
command:

   pip install --user pandas==0.14.0

change the package name and version number as needed. I've automated getting the astrisked packages.
"""

package_version_list = ["Cython==0.20.2", "h5py==2.3.1", "ipython==2.1.0", "matplotlib==1.3.1",
                        "mpi4py==1.3.1", "numpy==1.9.1", "openpyxl==1.8.6", "pandas==0.14.0",
                        "patsy==0.3.0", "pip==1.5.6", "scipy==0.14.0", "statsmodels==0.6.1"]

os.chdir(home)
for package_and_version in package_version_list:
    try:
        call(['pip', 'install', package_and_version, '--user'])
    except:
        print('Error while installing {}'.format(package_and_version))

"""
The package ITCHy was written by Roy and I.  Only a few features of it are still used since we have 
new code.  You have to custom install it from the ITCH_Library repo.

The repo called itch_project has most all of the code to process the raw data and run the 
regressions.

There is a third repo, called project_ITCH.  We migrated our code out of this repo. It is still on 
FSL just in case.

As I recall, you don't need to do anything to see our repos on FSL. They should already be in our 
group files.  Please note that we all work on the same copy, so be careful what you change and 
update the repo with your changes often.
"""

# Move to our group files
os.chdir(home+'fsl_groups/fslg_condie_itch/compute')

# Move into the repo to the directory containing the setup.py file
os.chdir('ITCH_Library/')
# Run the setup file to install the ITCHy Library
call(['python', 'setup.py', 'install', '--user'])
# This can be tempramental.  Hope it works.

"""
Now you have a Python environment that should be very similar to mine when I wrote this and tested 
everything. I used the repos with the last commit before 5 May 2015. If you need to use an earlier 
version, consult with Scott and email me before attempting to checkout anything.  It's really easy 
to mess things up when you go back.  I'm just including this date in case the code breaks due to 
later changes.


Finally, we're ready to start talking about how the processing works. All of the relevant code is 
in itch-project/DataProcessing.  That's why I used sys.path.append to import a bunch of files.  
Normally they would be run on their own, but I'll use them here as an example.

The first file we need is called download_only.py Roy wrote it and it works really well. The key 
part is the try_download function. The try_download fuction takes in a string in the format "MMDDYY"
and attempts to download the file off of the NASDAQ server.

The credentials used to authenticate with the server must be in your .netrc file in the home 
directory. This might need certain file permissions, here's what mine says (Scott or I can explain 
how to read this and change file permissions on FSL if this is a problem.

-rwx------ 1 buss2 buss2 59 2014-07-16 09:10 .netrc

"""

# Check to see if you have a .netrc file.
os.chdir(home)
if os.path.isfile('.netrc'):
   pass
else:
    # Make a .netrc file. This only needs to be done once.
    print('problem')
    with open(home+'.netrc', 'w') as netrc:
        netrc.write("machine itchdata.nasdaq.com")
        netrc.write("login D47301")
        netrc.write("password condie01")

"""
With the .netrc file in place, let's download a day and see what happens.
"""

# Move to the directory as if we were actually running the program
os.chdir(home+'/fsl_groups/fslg_market_data/compute/NASDAQ/' + 'ProcessingFiles/')
if dates is None:
    date = '040116'  # Feb 3rd, 2014
    download.try_download(date)
else:
    for date in dates:
        download.try_download(date)

"""
Once we have a raw data file from the server, we do our initial processing using `process_raw.py`  

`process_raw.py` uses MPI to process the day.  How it does so is a bit too involved for this simple 
demonstration of how to get started. The code is fairly involved, but there are some comments to 
help you along.  You can always contact me if needed.

I'd recommend reading through `process_raw.py`, then going to ITCH_Library/ITCHy/grouped_order.py 
and reading about the process_parallelday, OrderGroup, and ParallelDay classes. This will require 
you to understand the message types in the NASDAQ ITCH 4.1 data. Search for the documentation 
online (it is a pdf).

Don't worry about that unless you need to build a similar program to process data from another 
market. (I hope a future reader will do so.)  I have a lot of technical knowledge I can share about 
doing just that, please contact me if that is the case.

Anyways, let's process the raw file we just downloaded. I'll assume that you are familiar with 
sbatch files on fsl. If not, go to fsl.byu.edu and read the documentation. Then run the next few 
lines.
"""

os.chdir(home + 'fsl_groups/fslg_market_data/compute/NASDAQ/'+ 'ProcessingFiles/')
if dates is None:
    date = '040116'  # Feb 3rd, 2014
    message = check_output(['sbatch', 'process_raw_50.sbatch', date])
else:
    for date in dates:
        message = check_output(['sbatch', 'process_raw_50.sbatch', date])
job_no = message.split()[-1]

# I like to watch the output. Uncomment this to watch the output. Press Control-C to exit
# the watch command.
# call(['watch', 'tail', '-n', 'slurm_files/raw'+job_no+'.out'])

"""
The results are stored in project_ITCH/Results/Groups/ They are divided into directories by date. 
We just made the 040116 directory, which is populated with the OrderGroup files for that day. 
OrderGroup csv files are a minimally processed version of the NASDAQ ITCH data.  From them we 
generate processed data using the process_orderbooks.py program in the itch_project/DataProcessing 
directory. The main change between the OrderGroup files and the Processed files is that the 
Processed files have the spread added in.
"""

os.chdir(home + 'fsl_groups/fslg_market_data/compute/NASDAQ/' + 'ProcessingFiles/')

time.sleep(5*60**2 + 5*60)
if dates is None:
    date = '040116'  # Feb 3rd, 2014
    message = check_output(['sbatch', 'process_orderbooks.sbatch', date])
else:
    for date in dates:
        message = check_output(['sbatch', 'process_orderbooks.sbatch', date])
job_no = message.split()[-1]

call(['watch', 'tail', '-n', 'slurm_files/spread'+job_no+'.out'])

