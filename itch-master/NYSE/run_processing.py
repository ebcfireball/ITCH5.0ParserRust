from subprocess import call
import sys
import os
import glob

month = sys.argv[1]
year = sys.argv[2]

type = "process"

# Get a list of days which must be processed
base = os.getcwd()
os.chdir('/panfs/pan.fsl.byu.edu/scr/grp/fslg_market_data/NYSE/RawData/20{}/'.format(year))
days = glob.glob("{}*{}".format(month, year))

# Return to the correct directory to call the right files
os.chdir(base)

for day in days:
   call(['sbatch', 'submit_process.sh', type, day])
