from subprocess import call
import sys
import os
import glob

month = sys.argv[1]
year = sys.argv[2]

# Compile a list of days for which data exists
# In other words, create a list of trading days for the month/year given
current = os.getcwd()
base = '/panfs/pan.fsl.byu.edu/scr/grp/fslg_market_data/NYSE/ProcessedData/'

os.chdir(base+"20{}".format(year))
days = glob.glob("{}*{}".format(month, year))

os.chdir(current)

size = 125

for day in days:
    print("Processing tickers for {}".format(day))
    for i in range(size):
        call(['sbatch', 'submit_ob.sh', day, str(i), str(size), year])
