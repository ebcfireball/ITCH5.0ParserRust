#!/bin/bash

#SBATCH --time=01:30:00                             # walltime
#SBATCH --ntasks=1                                  # number of processor cores (i.e. tasks)
#SBATCH --mem-per-cpu=1G                            # memory per CPU core

module load gcc/6.4
module load python/3.6

python3 $HOME/Code/Work/AHT/itch/NASDAQ/download.py --date $(date +\%m\%d\%y -d "yesterday")
