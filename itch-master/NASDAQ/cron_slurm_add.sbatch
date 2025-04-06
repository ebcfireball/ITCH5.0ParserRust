#!/bin/bash

#SBATCH --time=00:00:45                             # walltime
#SBATCH --ntasks=1                                  # number of processor cores (i.e. tasks)
#SBATCH --mem-per-cpu=64MB                          # memory per CPU core

module load gcc/6.4
module load python/3.6

python3 $HOME/Code/Work/AHT/itch/NASDAQ/slurm_auto_add.py
