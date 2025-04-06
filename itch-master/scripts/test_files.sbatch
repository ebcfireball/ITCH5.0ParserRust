#!/bin/bash

#SBATCH --time=00:35:30                     # walltime
#SBATCH --ntasks=24                         # number of processor cores (i.e. tasks)
#SBATCH --nodes=1                           # number of nodes
#SBATCH --mem-per-cpu=2048M                 # memory per CPU core

# LOAD MODULES, INSERT CODE, AND RUN YOUR PROGRAMS HERE
python3 test_files.py --max-diff 0.001 $1 $2
