#!/bin/bash

#SBATCH --time=03:00:00                         # walltime
#SBATCH --ntasks=1                              # number of processor cores (i.e. tasks)
#SBATCH --mem-per-cpu=9G                        # memory per CPU core
#SBATCH --output=slurm_files/proc%A_%a.out      # STDOUT file location
#SBATCH --error=slurm_files/proc%A_%a.err       # STDERR file location

python3 process_raw.py $1 $SLURM_ARRAY_TASK_COUNT $SLURM_ARRAY_TASK_ID

