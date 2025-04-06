#!/bin/bash

#SBATCH --time=00:35:00                         # walltime
#SBATCH --ntasks=1                              # number of processor cores (i.e. tasks)
#SBATCH --mem-per-cpu=5G                        # memory per CPU core
#SBATCH --output=slurm_files/spread%A_%a.out    # STDOUT file location
#SBATCH --error=slurm_files/spread%A_%a.err     # STDERR file location

python3 process_orderbooks.py $1 $SLURM_ARRAY_TASK_COUNT $SLURM_ARRAY_TASK_ID
