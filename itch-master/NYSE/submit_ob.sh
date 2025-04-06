#!/bin/bash

#SBATCH --time=15:00:00   # walltime
#SBATCH --ntasks=1   # number of processor cores (i.e. tasks)
#SBATCH --mem-per-cpu=8G   # memory per CPU core
#SBATCH --mail-user=wilsonjohnross@gmail.com   # email address
#SBATCH --output=slurm_files/proc%j.out
#SBATCH --error=slurm_files/proc%j.err

# LOAD MODULES, INSERT CODE, AND RUN YOUR PROGRAMS HERE
mpirun python orderbook_nyse.py $1 $2 $3 $4

exit 0

