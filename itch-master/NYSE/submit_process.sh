#!/bin/bash

#SBATCH --time=24:00:00   # walltime
#SBATCH --ntasks=1   # number of processor cores (i.e. tasks)
#SBATCH --nodes=1   # number of nodes
#SBATCH --mem-per-cpu=16G  # memory per CPU core
#SBATCH --output=slurm_files/down%j.out
#SBATCH --error=slurm_files/down%j.err



# LOAD MODULES, INSERT CODE, AND RUN YOUR PROGRAMS HERE
mpirun python nysedata.py $1 $2

exit 0

