#!/usr/bin/env bash

# Get the environment configured correctly
# (This is a little hacky, but couldn't get it to work otherwise)
. /usr/share/Modules/init/bash
. $HOME/.bash_profile

export MODULEPATH="/usr/share/Modules/modulefiles:/etc/modulefiles:/apps/.modulefiles"

# Change to the correct directory
cd $HOME/Code/Work/AHT/itch/NASDAQ

sbatch  --output=$HOME/logs/%j_cron_slurm_add.out   \
        --error=$HOME/logs/%j_cron_slurm_add.err    \
        cron_slurm_add.sbatch
