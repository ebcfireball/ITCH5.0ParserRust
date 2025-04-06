#! /usr/bin/env bash

# Use this script to launch the process_orderbooks.py jobs on a local server

DATE=$1
SIZE=$2
OUTDIR=proc_ob_output

if [[ -z "$1" ]]; then
    echo "No DATE supplied, exiting"
    exit
fi

if [[ -z "$2" ]]; then
    echo "No SIZE supplied, exiting"
    exit
fi

RANKS=`seq -s ' ' 0 $((SIZE-1))`

if [[ -d ${OUTDIR} ]]; then
    echo "Found an existing log out dir, deleting"
    rm -r ${OUTDIR}
fi

parallel --ungroup --results ${OUTDIR} python process_orderbooks.py ${DATE} ${SIZE} ::: ${RANKS}
