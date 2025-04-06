Author: Sean Lane

1. Create an empty directory somewhere on your local machine
    - The project assumes you have some "base" location used for reading and writing files, as seen here: https://gitlab.com/idealabs/itch/-/blob/master/ITCH/locations.py. We'll summarize those steps below.
2. Within that directory, create three folders:
    1. `binary_data`
    2. `grouped_data`
    3. `processed_data`
3. Let's say we want to process one day, April 17, 2017. 
4. Create a folder `2017` within `BASE_DIR/binary_data/`.
5. Download `S041717_v50.txt.gz` to `BASE_DIR/binary_data/2017`
6. Export the BASE location to an env var: `export ITCH_BASE_PATH=<YOUR BASE DIR here>`
7. Change directory to `itch/NASDAQ`.
8. With the project installed (`python setup.py install` in the root dir of this repo), run the following:
    - `python process_raw.py 041717 1 0`
    - To parallelize, run `./local_process_raw.sh 041717 NNNN` where `NNNN` is the number of threads to use
9. Once that's done, process the grouped data:
    - `python process_orderbooks.py 041717 1 0`
    - To parallelize, run `./local_process_orderbooks.sh 041717 NNNN` where `NNNN` is the number of threads to use


---
The following was written before 2019, and is included for legacy purposes

Author: John Wilson

This is an instruction document for the complete NASDAQ processing
workflow.


All the files which the next paragraph references should be in the 
`~/fsl_groups/fslg_market_robustness/compute/NASDAQ/ProcessedData/{Day}`
directory.

First note that the NASDAQ data is all in different stages of processing.
The first stage of data processing was deleted on some days to accomodate
for the large file size of all the processed data. For those days, you 
need to run both stages of the processing following all the steps below.
The first stage processed files are named something like
"OrderGroups_{day}_{ticker}.csv.gz", and the final stage files are named
"{day}_{ticker}.csv.gz". The final stage files are being replaced and
must be deleted if they exist. If the first stage is done for a day, and
the "OrderGroups" files are still there, delete the second stage files
and skip to step 7. There should be around the order of 8300 tickers for
each day.

To delete a day's final stage processing files, simply navigate to the 
ProcessedData directory written above and type `rm -r {Day}/{Day}*`. 
Please be very careful to not delete the wrong data, since it can take 
a while to restore. Once you have ensured that the data is deleted for 
that day, you may begin processing.

1)  Navigate to the NASDAQ processing repository (the one this file is 
    in). All the following steps assume you are in that directory.

2)  Open the file `launch_month1.py` and check which arguments it takes
    and make sure you understand what it does. Close the file.

3)  If you do not already have one, create a folder for the job output
    called `slurm_files`. This can be done by typing `mkdir slurm_files`
    into your command line. If you do have one, make sure it is empty
    by typing `rm slurm_files/*` into command line.

4)  On your command line, type `python launch_month1.py {month} {year}` 
    to start the process. Do not actually type the brackets. Make sure 
    month and year are each two digits long.

5)  You will see a bunch of lines print on your screen, telling you which
    jobs it is submitting to the super computer's job managing system. Once
    this is done (it may take a while since there are about 3000 jobs) you
    can check the status of your jobs by typing `squeue -u {Username}`.
    This will show you each job you have on the queue, and if it is 
    running or pending while the computer allocates nodes for it to run.
    If your queue is empty, it means that processing has finished.

6)  Once all your jobs are finished, make sure there were no errors. Any
    error output can be found in the job output files. Jobs with no errors
    will have an empty error output file. To find the error output files
    which are not empty, type 
        `du -a slurm_files/*.err | sort -n -r | head -n 10`
    This will show you the 10 largest error output files. If the largest
    of these files has more than 0 bytes in it, some error occurred.
    Determine if the error was fatal or not. If it is fatal and you don't
    know how to resolve it, contact John or Dr. Condie. If it was not
    fatal or no error occured, move on the the next step.

7)  Open the file `launch_month2.py` and check which arguments it takes
    and make sure you understand what it does. Close the file.

8)  On your command line, type `python launch_month2.py {month} {year}` 
    to start the process. Do not actually type the brackets. Make sure 
    month and year are each two digits long.

9)  You will see a bunch of lines print on your screen, telling you which
    jobs it is submitting to the super computer's job managing system. Once
    this is done (it may take a while since there are about 3000 jobs) you
    can check the status of your jobs by typing `squeue -u {Username}`.
    This will show you each job you have on the queue, and if it is 
    running or pending while the computer allocates nodes for it to run.
    If your queue is empty, it means that processing has finished.

10) Once all your jobs are finished, make sure there were no errors. Any
    error output can be found in the job output files. Jobs with no errors
    will have an empty error output file. To find the error output files
    which are not empty, type 
        `du -a slurm_files/*.err | sort -n -r | head -n 10`
    This will show you the 10 largest error output files. If the largest
    of these files has more than 0 bytes in it, some error occurred.
    Determine if the error was fatal or not. If it is fatal and you don't
    know how to resolve it, contact John or Dr. Condie. If it was not
    fatal or no error occured, move on the the next step.

11) Empty your slurm_files folder by typing `rm slurm_files/*` into
    command line.

12) Move on to the next month, or let John or Dr. Condie know that
    you have finished.
