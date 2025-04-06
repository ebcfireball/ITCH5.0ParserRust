Notes by Lehner White (lehner.white@gmail.com) and John Wilson (wilsonjohnross@gmail.com)

This repository contains the base files necessary to process and analyze raw ITCH data files from NASDAQ's 
data feed and raw data from NYSE. All NASDAQ files in this repository are designed to handle ITCH 5.0 data 
unless otherwise specified. The files are separated into two categories: primary ITCH files and processing 
ITCH files. The ITCH processing files are found in the 'processing' subdirectory. The NYSE files are in the
NYSE directory.

The main processing files are as follows:

--------------------
processing/decode.py
--------------------

This is a file that is designed to interpret the binary code. It detects the first character of the message 
and then based on the type of message that is found, it uses the formatting provided by NASDAQ to detect the
following portions of that message. Each message has a unique format which makes this file necessary for 
converting the binary data as without it we would not know when one message ended and the next began. There 
are also different extensions for this file (.pyx, .so) in order to help speed up its use. 

---------------------------
processing/download_only.py
---------------------------

This file contains the function try_download which is used to retrieve the binary data from the NASDAQ 
server. The locations in this file are specified for our preferences but can be easily changed. In order
for this function to run properly the user needs to have a well formatted .netrc file with a working 
username and password for the ITCH server. To download the data you simply pass the try_download function 
the date (MMDDYY) as a string and it will first check if the data already exists in the specified path, 
then search for the data on the NASDAQ server, if it exists it will attempt to download the data. 

----------------------
processing/example.txt
----------------------

This is a text file that contains an example of how to use the files in this directory to download and 
process the raw data form NASDAQ's servers. 

---------------------------
processing/grouped_order.py
---------------------------

This file contains two classes and a function. The first class is OrderGroups which is a class that saves 
an initial message and all messages that are subsequently related to the original. The second class is 
ParallelDay that will accept a day and is designed to create the OrderGroups for the data found on that
day. The function process_parallelday is written to utilize these classes on data. 

---------------------------------
processing/partial_read_buffer.py
---------------------------------

This is a helper file for grouped_order.py. When creating the order groups we needed a buffer that would 
allow us to read in increments and this is what allows us to do that. 

--------------------------------
processing/process_orderbooks.py
--------------------------------

This is a file that takes the processed ordergroups, which contains the decoded messages grouped together 
by order, and calculates the current best ask, current best bid, spread and orders them chronologically. 

-------------------------
processing/process_raw.py
-------------------------

This is a file that will process the raw binary data into the order groups using other files already 
described. It is written to accept the date, and two other variables used for the parallelization of 
processing. It will check for a directory to place the data in, potentially clean out that directory if 
it exists, or create it if it does not before it begins processing the data. 

-------------------------------------------

There are other files in the processing folder that are very useful for many of the tasks that we needed 
to accomplish. All .sbatch files were used for submitting jobs to BYU's supercomputer. The launch_* files 
were used for submitting jobs for all of the days that we were working on in the month of April 2016. 

-------------------------------------------


What follows is an outline for running the processing of NASDAQ data for a given month. Suppose we are
trying to process data from January 2019. Navigate to the processing folder and type

```console
python download_month.py 01 19
```
into the console. This process requires that you be on the interactive node, so it cannot be submitted
to the supercomputer using a `.sbatch` or `.sh` file. This will take a few hours, so make sure there is
time when you begin to run it. Following its completion, the rest of the steps are detailed in the file
`instructions.txt` located in the same processing directory.

Complete instructions for processing NYSE data are found in the NYSE directory in the `instructions.txt`
file.