## Name: nysedata.py
# Authors: Scott Condie (sscondie@gmail.com), Evan Argyle (easocks@gmail.com),
#          Matt Schaelling (mattschaelling@gmail.com), and Alex Hoagland (alexhoagland16@gmail.com)
# Description: Provides a class that downloads data (if necessary) and processes it.


import os, sys
import shutil
import ftplib, netrc
import zipfile
import csv
import gzip
import datetime
import contextlib
import struct as st
from partial_read_buffer_nyse import PartialReadBuffer as prb
#from decode_50 import decode_next
#from grouped_order_50 import OrderGroup_50
import itertools


class NYSEData(object):

    def __init__(self,days,tickers,download_location='/panfs/pan.fsl.byu.edu/scr/grp/fslg_market_data/NYSE/RawData/20{0}/{1}/',processed_location='/panfs/pan.fsl.byu.edu/scr/grp/fslg_market_data/NYSE/ProcessedData/20{0}/{1}/',force_download=False):
        # the {0} in download and processed locations needs to be formatted with the day
        # '/panfs/pan.fsl.byu.edu/scr/grp/fslg_market_data/NYSE/RawData/sample_data'
        #'/panfs/pan.fsl.byu.edu/scr/grp/fslg_market_data/NYSE/ProcessedData/sample_data'

        '''NYSEData(days,download_location,force_download=False)

            arguments:
                days: a list of days in the format `MMDDYY`
                download_location: A relative (or absolute) folder location where
                                the raw data should be saved.  This is also the
                                location where data is looked for to see if it
                                needs to be downloaded.
                processed_location: A path where processed data for each ticker
                                    will be saved.
                force_download: Download the data even if it already exists
                                in `download_location`.  Defaults to False.
        '''
        self.current_day = ''
        self.match_no = {}
        self.no_hidden = 0
        self.processed_location = processed_location
        self.locate_codes = [0]*9000            # There are no more than 9000 tickers.
        self.tickers = tickers
        self.download_location = download_location
        self.groups = {}
        self.days = days
        self.force_download = force_download
        #self.host = ####NEED HOST NAME AND CREDENTIALS
        #credentials = netrc.netrc()
        day=days[0]
        host="FTP2.NYXDATA.COM/EQY_US_NYSE_BOOK/EQY_US_NYSE_BOOK_{0}/EQY_US_NYSE_BOOK_{1}/EQY_US_NYSE_BOOK_{2}.zip".format(day[:4],day[:6],day)
        host='FTP2.NYXDATA.COM/EQY_US_NYSE_BOOK/EQY_US_NYSE_BOOK_YYYY/EQY_US_NYSE_BOOK_YYYYMM/EQY_US_NYSE_BOOK_YYYYMMDD.zip'
        #self.user, self.account, self.password = credentials.authenticators(host)
        self.converter=st.Struct(">IHI11sHHIHBcIBBIIIHccccIII")
        # The above generates the list of file names for saving things.
        # write_cache_keys = ['{}_{}'.format(ii[0],ii[1]) for ii in itertools.product(days,tickers)]
        self.csvheader = ['Type', 'Seconds', 'Message_Sequence_Number', 'Price', 'Total_Volume', 'Order_Quantity', 'Num_Orders_at_Price', 'Side', 'Trading_Status']
        self.write_cache = {}
        self.write_cache_len = {} # How many orders are in each entry
        #self.write_cache_max = write_cache_max
        self.write_cache_max = 7000
        self.keep_processing = True
        self.counter = 0
        self.allTickers =[]
        self.non_trade_day = []
        self.user = 'ssc'
        self.password = 'ssc6779'

    ####CHOOSE NAMES FOR FILES
    def nyse_day(self, day):
        nyse_day = '20' + day[4:6] + day[0:2] + day[2:4]
        return nyse_day

    def build_file_name(self,day):
        '''Builds the file name for certain day.
        Arguments: day
        '''
        return 'S%s_nyse.zip' % day

    def check_for_data(self, year, day):
        '''Checks to see if the data exists already.

        Return: True if the file exists and False if not.
        '''
        file_name = self.build_file_name(day)
        file_name_with_path = os.path.join(self.download_location.format(year,day),file_name)
        return os.path.isfile(file_name_with_path)

    ####UNDER CONSTRUCTION
    def download_day(self, year, day):
        '''Download the data for `day` respecting self.force_download.'''
        filen = self.build_file_name(day)   # Name of file name of raw data
        base = self.download_location.format(year, day)
        filen_w_path = os.path.join(base,filen)
        try:
            os.mkdir(self.download_location.format(year, day))
        except:
            pass
        temp = filen_w_path + ".tmp" # Use this until the file is fully downloaded
        try:
            os.remove(temp) # Deleting any incomplete downloads
        except:
            pass
        print("Checking for %s" %filen)
        if os.path.isfile(filen_w_path) and self.force_download == False:
            print("%s already exists! Moving on to submitting job"%filen)
            return True
        else:
            # 'FTP2.NYXDATA.COM/EQY_US_NYSE_BOOK/EQY_US_NYSE_BOOK_YYYY/EQY_US_NYSE_BOOK_YYYYMM/EQY_US_NYSE_BOOK_YYYYMMDD.zip'
            host= 'FTP2.NYXDATA.COM'
            nyseday = self.nyse_day(day)
            path = 'EQY_US_NYSE_BOOK/EQY_US_NYSE_BOOK_{YYYY}/EQY_US_NYSE_BOOK_{YYYYMM}/'.format(\
                                    YYYY=nyseday[:4], YYYYMM=nyseday[:6])
            filename = 'EQY_US_NYSE_BOOK_{YYYYMMDD}.zip'.format(YYYYMMDD=nyseday) 
            print("""%s does not exist on the system, attempting to download it from %s""" %(filen, host))
            print("Downloading: {0}".format(filename))
            ftp = ftplib.FTP(host)
            ftp.login(user = self.user, passwd = self.password)
            ftp.cwd(path)
            try:
                ftp.retrbinary('RETR ' + filename,open(temp, 'wb').write)
                os.rename(temp, filen_w_path)
                print("Extracting all files")
                with contextlib.closing(zipfile.ZipFile(filen_w_path, "r")) as z:
                    z.extractall(base)
                print ('Succesfully downloaded and decompressed {0}'.format(filen))
                return True
            except ftplib.error_perm as e:
                print("{} not a trading day.".format(nyseday))
                file_path = self.download_location.format(year, day)
                # Delete temporary files for non-trading days. Note that there is some issue 
                # here which necessitates manually deleting the empty directories after use
                self.non_trade_day.append(file_path)

    def download(self, year):
        try:
            for dd in self.days:
                self.current_day = dd
                retVal = self.download_day(year, dd)
                if retVal:
                    pass
                else:
                    print('Failed to download {}'.format(dd))
        except ValueError:
            pass
        return self.non_trade_day

    ######## Methods for saving a printing data to csv #######

    def put_in_cache(self,ticker,mtype,mess,year):
        """
        This method combines mtype and mess into one list and puts in in the
        write cache dictionary with fn as the key.

        Arguments:
            ticker: a string containing the name of ticker
            mtype: a string containing the type of message (O, C, E, X)
            mess: a list containing time in seconds, order number, price, volume
                quantity of order, number of orders at price point, and whether
                the message is on the buy or sell side
        """
        mtype = [mtype]
        mtype.extend(mess)
        fn = "{0}_{1}_NYSE.csv.gz".format(ticker,self.current_day)
        if fn in self.write_cache:
            self.write_cache[fn].append(mtype)
        elif fn in os.listdir(self.processed_location.format(year, self.current_day)):
            self.write_cache[fn] = [mtype]
        else:
            self.write_cache[fn] = [self.csvheader, mtype]
        if len(self.write_cache[fn]) >= self.write_cache_max:
            self.clear_cache(fn)

    def clear_cache(self, file):
        """
        This method writes the messages in the cache to the csv and then empties
        that file's cache.
        """
        # print("Writing to cache file {}".format(file))
        the_path = os.path.join(self.processed_location.format(year, self.current_day),file)
        with gzip.open(the_path, 'a') as buf:
            writer = csv.writer(buf)
            writer.writerows(self.write_cache[file])
        self.write_cache[file] = []

    def remove_order(self, orn):
        """
        CURRENTLY NOT BEING USED, JUST REMAINS FROM NASDAQ
        This writes the order to file and deletes it. This keeps hidden,
        cancel-replaced, and deleted orders from staying in memory.
        """
        g = self.groups[orn]
        ticker = g.stock
        file = '{0}_{1}.csv.gz'.format(self.date, ticker)
        self.groups[orn] = self.groups[orn].to_csv_format()
        try:
            self.write_cache[file].append(orn)
            self.write_cache_len[file] += 1
        except KeyError: #if we haven't started this cache yet
            self.write_cache[file] = [orn]
            self.write_cache_len[file] = 1
        else:
            if self.write_cache_len[file] >= self.write_cache_max:
                self.clear_cache(file)

    def process_day_files(self,file_name, year):
        """
        This method takes all the data from a whole day and sends it to
        process_next to process each individual message

        Arguments:
            day_file_name: the complete path of the day file
        """
        self.keep_processing = True
        print("Processing file name: {0}".format(file_name))
        with prb(file_name) as file_buff:
            while self.keep_processing == True:
                self.process_next(file_buff, year)
        for key in self.write_cache:
            self.clear_cache(key)
        self.write_cache = {}   # Clears dictionary
        print("Completed processing of: {0}".format(file_name))

    # Methods for processing messages

    def process_next(self, buf, year):
        '''
        This processes the next message in the buffer.

        Arguments:
            buf: a Partial Read Buffer object
        '''
        try:
            info = self.converter.unpack(buf.read_message(69))
        except st.error as err:
            x = buf.read_all()
            print(err)
            print("Number of bytes left: " + str(len(x)))
            if len(x) != 0:
                print("WARNING: There are more than 0 unread bytes left in this file, further investigation is recommended to ensure the data were processed correctly.")
            self.keep_processing = False
        else:
            check = self.pre_filter(info)       # Apply the pre-filter
            if check:
                info = check
                info = list(info) # convert the tuple to a list
                self.Add(info, year)
                self.counter +=1

    def Add(self, info, year):
        """
        This method creates a new message 'mess' from info (the information translated
        from binary) and sends it to the ticker's cache

        Arguments:
            info:  a list containing an individual message
        """
        ticker = info[3].strip('\x00')
        ticker = ticker.replace(' ', '_')
        reason = info[19]
        # The below mess contains time in seconds, order number, price,
        # total volume, quantity of order, number of orders at price, and buy/sell
        mess = [(float(info[6])*10**-3) + (float(info[7])*10**-6), info[0], float(info[13])/(10**info[12]), info[14], info[15], info[16], info[17], info[9]]
        self.put_in_cache(ticker, reason, mess, year)
        '''
        print("{} message, ticker: {}".format(reason,ticker))
        if ticker in self.tickers:
            if reason=="O":
                self.put_in_cache(ticker, reason, mess)
            if reason=="C":
                self.put_in_cache(ticker, reason, mess)
            if reason=="E":
                self.put_in_cache(ticker, reason, mess)
            if reason=="X":
                self.put_in_cache(ticker, reason, mess)
        '''

    def pre_filter(self,mess): #deleted mtype
        '''Used for filtering out messages that we don't care about.
            This gets applied before processing each message.  If we
            care about it, then return the message.  If not then return
            None. This should be overwritten with specific filters.
        '''
        return mess

    def post_filter(self,mess): #deleted mtype
        '''Used for filtering out messages that we don't care about.
            This gets applied after processing each message.  If we
            care about it, then return the message.  If not then return
            None. This should be overwritten with specific filters.
        '''
        return mess



    def process(self, year):
        '''
        This method loops through the days that were passed in from the main
        function and then sends each binary file for each day for processing
        '''
        for dd in self.days:
            self.current_day = dd
            try:
                os.mkdir(self.processed_location.format(year, self.current_day))
            except:
                pass
            print("\nProcessing files for {0}".format(self.current_day))
            for i in os.listdir(self.download_location.format(year, self.current_day)):
                if i.startswith('openbookultra'):
                    fn = os.path.join(self.download_location.format(year, self.current_day),i)
                    self.process_day_files(fn, year)
                else:
                    pass
            print("Completed processing for {0}".format(self.current_day))


class myNYSEData(NYSEData):

    def __init__(self,*args):
        super(myNYSEData,self).__init__(*args)
    '''
    def pre_filter(self,mtype,mess):
        if mtype in set(['R','A','D','H','Y','L','K','F','E','C','X','U','P','Q','B','I']):
            return mtype,mess
        else:
            return None
    '''

if __name__ == '__main__':
    '''
    Ensure that days are in format MMDDYY
    '''
    type = sys.argv[1]
    
    if type == "download":
        month = sys.argv[2]
        year = sys.argv[3]
        days = []
        for i in range(1,32):
            if i < 10:
                days.append("{}0{}{}".format(month,i,year))
            else:
                days.append("{}{}{}".format(month,i,year))
    
        I = myNYSEData(days,[])
        delete_days = I.download(year)
        print("Deleting empty directories which were created for the following non-trading days:")    
        for i in delete_days:
            print(i[-7:-1])
            shutil.rmtree(i)

    elif type == "process":
        day = sys.argv[2]
        year = day[-2:]
        I = myNYSEData([day], [])
        I.process(year)

    else:
        raise ValueError("Type must be 'download' or 'process'.")
