# Author: Christopher Hair (christopher.m.hair@gmail.com)
# Description: This orderBook class contains a large collection of useful functions to analyze the
# orderbook. It loads a csv file of processed messages and is able to recreate the order book at
# any given time.

import traceback
import numpy as np
import time
from multiprocessing import Array, Process, Queue
import scipy.misc
import pandas as pd
from scipy.optimize import minimize
from scipy.stats import chi2 

# Fast cython module to pull out the shares component of the tuple saved in the orderbook dictionary
from .shrs_from_dict import get_shares_from_dictionary


class orderBook:
    """
    NOTES:
    This class contains a few sections: 
        - Initialization sets up the class.
        - Time and Processing manages building and maintaining the order book as messages are
        processed throughout the day
        - Stats

    The class has some performance and memory issues. Many intensive or time-critical sections of
    code have been implemented to the best of my ability, but are still slow. Consider rewriting
    critical sections in Cython or C. Also, memory-wise, this class leans memory heavy in order to
    reduce compute times.
    """

    ###########################
    #     INITIALIZATION      #
    ###########################
    def __init__(self, fName, messages=None, name="none", debug_level="D"):
        """
        Initializes the orderBook class. Reads in either a file (given as fName), or a matrix of
        messages (as specified by messages). The orderBook can have a name (which affects the name
        of output files) and a debug_level (different logging verbosity and error handling).
        """

        if messages is not None: # Read in messages instead of reading file
            if fName is not None:
                raise Exception('''Argument 'messages' should not be specified while also passing 
                in a file name for the orderBook constructor. Try passing None as the file name if 
                you are trying to initialize an orderBook with messages.''')
            self.messages = messages
            self.name = name
        
        else: # Otherwise get messages from file
            if fName is None:
                raise Exception('''Constructor for orderBook must be called with either a file 
                name or with the 'messages' argument.''')

            # Read in the file and add the messages
            try:
                # By specifying the types of the columns, we get a slightly better memory profile
                # This section right here is probably the most memory-intensive section.
                _df = pd.read_csv(fName, header=0, usecols=['type', ' seconds', ' orn', ' side', ' shares', ' price'], dtype={'type': 'S1', ' seconds':float, ' orn':int, ' side':object, ' shares': int, ' price': float})
                _df.rename(columns={' seconds': 'seconds', ' orn': 'orn', ' side': 'side', ' shares': 'shares', ' price': 'price'}, inplace=True) # Get rid of spaces in names
                _df['side'] = _df['side'].str.strip() # Get rid of spaces in the 'side' variable
            # Catch errors early
            except Exception as e:
                print(e)
                print(traceback.format_exc())
                print("Could not load the file correctly.")
                raise Exception("orderBook initialization could not read in the CSV file correctly. Possibly some necessary columns are missing.")

            # Self.messages will keep the messages for the duration of the life of this object
            self.messages = _df.values

        # Set name
        if name is None or name == "none":
            self.name = fName
        else:
            self.name = name

        # Various public fields
        self.currentTime = 0  # currentTime is the time of the last processed message
        self.currentLoc = -1  # current message number of the last processed message
        self.orderBook = {}   # orderBook is the ACTUAL DICTIONARY of prices (being keys) and a tuple of (shares, side, time, ordernumber) as the value.
        self.asks = {}        # Keeps the asks separated from self.orderBook, which speeds up analysis later
        self.bids = {}        # Same asks, but for bids
        self.ornLookup = {}   # This dictionary keeps the details about order number and is only used with "C" message types
        self.hiddenExecutes = {}  # Dictionary of hiddenExecutes. Currently not used.
        self.executes = {}    # Dictionary of executes, currently unused.
        self.u_means_remove = True  # Keeps track of whether the next 'U' message will be one that deletes the shares or adds the shares.
        self.savedOrderBooks = []  # List of orderbooks. When processing at multiple times, this saves the orderbook at each of the processing times for use later
        self.debug_level = debug_level #P = fail on error, D = fail on major problems only, otherwise warn, W = warn only, try to continue

        # Saves previous results from book_to_pct_bins() to speed up function
        self.previous_bids_results = None  
        self.previous_asks_results = None

        # Heatmap stuff
        self.heatMapData = None  # Keeps track of the heatmap if being used

        # StatsRecorder... never fully implemented.
        self.statsRecorder = False  # statsRecorder isn't currently used much... meant to be a way to record various statistics as the orderBook is processed. Never fully developed.
        self.stats = {}       # Keeps the statistics recorded by statsRecorder

    #############################
    #### TIME AND PROCESSING ####
    #############################

    def __updateCurrentTime(self):
        '''
        Updates the time based on the last processed message
        '''
        self.currentTime = float(self.messages[self.currentLoc][1])
        return

    def __getNextMessageTime(self):
        '''
        Returns what the time will be after the next message is processed.
        '''
        return float(self.messages[self.currentLoc + 1][1])

    def __processMessage(self, r):
        '''
        Processes message r
        Sensitive code... recommend not to touch
        This works only with ITCH format data
        Possibly one day, this will work with an intermediate message language...?
        '''

        #take r and split it into the named variables we need, make proper casts, etc.
        messageType = r[0]
        time = float(r[1])
        orn = int(r[2])
        b_s = r[3]
        numShares = int(r[4])
        price = round(float(r[5]), 2) #IMPORTANT TO ROUND (floats are not always stored identically, and there were problems with this, but rounding fixed it.)

        #Depending on the message type, process it in the right way.
        if messageType == "A": #Addition
                self.__add(price, numShares, b_s, time, orn)
                if self.statsRecorder is not None:
                    if "additions" in self.stats:
                        self.stats["additions"].append([numShares, price, b_s, time, orn, "A"])

        elif messageType == "C": #Execute with price
                result = self.__removeAsC(orn, numShares, b_s)
                if self.statsRecorder:
                    if "executes" in self.stats:
                        self.stats["executes"].append([numShares, price, b_s, time, orn, "C"])
                if result != 0:
                    if self.debug_level == "P" or self.debug_level == "D":
                        raise Exception("Invalid message data: failed on a C message, around line " + str(self.currentPos))
                    else:
                        print("Invalid message data: failed on a C message, around line " + str(self.currentPos))

        elif messageType == "D" or messageType == 'n': #Delete
            if messageType != "D":
                print("In the print block", time, orn)
            code = self.__remove(price, numShares, b_s, orn)
            if self.statsRecorder:
                if "removes" in self.stats:
                    self.stats["removes"].append([numShares, price, b_s, time, orn, "D"])
            if code == -1:
                if self.debug_level == "P" or self.debug_level == "D":
                    raise Exception("Invalid message data: failed on a D message, around line " + str(self.currentPos))
                else:
                    print("Invalid message data: failed on a D message, around line " + str(self.currentPos))

        elif messageType == "E": #Execute
                code = self.__remove(price, numShares, b_s, orn)
                if self.statsRecorder:
                    if "executes" in self.stats:
                        self.stats["executes"].append([numShares, price, b_s, time, orn, "E"])
                if code == -1:
                    if self.debug_level == "P" or self.debug_level == "D":
                        raise Exception("Invalid message data: failed on a E message, around line " + str(self.currentPos))
                    else:
                        print("Invalid message data: failed on a E message, around line " + str(self.currentPos))

                if price in self.executes:
                    if b_s == "B" or b_s == " B":
                        self.executes[price] = (self.executes[price][0] + numShares, "B", time)
                    else:
                        self.executes[price] = (self.executes[price][0] + numShares, "S", time)

                else:
                    if b_s == "B" or b_s == " B":
                        self.executes[price] = (numShares, "B", time)
                    else:
                        self.executes[price] = (numShares, "S", time)

        elif messageType == "F": #Add with MPID
                self.__add(price, numShares, b_s, time, orn)
                if self.statsRecorder:
                    if "additions" in self.stats:
                        self.stats["additions"].append([numShares, price, b_s, time, orn, "F"])

        elif messageType == "P": #Execute non-displayable trade
                if price in self.hiddenExecutes:
                    if b_s == "B" or b_s == " B":
                        self.hiddenExecutes[price] = (self.hiddenExecutes[price][0] + numShares, "B", time)
                    else:
                        self.hiddenExecutes[price] = (self.hiddenExecutes[price][0] + numShares, "S", time)
                else:
                    if b_s == "B" or b_s == " B":
                        self.hiddenExecutes[price] = (numShares, "B", time)
                    else:
                        self.hiddenExecutes[price] = (numShares, "S", time)

                if self.statsRecorder:
                    if "executes" in self.stats:
                        self.stats["executes"].append([numShares, price, b_s, time, orn, "P"])

        elif messageType == "U": #Update
                if orn in self.ornLookup:
                    code = self.__remove(price, numShares, b_s, orn)
                    if self.statsRecorder:
                        if "removes" in self.stats:
                            self.stats["removes"].append([numShares, price, b_s, time, orn, "U"])
                    if code == -1:
                        if self.debug_level == "P" or self.debug_level == "D":
                            raise Exception("Invalid message data: failed on a U message, around line " + str(self.currentPos))
                        else:
                            print("Invalid message data: failed on a U message, around line " + str(self.currentPos))
                else:
                    self.__add(price, numShares, b_s, time, orn)
                    if self.statsRecorder:
                        if "additions" in self.stats:
                            self.stats["additions"].append([numShares, price, b_s, time, orn, "U"])

        elif messageType == "X": #Cancel
                code = self.__remove(price, numShares, b_s, orn)
                if self.statsRecorder:
                    if "removes" in self.stats:
                        self.stats["removes"].append([numShares, price, b_s, time, orn, "X"])
                if code == -1:
                    if self.debug_level == "P" or self.debug_level == "D":
                        raise Exception("Invalid message data: failed on a X message, around line " + str(self.currentPos))
                    else:
                        print("Invalid message data: failed on a X message, around line " + str(self.currentPos))

        else:
            print("Problem",r)
            if self.debug_level == "P":
                raise Exception("Invalid message data: failed on an unknown message type, around line " + str(self.currentPos))
            else:
                print("Invalid message data: failed on an unknown message type, around line " + str(self.currentPos))


        #Do the stats thing here.
        if self.statsRecorder is not None:
            #print "statsRecorder"
            if "slopes" in self.stats:
                self.stats["slopes"].append(self.book_to_pct_bins([0.001,0.005,0.01,0.05,0.10,0.15,0.30], [0.001,0.005,0.01,0.05,0.10,0.15,0.30], b_s.strip()))

    def __add(self, price, numShares, b_s, t, orn):
        '''
        Private method. Adds shares to the order book.
        Arguments:
            - price (float) : price at which it is added
            - numShares (int) : number of shares to be added
            - b_s (char, "B", or "S") : side of the orderBook ("B"=bid, "S"=ask)
            - t (float) : time of the message
            - orn (int) : order number

        Updates self.orderBook inplace.
        Returns : None
        '''
        
        price = round(price, 2)

        try:
            orn = float(orn)
            self.ornLookup[orn] = (price, numShares)
        except Exception as e:
            if self.debug_level == "D":
                print("orn unable to be cast to float for the __add method...")

        #If there is already an order at the price, we need to add it to the existing order, otherwise we'll create a new entry in the orderBook dictionary.
        if price in self.orderBook:
                if b_s == "B":
                    try:
                        self.orderBook[price] = (self.orderBook[price][0] + numShares, "B", t, orn)
                        self.bids[price] = (self.bids[price][0] + numShares, "B", t, orn)
                    
                    # This KeyError is triggered in rare situations where both a "B" and an "S" exist on the book at the same price at the same time. 
                    # Very strange, and causes a bug because the existing order was only in self.bids and not self.asks (or vice versa...)
                    # This is a work around, but it would be interesting to look into why this is happening...
                    except KeyError:
                        self.orderBook[price] = (self.orderBook[price][0] + numShares, "B", t, orn)
                        self.bids[price] = (numShares, "B", t, orn)
                else:
                    try:
                        self.orderBook[price] = (self.orderBook[price][0] + numShares, "S", t, orn)
                        self.asks[price] = (self.asks[price][0] + numShares, "S", t, orn)
            
                    #Same reasoning about KeyError as above
                    except KeyError:
                        self.orderBook[price] = (self.orderBook[price][0] + numShares, "S", t, orn)
                        self.asks[price] = (numShares, "S", t, orn)
        else:
                if b_s == "B":
                    self.orderBook[price] = (numShares, "B", t, orn)
                    self.bids[price] = (numShares, "B", t, orn)
                else:
                    self.orderBook[price] = (numShares, "S", t, orn)
                    self.asks[price] = (numShares, "S", t, orn)
        return

    #Removes a limit order from the orderbook
    def __remove(self, price, numShares, b_s, orn):
        '''
        Private method. Removes shares from the order book.
        Arguments:
            - price (float) : price at which it is removed
            - numShares (int) : number of shares to be removed
            - b_s (char, "B", or "S") : side of the orderBook ("B"=bid, "S"=ask)
            - t (float) : time of the message
            - orn (int) : order number

        Updates self.orderBook inplace.
        Returns : None
        '''

        price = round(price, 2) #Rounding to avoid floating point errors

        #Need to update the ornLookup dictionary
        try:
            orn = int(orn)
            try:
                self.ornLookup[orn] = (price, int(self.ornLookup[orn][1]) - numShares)
                if self.ornLookup[orn][1] == 0: #delete if at 0
                    self.ornLookup.pop(orn, None)
            except KeyError:
                pass

        except Exception as e:
            if self.debug_level == "D":
                print("Could not cast orn to int in the __remove method...")

        #price should be in self.orderBook, otherwise we get an error
        if price in self.orderBook:

                if self.orderBook[price][0] >= numShares:
                    self.orderBook[price] = (self.orderBook[price][0] - numShares, b_s, self.orderBook[price][2], self.orderBook[price][3])
                    
                    #Update bids and asks
                    if b_s == "B":
                        self.bids[price] = (self.bids[price][0] - numShares, b_s, self.bids[price][2], self.bids[price][3])
                        if self.bids[price][0] == 0:
                            del self.bids[price]
                    else:
                        self.asks[price] = (self.asks[price][0] - numShares, b_s, self.asks[price][2], self.asks[price][3])
                        if self.asks[price][0] == 0:
                            del self.asks[price]

                else:
                    if self.debug_level == "P":
                        raise Exception("At the specified price, there are not enough shares to remove. Existing entry in the book: " + str(self.orderBook[price]) + ". Desired removal: " + str(numShares))
                    return -1 #bad

                # If no more shares left at the price, delete the entry.
#                if self.orderBook[price][0] == 0:
#                    self.orderBook.pop(price, None) #remove entry
#                    if b_s == "B":
#                        self.bids.pop(price, None)
#                    else:
#                        self.asks.pop(price, None)

                return 0 #good

        else: #right now this is only set to raise an exception on production, but on development and testing it returns a -1 which is handled elsewhere and the processing attempts to continue
            if self.debug_level == "P":
                raise Exception("price was not available in the orderbook in the __remove method")
            else:
                return -1 #bad,

    #Special remove function to deal with "C" type messages
    def __removeAsC(self, orn, numShares, b_s):
        #'C' messages are executes at a different price.
        #This attempts to get the original price of the order from the ornLookup dictionary, then calls __remove at that price
        origPrice = self.ornLookup[orn][0]
        return self.__remove(origPrice, numShares, b_s, orn)

    #Processes the next message
    def processNext(self):
        try:
            self.currentLoc += 1
            self.__processMessage(self.messages[self.currentLoc])
            self.__updateCurrentTime()
        except:
            pass
        return

    #Processes all the messages up until end_time
    def processToTime(self, end_time):

        #This try/catch ensures that we won't step over the last message (which would happen if end_time is greater than the message time of the very last message for the ticker)
        try:
            nextTime = self.__getNextMessageTime()
        except IndexError:
            return # End the processing, since we are already there (nothing left to process, even if we haven't made it to the time yet.)

        #We need to make sure we haven't already processed past the time we're supposed to process to
        if self.currentTime > end_time:
                if self.debug_level == "P" or self.debug_level == "D":
                    raise Exception("end_time " + str(end_time) + " has already passed. Current time is: " + str(nextTime) + ". Use reset().")
                else:
                    print("End_time " + str(end_time) + " has already passed. Current time is: " + str(nextTime) + ". Attempting to reset().")
                    self.reset()

        #It will not process PAST end_time, only messages equal to or less than end_time
        while nextTime <= end_time:
                self.processNext()
                try:
                    nextTime = self.__getNextMessageTime() # End the processing, since we are already there (nothing left to process, even if we haven't made it to the time yet.)
                except IndexError:
                    break
        return


    def processAndSaveAtTimes(self, times):
        '''
        Given an array of times, process the orderbook at each time
        The orderbooks are then saved into the array self.savedOrderBooks
        Arguments:
            - times (list of floats) : the times it is supposed to process to. These need to be sorted from first to last
        '''
        self.reset()
        length = len(times)
        for time in times:
            self.processToTime(time)
            self.savedOrderBooks.append(self.orderBook.copy())
        return

    def processAndSaveInterval(self, start_time, end_time, numIntervals=None, timeStep=None):
        '''
        Given a start and end time and number of intervals, process the messages into orderbooks on the intervals
        It's like np.linspace() except it makes orderbooks at all the times
        Specify either numIntervals, or timeStep (the time between each interval), but not both.
        Arguments:
            - start_time : start time of interval
            - end_time : end time of interval
            - numIntervals : number of samples. This automatically calculates the timeSteps.
            - timeStep : time delta, distance between samples (numIntervals automatically calculated.
        '''

        if numIntervals == timeStep == 0:
                raise Exception("Must specify either numIntervals or timeStep")
        if numIntervals != None:
                times = np.linspace(start_time, end_time, numIntervals)

        else:
                times = [start_time]
                i = start_time
                while i < end_time:
                    if i + timeStep > end_time:
                        times.append(end_time)
                    else:
                        times.append(i + timeStep)
                    i += timeStep
        self.processAndSaveAtTimes(times)
        return

    def filterLongPositions(self, t):
        '''
        Filter out orders that sit on the books for more than time t
        A bit slow since it's using pandas groupby and apply and merge
        The actual measurement of order length may be complicated due to complex order types such as U
        Strictly filters on order number-- when order was first placed to when it was completely removed
        Note that it is replacing the messages, but is not reseting the actual orderbook.
        So, you may need to reset the orderbook after this is done to fix the currentLoc and currentTime objects

        Arugments:
            - t : A cutoff value. Messages that stay on the book for longer than t seconds will be removed.

        Returns:
            - None
        '''

        if self.debug_level == "W":
            print("Filtering messages to only contain orders lasting less than", t, "seconds...")

        #Change the messages into a pandas DataFrame
        mdf = pd.DataFrame({"type": self.messages[:, 0], "time": self.messages[:, 1], "orderNumber": self.messages[:, 2], "side": self.messages[:, 3], "shares": self.messages[:, 4], "price": self.messages[:,5], "sharesRem": self.messages[:, 6], "bestB" : self.messages[:,7], "bestS": self.messages[:, 8], "spread": self.messages[:, 9]})
        mdf["time"] = map(str.strip, mdf["time"])
        mdf = mdf.sort(["orderNumber", "time"])

        #Function that calculates the duration
        def getTimeOfOrder(x):
            return float(x["time"].iloc[-1].strip()) - float(x["time"].iloc[0].strip())

        #Calculate duration of message and get rid of the messages whose duration
        result = mdf.groupby("orderNumber").apply(getTimeOfOrder).reset_index()
        result.rename(columns={0:"duration"}, inplace=True)
        result = result.query("duration < @t")

        #Now put it all back together and overwrite self.messages as the filtered messages
        mdf = pd.merge(mdf, result, on="orderNumber", how="inner")
        cols = ["type", "time", "orderNumber", "side", "shares", "price", "sharesRem", "bestB", "bestS", "spread"]
        mdf = mdf.sort("time")
        self.messages = mdf[cols].values.astype("S17")
        if self.debug_level == "W":
            print("Filtered.")

    #Resets everything in the class, clears position in messages, saved orderbooks, etc.
    #Keeps the messages, name
    def reset(self):
        self.__init__(None, messages=self.messages, name=self.name)
        return

    #################################
    ############# STATS #############
    ### RECORDING, RESETING, ETC. ###
    #################################

    #Begins recording the stats that are specified in the list statsToRecord (additions, removals, executes
    #This method adds the stats to record into a dictionary called stats
    #Each entry in this dictionary is a list of observations for the stat (empty at initialization)
    #Once this is turned on, processing futher observations automatically adds the stats into the dictionary
    def startRecordingStats(self, statsToRecord=['executes', 'additions', 'removals']):
        self.statsRecorder = True
        #Notice that this clears out the old objects
        self.stats = {}
        for s in statsToRecord:
            self.stats[s] = []

    #Resets the statstics recorded to zero
    def resetStats(self, statsToReset=['additions', 'removals', 'executions']):
        for s in statsToReset:
            self.stats[s] = []

    #Stops recording statistics.
    #Notice that the recorded stats are kept
    #They are reset either by calling resetStats() or by starting recording again (startRecordingStats())
    def stopRecordingStats(self):
        self.statsRecorder = False
        #Notice that we are not clearing the self.stats object, just stopping recording more.

    #Returns the executed volume since startRecordingStats was called
    def getVolumeExecutedFromStats(self):
        if "executes" not in self.stats:
            if self.debug_level == "P":
                raise Exception("No 'executes' list in stats. First call 'startRecordingStats' with 'executes' in the list of statsToRecord and process some messages.")
            else:
                print("No 'executes' in stats. Call 'startRecordingStats' with 'executes' in the list of statsToRecord first...")
        else:
            volume = 0
            for e in self.stats["executes"]:
                volume += int(e[0])
            return volume

    #Returns the volume added/removed from orderbook since startRecordingStats was called
    def getVolumeOrderbookActivityFromStats(self):
        if "additions" not in self.stats or "removals" not in self.stats:
            if self.debug_level == "P":
                raise Exception("No 'additions' or 'removals' list in stats. First call 'startRecordingStats' with the correct arguments and process some messages.")
            else:
                print("No 'additions' or 'removals' in stats. Call 'startRecordingStats' correctly first...")
        else:
            volume = 0
            for a in self.stats["additions"]:
                volume += int(a[0])
            for r in self.stats["removals"]:
                volume += int(r[0])
            return volume

    #Returns the average price and variance of the prices since startRecordingStats was called
    def getPriceMeanAndVarianceFromStats(self, simple=True):
        if "executes" not in self.stats:
            if self.debug_level == "P":
                raise Exception("No 'executes' list in stats. First call 'startRecordingStats' with 'executes' in the list of statsToRecord and process some messages.")
            else:
                print("No 'executes' in stats. Call 'startRecordingStats' with 'executes' in the list of statsToRecord first...")
        else:
            if simple:
                prices = []
                for e in self.stats["executes"]:
                    prices.append(float(e[1]))
                if len(prices) == 0:
                    return [-1, -1]
                else:
                    return np.array([np.mean(prices), np.var(prices)])

            else:
                raise Exception("Complex/weighted variances not yet supported. Please call with simple=True")


    #################################
    ###########  BINNING  ###########
    #################################

    #Bins any orderbook passed to it based on the highest bin value (highValue), number of bins, and size of each bin
    #includeAllPrices is a flag for whether entries outside the range should be aggregated into the top/bottom bins or
    #just ommitted entirely
    #Using np.digitize here is actually not faster. However, using Numba is marginally faster, so I'm going to keep it.
    @staticmethod
    #@jit
    def binOrderBook(orderBook, highValue, numBins, stepSize, debug_level, includeAllPrices=True):
        if numBins <= 0:
            raise Exception("Invalid numBins called: " + str(numBins))

        partitions = np.zeros(numBins) #object that will be filled. The "bins"

        #Catch an immediate, obvious error
        if np.isnan(highValue) or highValue == 0.0:
            if debug_level == "P" or debug_level == "D":
                raise Exception("binOrderBook was called with an invalid highValue of: " + str(highValue))
            return -1.0*np.ones(numBins) #Error

        #Bin the entries in the orderbook
        #iterate by the price key, calculate which bin it should all fall into, drop all the shares into that bin
        for entry in orderBook:
                if np.isnan(float(entry)):
                    if debug_level == "P":
                        raise Exception("Invalid entry in the orderbook: " + str(entry))
                    elif debug_level == "D":
                        print("Invalid entry in the orderbook: " + str(entry))
                try:
                    #Calculate which bin entry should fall into
                    i = int((highValue-float(entry))/stepSize)
                except Exception as e:
                    if debug_level == "D" or debug_level == "W":
                        print("Couldn't bin this one: " + str(entry) + "... params (high/num/stepSize):", highValue, numBins, stepSize)
                    continue

                #If we're including everything, drop prices outside bin into highest/lowest bins
                if includeAllPrices:
                    if i < 0:
                        i = 0
                    if i > numBins - 1:
                        i = numBins - 1
                    partitions[i] += orderBook[entry][0]

                #Otherwise, just skip ones that fall outside the range
                else:
                    if i >= 0 and i <= numBins - 1:
                        partitions[i] += orderBook[entry][0]

        return partitions

    #Bins the current orderbook based on the highest bin values (highValue), numBins, and the stepSize for each bin
    def binCurrent(self, highValue, numBins, stepSize):
        return orderBook.binOrderBook(self.orderBook, highValue, numBins, stepSize, self.debug_level)

    #Automatically bin all spreads in self.savedOrderBooks
    #Takes the price step size and the totalPriceRange (the amount between high and low) to use for the binning process
    def autoBinAllSavedSpreads(self, priceStepSize, totalPriceRange, includeAllPrices=True, verbose=False, variableCenter=False):
        if len(self.savedOrderBooks) == 0:
            if self.debug_level == "P":
                raise Exception("No saved order books in savedOrderBooks to bin...")

        length = len(self.savedOrderBooks)
        if (self.debug_level == "D" or self.debug_level == "W") and verbose:
                print("Prepping bins...")

        numBins = totalPriceRange/priceStepSize
        binnedData = []

        if variableCenter == False:

            #Make a first pass through the whole saved spreads to see how our bins should be aligned
            #Save the spreads that are valid from the range into spreads below
            spreads = []
            for o in self.savedOrderBooks:
                s = orderBook.getSpreadFromOrderbook(o, bids=self.bids, asks=self.asks)
                #Make sure the spread is valid
                #If it's not valid, toss it out for the purposes of calculation
                if s[0] is not None and not np.isnan(s[0]) and s[0] != np.inf and s[0] != -np.inf and s[1] is not None and not np.isnan(s[1]) and s[1] != np.inf and s[1] != -np.inf:
                    spreads.append(s)
                else:
                    if self.debug_level == "W":
                        print("A calculated spread was non-finite:", s)
            center = np.mean(spreads) #Find the center of where the spreads were to set the center of the bins

            #Set the number of bins and the high value for the bins
            highValue = center + numBins/2 * priceStepSize
            highValues = [highValue] * len(self.savedOrderBooks)

            if (self.debug_level == "D" or self.debug_level == "W") and verbose:
                    print("Binning...")
                    print("High Value:", highValue)
                    print("Num Bins:", numBins)
                    print("Price Step Size:", priceStepSize)
        else:
            spreads = []
            highValues = []
            if (self.debug_level == "D" or self.debug_level == "W") and verbose:
                    print("Binning...")

        #Bin each saved order book and add it to binnedData to create a 2-D array of bins
        for o in self.savedOrderBooks:
            if variableCenter == False:
                binnedData.append(orderBook.binOrderBook(o, highValue, numBins, priceStepSize, includeAllPrices, self.debug_level))
            else:
                s = orderBook.getSpreadFromOrderbook(o, bids=self.bids, asks=self.asks)
                spreads.append(s)
                center = np.mean(s)
                highValue = center + numBins/2 * priceStepSize
                highValues.append(highValue)
                binnedData.append(orderBook.binOrderBook(o, highValue, numBins, priceStepSize, includeAllPrices, self.debug_level))

        return binnedData, spreads, highValues

    #Multiprocessing-enabled function to automatically bin all saved spreads
    #Threads is the keyword argument for how many processes to use
    #Otherwise functions virutally identically to autoBinAllSavedSpreads
    def fastAutoBinAllSavedSpreads(self, priceStepSize, totalPriceRange, includeAllPrices=True,
                                   verbose=False, threads=8, variableCenter=False):

        #TODO: Test this. I may have broken it when I added the capability to do variableCenters.
        # The variableCenter capability is not tested at all.

        if not variableCenter:
            if verbose:
                    print("Prepping bins...")

            #Prep the variables that need to be shared among the processes
            length = len(self.savedOrderBooks)
            centers = Array('d', int(length))
            highSpreads = Array('d', int(length))
            lowSpreads = Array('d', int(length))

            queue = Queue()

            # Put each orderbook into the queue
            # Warning: having too many items in the queue can result in a bug where the queue does
            # not work, it breaks the pipe, etc.
            for ii in range(0, length):
                    queue.put(ii)

            #First pass: find the centers of the data.
            #This calls __centersMP to calculate the centers.
            processes= []
            for i in range(threads):
                    queue.put('DONE')
                    t = Process(target=orderBook.__centersMP,
                                args=[queue, centers, highSpreads, lowSpreads, self.savedOrderBooks,
                                      self.debug_level])
                    processes.append(t)
                    t.start()

            #End the processes when they are finished
            for i in range(threads):
                    processes[i].join()

            centers = np.frombuffer(centers.get_obj())

            #Get the center of the spreads that were calculated by the first pass
            #Ignore any centers with a value of -1.0, as this was the error code
            center = np.mean(centers[np.where(centers != -1.0)])

            #Calculate the number of bins and the value of the highest bin
            numBins = totalPriceRange/priceStepSize
            highValue = center + numBins/2 * priceStepSize

            if verbose:
                    print("Binning...")

            #Create a shared memory numpy array
            shared_array_base = Array('d', int(numBins*length))
            shared_array = np.ctypeslib.as_array(shared_array_base.get_obj())
            shared_array = shared_array.reshape(numBins, length)

            queue = Queue()

            for ii in range(0, length):
                    queue.put(ii)

            #Second pass: bin all the orderbooks with parameters calculated by first pass
            #Calls __binnerMP as the helper function
            processes= []
            for i in range(threads):
                    queue.put('DONE')
                    t = Process(target=orderBook.__binnerMP,
                                args=[queue, shared_array, self.savedOrderBooks, highValue, numBins,
                                      priceStepSize, includeAllPrices, self.debug_level])
                    processes.append(t)
                    t.start()

            #Terminate the threads when they finish
            for i in range(threads):
                    processes[i].join()

            #Collapse the spreads into a numpy array
            spreads = np.array([(highSpreads[i], lowSpreads[i]) for i in range(length)])

            return shared_array, spreads, [highValue]*length

        else:
            if verbose:
                    print("Prepping bins...")

            #Prep the variables that need to be shared among the processes
            length = len(self.savedOrderBooks)
            centers = Array('d', int(length))
            highSpreads = Array('d', int(length))
            lowSpreads = Array('d', int(length))
            highValues = Array('d', int(length))

            queue = Queue()

            #Calculate the number of bins and the value of the highest bin
            numBins = totalPriceRange/priceStepSize

            if verbose:
                    print("Binning...")

            #Create a shared memory numpy array
            shared_array_base = Array('d', int(numBins*length))
            shared_array = np.ctypeslib.as_array(shared_array_base.get_obj())
            shared_array = shared_array.reshape(numBins, length)

            for ii in range(0, length):
                    queue.put(ii)

            #Bin all the orderbooks with parameters (Note: only one pass here)
            #Calls __binMP as the helper function
            processes= []
            for i in range(threads):
                    queue.put('DONE')
                    t = Process(target=orderBook.__binMP,
                                args=[queue, shared_array, highSpreads, lowSpreads, highValues,
                                      self.savedOrderBooks, numBins, priceStepSize,
                                      includeAllPrices, self.debug_level])
                    processes.append(t)
                    t.start()

            #Terminate the threads when they finish
            for i in range(threads):
                    processes[i].join()

            #Collapse the spreads into a numpy array
            spreads = np.array([(highSpreads[i], lowSpreads[i]) for i in range(length)])

            return shared_array, spreads, highValues
    
    '''
    #Helper method for fastAutoBinAllSavedSpreads to bin all the orderbooks
    @staticmethod
    def __binnerMP(queue, binnedData, orderBooks, highValue, numBins, priceStepSize, includeAllPrices, debug_level):
        while True:
                msg = queue.get()
                if (msg == 'DONE'):
                    break
                else:
                    msg = long(msg)
                binnedData[:, msg] = orderBook.binOrderBook(orderBooks[msg], highValue, numBins, priceStepSize, includeAllPrices, debug_level)

    #Helper method used by fastAutoBinAllSavedSpreads to detect the center of all the spreads for the time frame
    #Helps align the bins correctly
    @staticmethod
    def __centersMP(queue, centers, highSpreads, lowSpreads, orderBooks, debug_level):
        while True:
                msg = queue.get()
                if (msg == 'DONE'):
                    break
                else:
                    msg = long(msg)
                s = orderBook.getSpreadFromOrderbook(orderBooks[msg])

                #Ignore "bad" spread entries so it does not corrupt the whole thing
                if s[0] is not None and not np.isnan(s[0]) and s[0] != np.inf and s[0] != -np.inf and s[1] is not None and not np.isnan(s[1]) and s[1] != np.inf and s[1] != -np.inf:
                    #Put the resulting values to the arrays in the msg location
                    highSpreads[msg] = s[0]
                    lowSpreads[msg] = s[1]
                    centers[msg] = np.average(s)
                else:
                    #The -1.0 signifies an error
                    highSpreads[msg] = -1.0
                    lowSpreads[msg] = -1.0
                    centers[msg] = -1.0
                    if debug_level == "W":
                        print "A calculated spread was non-finite:", s

    @staticmethod
    def __binMP(queue, binnedData, highSpreads, lowSpreads, highValues, orderBooks, numBins, priceStepSize, includeAllPrices, debug_level):
        while True:
                msg = queue.get()
                if (msg == 'DONE'):
                    break
                else:
                    msg = long(msg)
                o = orderBooks[msg]
                s = orderBook.getSpreadFromOrderbook(o)
                center = np.mean(s)
                highValue = center + numBins/2 * priceStepSize
                highSpreads[msg] = s[0]
                lowSpreads[msg] = s[1]
                highValues[msg] = highValue
                binnedData[:, msg] = orderBook.binOrderBook(o, highValue, numBins, priceStepSize, includeAllPrices, debug_level)
    '''

    ################################
    ##########  ANALYSIS  ##########
    #########  SPREAD, ETC  ########
    ################################

    #Pass bids and asks to speed this up
    @staticmethod
    def getSpreadFromOrderbook(orderBook, bids=None, asks=None):
        '''
        Gets the spread from the orderBook that is passed.
        Arguments:
            - orderBook : the orderBook DICTIONARY (not orderBook class)
            - bids (dictionary, optional) : the dictionary of the bids side. Speeds up calculation dramatically if present with asks
            - asks (dictionary, optional) : the dictionary of the asks side. Speeds up calculation dramatically if present with bids

        Returns:
            (spreadHigh, spreadLow) : A tuple of floats, equivalent to (best ask, best bid), or (lowest ask, highest bid)
        '''

        #Optimized version, if bids and asks are present
        if bids is not None and asks is not None:
            # print(bids)
            # print(asks)
            spreadLow = np.amax(bids.keys())
            spreadHigh = np.amin(asks.keys())
            # print(spreadLow)
            # print(spreadHigh)

            # Check the type.  Is it a string?
            # Check the exexute message processing

            #Catch obvious errors
            if spreadLow > spreadHigh:
                raise Exception("spreadLow higher than spreadHigh (non-empty): " + str(spreadLow) + "," + str(spreadHigh))

            return spreadHigh, spreadLow

        #Otherwise, we need to do it the old fashioned way
        #Split the orderbook into bids and asks, then find the best offers on both sides
        else:
            #print "Since bids and asks are not defined, getSpreadFromOrderbook will be much slower."
            #initial points
            lowSell = np.inf
            highBuy = -np.inf

            highBuy = np.amax([ii for ii in orderBook if orderBook[ii][1] == "B"])
            lowSell = np.amin([ii for ii in orderBook if orderBook[ii][1] == "S"])

            spreadHigh, spreadLow = lowSell, highBuy
            
            # Catch obvious errors
            if spreadLow > spreadHigh:
                raise Exception("spreadLow higher than spreadHigh: " + str(spreadLow) + "," + str(spreadHigh))

            return spreadHigh, spreadLow

    def getCurrentMidpoint(self):
        '''
        Returns the current midpoint.
        Arguments: 
            - self
        Returns:
            midpoint (float) : The midpoint of the current orderBook, which is equivalent to (best ask + best bid) * 0.5. If it was unsuccessful, returns -1 as an error code.
        '''
    
        try:
            return np.mean(orderBook.getSpreadFromOrderbook(self.orderBook, bids=self.bids, asks=self.asks))
        except:
            print("Couldn't calculate the midpoint.")
            raise Exception("Couldn't calculate the midpoint.")
            return -1

    #Returns the spread for the current orderbook
    def getCurrentSpread(self):
        return orderBook.getSpreadFromOrderbook(self.orderBook, bids=self.bids, asks=self.asks)

    def getDailyVolatility(self, start_time=34200, end_time=57600, time_delta=15*60): #time_delta defaults to 15 minutes
        midpoints = []
        t = start_time
        while t < end_time:
            self.processToTime(t)
            midpoints.append(self.getCurrentMidpoint())
            t += time_delta

        midpoints = np.array(midpoints)
        returns = (midpoints[1:] - midpoints[:-1])/midpoints[:-1]
        return np.sum(returns**2)

    #Calculate the total volume or orders on the book
    @staticmethod
    def getVolumeOnOrderbook(orderBook):
        totalVolume = 0
        for entry in orderBook:
            totalVolume += orderBook[entry][0]

        return totalVolume

    #Apply function f to all the saved orderBooks
    def applyToAllSavedOrderBooks(self, f):
        results = []
        for o in self.savedOrderBooks:
            results.append(f(o))
        return results


    @staticmethod
    def orderBookToArray(orderBook, window=None, window_pct=None, window_value=None, sparse=False, aggregate=False):
        '''
        Takes an orderbook dictionary and converts it to a numpy array with the values of the array equal to the depth at that price
        The function takes in an orderbook and an argument about how to construct the start and end of the array (to avoid an excessively long array)
        window argument should be a tuple with the minimum and maximum value for the window to create the array from
        window_pct should be a percentage value, which will construct a window at +/- % around the center of the spread of the orderBook
        window_value should be a float, giving an exact number. The window then is (center - window_value, center + window_value), where center is the center of the spread
        The sparse argument allows for the vectors to be stored as sparse (compressed) vectors (not yet implemented)
        The method returns the numpy array which has value equal to the depth and also the minimum price value that the array holds (the price that element 0 is at)
        
        Arugments:
            - orderBook : The orderBook DICTIONARY to convert to an array
            - window : window of ABSOLUTE prices
            - window_pct : window of percent values around midpoint of orderBook
            - window_value : window of RELATIVE prices (relative to midpoint)
            - spares : Currently unsupported
            - aggregate : Whether to aggregate the shares on both sides of the spread.

        Returns:
            - depth [or agg_depth] (an array of ints) : the number of shares on the book at that value of the book (or, for agg_depth, the aggregated number of shares)
            - mi (float) : The price that the index 0 of depth/agg_depth represents.
            - spread (tuple of floats) : the results of orderBook.getSpreadFromOrderBook, which is a tuple with (best ask, best bid) in it.
        '''
        #TODO: support for sparse matrices
        if (window is not None) + (window_pct is not None) + (window_value is not None) > 1:
            raise Exception("Only one argument must be specified out of window, window_pct, and window_value.")

        spread = orderBook.getSpreadFromOrderbook(orderBook)

        if window is not None:
            cent = round(np.mean(spread), 2)
            mi = round(window[0], 2)
            ma = round(window[1], 2)

        elif window_pct is not None:
            cent = round(np.mean(spread), 2)
            mi = round(cent - cent*window_pct, 2)
            ma = round(cent + cent*window_pct, 2)

        elif window_value is not None:
            cent = round(np.mean(spread), 2)
            mi = round(cent - window_value, 2)
            ma = round(cent + window_value, 2)

        else:
            raise Exception("Either window, window_pct, or window_value must be specified.")

        if np.isnan(mi) or np.isnan(ma) or np.isnan(cent):
            raise Exception("mi, ma, or center is invalid: " + str(mi) + "/" + str(ma) + "/" + str(cent))

        depth = np.zeros(int(round((ma - mi) * 100, 0))+1)
        for j in range(len(depth)):
            val = round(j * 0.01 + mi, 2)
            if val in orderBook:
                depth[j] = orderBook[val][0]
            else:
                depth[j] = 0

        if not aggregate:
            return depth, mi, spread

        else:
            agg_depth = depth.copy()

            #TODO-- I sped up this aggregation somewhat, but Cython might be needed if this is still too slow.

            #upper half.
            start_pos = int((cent - mi) * 100)
            agg_depth[start_pos] = 0
            for i in range(start_pos + 1, len(agg_depth)):
                agg_depth[i] = agg_depth[i-1] + depth[i]

            #lower half.
            start_pos = int((cent - mi) * 100)
            agg_depth[start_pos] = 0
            for i in range(start_pos - 1, -1, -1):
                agg_depth[i] = agg_depth[i+1] + depth[i]

            return agg_depth, mi, spread


    @staticmethod
    def getSlopeOfOrderbook(orderBook, calc_pts):
        '''
        calc_pts are the percentage increases from the center of the spread where the slope should be calculated
        for example, if it is .5 and 2 then a line is drawn from the spread center (zero volume) to the y value of the aggregate orderbook depth at the x value which is
        .5% above the center value and the slope is reported. Another line is drawn from the spread center to the y value of the aggregate depth at the x value + 2% of the center
        The percents should be positive, they are mirrored onto the other side.

        Arguments:
            - orderBook : the orderBook DICTIONARY
            - calc_pts : the percentages away from the midpoint that we want to use as the points to calculate the slope

        Returns:
            - upper_half : the slope from the midpoint to the points described by pts, where the percentages are above the midpoint
            - lower_half : the slope from the midpoint to the points described by pts, where the percentages are below the midpoint
            - agg_depth : the agg_depth array returned by orderBookToArray() with aggregate=True.
            - mi : the price corresponding to the index 0 element of agg_depth
        '''

        max_slope_pct = max(calc_pts) #set window_pct to be the largest percent away from the center that is needed
        agg_depth, mi, spread = orderBook.orderBookToArray(orderBook, window_pct=max_slope_pct, aggregate=True)
        center = np.mean(spread) #center of the spread
        upper_half = []
        lower_half = []

        for p in calc_pts:
            #upper part
            point = center + (center * p) #this is the price that is p % away from the center of the spread
            pos = int((point - mi)*100) #position in the agg_arr array
            if pos == len(agg_depth): #catch errors
                pos -= 1
            delta_x = point - center
            delta_y = agg_depth[pos] # - 0, since the depth at the center of the spread should be zero
            slope = delta_y/float(delta_x)
            upper_half.append(slope)

            #lower part
            #same as above
            point = center - (center * p)
            pos = int((point - mi)*100)
            if pos < 0:
                pos = 0
            delta_x = point - center
            delta_y = agg_depth[pos] # - 0, since the depth at the center of the spread should be zero
            slope = delta_y/float(delta_x)
            lower_half.append(slope)

        return np.array(upper_half), np.array(lower_half), agg_depth, mi


    #This method is not particularly fast... I've tried my best to optimize it, but there is potential to further optimize it, maybe not in Python
    def book_to_pct_bins(self, bid_split_points, ask_split_points, message_side="X"):
        '''
        book_to_pct_bins is the function currently used for shape regressions. It takes the orderbook and breaks it into multiple bins, as specified by bid_split_points and ask_split_points.
        It then returns the percentage of the total shares in each of these bins.
    
        Arguments:
            - bid_split_points (float array) : floats that describe the percentage breakpoints from the midpoint. 
                    Example: [0.01, 0.05] would give the percent of all shares on the book in [midpoint, midpoint * 0.99], [midpoint * 0.99, midpoint * 0.95], and [midpoint * 0.95, 0]
            - ask_split_points (float array) : floats that describe the percentage breakpoints from the midpoint for the ask side. 
                    Example: [0.01, 0.05] would give the percent of all shares on the book in [midpoint, midpoint * 1.01], [midpoint * 1.01, midpoint * 1.05], and [midpoint * 1.05, inf]
            - message_side (char, "B"/"S") : the side the last message affected. If you are processing one message at a time, then this can be used and the function will only recalculate
                    the side that was affected (the side that was not affected will not be changed). But if you are processing multiple messages, then unless you can guarantee that only
                    one side has been changed, then you should not give a value here, as it will fail to calculate one side.

        Returns: 
            - bid_outbins : the percentage of total orders occuring between the bins specified by bid_split_points
            - ask_outbins : the percentage of total orders occuring between the bins specified by ask_split_points


        NOTE: THE MESSAGE_SIDE ARGUMENT SHOULD BE USED WITH CAUTION. IF THE SIDE IS SPECIFIED, IT ASSUMES THAT ONLY THAT SIDE SHOULD BE RECALCULATED. THUS, THE RESULTS COULD BE STALE FOR
              THE SIDE NOT GIVEN IF IT HAS CHANGED.
        
        '''

        bids = self.bids
        asks = self.asks

        #If there are no bids or no asks, then we're just going to return all zeros
        if len(bids) == 0 or len(asks) ==0 :
            return np.zeros(len(bid_split_points)+1,dtype=np.float32), np.zeros(len(ask_split_points)+1,dtype=np.float32)

        #If this is the first time running book_to_pct_bins, we need to initialize the saved previous result values to be 0
        if self.previous_bids_results is None:
            self.previous_bids_results = np.zeros(len(bid_split_points)+1,dtype=np.float32)
        if self.previous_asks_results is None:
            self.previous_asks_results = np.zeros(len(ask_split_points)+1,dtype=np.float32)

        #If we only need to update the bid side
        if message_side == "B":
            bid_prices = np.array(bids.keys()) #get the bid prices
            best_bid = np.amax(bid_prices)     

            bid_shares = get_shares_from_dictionary(bids)  # Use the fast Cython method to pull out the number of shares from the dictionary.
            #Now we have a list of prices, and we have a list of shares associated with those prices. They are in the same order.

            bid_cutoffs = [ best_bid*(1 - float(ii)) for ii in bid_split_points ][::-1] #cutoffs are determined by the pct values passed in. Then we reverse them so it's monotonically increasing
            bid_bins = np.digitize(bid_prices,bid_cutoffs)      # digitize bins them fast using numpy. Now bid_bins is an array in the same order as bid_prices, but bid_bins has an integer designating which bin the price is associated with.

            bid_outbins = np.zeros(len(bid_split_points)+1,dtype=np.float32) # initialize
            for ii in range(len(bid_split_points)+1):  #now loop through the bins...
                bid_outbins[ii] = np.sum(bid_shares[bid_bins==ii]) # add up the number of shares for all the bids that are in each bin

            bid_outbins /= np.sum(bid_outbins)  # Normalize
            self.previous_bid_results = bid_outbins  # Save result for next time.
            
            #We're going to reverse the results here (because that's how we've been doing it). It puts them back in the correct order (smallest pcts to largest pcts)
            return bid_outbins[::-1], self.previous_asks_results[::-1]

        #Only update the ask side... essentially the same as above except the opposite side (and we don't have to worry about the reversing thing)
        elif message_side == "S":
            ask_prices = np.array(asks.keys())
            best_ask = np.amin(ask_prices)

            ask_shares = get_shares_from_dictionary(asks)
            ask_cutoffs = [best_ask*(1 + float(ii)) for ii in ask_split_points]
            ask_bins = np.digitize(ask_prices,ask_cutoffs)

            ask_outbins = np.zeros(len(ask_split_points)+1,dtype=np.float32)
            for ii in range(len(ask_split_points)+1):
                ask_outbins[ii] = np.sum(ask_shares[ask_bins==ii])

            ask_outbins /= np.sum(ask_outbins)
            self.previous_ask_results = ask_outbins
            return self.previous_bids_results, ask_outbins

        else:
            '''
            If you don't know what the last message was, then we need to run both sides because we don't know which has changed.
            Essentially this is the same as above, except we have to redo both sides.
            '''

            #BID
            bid_prices = np.array(bids.keys())
            best_bid = np.amax(bid_prices)

            bid_shares = get_shares_from_dictionary(bids)
            bid_cutoffs = [ best_bid*(1 - float(ii)) for ii in bid_split_points ][::-1]
            bid_bins = np.digitize(bid_prices,bid_cutoffs)

            bid_outbins = np.zeros(len(bid_split_points)+1,dtype=np.float32)
            for ii in range(len(bid_split_points)+1):
                bid_outbins[ii] = np.sum(bid_shares[bid_bins==ii])

            bid_outbins /= np.sum(bid_outbins)

            #ASK
            ask_prices = np.array(asks.keys())
            best_ask = np.amin(ask_prices)

            ask_shares = get_shares_from_dictionary(asks)
            ask_cutoffs = [best_ask*(1 + float(ii)) for ii in ask_split_points]
            ask_bins = np.digitize(ask_prices,ask_cutoffs)

            ask_outbins = np.zeros(len(ask_split_points)+1,dtype=np.float32)
            for ii in range(len(ask_split_points)+1):
                ask_outbins[ii] = np.sum(ask_shares[ask_bins==ii])

            ask_outbins /= np.sum(ask_outbins)

            return bid_outbins[::-1], ask_outbins


    @staticmethod
    def calculateNewSpreadAfterTrade(orderBook, vol, starting_pct = .01, debug=False):
        '''
        Calculates what the spread would be if a trade of vol was executed on both sides of the spread.
        This initially arose from trying to calculate the CRT, but it is not the same as the CRT.
        starting_pct is the window_pct to try the first time when the orderBook is converted into an array.
        If the needed values are outside this window, it is increased until it is found.

        Arguments:
            - orderBook : the orderBook DICTIONARY
            - vol : the volume of shares that will be "traded" (simulated as if they are executed)
            - starting_pct (optional) : determines how much of the orderBook will be converted to array format for the first guess. If it's too low, the method will choose a larger value and try again until it works.
        
        Returns:
            - newSpread : the new spread amount after vol has been executed
            - newUpper : the new best ask after vol has been executed
            - newLower : the new best bid after vol has been executed

        NOTE: Not currently used by any project stuff.
        '''

        s = orderBook.getSpreadFromOrderbook(orderBook)
        center = np.mean(s)

        upper = 0 #what the new best ask will be
        lower = 0 #what the new best bid will be

        pct = starting_pct # how much of the orderbook to convert to an array

        while True: #just keep going

            agg_depth, mi, spread = orderBook.orderBookToArray(orderBook, window_pct=pct, aggregate=True) #convert to array

            if len(agg_depth) > 100000:
                raise Exception("Searching too much of the orderbook. Possibly trades of vol are too large for the orderBook.")

            center = np.mean(spread)
            half = len(agg_depth)/2  # this is the middle of the array of the orderbook
            hits = np.where(agg_depth > vol)[0]  # this will return the indices of any point where the agg_depth is greater than the volume desired to be traded.
            
            if len(hits) == 0: #if nowhere in the array had a large enough agg_depth, then we need to search a larger section of the orderbook
                if debug:
                    print("Expanding search...")
                pct = pct*2     #try again

            else:
                uppers = hits[np.where(hits > half - 1)[0]] #uppers are only those viable indices on the upper half (asks)
                if len(uppers) == 0: #if there were no asks that are viable, then try again with a larger window
                    if debug:
                        print("Expanding search...")
                    pct = pct*2
                else:  
                    upper = np.min(uppers) #upper is the best ask
                    lowers = hits[np.where(hits < half + 1)[0]]  # get viable bids
                    if len(lowers) == 0: #if there are none, try again
                        if debug:
                            print("Expanding search...")
                        pct = pct*2
                    else:       #other wise, record lower and finish
                        lower = np.max(lowers)
                        break

        newSpread = round((upper - lower)/100.0, 2)
        newUpper = mi + upper/100.0
        newLower = mi + lower/100.0
        return newSpread, newUpper, newLower

    @staticmethod
    def CRT(orderBook, D):
        '''
        Calculate the Cost of Round Trip (CRT). Maybe useful one day, but we're not using it now.
        It calculates what the cost would be (in percentage terms) of trading D dollars of the security right now (buying then selling)
        It referenced a paper, but I can't remember it off the top of my head. But I've debugged this and I believe it is accurate.
        '''

        spreadHigh, spreadLow = orderBook.getSpreadFromOrderbook(orderBook, orderBook.bids, orderBook.asks)

        bid_prices = orderBook.bids.keys()
        bid_shares = get_shares_from_dictionary(orderBook.bids)
        bid_arr = np.array([bid_prices, bid_shares]).T
        bid_arr = bid_arr[bid_arr[:,0].argsort()] #sort the whole array by the first column (price), ascending

        ask_prices = orderBook.asks.keys()
        ask_shares = get_shares_from_dictionary(orderBook.asks)
        ask_arr = np.array([ask_prices, ask_shares]).T
        ask_arr = ask_arr[ask_arr[:, 0].argsort()]
        def Buying(shares, arr):
            sharesRem = shares
            cost = 0
            idx = 0
            while sharesRem > 0 and idx < len(arr):
                if arr[idx, 1] <= sharesRem:
                    cost += arr[idx, 1] * arr[idx, 0]
                    sharesRem -= arr[idx, 1]
                    idx +=1
                else:
                    cost += sharesRem * arr[idx, 0]
                    sharesRem = 0
            if idx == len(arr):
                return np.inf
            return cost

        def Selling(shares, arr):
            sharesRem = shares
            cost = 0
            idx = len(arr) - 1
            while sharesRem > 0 and idx >= 0:
                if arr[idx, 1] <= sharesRem:
                    cost += arr[idx, 1] * arr[idx, 0]
                    sharesRem -= arr[idx, 1]
                    idx -= 1
                else:
                    cost += sharesRem * arr[idx, 0]
                    sharesRem = 0
            if idx < 0:
                return -np.inf
            return cost

        TD = (2*D) / float(spreadHigh + spreadLow)

        selling = Selling(TD, bid_arr)
        buying = Buying(TD, ask_arr)

        if selling == -np.inf or buying == np.inf:
            raise Exception("Unable to calculate the CRT for this D value because the orderbook is too thin.")

        return (buying - selling)/float(D)

    def aggSharesAtPrice(self, p, p0):
        '''
        Given a price p and the midpoint value p0, this method tell what the aggregate orderBook depth is at p (p can be either a bid or ask price).

        Arguments:
            - p : the price at which to calculate the aggregate depth.
            - p0 : the current midpoint price

        Returns:
            - cumshares : the aggregated number of shares offered for price p or better. 

        EXAMPLE:
            If the orderbook had a current midpoint of $5.00, with 100 shares offered at $5.10, 200 shares at $5.15, and 50 shares at $5.17, the 
            cumshares at $5.10 is 100, at $5.12 is 100, at $5.16 is 300, and at $5.20 is 350. This only counts the asks, since the p values were greater than p0. 
            If p < p0, then it would have considered all the bids.
        '''

        if p > p0:
            prices =list(self.asks.keys())
            shares = get_shares_from_dictionary(self.asks)

            cumshares = 0
            for i in range(len(prices)):
                if prices[i] <= p:
                    cumshares += shares[i]

            return -cumshares

        else:
            prices = list(self.bids.keys())
            shares = get_shares_from_dictionary(self.bids)

            cumshares = 0
            for i in range(len(prices)):
                if prices[i] >= p:
                    cumshares += shares[i]

            return cumshares

    def getVariablesForGMM(self, prices, time_step, relative_prices=True, time_window=(34200, 57600)):
        '''Returns the necessary variables for GMM estimation. Currently under development.

        Args:
            prices (float array): A list of prices (p values in the estimation)
            time_step (numeric): The duration between each observation
            relative_prices (boolean): Whether the prices array are absolute or are relative and should be adjusted according to the day's average midpoint. Default is True.
            trading_day_only (boolean): Determines whether observations start and end at 34200 and 57600, or should span the whole day.

        Returns:
            A (matrix): A matrix corresponding to the observations of the variables with each column representing one p from prices and each row an observation
            B (matrix): A matrix corresponding to the observations of the variables with each column representing one p from prices and each row an observation
            C (matrix): A matrix corresponding to the observations of the variables with each column representing one p from prices and each row an observation 
            D (matrix): A matrix corresponding to the observations of the variables with each column representing one p form prices and each row an observation
        '''

        K = len(prices)

        start_time = time_window[0]
        end_time = time_window[1]

        N = (end_time - start_time)/time_step #NOTE: The denominator is not a float, so this will cut off so the end is always <= end_time if it does not divide evenly.

        if relative_prices is True:
            self.processAndSaveInterval(start_time, end_time, N)
            midpoints = []
            for obo in self.savedOrderBooks:
                midpoints.append(np.mean(orderBook.getSpreadFromOrderbook(obo)))

            midpoint = np.mean(midpoints)
            prices = prices + midpoint
            self.reset()

        times = np.linspace(start_time, end_time, N)

        #print("Running GMM from " + str(times[0]) + " to " + str(times[-1])  + " on " + str(len(prices)) + " prices from " + str(np.min(prices)) + " to " + str(np.max(prices)) + ".")

        #Initialize empty matrices that will eventually hold all the values we need.
        delta_y = np.empty((N-1, K))
        p_p0 = np.empty((N-1, K))
        p_p0_prime = np.empty((N-1, K))
        p0_p0_prime = np.empty((N-1, K))
    
        self.processToTime(times[0]) #Process to first time

        p0 = self.getCurrentMidpoint() #we need to keep track of the previous midpoint
        y = np.array([self.aggSharesAtPrice(p, p0) for p in prices])  # and also the previous y array

        for idx, t in enumerate(times[1:]):  # keep processing for all the times
            self.processToTime(t)
            
            p0_prime = self.getCurrentMidpoint()

            p_p0[idx] = np.array([p - p0 for p in prices])
            p_p0_prime[idx] = np.array([p - p0_prime for p in prices])

            y_prime = np.array([self.aggSharesAtPrice(p, p0_prime) for p in prices])

            delta_y[idx] = y_prime - y
            p0_p0_prime[idx] = np.array([p0 - p0_prime for p in prices])

            #save the results as the previous results to be used for next time
            p0 = p0_prime
            y = y_prime

        return delta_y, p_p0_prime, p_p0, p0_p0_prime


    def GMM(self, prices, time_delta, relative_prices=True, time_window=(34800, 57000)):
        '''
        Performs GMM estimation of the slope (R coefficient).
        
        Arguments:
            - prices (array of floats) : list of prices at which to sample
            - time_delta (float) : some amount of time between samples
            - relative_prices : if True, prices are added to the average midpoint over the whole time so they become relative to the security being measured and not absolute
            - time_window (tuple of floats) : the start and end times in a tuple
    
        Returns:
            - slope_val : estimate of R
            - std_error : the standard error of the estimate for R
            - J : the J-Statistic
            - deg_freedom : the degrees of freedom of the J statistic
            - pvalue: the p-value of the J-statistic when compared to a Chi-square distribution with deg_freedom degrees of freedom.

        '''
        prices = np.array([np.round(p,2) for p in prices])
        A, B, C, D = self.getVariablesForGMM(prices, time_delta, time_window=time_window, relative_prices=relative_prices)
        # A = Y' - Y
        # B = P - P_0'
        # C = P - P_0
        # D = P_0 - P_0'

        N, K = C.shape
        iters = 5

        W = [np.eye(K)]
        R = [100.0]

        def get_moments(R):
            return A - 2*R*B + 2*R*C

        def f(R, W):
            moments = get_moments(R)
            moments = np.mean(moments, axis=0)
            return np.dot(np.dot(moments.T, W), moments)

        for iter in range(iters):   # ITERATE iters TIMES
            sol = minimize(f, R[-1], args=(W[-1],), method='Nelder-Mead')  # MINIMIZE LOSS FUNCTION WITH PREVIOUS SOLUTION AS THE INITIAL GUESS, USING PREVIOUS WEIGHTING MATRIX
            R.append(sol['x'][0].copy())  # ADD SOLUTION (SCALAR) TO END OF LIST 'R'
            moments = get_moments(R[-1])   # GET THE MOMENTS USING THE NEW R
        
        weighting_matrix = np.empty((K, K))  # SETUP EMPTY WEIGHTING MATRIX
        
        for i in range(N):   # FOR EACH OBSERVATION OF MOMENTS...
            single_mat = np.outer(moments[i], moments[i])  # GET THE OUTER PRODUCT OF THOSE MOMENTS
            weighting_matrix += single_mat  # ADD IT TO THE NEW, EMPTY WEIGHTING MATRIX

        weighting_matrix /= float(N)  # AVERAGE THE MATRIX
        weighting_matrix = np.linalg.inv(weighting_matrix)  # INVERT
        W.append(weighting_matrix.copy())  # SAVE THIS NEW MATRIX TO THE END OF THE LIST 'W'

        slope_val = R[-1] #PRINTS SIG_U/SQRT(SIG_0), SINCE ACTUALLY R IS 0.5 * (SQRT(SIG_0)/SIG_U)
        J = N * f(R[-1], W[-1])
        deg_freedom = K - 1
        G = np.mean(2*D, axis=0)  # DELTA METHOD FOR SLOPE ESTIMATION
        std_error = np.dot(np.dot(G.T, W[-1]), G)  #STANDARD ERROR CALCULATION USING DELTA METHOD
        pvalue = 1-chi2.cdf(J, deg_freedom)
        return slope_val, std_error, J, deg_freedom, pvalue

    @staticmethod
    def splitOrderbookIntoSides(orderBook):
        '''
        Takes and orderBook dictionary and returns it as just asks and bids in a dictionary format, plus the spread (in a tuple)
        Arguments:
            - orderBook : the orderBook DICTIONARY
        Returns:
            - asks : a dictionary in the same format as orderBook but only with asks (side == "S")
            - bids : a dictionary in the same format as orderBook but only with bids (side == "B")
            - s : a tuple of the spread in the format (best ask, best bid)
        '''

        asks = { ii: orderBook[ii] for ii in orderBook.keys() if orderBook[ii][1] == "S" }
        bids = { ii: orderBook[ii] for ii in orderBook.keys() if orderBook[ii][1] == "B" }
        s = orderBook.getSpreadFromOrderbook(orderBook)
        return asks, bids, s

    ################################
    ########### HEATMAPS ###########
    ################################
    '''
    All this stuff is really interesting, but is not currently used by our projects. This was used for some data visualization.
    It was also hooked up to a webserver for a while, which was pretty cool. You could give it parameters and it would produce
    a heatmap visualization on the screen.
    When it was hooked up to a computer with 16 cores, it could render large heatmaps very quickly (5 seconds or so).
    The supercomputer makes this more difficult, but maybe it's still possible.

    I'm going to leave all of this here for legacy purposes, but it isn't actively being used much. The code is fairly well
    commented, so it shouldn't be too hard to use again.

    Also, Scott Condie should have some examples of it being used with the webserver saved somewhere if you need to consult
    something for reference...
    '''

    #Generates a heatmap from binned data
    #Just fixes the heatmap data and scales it properly
    @staticmethod
    def generateHeatmapFromBinnedData(data, transpose=True, trim=True, blackOnWhite=True):
        data = np.array(data)
        if transpose:
                data = data.T
        if trim:
                heatmap = data[1:-1]

        heatmap = heatmap.astype(np.float64)
        heatmap = heatmap + 0.01 #slight perturbation so log works
        heatmap = np.log(heatmap) #log transformed

        #scale
        m = np.amax(heatmap)
        heatmap /= m

        if blackOnWhite: #flip the colors if desired
                heatmap = np.ones(heatmap.shape) - heatmap
        return heatmap

    #Automatically creates and plots a heatmap given the parameters
    def autogenHeatmap(self, start_time, end_time, numIntervals, priceStepSize, totalPriceRange, grayscale=True, verbose=False, fName='./heatmap.png', returnImage=False, returnMatrix=False, variableBinCenter=False):
        if returnImage and returnMatrix:
            raise Exception("Cannot return both image and matrix: set only one or the other flags.")

        clock_start = time.clock()

        #First save the orderbooks at the times specified
        if verbose:
                print("Beginning processing...")
        self.processAndSaveInterval(start_time, end_time, numIntervals, verbose=verbose)

        #Now bin the saved spreads with the price steps and spread specified
        if verbose:
                print("Binning...")
        if variableBinCenter:
            a = self.binAllSavedSpreadsVariableCenter(priceStepSize, totalPriceRange, verbose=verbose)

        else:
            a, spreads, highVs = self.autoBinAllSavedSpreads(priceStepSize, totalPriceRange, verbose=verbose)

        #Generate heatmap from the binned data
        if verbose:
                print("Generating heatmap...")
        if grayscale:
                heatmap = self.generateHeatmapFromBinnedData(a)
        else:
                heatmap = self.generateColoredHeatmapFromData(a, spreads, highVs[0], priceStepSize)

        if verbose:
                print("Rendering...")

        #Return back the image if requested
        if returnImage:
            clock_end = time.clock()
            if verbose:
                print("Heatmap rendered from", start_time, "to", end_time, "with", numIntervals, "time intervals and increments of", priceStepSize, "dollars over a total spread of", totalPriceRange)
                print("Complete.")
                print("Time elapsed:", clock_end - clock_start)
            return scipy.misc.toimage(heatmap)

        #Otherwise, save the image to fName
        else:
            scipy.misc.toimage(heatmap).save(fName)
            clock_end = time.clock()
            if verbose:
                print("Heatmap rendered from", start_time, "to", end_time, "with", numIntervals, "time intervals and increments of", priceStepSize, "dollars over a total spread of", totalPriceRange)
                print("Complete.")
                print("Time elapsed:", clock_end - clock_start)
            #If requested, send back the image data
            if returnMatrix:
                return heatmap

    #Automatically generates heatmap with the parameters using multiprocessing
    #Considerably faster than autogenHeatmap for long time frames or high quality heatmaps
    def fastAutogenHeatmap(self, start_time, end_time, numIntervals, priceSteps, priceSpread, verbose=False, threads=8, fName='./heatmap.png', returnMatrix=False, returnImage=False):
        if returnImage and returnMatrix:
            raise Exception("Cannot return both image and matrix: set only one or the other flags.")

        clock_start = time.clock()

        #Process the times and save them
        if verbose:
                print("Beginning processing...")
        self.processAndSaveInterval(start_time, end_time, numIntervals, verbose=verbose)

        #Bin them using the MP-enabled binner
        #(This is the slowest part)
        if verbose:
                print("Binning...")
        a, s, h = self.fastAutoBinAllSavedSpreads(priceSteps, priceSpread, verbose=verbose, threads=threads).T

        #Generate the heatmap from the binned data
        if verbose:
                print("Generating heatmap...")
        heatmap = self.generateHeatmapFromBinnedData(a)


        clock_end = time.clock()
        if verbose:
            print("Rendering...")
            print("Heatmap rendered from", start_time, "to", end_time, "with", numIntervals, "time intervals and increments of", priceSteps, "dollars over a total spread of", priceSpread)
            print("Complete.")
            print("Time elapsed:", clock_end - clock_start)

        #Save the image to fName
        scipy.misc.toimage(heatmap, cmin=0.0, cmax=np.max(heatmap)).save(fName)

        if returnMatrix:
            return heatmap

        elif returnImage:
            return scipy.misc.toimage(heatmap, cmin=0.0, cmax=np.max(heatmap))

    #Generates a colored heatmap instead of a grayscale heatmap from the binned data
    #Still requires the parameters from the binning to determine the colors
    #askColor and bidColor are the colors for the bid and ask side, respectively. They are RGB tuples, scaled so 1.0 is the max for a color channel
    @staticmethod
    def generateColoredHeatmapFromData(data, spreads, highBinValue, priceStep, transpose=True, trim=True, askColor=[0.0, 0.0, 1.0], bidColor=[1.0, 0.0, 0.0]):
        data = np.array(data)
        if transpose:
                data = data.T
        if trim:
                heatmap = data[1:-1]

        #log transform
        heatmap += 1.0
        heatmap = np.log(heatmap)

        #scale
        m = np.amax(heatmap)
        heatmap /= m

        #generate the heatmap by taking each row and changing it to be colored and collecting them
        #this is slow and could be parallelized, but is not super critical
        #TODO: parallelize
        coloredHeatmap = np.array([orderBook.__produceColorRowFromRow(heatmap[:, i], highBinValue, spreads[i][0], priceStep, askColor, bidColor) for i in range(len(heatmap[0]))])

        return coloredHeatmap.T

    #Takes a row and the parameters about the row and produces the color vector for the row needed to generate a colored heatmap
    @staticmethod
    def __produceColorRowFromRow(row, highValue, spreadHigh, priceStep, askColor, bidColor):
        #I don't remember what I did here, but it works
        n = len(row)
        upperHalfSplitPos = (highValue - spreadHigh)/priceStep
        upperColors = np.array([askColor for i in range(n)])
        lowerColors = np.array([bidColor for i in range(n)])
        dataHigh = np.zeros(n)
        dataHigh[:upperHalfSplitPos+1] = row[:upperHalfSplitPos+1]
        dataLow = np.zeros(n)
        dataLow[upperHalfSplitPos+1:] = row[upperHalfSplitPos+1:]

        def blackToWhite(a):
                if a[0] == a[1] == a[2] == 0.0:
                    return [1., 1., 1.]
                else:
                    return a

        blackBackgroundRow = np.array([upperColors[i] * dataHigh[i] for i in range(n)]) + np.array([lowerColors[i] * dataLow[i] for i in range(n)])

        return np.array([blackToWhite(blackBackgroundRow[i]) for i in range(len(blackBackgroundRow))])


    ################################
    ####### PATTERN MATCHING #######
    ################################

    '''
    This pattern matching stuff was experimental and didn't go anywhere.
    I deleted it to remove clutter, but it should still be available in the repository as previous commits.
    '''
