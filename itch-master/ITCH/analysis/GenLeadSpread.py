#  Name: GenLeadSpread.py
#  Authors: Scott Condie (ssc@byu.edu), additions by Chelsea Hunter and Christopher Hair (christopher.m.hair@gmail.com)
#  Description:  A library that will generate the leading spreads for a given 
#  time delta.  This method makes use of liblag_gen.so, a library written in C 
#  that speeds up the processor intensive portion of this task.

import numpy as np
import pandas as pd
import ctypes as ct
import numpy.ctypeslib as npct
import os

def spread(df,lead_length):
    """Calculate a column of futures spreads for a constant distance in the future.
       
       spread(df,lead_length)
            Arguments:
            -df:            The pandas dataframe to be used.  This dataframe
                             must have a column labelled 'seconds' that are 
                             used as the timestamps for messages.
            -lead_length:   The length of time in the future at which the 
                             leading spread should be calculated.  This value 
                             should be expressed in seconds.  For example, 5 
                             milliseconds would be 0.005.

    """
    array_1d_double = npct.ndpointer(dtype=np.double,ndim=1,flags="CONTIGUOUS")
    array_1d_int = npct.ndpointer(dtype=np.int32,ndim=1,flags="CONTIGUOUS")
    base = os.getcwd()
    liblag_test = npct.load_library("liblag_gen.so",base+"/lag_gen/")  # Notice the loader_path as the second argument
    liblag_test.create_lags.restype = None
    liblag_test.create_lags.argtypes = [array_1d_double,array_1d_int,ct.c_int,
            ct.c_double]

    a1 = np.array(df['seconds'],dtype=np.double)
    a2 = np.zeros(a1.shape,dtype=np.int32)
    np.ascontiguousarray(a2,dtype=np.int32)
    n = a1.shape[0]
    c_int_p= ct.POINTER(ct.c_int)
    ret = a2.ctypes.data_as(c_int_p)
    liblag_test.create_lags(a1,a2,n,lead_length)
    l_idx = pd.Series(a2)
    #print(l_idx)
    # index lookups are slow.
    dictionary = dict(zip(df.index, df['spread'].values))
    #print('df shape: ' + repr(df.shape))
    #print('Length of dictionary: ' + repr(len(list(dictionary.keys()))))
    #print('Shape of df[l_idx]: ' + repr(df['l_idx'].shape))
    #print('Shape of df[seconds]: ' + repr(df['seconds'].shape))
    df1 = df.copy()
    df1['lead_spread'] = l_idx.apply(lambda x: dictionary[x]).copy()
    #df.drop('l_idx',axis=1,inplace=True)
    return df1

def midpoint(df,lead_length):
    """Calculate a column of futures spreads for a constant distance in the future.
       
       spread(df,lead_length)
            Arguments:
            -df:            The pandas dataframe to be used.  This dataframe
                             must have a column labelled 'seconds' that are 
                             used as the timestamps for messages.
            -lead_length:   The length of time in the future at which the 
                             leading spread should be calculated.  This value 
                             should be expressed in seconds.  For example, 5 
                             milliseconds would be 0.005.

    """
    array_1d_double = npct.ndpointer(dtype=np.double,ndim=1,flags="CONTIGUOUS")
    array_1d_int = npct.ndpointer(dtype=np.int32,ndim=1,flags="CONTIGUOUS")
    liblag_test = npct.load_library("liblag_gen.so","/fslhome/lehnerjw/work/itch/ITCH/lag_gen/")  # Notice the loader_path as the second argument
    liblag_test.create_lags.restype = None
    liblag_test.create_lags.argtypes = [array_1d_double,array_1d_int,ct.c_int,
            ct.c_double]

    a1 = np.array(df['seconds'],dtype=np.double)
    a2 = np.zeros(a1.shape,dtype=np.int32)
    np.ascontiguousarray(a2,dtype=np.int32)
    n = a1.shape[0]
    c_int_p= ct.POINTER(ct.c_int)
    ret = a2.ctypes.data_as(c_int_p)
    liblag_test.create_lags(a1,a2,n,lead_length)
    l_idx = pd.Series(a2)
    #print("a1: " + repr(a1))
    #print("a2: " + repr(a2))
    #print(l_idx)
    # index lookups are slow.
    #print("df.index: {}".format(df.index))
    #print("df midpoint price: {}".format(df['midpoint_price'].values))
    
    dictionary = dict(zip(df.index, df['midpoint_price'].values))
    #print("dictionary.keys(): {}".format(sorted(list(dictionary.keys()))))
    #print("dictionary[1]: {}".format(dictionary[1]))
    #print('df shape: ' + repr(df.shape))
    #print('Length of dictionary: ' + repr(len(list(dictionary.keys()))))
    #print('Shape of df[l_idx]: ' + repr(df['l_idx'].shape))
    #print('Shape of df[seconds]: ' + repr(df['seconds'].shape))
    df1 = df.copy()
    df1['lead_midpoint_price'] = l_idx.apply(lambda x: dictionary[x]).copy()
    #df.drop('l_idx',axis=1,inplace=True)
    return df1

def lead_spread_idx_by_day(df_seconds,lead_length):
    """
    Calculate the index of the  column of future spreads for a 
    constant distance in the future.  This function is meant to 
    be applied to a dataframe that has been grouped by date using 
    pandas.groupby. 
    days in the DataFrame and leads should not be calculated across days.
       
       lead_spread_idx_by_day(df,lead_length)
            Arguments:
            -df_seconds:    The seconds series from the df to be used.  
            -lead_length:   The length of time in the future at which the 
                             leading spread should be calculated.  This value 
                             should be expressed in seconds.  For example, 5 
                             milliseconds would be 0.005.

    """
    #print("df_seconds.shape",df_seconds.shape)
    array_1d_double = npct.ndpointer(dtype=np.double,ndim=1,flags="CONTIGUOUS")        
    array_1d_int = npct.ndpointer(dtype=np.int32,ndim=1,flags="CONTIGUOUS")
    liblag_test = npct.load_library("liblag_gen.so","/fslhome/lehnerjw/work/itch/ITCH/lag_gen/")  # Notice the loader_path as the second argument
    liblag_test.create_lags.restype = None
    liblag_test.create_lags.argtypes = [array_1d_double,array_1d_int,ct.c_int,ct.c_double]

    a1 = np.array(df_seconds,dtype=np.double)
    a2 = np.zeros(a1.shape,dtype=np.int32)
    np.ascontiguousarray(a2,dtype=np.int32)
    n = a1.shape[0]
    c_int_p= ct.POINTER(ct.c_int)
    ret = a2.ctypes.data_as(c_int_p)
    liblag_test.create_lags(a1,a2,n,lead_length)
    return a2

    
def lead_bid(df,lead_length):
    """Calculate a column of future bids for a constant distance in the future.
       
       lead_bid(df,lead_length)
            Arguments:
            -df:            The pandas dataframe to be used.  This dataframe
                             must have a column labelled 'seconds' that are 
                             used as the timestamps for messages.
            -lead_length:   The length of time in the future at which the 
                             leading bid should be calculated.  This value 
                             should be expressed in seconds.  For example, 5 
                             milliseconds would be 0.005.

    """
    array_1d_double = npct.ndpointer(dtype=np.double,ndim=1,flags="CONTIGUOUS")
    array_1d_int = npct.ndpointer(dtype=np.int32,ndim=1,flags="CONTIGUOUS")
    liblag_test = npct.load_library("liblag_gen.so","/panfs/pan.fsl.byu.edu/scr/grp/fslg_condie_itch/itch_project/RegressionAnalysis/")  # Notice the loader_path as the second argument
    liblag_test.create_lags.restype = None
    liblag_test.create_lags.argtypes = [array_1d_double,array_1d_int,ct.c_int,
            ct.c_double]

    a1 = np.array(df['seconds'],dtype=np.double)
    a2 = np.zeros(a1.shape,dtype=np.int32)
    np.ascontiguousarray(a2,dtype=np.int32)
    n = a1.shape[0]
    c_int_p= ct.POINTER(ct.c_int)
    ret = a2.ctypes.data_as(c_int_p)
    liblag_test.create_lags(a1,a2,n,lead_length)
    l_idx = pd.Series(a2)
    # index lookups are slow.
    dictionary = dict(zip(df.index, df['current bid'].values))
    #print('df shape: ' + repr(df.shape))
    #print('Length of dictionary: ' + repr(len(list(dictionary.keys()))))
    #print('Shape of df[l_idx]: ' + repr(df['l_idx'].shape))
    #print('Shape of df[seconds]: ' + repr(df['seconds'].shape))
    new_lead_bid  =  l_idx.apply(lambda x: dictionary[x]).copy()
    df['lead_bid'] = new_lead_bid
    #df.drop('l_idx',axis=1,inplace=True)
    return df

def lead_ask(df,lead_length):
    """Calculate a column of future asks for a constant distance in the future.
       
       lead_ask(df,lead_length)
            Arguments:
            -df:            The pandas dataframe to be used.  This dataframe
                             must have a column labelled 'seconds' that are 
                             used as the timestamps for messages.
            -lead_length:   The length of time in the future at which the 
                             leading ask should be calculated.  This value 
                             should be expressed in seconds.  For example, 5 
                             milliseconds would be 0.005.

    """
    array_1d_double = npct.ndpointer(dtype=np.double,ndim=1,flags="CONTIGUOUS")
    array_1d_int = npct.ndpointer(dtype=np.int32,ndim=1,flags="CONTIGUOUS")
    liblag_test = npct.load_library("liblag_gen.so","/panfs/pan.fsl.byu.edu/scr/grp/fslg_condie_itch/itch_project/RegressionAnalysis/")  # Notice the loader_path as the second argument
    liblag_test.create_lags.restype = None
    liblag_test.create_lags.argtypes = [array_1d_double,array_1d_int,ct.c_int,
            ct.c_double]

    a1 = np.array(df['seconds'],dtype=np.double)
    a2 = np.zeros(a1.shape,dtype=np.int32)
    np.ascontiguousarray(a2,dtype=np.int32)
    n = a1.shape[0]
    c_int_p= ct.POINTER(ct.c_int)
    ret = a2.ctypes.data_as(c_int_p)
    liblag_test.create_lags(a1,a2,n,lead_length)
    l_idx = pd.Series(a2)
    # index lookups are slow.
    dictionary = dict(zip(df.index, df['current ask'].values))
    #print('df shape: ' + repr(df.shape))
    #print('Length of dictionary: ' + repr(len(list(dictionary.keys()))))
    #print('Shape of df[l_idx]: ' + repr(df['l_idx'].shape))
    #print('Shape of df[seconds]: ' + repr(df['seconds'].shape))
    new_lead_ask  =  l_idx.apply(lambda x: dictionary[x]).copy()
    df['lead_ask'] = new_lead_ask
    #df.drop('l_idx',axis=1,inplace=True)
    return df


def lead_volatility(df, lead_length):
    #Added by Christopher Hair for use on volatility regressions.
    """Calculate a column of future volatility for a constant distance in the future.
       
       lead_volatility(df,lead_length)
            Arguments:
            -df:            The pandas dataframe to be used.  This dataframe
                             must have a column labelled 'seconds' that are 
                             used as the timestamps for messages.
            -lead_length:   The length of time in the future at which the 
                             leading volatility should be calculated.  This value 
                             should be expressed in seconds.  For example, 5 
                             milliseconds would be 0.005.

    """
    array_1d_double = npct.ndpointer(dtype=np.double,ndim=1,flags="CONTIGUOUS")
    array_1d_int = npct.ndpointer(dtype=np.int32,ndim=1,flags="CONTIGUOUS")
    liblag_test = npct.load_library("liblag_gen.so","/fslhome/lehnerjw/work/itch/ITCH/lag_gen/")  # Notice the loader_path as the second argument
    liblag_test.create_lags.restype = None
    liblag_test.create_lags.argtypes = [array_1d_double,array_1d_int,ct.c_int, ct.c_double]

    a1 = np.array(df['seconds'],dtype=np.double)
    a2 = np.zeros(a1.shape,dtype=np.int32)
    np.ascontiguousarray(a2,dtype=np.int32)
    n = a1.shape[0]
    c_int_p= ct.POINTER(ct.c_int)
    ret = a2.ctypes.data_as(c_int_p)
    liblag_test.create_lags(a1,a2,n,lead_length)
    l_idx = pd.Series(a2) #This is the index of the message that is the lagged distance ahead

    df['idx'] = df.index
    df['l_idx'] = l_idx

    def get_volatility(x):
        # Gets the volatility for the next lead_length seconds ahead of the message
        # Volatility here means volatility of the midpoint (not of the executions)
        try:
            slice = df['midpoint_price'][int(x['idx']):int(x['l_idx']+1)] #take a slice from the current index to and including the index of the messages that are within the lagged time ahead
        except: #Log out the errors, if any... (shouldn't be any)
            print "Error in processing volatility."
            print x
            print x.columns
            print x.dtypes
            print df
            print "idx:", x['idx']
            print "l_idx", x['l_idx']
 
        if len(slice) <= 1:
            return np.nan #note that if there is only the single observation, then the variance is nan, not 0. 0 will only be returned if the midpoint does not change over more than one observation.
        else:
            return np.var(slice)

    df['volatility'] = df.apply(get_volatility, axis=1) # makes a new column
    df.drop('l_idx',axis=1,inplace=True) #drops these ones, as they are no longer needed
    df.drop('idx', axis=1, inplace=True) #drops this one too, to save memory requirements
    return
