import numpy as np
import csv
import timeit
import pandas as pd

def data_testing(data_file, cont):
	"""This function will test if the data read in is ITCH or NYSE format, and will proceed accordingly."""
	data_type = None
	df = None
	try:	
		df = pd.read_csv(data_file, header=0, usecols=['type', 'seconds', 'Message_Sequence_Number', 'side', 'price', 'Total_Volume', 'Order_Quantity', 'Num_Orders_at_Price', 'Trading_Status', 'Spread', 'Best_bid', 'Best_ask'], 
			dtype={'Type': 'S1', 'Seconds':float, 'Message_Sequence_Number':int, 'Side':object, 'Price':float, 'Total_Volume':int, 'Order_Quantity':int, 'Num_Orders_at_Price':int, 'Trading_Status':object, 'Spread':float, 'Best_bid':float, 'Best_ask':float})
		df.rename(columns={'Type':'type', 'Seconds':'seconds', 'Message_Sequence_Number':'orn', 'Side':'side', 'Price':'price', 'Total_Volume':'volume', 'Order_Quantity':'shares', 'Num_Orders_at_Price':'num_orders_at_price', 
			'Trading_Status':'status', 'Spread':'spread', 'Best_bid':'current_bid', 'Best_ask':'current_ask'}, inplace=True)
		df['side'] = df['side'].str.strip()
		data_type = 'NYSE'
		print("This data is", data_type)
	except:
		try:
			df = pd.read_csv(data_file, header=0, usecols=['type', ' seconds', ' orn', ' side', ' shares', ' price'], dtype={'type': 'S1', ' seconds':float, ' orn':int, ' side':object, ' shares': int, ' price': float})
			df.rename(columns={' seconds': 'seconds', ' orn': 'orn', ' side': 'side', ' shares': 'shares', ' price': 'price'}, inplace=True) # Get rid of spaces in names
			df['side'] = df['side'].str.strip()
			data_type = 'ITCH'
			print("This data is", data_type)
			return df
		except:
			print("Could not read in the table")
	if data_type is not 'ITCH' and df is not None:
		return convert_data_nyse(df, cont)


def convert_data_nyse(df, cont):
	"""This will adjust the NYSE dataframe to look just like the ITCH dataframe. That way, the orderBook class will work for all data we feed it."""
	#First, we change the message types to match.
	df.replace(to_replace='O', value='A', inplace=True)
	df.replace(to_replace='C', value='D', inplace=True)
	df.sort_values(by=['price'], inplace=True)
	df.loc[df.price == df.price.shift(1), 'Change'] = df['shares'] - df['shares'].shift(1) 
	df.loc[df.price != df.price.shift(1), 'Change'] = df['shares']
	df.loc[(df.Change > 0) & (df.type=='X'), 'type'] = 'A'
	df.loc[(df.Change < 0) & (df.type=='X'), 'type'] = 'D'
	df.loc[(df.Change == 0) & (df.type=='X'), 'type'] = np.nan

	#Second, we switch the columns around to the right places.
	cols = list(df)
	cols.insert(3, cols.pop(7))
	cols.insert(4, cols.pop(6))
	df = df.ix[:, cols]

	#Third, we drop the columns we aren't interested in.
	df = df.drop(['num_orders_at_price', 'status', 'Change'], 1)

	#Finally, we continue to add columns only if we are in recursiveOLS, not in orderBook
	if cont == True:
		cols = list(df)
		cols.insert(3, cols.pop(4))
		cols.insert(4, cols.pop(6))
		cols.insert(5, cols.pop(6))
		cols.insert(9, cols.pop(7))
		df = df.ix[:, cols]
		df.rename(columns={'volume': 'shares_remaining'}, inplace=True)
	else:
		df = df.drop(['volume', 'current_bid', 'current_ask', 'spread'], 1)
	df.sort_values(by=['seconds'], inplace=True)
	return df

"""def add_spread_count(df):
	This function will add the categories of shares remaining, best ask, best bid, and spread required by recursiveOLS.
	#First, reorder the columns as necessary.
	cols = list(df)
	cols.insert(3, cols.pop(4))
	cols.insert(4, cols.pop(5))
	cols.insert(4, cols.pop(6))
	df = df.ix[:, cols]

	#Rename volume column and add the three missing columns as necessary.
	df.rename(columns={'volume': 'shares_remaining'}, inplace=True)
	df.sort_values(by=['seconds'], inplace=True)

	df['current bid'] = 0
	df['current ask'] = 0
	#update(df)
	#for i in range(len(df.index)):
		#df.ix[i,8], df.ix[i,7] = get_Spread(df[0:i+1])
	df['spread'] = df['current ask'] - df['current bid']
	return df"""

_df = data_testing('C:\\Users\\Alex\\Documents\\040313_AXP_orderbook_NYSE.csv', True)
#_df = data_testing('C:\\Users\\Alex\\Documents\\NBG_040313_NYSE.csv', True)
#_df = data_testing('C:\\Users\\Alex\\Documents\\040516_AAMC.csv', True)
print(_df)