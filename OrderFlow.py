from datetime import datetime
import numpy as np
import pandas as pd

def readTxt(path2file, verbose = True):
    """
    Read txt with order book messages after qsh2txt.exe
    """

    if (verbose):
	    print('Parsing file ', path2file)

    ret = pd.read_csv(path2file, sep=';', header = None, names = ['Received', 'ExchTime', 'OrderId', 'Price', 'Amount',
         'AmountRest', 'DealId', 'DealPrice', 'OI', 'Flags'], skiprows = 3, parse_dates = ['Received', 'ExchTime'], 
         date_parser = lambda x: datetime.strptime(x, '%d.%m.%Y %H:%M:%S.%f'),
         converters = {'OrderId': int, 'Price': int, 'Amount': int,'AmountRest': int, 'DealId': int, 'DealPrice': int, 'OI': int, 'Flags': str})

    if (verbose):
        print('Finished parsing ', path2file)

    return ret

class OrderFlow:
    """
    Implements data structure to make queries to raw pandas data frame
    """

    # all data from csv
    __df = None
    # filtered data with ID, timeIn and timeOut
    __backoffice = None
    # order info for each ID
    __order_info = None

    def clear(self):
    	self.__df = None
    	self.__backoffice = None
    	self.__order_info = None

    def append(self, df):
    	if (self.__df is None):
    		self.__df = df
    	else:
    		self.__df.append(df)
    	self.__backoffice = None
    	self.__order_info = None

    def convert(self):
        if (self.__df is None):
            raise EmptyOrderFlow('Please set df variable of OrderFlow')

        self.__backoffice = self.__df[['OrderId', 'ExchTime']].groupby('OrderId').agg([np.min, np.max])
        self.__order_info = self.__df.drop_duplicates(subset = 'OrderId').set_index('OrderId').loc[:, ['Price', 'Amount', 'Flags']]
        self.__order_info['Flags'] = self.__order_info['Flags'].apply(lambda x: 1 if 'Sell' in x else -1)
        self.__order_info.rename(mapper={'Flags': 'BuySell'}, axis=1, inplace = True)

    def getStart(self):
    	return self.__df.iloc[0]['ExchTime']

    def getEnd(self):
    	return self.__df.iloc[-1]['ExchTime']

    def getIDbyTimestamp(self, timestamp):
    	if self.__df is None:
    		raise EmptyBackoffice('Please do OrderFlow.convert()')

    	return self.__backoffice[(self.__backoffice['ExchTime'].amin < timestamp) & (timestamp < self.__backoffice['ExchTime'].amax)].iloc[:,0].index.values

    def query(self, timestamp):
    	ids = self.getIDbyTimestamp(timestamp)

    	return self.__order_info.loc[ids]

    def orderBook(self, timestamp):
        # needs at least 5 orders for bid and ask each
        query = self.query(timestamp)
        ask = query.loc[query['BuySell'] == 1, ['Price', 'Amount']]
        bid = query.loc[query['BuySell'] == -1, ['Price', 'Amount']]
        ask = ask.sort_values(by = 'Price').values[:5].reshape(1,5,2) # level 2, only 5 best
        bid = bid.sort_values(by = 'Price', ascending = False).values[:5].reshape(1,5,2) # level 2, only 5 best
        return bid, ask
    
if __name__ == "__main__":
    flow = OrderFlow()
    df =  readTxt('../../Data/Si-3.18/txt/Si-3.18.2017-11-16.OrdLog.{1-OrdLog}.txt')
    flow.append(df)
    flow.convert()

    offset = pd.Timedelta('30 min') # don't save 10 mins after first message and 10 mins before last message
    st = flow.getStart() + offset
    en = flow.getEnd() - offset

    delta = pd.Timedelta('100 ms')

    bids = np.zeros((0,5,2), dtype='int32')
    asks = np.zeros((0,5,2), dtype='int32')

    nxt = st
    step = 1
    while(nxt < en):
        if (step % 1000 == 0):
            print(round((nxt - st) / (en - st) * 100, 3), '%')
        bid, ask = flow.orderBook(nxt)
        bids = np.concatenate((bids, bid))
        asks = np.concatenate((asks, ask))
        nxt += delta
        step += 1

    np.save('bids.npy', bids)
    np.save('asks.npy', asks)
    