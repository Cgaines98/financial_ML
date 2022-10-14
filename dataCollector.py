import pandas as pd
import numpy as np
import os
import time
from binance.client import Client
from datetime import datetime

def cleanData(data):
    """
    Converts epoch time to readable time. No need to sort, data comes in chronological order
    casts open high low and volume to floats
    Drops noisy columns
    
    """
    df = pd.DataFrame(data, columns=['date','open', 'high', 'low', 'close', 'volume','close_time', 'qav', 'num_trades',
                    'taker_base_vol', 'taker_quote_vol', 'ignore'])
    df = df.drop(columns=['close_time', 'qav','taker_base_vol', 'taker_quote_vol', 'ignore'])
    df['open'] = df['open'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    df['volume'] = df['volume'].astype(float)
    return df

def writeAverages(df):
    """
    Returns the Min, Max, Average, and delta of mix/max (maximum possible profit/maximum possible loss)
    """
    highestHigh = df['high'].astype(float).max()
    lowestLow = df['high'].astype(float).min()
    delta = highestHigh - lowestLow
    dateHigh = df.loc[df['high'].idxmax(), 'date']
    dateLow = df.loc[df['low'].idxmin(), 'date']
    profit = delta if dateHigh > dateLow else delta * -1
    netPercentage = df.iloc[0]['open']

    print('Maximum profit/loss if purchased at peak/valley times:', profit)
    print('------------------------------------High over 30 days:', highestHigh)
    print('-------------------------------------Low over 30 days:', lowestLow)

def writeFilesToCSV(df,symbol):
    """
    Conver Epoch time to human readable and write to CSV
    """
    df['date'] = df['date'].apply(lambda t : time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(t/1000)))
    path = "./data/crypto/" + symbol + "-" + datetime.now().strftime("%Y-%m-%d") + ".csv"
    df.to_csv(path,index=False)


if __name__ == "__main__":
    """
    Establishes connection to binance
    Retrieves crypto data
    Sends data to be processed/written to csvs
    """
    #Connection
    apikey = os.getenv('PYTHON_BINANCE_API_KEY')
    secret = os.getenv('PYTHON_BINANCE_SECRET')
    client = Client(api_key = apikey, api_secret = secret)

    #Main loop that will grab data from binance API then send it off for cleaning and writing to 
    symbolList = ['BTCUSDT','ETHUSDT','DOGEUSDT']
    for symbol in symbolList:
        data = client.get_historical_klines(symbol,interval = '1h',start_str = '30d')
        print('\n\nProcessing:', symbol)
        df = cleanData(data)
        writeAverages(df)
        writeFilesToCSV(df,symbol)