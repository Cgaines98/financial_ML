from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import yfinance as yf
from cryptoDataCollector import writeAverages


load_dotenv()


def getStockHistory(stock: str = "AAPL", timespan: str = "1mo", intervals: str = "1d"):
    """
    Returns yfinance stock history
    """
    x = yf.Ticker(stock)
    return x.history(period=timespan, interval=intervals)


def cleanData(df):
    """
    Converts number types to floats and drops unneeded columns
    """
    df = df.drop(columns=["Dividends", "Stock Splits"])
    df = df.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    df["open"] = df["open"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["close"] = df["close"].astype(float)
    df["volume"] = df["volume"].astype(float)
    return df


def writeToCSV(df, symbol):
    """
    write the dataframe to a csv file with standard naming convention of [symbol]-date.csv
    """
    path = (
        "./data/yfinance/" + symbol + "-" + datetime.now().strftime("%Y-%m-%d") + ".csv"
    )
    df.to_csv(path, index=False)


if __name__ == "__main__":
    stockList = ["MSFT", "AAPL", "TSLA"]
    for s in stockList:
        print(f"\n\n---------------------Processing:{s}---------------------")
        df = getStockHistory(stock=s)
        df = cleanData(df)
        writeAverages(df)
        writeToCSV(df, s)
