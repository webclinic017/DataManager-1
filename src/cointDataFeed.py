import os

import dateparser
import pandas_datareader as pdr
from dotenv import load_dotenv
import sqlalchemy
import pandas as pd
import os

from src import cointBTEngine

load_dotenv()
DB_CONNECTION = sqlalchemy.create_engine(
    f'mysql+mysqlconnector://{os.getenv("DB_USER")}:'
    f'{os.getenv("DB_PW")}@{os.getenv("DB_HOST")}/{os.getenv("DB_NAME")}'
)

print(DB_CONNECTION)


def cleanData(dataDetails, dataset):
    print("Cleaning Data:", dataDetails.sym, dataDetails.startDate, dataDetails.endDate)

    dataset.fillna(method="ffill", inplace=True)
    dataset = dataset[~dataset.index.duplicated(keep="first")]

    runningDupes = 0
    runningClose = 0.0

    for x in dataset.index:
        if dataset["Close"][x] == runningClose:
            runningDupes += 1
        else:
            runningClose = dataset["Close"][x]
            runningDupes = 0

        if runningDupes >= 10:
            oldTick = dataset["Close"][x]
            dataset.at[x, "Open"] = dataset["Open"][x] * 1.0001
            dataset.at[x, "High"] = dataset["High"][x] * 1.0001
            dataset.at[x, "Low"] = dataset["Low"][x] * 1.0001
            dataset.at[x, "Close"] = dataset["Close"][x] * 1.0001
            print("Warning:", runningDupes, "Dupes", x, oldTick, dataset["Close"][x])
            runningDupes = 0

    return dataset


def downloadData(dataDetails):
    print(
        "Downloading Data:", dataDetails.sym, dataDetails.startDate, dataDetails.endDate
    )
    thisDataset = pdr.get_data_yahoo(
        dataDetails.sym, start=dataDetails.startDate, end=dataDetails.endDate
    )
    return thisDataset


def databaseData(dataDetails):
    print(
        "Retrieving DB Data:",
        dataDetails.sym,
        dataDetails.startDate,
        dataDetails.endDate,
    )
    start, end = None, None
    if dataDetails.startDate:
        start = dateparser.parse(dataDetails.startDate)
    if dataDetails.endDate:
        end = dateparser.parse(dataDetails.endDate)
    return readFromTable(
        dataDetails.sym.upper(), dataDetails.interval, start=start, end=end
    )


def loadData(dataDetails):
    if dataDetails.source == 1:
        thisDataset = downloadData(dataDetails)
    elif dataDetails.source == 2:
        thisDataset = databaseData(dataDetails)

    return cleanData(dataDetails, thisDataset)


def readFromTable(symbol, interval, start=None, end=None):
    table_name = f"{symbol}_{interval}"
    column = getIndexColumn(interval)
    if start is not None and end is not None:
        print(start)
        print(end)
        return pd.read_sql(
            f"SELECT * FROM {table_name} Where '{column}' <= '{end}'", con=DB_CONNECTION
        )
    if start is not None:
        print(start)
        return pd.read_sql(
            f"SELECT * FROM {table_name} Where '{column}' >= '{start}'",
            con=DB_CONNECTION,
        )
    return pd.read_sql(f"SELECT * FROM {table_name}", con=DB_CONNECTION)


"""
We need this method because yahoo dataframe doesn't use the same name for the timestamp column
between different intervals. Kind of annoying but this should help us deal with it for now.
"""


def getIndexColumn(interval):
    columnMap = {"1d": "Date", "1h": "index", "30m": "Datetime"}
    if interval not in columnMap.keys():
        raise Exception(
            f"All the {interval} to the columnMap with the associated index name as "
            f"the value"
        )
    return columnMap[interval]


if __name__ == "__main__":
    start = "Fri, 16 Sep 2022 10:55:50"
    end = "Fri, 23 Sep 2022 10:55:50"
    s = "hello"
    dataDetails = cointBTEngine.dataDetails("cci", "1d", start, None, None)
    print(dataDetails)
    print(databaseData(dataDetails))
