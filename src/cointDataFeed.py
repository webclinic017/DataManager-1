import dateparser
import pandas_datareader as pdr
from dotenv import load_dotenv
import sqlalchemy
import pandas as pd
import os

load_dotenv()
DB_CONNECTION = sqlalchemy.create_engine(
    f'mysql+mysqlconnector://{os.getenv("DB_USER")}:'
    f'{os.getenv("DB_PW")}@{os.getenv("DB_HOST")}/{os.getenv("DB_NAME")}'
)


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
    return readFromDatabaseTable(
        dataDetails.sym.upper(), dataDetails.interval, start=start, end=end
    )


def readFromDatabaseTable(symbol, interval, start=None, end=None):
    """Supported intervals are 1d, 1h, and 30m"""
    table_name = f"{symbol}_{interval}"
    if start is not None and end is not None:
        return pd.read_sql(
            f"SELECT DISTINCT * FROM {table_name} WHERE Time >= '{start}' AND Time <= '{end}'",
            con=DB_CONNECTION,
            index_col="Time",
        )
    if start is not None:
        return pd.read_sql(
            f"SELECT DISTINCT * FROM {table_name} WHERE Time >= '{start}'",
            con=DB_CONNECTION,
            index_col="Time",
        )
    return pd.read_sql(
        f"SELECT DISTINCT * FROM {table_name}", con=DB_CONNECTION, index_col="Time"
    )


def loadData(dataDetails):
    if dataDetails.source == 1:
        thisDataset = downloadData(dataDetails)
    elif dataDetails.source == 2:
        thisDataset = databaseData(dataDetails)

    return cleanData(dataDetails, thisDataset)
