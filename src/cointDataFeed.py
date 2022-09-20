import pandas as pd
import pandas_datareader as pdr

def cleanData(dataDetails, dataset):
	print('Cleaning Data:', dataDetails.sym, dataDetails.startDate, dataDetails.endDate)

	dataset.fillna(method='ffill', inplace=True)
	dataset = dataset[~dataset.index.duplicated(keep='first')]

	runningDupes = 0
	runningClose = 0.0

	for x in dataset.index:
		if dataset['Close'][x] == runningClose:
			runningDupes += 1
		else:
			runningClose = dataset['Close'][x]
			runningDupes = 0

		if runningDupes >= 10:
			oldTick = dataset['Close'][x]
			dataset.at[x, 'Open'] = dataset['Open'][x] * 1.0001
			dataset.at[x, 'High'] = dataset['High'][x] * 1.0001
			dataset.at[x, 'Low'] = dataset['Low'][x] * 1.0001
			dataset.at[x, 'Close'] = dataset['Close'][x] * 1.0001
			print('Warning:', runningDupes, 'Dupes', x, oldTick, dataset['Close'][x])
			runningDupes = 0

	return dataset

def downloadData(dataDetails):
	print('Downloading Data:', dataDetails.sym, dataDetails.startDate, dataDetails.endDate)
	thisDataset = pdr.get_data_yahoo(dataDetails.sym, start=dataDetails.startDate, end=dataDetails.endDate)
	return thisDataset

def databaseData(dataDetails):
	print('Retrieving DB Data:', dataDetails.sym, dataDetails.startDate, dataDetails.endDate)
	return 1

def loadData(dataDetails):
	if dataDetails.source == 1:
		thisDataset = downloadData(dataDetails)
	elif dataDetails.source == 2:
		thisDataset = databaseData(dataDetails)

	return cleanData(dataDetails, thisDataset)
