import argparse
import pandas as pd
import cointDataFeed as df
import cointLib as cl
import cointBackTrader as cbt

syms = {}
tickData = {}
cointGroups = {}
btResults = {}

parser = argparse.ArgumentParser()

class dataDetails:
	def __init__(self, dSym, dInterval, dStartDate, dEndDate, dSource):
		self.sym = dSym
		self.interval = dInterval
		self.startDate = dStartDate
		self.endDate = dEndDate
		self.source = dSource

def loadConf(confFile):
	global syms

	confData = pd.read_csv(confFile)
	for iter in confData.index:
		theseDetails = dataDetails(confData['Ticker'][iter], confData['Interval'][iter], confData['StartDate'][iter], confData['EndDate'][iter], confData['DataSource'][iter])
		syms[confData['Ticker'][iter], confData['Interval'][iter]] = theseDetails

def getData():
	global syms
	global tickData

	for sym in syms:
		tickData[sym] = df.loadData(syms[sym]) #send dataDetails object
#		print(sym)
#		print(tickData[sym])

def createSynth(group):
	global tickData
	synth = None
	return synth

def runCointTests(): #send whatever datasets need to be tested for cointegration
	global cointGroups
	global tickData

	cointGroups = cl.findAllCoints(tickData, 4) #dictionary of sym dataDetails, dataset, maxGroupSize

def backtestGroups():
	global cointGroups
	global tickData
	global btResults

	for group in cointGroups:
		btResults[group] = cbt.runBacktesting(createSynth(group))

def sortResults():
	global btResults

def readCommandLine():
    parser.add_argument('whichConf', help='configuration file')
    args = parser.parse_args()
    return (args)

def main():
	global syms
	global tickData

	args = readCommandLine()

	loadConf(args.whichConf)
	getData()
	runCointTests()
	backtestGroups()
	sortResults()

if __name__ == '__main__':
	main()
