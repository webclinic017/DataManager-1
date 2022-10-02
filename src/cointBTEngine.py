import argparse
import pandas as pd
import cointDataFeed as df
import cointLib as cl
import cointBackTrader as cbt

syms = {}
tickData = {}
cointGroups = {}
btResults = {}
topGroups = {}

parser = argparse.ArgumentParser()

OPT_METRIC = 'pnl' #pnl, sharpe
SHARPE_MIN = 1.5

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
	synth = None

	if cointGroups[group].whichTest == 'ADF':
		synth = tickData[group[1]] * cointGroups[group].regressionParamsADF[1] + cointGroups[group].regressionParamsADF[0]

	return synth

def runCointTests(): #send whatever datasets need to be tested for cointegration
	global cointGroups
	global tickData

	print('Finding cointegrations...')
	cointGroups = cl.findAllCoints(tickData, 4) #dictionary of sym dataDetails, dataset, maxGroupSize
	print('Groups found:')
	print(list(cointGroups.keys()))

def backtestGroups():
	global cointGroups
	global tickData
	global btResults

	print('Backtesting...')
	for group in cointGroups:
		print('Group:', group)
		btResults[group] = cbt.runBacktesting(createSynth(group), OPT_METRIC)

def sortResults():
	global btResults
	global topGroups
	topgroup = None

	if OPT_METRIC == 'pnl':
		topPnl = None

		for group in btResults:
			topGroups[group] = btResults[group].pnl

			if topPnl is None:
				topPnl = btResults[group].pnl
				topGroup = group
			elif btResults[group].pnl > topPnl:
				topPnl = btResults[group].pnl
				topGroup = group
	elif OPT_METRIC == 'sharpe':
		topSharpe = None

		for group in btResults:
			topGroups[group] = btResults[group].sharpe

			if btResults[group].sharpe > SHARPE_MIN:
				if topSharpe is None:
					topSharpe = btResults[group].sharpe
					topGroup = group
				elif btResults[group].sharpe > topSharpe:
					topSharpe = btResults[group].sharpe
					topGroup = group

	resultsList = sorted(topGroups.items(), key=lambda x:x[1])
	sortedGroups = dict(resultsList)

	print('\nResults\n----------------------------')
	for group in sortedGroups:
		print(group)
		if OPT_METRIC == 'pnl':
			print('Pnl:', sortedGroups[group])
		elif OPT_METRIC == 'sharpe':
			print('Sharpe:', sortedGroups[group])
		print('Test:', cointGroups[group].whichTest)
		if cointGroups[group].whichTest == 'ADF':
			print('Params: m =', cointGroups[group].regressionParamsADF[1], 'b =', cointGroups[group].regressionParamsADF[0])

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
