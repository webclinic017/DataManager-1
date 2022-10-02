import pandas as pd
import statsmodels.api as stat
import statsmodels.tsa.stattools as ts
from statsmodels.tsa.vector_ar.vecm import coint_johansen

ADF_PASS_RATE = 0.05

class cointResult:
	def __init__(self, cSymL, cWhichTest):
		self.symL = cSymL
		self.whichTest = cWhichTest
		self.tStatADF = None
		self.pValADF = None
		self.critValsADF = None
		self.regressionParamsADF = None
		self.weightsJo = None
		self.tStatsJo = None
		self.tValsJo = None
		self.eigenStatsJo = None
		self.eigenValsJo = None
		self.passBoth = False

def ADFTest(tickData):
	ADFResultsAll = {}
	symsL = list(tickData.keys())
	levelsMap = {0.01:'1%', 0.05:'5%', 0.1:'10%'}

	for a in range(len(symsL)-1):
		for b in range(a+1, len(symsL)):
			td1 = tickData[symsL[a]]['Close']
			td2 = tickData[symsL[b]]['Close']
			td2 = stat.add_constant(td2)
			regressionResult = stat.OLS(td1, td2).fit()
			ADFResult = ts.adfuller(regressionResult.resid)
#			print(regressionResult.summary())
#			print(regressionResult.params[0], regressionResult.params[1])

			if True:
#			if ADFResult[0] <= ADFResult[4][levelsMap[ADF_PASS_RATE]] and ADFResult[1] <= ADF_PASS_RATE:
				ADFResultsAll[symsL[a], symsL[b]] = cointResult([symsL[a], symsL[b]], 'ADF')
				ADFResultsAll[symsL[a], symsL[b]].tStatADF = ADFResult[0]
				ADFResultsAll[symsL[a], symsL[b]].pValADF = ADFResult[1]
				ADFResultsAll[symsL[a], symsL[b]].critValsADF = ADFResult[4]
				ADFResultsAll[symsL[a], symsL[b]].regressionParamsADF = regressionResult.params

	return ADFResultsAll

def runJohansen(group, tickData):
	testPass = True
	testResult = coint_johansen(tickData, 0, 1)

	weights = list(testResult.evec[0])
	for x in range(len(weights)):
		weights[x] = weights[x] / weights[0]
	JoResult = cointResult(group, 'Johansen')
	JoResult.weightsJo = weights
	JoResult.tStatsJo = testResult.lr1
	JoResult.tValsJo = testResult.cvt
	JoResult.eigenStatsJo = testResult.lr2
	JoResult.eigenValsJo = testResult.cvm

	for x in range(len(JoResult.tStatsJo) - 1, -1, -1):
		if JoResult.tStatsJo[x] < JoResult.tValsJo[x][1]:
			testPass = False

	# print('r\tTraceStat\t90%\t95%\t99%:')
	# for x in range(len(testResult.lr1)):
	# 	print('{}\t{:.4f}\t\t{:.4f}\t{:.4f}\t{:.4f}'.format(x, testResult.lr1[x], testResult.cvt[x][0], testResult.cvt[x][1], testResult.cvt[x][2]))

	# print('r\tEigenStat\t90%\t95%\t99%:')
	# for x in range(len(testResult.lr2)):
	# 	print('{}\t{:.4f}\t\t{:.4f}\t{:.4f}\t{:.4f}'.format(x, testResult.lr2[x], testResult.cvm[x][0], testResult.cvm[x][1], testResult.cvm[x][2]))
	# print('weights:', JoResult.weightsJo)

	return testPass, JoResult

def JohansenTest(tickData, maxGroupSize):
	JohansenResultsAll = {}
	symsL = list(tickData.keys())

	for a in range(len(symsL)-1):
		for b in range(a+1, len(symsL)):
#			ticks1 = tickData[symsL[a]]['Close'].copy()
#			ticks1.rename(columns={'Close':symsL[a][0]}, inplace=True)
#			ticks2 = tickData[symsL[b]]['Close'].copy()
#			ticks2.rename(columns={'Close':symsL[b][0]}, inplace=True)
			testDF = pd.concat([tickData[symsL[a]]['Close'], tickData[symsL[b]]['Close']], axis=1)
#			testDF2 = pd.concat([ticks1, ticks2], axis=1)

			testPass2, JoResult2 = runJohansen([symsL[a], symsL[b]], testDF)
			if testPass2 == True:
				JohansenResultsAll[symsL[a], symsL[b]] = JoResult2

			if maxGroupSize >= 3:
				for c in range(a+1, len(symsL)):
					if symsL[b] != symsL[c]:
						ticks3 = tickData[symsL[c]][['Close']].copy()
						ticks3.rename(columns={'Close':symsL[c][0]}, inplace=True)
						testDF3 = pd.concat([testDF2, ticks3], axis=1)

						testPass3, JoResult3 = runJohansen([symsL[a], symsL[b], symsL[c]], testDF3)
						if testPass3 == True:
							JohansenResultsAll[symsL[a], symsL[b], symsL[c]] = JoResult3

						if maxGroupSize >= 4:
							for d in range(a+1, len(symsL)):
								if symsL[c] != symsL[d]:
									ticks4 = tickData[symsL[d]][['Close']].copy()
									ticks4.rename(columns={'Close':symsL[d][0]}, inplace=True)
									testDF4 = pd.concat([testDF3, ticks4], axis=1)

									testPass4, JoResult4 = runJohansen([symsL[a], symsL[b], symsL[c], symsL[d]], testDF4)
									if testPass4 == True:
										JohansenResultsAll[symsL[a], symsL[b], symsL[c], symsL[d]] = JoResult4

	return JohansenResultsAll

def findAllCoints(tickData, maxGroupSize):
	if maxGroupSize > 4:
		maxGroupSize = 4

	ADFResults = ADFTest(tickData)
	JohansenResults = JohansenTest(tickData, maxGroupSize)

	allResults = ADFResults
	for group in JohansenResults:
		if allResults.has_key(group):
			allResults[group].passBoth = True
		else:
			allResults[group] = JohansenResults[group]

	return allResults
