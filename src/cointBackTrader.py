import pandas as pd
import matplotlib.pyplot as plt
import backtrader as bt
import backtrader.indicators as btind
import backtrader.feeds as btfeeds
import backtrader.filters as btfilters
import backtrader.analyzers as btanalyzers
import datetime as dt
from datetime import datetime, timedelta

BROKER_COMMISSION, BROKER_MARGIN, BROKER_MULT = 0.0, None, 1
QUICK_TEST = False
PRINT_TICKS = False
USE_RUNNING_STOP = False
RUNNING_STOP = 0.1 #as percentage of ATR
HARD_STOP = 1.0
HARD_GAIN = 1.0
COOL_TICKS = 3
MIN_EMA_WIDTH = 0.25 #as percentage of ATR
MIN_ATR = 0.00020

class btParams():
	def __init__(self, emaFast=None, emaSlow=None, smaFast=None, smaSlow=None):
		self.emaFast = emaFast
		self.emaSlow = emaSlow
		self.smaFast = smaFast
		self.smaSlow = smaSlow

class testResults():
	def __init__(self, pnl, sharpe, sqn, totClosed, totWon, totLost, strikeRate, winStreak, loseStreak):
		self.pnl = pnl
		self.sharpe = sharpe
		self.sqn = sqn
		self.totClosed = totClosed
		self.totWon = totWon
		self.totLost = totLost
		self.strikeRate = strikeRate
		self.winStreak = winStreak
		self.loseStreak = loseStreak

class TestStrategy(bt.Strategy):
	params = (
		('emaFast', 50), 
		('emaSlow', 100), 
		('BBandsperiod', 20),
	)

	def log(self, txt, dt=None):
		dt = dt or self.datas[0].datetime.date(0)
		print(f'{dt.isoformat()} {self.datas[0].datetime.time(0)} {txt}') # Comment this line when running optimization

	def __init__(self):
		self.startcash = self.broker.getvalue()
		self.dataClose = self.datas[0].close
		self.dataHigh = self.datas[0].high
		self.dataLow = self.datas[0].low
		self.stopLossTrail = 0.0
		self.stopLossBand = 0.0
		self.stopLossHard = 0.0
		self.stopGainHard = 0.0
		self.order = None
		self.runningPnlTicks = 0.0
		self.runningTradePrice = 0.0
		self.runningCoolOff = 0
		self.tradeEntryTime = dt.time(0, 0, 0)

		self.EMA_SPAN_FAST = bt.indicators.EMA(self.datas[0], period=self.params.emaFast)
		self.EMA_SPAN_SLOW = bt.indicators.EMA(self.datas[0], period=self.params.emaSlow)
		self.ATR = bt.indicators.ATR(self.datas[0])
		self.sto = bt.indicators.Stochastic(self.datas[0])

		self.lastStage = 0

	def notify_order(self, order):
		if order.status in [order.Submitted, order.Accepted]:
			return

		if order.status in [order.Completed]:
			if order.isbuy():
				if self.position.size == 0:
					self.runningPnlTicks += self.runningTradePrice - order.executed.price
				else:
					self.runningTradePrice = order.executed.price
					self.stopLossTrail = self.dataLow[0] - RUNNING_STOP * self.ATR[0]
					self.stopLossBand = RUNNING_STOP * self.ATR[0]
					self.stopLossHard = self.dataLow[0] - HARD_STOP * self.ATR[0]
					self.stopGainHard = self.dataLow[0] + HARD_GAIN * self.ATR[0]
					print('Stop Loss Set: E:{} T:{:.5f} HG:{:.5f} HL:{:.5f} {:.2f}'.format(
						self.order.executed.price, 
						self.stopLossTrail, 
						self.stopGainHard,
						self.stopLossHard,
						(self.order.executed.price - self.stopLossTrail) * 100000))

				self.log(f'BUY EXECUTED, {order.executed.price:.5f}, {self.runningPnlTicks:.5f}')
			elif order.issell():
				if self.position.size == 0:
					self.runningPnlTicks += order.executed.price - self.runningTradePrice
				else:
					self.runningTradePrice = order.executed.price
					self.stopLossTrail = self.dataHigh[0] + RUNNING_STOP * self.ATR[0]
					self.stopLossBand = RUNNING_STOP * self.ATR[0]
					self.stopLossHard = self.dataHigh[0] + HARD_STOP * self.ATR[0]
					self.stopGainHard = self.dataHigh[0] - HARD_GAIN * self.ATR[0]
					print('Stop Loss Set: E:{} T:{:.5f} HG:{:.5f} HL:{:.5f} {:.2f}'.format(
						self.order.executed.price, 
						self.stopLossTrail,
						self.stopGainHard,
						self.stopLossHard,
						(self.stopLossTrail - self.order.executed.price) * 100000))

				self.log(f'SELL EXECUTED, {order.executed.price:.5f}, {self.runningPnlTicks:.5f}')
			self.bar_executed = len(self)

		elif order.status in [order.Canceled, order.Margin, order.Rejected]:
			self.log('Order Canceled/Margin/Rejected')

		self.order = None

	def next(self):
		if self.order:
			return

		if self.EMA_SPAN_FAST < self.EMA_SPAN_SLOW:
			if self.dataClose < self.EMA_SPAN_FAST:
				self.lastStage = -1
			elif self.dataClose > self.EMA_SPAN_SLOW:
				self.lastStage = 0
		elif self.EMA_SPAN_FAST > self.EMA_SPAN_SLOW:
			if self.dataClose > self.EMA_SPAN_FAST:
				self.lastStage = 1
			elif self.dataClose < self.EMA_SPAN_SLOW:
				self.lastStage = 0

		self.runningCoolOff += 1

		if not self.position:
			if self.ATR[0] >= MIN_ATR and abs(self.EMA_SPAN_FAST - self.EMA_SPAN_SLOW) > MIN_EMA_WIDTH * self.ATR[0]:
#					print('{} C:{:.5f} H:{:.5f} L:{:.5f} EF:{:.5f} ES:{:.5f} ATR:{:.5f} P:{}'.format(self.datas[0].datetime.time(),
#						self.dataClose[0], self.dataHigh[0], self.dataLow[0], self.EMA_SPAN_FAST[0], self.EMA_SPAN_SLOW[0], self.ATR[0], self.position.size))

				if self.EMA_SPAN_FAST < self.EMA_SPAN_SLOW:
					if self.lastStage == -1:
						if self.dataHigh[0] > self.EMA_SPAN_SLOW and self.dataClose < self.EMA_SPAN_SLOW and self.sto > 80.0:
							self.log(f'SELL CREATE {self.dataClose[0]:2f}')
							self.order = self.sell()
							self.runningCoolOff = 0
							self.tradeEntryTime = self.datas[0].datetime.time(0)
#							self.stopLossTrail = self.dataHigh[0] + RUNNING_STOP * self.ATR[0]
#							print('Stop Loss Set:', self.order.executed.price, self.stopLossTrail)
				elif self.EMA_SPAN_FAST > self.EMA_SPAN_SLOW:
					if self.lastStage == 1:
						if self.dataLow[0] < self.EMA_SPAN_SLOW and self.dataClose > self.EMA_SPAN_SLOW and self.sto < 20.0:
							self.log(f'BUY CREATE {self.dataClose[0]:2f}')
							self.order = self.buy()
							self.runningCoolOff = 0
							self.tradeEntryTime = self.datas[0].datetime.time(0)
#							self.stopLossTrail = self.dataLow[0] - RUNNING_STOP * self.ATR[0]
#							print('Stop Loss Set:', self.order.executed.price, self.stopLossTrail)
		else:
			if PRINT_TICKS == True:
				if USE_RUNNING_STOP:
					print('({}) C:{:.5f} H:{:.5f} L:{:.5f} T:{:.5f}'.format(self.datas[0].datetime.time(0), self.dataClose[0], self.dataHigh[0], self.dataLow[0], self.stopLossTrail))
				else:
					print('({}) C:{:.5f} H:{:.5f} L:{:.5f} HG:{:.5f} HS:{:.5f}'.format(self.datas[0].datetime.time(0), self.dataClose[0], self.dataHigh[0], self.dataLow[0], self.stopGainHard, self.stopLossHard))

			if self.position.size > 0:
				if self.runningCoolOff >= COOL_TICKS:
					if USE_RUNNING_STOP:
						if self.dataLow[0] < self.stopLossTrail:
							self.log(f'SELL CLOSE {self.dataClose[0]:2f} H:{self.dataHigh[0]}, L:{self.dataLow[0]}')
							self.order = self.close()
#							input()
						elif self.dataHigh[0] - self.stopLossTrail > self.stopLossBand:
							self.stopLossTrail = self.dataHigh[0] - self.stopLossBand
#						elif self.dataHigh[0] - self.stopLossTrail > RUNNING_STOP * self.ATR[0]:
#							self.stopLossTrail = self.dataHigh[0] - RUNNING_STOP * self.ATR[0]
					else:
						if self.dataLow[0] < self.stopLossHard or self.dataHigh[0] > self.stopGainHard:
							self.log(f'SELL CLOSE {self.dataClose[0]:2f} H:{self.dataHigh[0]}, L:{self.dataLow[0]}')
							self.order = self.close()				
#							input()
			else:
				if self.runningCoolOff >= COOL_TICKS:
					if USE_RUNNING_STOP:
						if self.dataHigh[0] > self.stopLossTrail:
							self.log(f'BUY CLOSE {self.dataClose[0]:2f} H:{self.dataHigh[0]}, L:{self.dataLow[0]}')
							self.order = self.close()
#							input()
						elif self.stopLossTrail - self.dataLow[0] > self.stopLossBand:
							self.stopLossTrail = self.dataLow[0] + self.stopLossBand
#						elif self.stopLossTrail - self.dataLow[0] > RUNNING_STOP * self.ATR[0]:
#							self.stopLossTrail = self.dataLow[0] + RUNNING_STOP * self.ATR[0]
					else:
						if self.dataHigh[0] > self.stopLossHard or self.dataLow[0] < self.stopGainHard:
							self.log(f'BUY CLOSE {self.dataClose[0]:2f} H:{self.dataHigh[0]}, L:{self.dataLow[0]}')
							self.order = self.close()
#							input()

	def stop(self):
		pnl = round(self.broker.getvalue() - self.startcash, 2)
		print('EMAFast: {} EMASLOW: {}  Final PnL: {}'.format(
            self.params.emaFast, self.params.emaSlow, pnl))

def printTradeAnalysis(analyzer):
	totalOpen = analyzer.total.open
	totalClosed = analyzer.total.closed
	totalWon = analyzer.won.total
	totalLost = analyzer.lost.total
	winStreak = analyzer.streak.won.longest
	loseStreak = analyzer.streak.lost.longest
	pnlNet = round(analyzer.pnl.net.total, 2)
	winPerc = round((totalWon / totalClosed) * 100, 2)
	h1 = ['Total Open', 'Total Closed', 'Total Won', 'Total Lost']
	h2 = ['Strike Rate','Win Streak', 'Losing Streak', 'PnL Net']
	r1 = [totalOpen, totalClosed,totalWon,totalLost]
	r2 = [winPerc, winStreak, loseStreak, pnlNet]
	if len(h1) > len(h2):
		headerLength = len(h1)
	else:
		headerLength = len(h2)
	printList = [h1,r1,h2,r2]
	rowFormat = "{:<10}" + "{:<15}" * (headerLength)
	print("Trade Analysis Results:")
	for row in printList:
		print(rowFormat.format('', *row))

def printSQN(analyzer):
	sqn = round(analyzer.sqn, 2)
	print('SQN: {}'.format(sqn))

def printSharpe(analyzer):
	sharpe = round(analyzer['sharperatio'], 4)
	print('Sharpe: {}'.format(sharpe))

def printReturns(analyzer):
	print('Return:', analyzer)

def addCerebro(tickData):
	global cerebro

	cerebro = bt.Cerebro()

	cerebro.addobserver(
		bt.observers.BuySell,
		barplot=True,
		bardist=0.0001
	)

	whichDataFeed = pd.DataFrame()
	whichDataFeed = tickData.copy()

	data = bt.feeds.PandasData(dataname=tickData, timeframe=bt.TimeFrame.Minutes)
	cerebro.adddata(data)

	cerebro.broker.setcash(100000.0)
	cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='myAnalysis')
	cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='mySharpe', timeframe=bt.TimeFrame.Minutes, 
		riskfreerate=0.0, annualize=True)
	cerebro.addanalyzer(bt.analyzers.Returns, _name='myReturns', timeframe=bt.TimeFrame.Minutes)
	cerebro.addanalyzer(bt.analyzers.SQN, _name="mySqn")

	cerebro.broker.setcommission(commission=BROKER_COMMISSION, margin=BROKER_MARGIN, mult=BROKER_MULT)
	cerebro.addsizer(bt.sizers.SizerFix, stake=4)

def runCerebro(theseParams, plotIt=False):
	cerebro.addstrategy(TestStrategy, emaFast=theseParams.emaFast, emaSlow=theseParams.emaSlow)
	start_portfolio_value = cerebro.broker.getvalue()

	myResults = cerebro.run(maxcpus=1)
	myResult = myResults[0]
	print(myResults[0])
	printTradeAnalysis(myResult.analyzers.myAnalysis.get_analysis())
	printSQN(myResult.analyzers.mySqn.get_analysis())
	printSharpe(myResult.analyzers.mySharpe.get_analysis())
	printReturns(myResult.analyzers.myReturns.get_analysis())

	end_portfolio_value = cerebro.broker.getvalue()
	pnl = end_portfolio_value - start_portfolio_value
	print(f'Starting Portfolio Value: {start_portfolio_value:2f}')
	print(f'Final Portfolio Value: {end_portfolio_value:2f}')
	print(f'PnL: {pnl:.2f}')

	if plotIt:
		cerebro.plot()

	return (pnl,
		myResult.analyzers.mySharpe.get_analysis()['sharperatio'], 
		myResult.analyzers.mySqn.get_analysis().sqn,
		myResult.analyzers.myAnalysis.get_analysis().total.closed,
		myResult.analyzers.myAnalysis.get_analysis().won.total,
		myResult.analyzers.myAnalysis.get_analysis().lost.total,
		round((myResult.analyzers.myAnalysis.get_analysis().won.total / myResult.analyzers.myAnalysis.get_analysis().total.closed) * 100, 2),
		myResult.analyzers.myAnalysis.get_analysis().streak.won.longest,
		myResult.analyzers.myAnalysis.get_analysis().streak.lost.longest)

def runOptimization(tickData, optMetric):
	allResults = {}
	optimizedParams = None

	for eachEMAFast in range(50, 55, 5):
		for eachEMASlow in range(100, 105, 5):
			theseParams = btParams(eachEMAFast, eachEMASlow)

			addCerebro(tickData)
			pnl, sharpe, sqn, totClosed, totWon, totLost, strikeRate, winStreak, loseStreak = runCerebro(theseParams)
			allResults[theseParams] = testResults(pnl, sharpe, sqn, totClosed, totWon, totLost, strikeRate, winStreak, loseStreak)

	for eachParam in allResults:
		if optimizedParams is None:
			optimizedParams = eachParam
		else:
			if optMetric == 'pnl':
				if allResults[eachParam].pnl > allResults[optimizedParams]:
					optimizedParams = eachParam
			elif optMetric == 'sharpe':
				if allResults[eachParam].sharpe > allResults[optimizedParams]:
					optimizedParams = eachParam

	return optimizedParams, allResults[optimizedParams]

def runBacktesting(tickData, optMetric):
	optimizedParams, optimizedResults = runOptimization(tickData, optMetric)
	return optimizedResults
