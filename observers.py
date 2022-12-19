from backtrader import Observer, TimeFrame
from backtrader.analyzers import TimeReturn

class DailyReturn(Observer):

    lines = ('dailyreturn', )

    def __init__(self):
        self.dreturn = self._owner._addanalyzer_slave(TimeReturn, timeframe=TimeFrame.Days)

    def next(self):
        self.lines.dailyreturn[0] = self.dreturn.rets.get(self.dreturn.dtkey,
                                                         float('NaN'))

class DailyPositions(Observer):

    def __init__(self):
        self.symbols = self._owner.symbols
        self.lines = (self.symbols[i] if i+1 < len(self.symbols) else 'cash' for i in range(len(self.symbols)+1))