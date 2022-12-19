import backtrader as bt
from signal_factories import COTSignalFactory, BreakoutSignalFactory
from indicators import DonchianChannel
from math import floor, isnan
import pandas as pd

class PortfolioStrategyBase(bt.Strategy):
    def __init__(self, symbols: str) -> None:
        self.symbols=symbols
        self._aum=self.broker.getvalue()

        # Defining (empty) Indicators & Signals dicts
        self.inds=self.dict_of_dicts(*symbols)
        self.signals=self.dict_of_dicts(*symbols)

    @property
    def aum(self):
        return self._aum
    
    @aum.setter
    def aum(self, aum: float):
        self._aum = aum
    
    def prenext(self):
        # Run portfolio strategy
        self.pnext()

        # Update AuM
        aum = self.broker.getvalue()
        self.aum = aum
    
    def next(self):
        # Run portfolio strategy
        self.pnext()

        # Update AuM
        aum = self.broker.getvalue()
        self.aum = aum

    def pnext(self):
        raise NotImplementedError
    
    def start(self):
        print(f'AuM inicial: {self.broker.getvalue()}')

    def stop(self):
        print(f'AuM final: {self.aum}')

    def next_open(self):
        self.pnext_open()
    
    def prenext_open(self):
        self.pnext_open()

    def pnext_open(self):
        raise NotImplementedError

    def _is_active(self, symbol: str):
        # Checking if all of the symbol's Indicators have been initiated
        for _, ind in self.inds[symbol].items():
            for line in ind.lines:
                if len(line) == 0 or isnan(line[0]):
                    return False

        # Checking if all of the symbol's Signals have been initiated
        for _, ind in self.signals[symbol].items():
            for line in ind.lines:
                if len(line) == 0 or isnan(line[0]):
                    return False

        return True
    
    @staticmethod
    def dict_of_dicts(*args, **kwargs):
        if not args and not kwargs:
            return dict()
        elif args and not kwargs:
            return {arg: dict() for arg in args}
        else:
            return {arg: dict(kwargs) for arg in args}

class DaniPortfolio(PortfolioStrategyBase):
    def __init__(self, symbols, intraday: bool, breakout_period: int, n_entries: int) -> None:
        super(DaniPortfolio, self).__init__(symbols)
        self.intraday=intraday
        self.breakout_period=breakout_period
        self.n_entries=n_entries
        self.entries_count={symbol: 0 for symbol in symbols}

        for symbol in symbols:
            data = self.getdatabyname(symbol)

            # Donchian Channel Indicator
            self.inds[symbol]['donchian'] = DonchianChannel(data, period=self.breakout_period)

            # Breakout Signal
            breakout_signal_factory = BreakoutSignalFactory(data)
            self.signals[symbol]['breakout'] = breakout_signal_factory.create_breakout_signal(period=self.breakout_period, 
                                                                                                intraday=self.intraday)

    #- RISK MANAGEMENT RULES
    def _get_position_sizing(self, mult: float, price: float, stop: float) -> int:
        # Worst Case Loss <= 0.5% AuM
        wc_loss = mult * abs(price - stop)
        size_wc = ((0.005 * self.aum) / self.n_entries) / wc_loss

        # Individual positions <= 30% AuM
        if price*size_wc > 0.3 * self.aum:
            size_ind_pos = 0.3 * self.aum / price
        else:
            size_ind_pos = size_wc
        
        # Position Sizing
        size = min(size_wc, size_ind_pos)

        # Size always FLOORED and >= 1:
        if size < 1:
            return 1
        else:
            return floor(size)

class BreakoutOnly(DaniPortfolio):
    
    def __init__(self, symbol, intraday=False, breakout_period=20, n_entries=3) -> None:
        super().__init__(symbol, intraday, breakout_period, n_entries)
    
    def pnext(self):
        if self._is_active():
            data = self.getdatabyname(self.symbol)
            comminfo = self.broker.getcommissioninfo(data)
            position = self.broker.getposition(data)
            donchian = self.inds['donchian']
            breakout_signal = self.signals['breakout'].signal[0]
            stop = donchian.mband[0]

            #- NEW POSITIONS
            if not position and self.entries_count == 0:

                # LONG side
                if breakout_signal > 1:
                    if self.intraday:
                        exec_price = donchian.hband[0]
                    else:
                        exec_price = data.close[0]
                    size = self._get_position_sizing(mult=comminfo.p.mult, price=exec_price, stop=stop)
                    self.buy(data=data, size=size)

                # SHORT side
                elif breakout_signal < -1:
                    if self.intraday:
                        exec_price = donchian.lband[0]
                    else:
                        exec_price = data.close[0]
                    size = self._get_position_sizing(mult=comminfo.p.mult, price=exec_price, stop=stop)
                    self.sell(data=data, size=size)

            #- ONGOING POSITIONS
            else:

                # LONG
                if position.size > 0:

                    # LONG POSITION STOP RULE
                    if (self.intraday and data.low[0] < stop) or data.close[0] < stop:
                        self.close(data=data)

                    # INCREASE position after CONSECUTIVE BREAKOUTS
                    elif breakout_signal > 1 and self.entries_count < self.n_entries:
                        if self.intraday:
                            exec_price = donchian.hband[0]
                        else:
                            exec_price = data.close[0]
                        size = self._get_position_sizing(mult=comminfo.p.mult, price=exec_price, stop=stop)
                        self.buy(data=data, size=size)

                # SHORT
                elif position.size < 0:

                    # SHORT POSITION STOP RULE
                    if (self.intraday and data.high[0] > stop) or data.close[0] > stop:
                        self.close(data=data)

                    # INCREASE position after CONSECUTIVE BREAKOUTS
                    elif breakout_signal < -1 and self.entries_count < self.n_entries:
                        if self.intraday:
                            exec_price = donchian.lband[0]
                        else:
                            exec_price = data.close[0]
                        size = self._get_position_sizing(mult=comminfo.p.mult, price=exec_price, stop=stop)
                        self.sell(data=data, size=size)          

class COT_Breakout(DaniPortfolio):

    def __init__(self, 
                symbols, intraday=False,
                breakout_period=20, n_entries=3,
                cot_component_name=None, cot_component_period=52, cot_threshold=70) -> None:
        super(COT_Breakout, self).__init__(symbols, intraday, breakout_period, n_entries)

        if not cot_component_name:
                raise ValueError("The COT component name must be passed as a parameter with keyword 'cot_component_name'")

        self.initial_cot_signal = dict()

        for symbol in symbols:
            # COT Signal
            data = self.getdatabyname(self._get_cot_dataname(symbol))
            cot_signal_factory = COTSignalFactory(data)
            self.signals[symbol]['cot'] = cot_signal_factory.create_COT_signal(name=cot_component_name,
                                                                    period=cot_component_period, 
                                                                    threshold=cot_threshold)
            self.initial_cot_signal[symbol] = None

    def pnext(self):
        for symbol in self.symbols:
            if self._is_active(symbol):
                data = self.getdatabyname(symbol)
                comminfo = self.broker.getcommissioninfo(data)
                position = self.broker.getposition(data)
                donchian = self.inds[symbol]['donchian']
                breakout_signal = self.signals[symbol]['breakout'].signal[0]
                stop = donchian.mband[0]

                #- NEW POSITIONS
                if not position and self.entries_count[symbol] == 0:
                    cot_signal = self.signals[symbol]['cot'].signal[0]

                    # LONG side
                    if cot_signal > 0 and breakout_signal > 1:
                        if self.intraday:
                            exec_price = donchian.hband[0]
                        else:
                            exec_price = data.close[0]
                        size = self._get_position_sizing(mult=comminfo.p.mult, price=exec_price, stop=stop)
                        self.buy(data=data, size=size)
                        self.entries_count[symbol] += 1
                        self.initial_cot_signal[symbol] = cot_signal

                    # SHORT side
                    elif cot_signal < 0 and breakout_signal < -1:
                        if self.intraday:
                            exec_price = donchian.lband[0]
                        else:
                            exec_price = data.close[0]
                        size = self._get_position_sizing(mult=comminfo.p.mult, price=exec_price, stop=stop)
                        self.sell(data=data, size=size)
                        self.entries_count[symbol] += 1
                        self.initial_cot_signal[symbol] = cot_signal

                #- ONGOING POSITIONS
                else:

                    # LONG
                    if position.size > 0:

                        # LONG POSITION STOP RULE
                        if (self.intraday and data.low[0] < stop) or data.close[0] < stop:
                            self.close(data=data)
                            self.entries_count[symbol] = 0

                        # INCREASE position after CONSECUTIVE BREAKOUTS
                        elif breakout_signal > 1 and self.entries_count[symbol] < self.n_entries:
                            if self.intraday:
                                exec_price = donchian.hband[0]
                            else:
                                exec_price = data.close[0]
                            size = self._get_position_sizing(mult=comminfo.p.mult, price=exec_price, stop=stop)
                            self.buy(data=data, size=size)
                            self.entries_count[symbol] += 1

                    # SHORT
                    elif position.size < 0:

                        # SHORT POSITION STOP RULE
                        if (self.intraday and data.high[0] > stop) or data.close[0] > stop:
                            self.close(data=data)
                            self.entries_count[symbol] = 0

                        # INCREASE position after CONSECUTIVE BREAKOUTS
                        elif breakout_signal < -1 and self.entries_count[symbol] < self.n_entries:
                            if self.intraday:
                                exec_price = donchian.lband[0]
                            else:
                                exec_price = data.close[0]
                            size = self._get_position_sizing(mult=comminfo.p.mult, price=exec_price, stop=stop)
                            self.sell(data=data, size=size)
                            self.entries_count[symbol] += 1

    @staticmethod
    def _get_cot_dataname(symbol: str) -> str:
        return f'{symbol}_cot'