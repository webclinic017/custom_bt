import backtrader as bt
from signal_factories import COTSignalFactory, BreakoutSignalFactory
from indicators import DonchianChannel
from math import floor, isnan

class CustomStrategy(bt.Strategy):
    
    def _get_open_orders(self, data: bt.DataBase, simulation=True) -> dict:
        all_open_orders = self.broker.get_orders_open()
        open_orders = dict(market=None, stop=None)
        
        for order in all_open_orders:
            if order.data == data:
                if order.exectype == bt.Order.Stop:
                    open_orders['stop'] = order
                if simulation:
                    if order.exectype == bt.Order.Market:
                        open_orders['market'] = order
                else:
                    if order.exectype == bt.Order.Market:
                        open_orders['market'] = order
        return open_orders
    
    def _update_stop_order(self, data: bt.DataBase, stop: float, size: float, isbuy: bool) -> bt.OrderBase:
        open_orders = self._get_open_orders(data=data)
        open_stop_order = open_orders['stop']

        if open_stop_order:
            if isbuy:
                return bt.StopBuyOrder(data=data, price=stop, size=size, exectype=bt.Order.Stop, oco=open_stop_order)
            else:
                return bt.StopSellOrder(data=data, price=stop, size=size, exectype=bt.Order.Stop, oco=open_stop_order)
        else:
            raise ValueError('No stop order to update')

    @staticmethod
    def _create_stop_order(data: bt.feed.DataBase, stop: float, size: int, isbuy: bool) -> bt.OrderBase:
        if isbuy:
            return bt.StopBuyOrder(data=data, price=stop, size=size, exectype=bt.Order.Stop)
        else:
            return bt.StopSellOrder(data=data, price=stop, size=size, exectype=bt.Order.Stop)
    
    @staticmethod
    def _create_market_order(data:bt.feed.DataBase, price: float, size: int, isbuy: bool, simulation=True) -> bt.OrderBase:
        if isbuy:
            if not simulation:
                return bt.BuyOrder(data=data, size=size, exectype=bt.Order.Market)
            else:
                return bt.BuyOrder(data=data, size=size, price=price, exectype=bt.Order.Market)
        else:
            if not simulation:
                return bt.SellOrder(data=data, size=size, exectype=bt.Order.Market)
            else:
                return bt.SellOrder(data=data, size=size, price=price, exectype=bt.Order.Market)

class PortfolioWrapper():
    def __init__(self, symbols) -> None:
        self.symbols=symbols
        self.inds=self.dict_of_dicts(*symbols)
        self.open_trades=self.dict_of_dicts(*symbols)
    
    @staticmethod
    def dict_of_dicts(*args, **kwargs):
        return {arg: dict(kwargs) for arg in args}

class PortfolioStrategyBase(PortfolioWrapper, CustomStrategy):
    def __init__(self, symbols) -> None:
        super().__init__(symbols)

    def submit_orders(self, orders: dict):
        for symbol in self.symbols:
            symbol_orders = orders[symbol]
            for order in symbol_orders.values():
                if order:
                    self.broker.submit(order)
    
    def _is_active(self, symbol: str, runonce=True):
        inds = self.inds[symbol]
        
        # Checking all of the symbol's indicators have been initiated
        for _, ind in inds.items():
            for line in ind.lines:
                if len(line) == 0:
                    return False
                else:
                    if isnan(line[0]):
                        return False
        return True

class DaniPortfolio(PortfolioStrategyBase):
    def __init__(self, symbols, intraday: bool, breakout_period: int, n_entries: int) -> None:
        super(DaniPortfolio, self).__init__(symbols)
        self.intraday=intraday
        self.breakout_period=breakout_period
        self.n_entries=n_entries

        self.entries_count = self.dict_of_dicts()

        for symbol in self.symbols:

            data = self.getdatabyname(symbol)

            # Breakout Signal and Donchian Channel Indicator
            self.inds[symbol]['donchian'] = DonchianChannel(data, period=self.breakout_period)
            breakout_signal_factory = BreakoutSignalFactory(data)
            self.inds[symbol]['breakout_signal'] = breakout_signal_factory.create_breakout_signal(period=self.breakout_period, 
                                                                                                intraday=self.intraday)

    #- RISK MANAGEMENT RULES
    def _get_position_sizing(self, data: bt.feeds.DataBase, exec_price: float, stop: float) -> int:
        aum = self.broker.getvalue()
        comminfo = self.broker.getcommissioninfo(data)
        mult = comminfo.p.mult

        # WC Loss <= 0.5% AuM
        worst_case_loss = mult * self.n_entries * (exec_price - stop)
        size_wc = 0.005 * aum / worst_case_loss

        # Individual positions <= 30% AuM
        if exec_price*size_wc > 0.3 * aum:
            size_ind_pos = 0.3 * aum / exec_price
        else:
            size_ind_pos = size_wc
        
        # Position Sizing
        size = min(abs(size_wc), abs(size_ind_pos))

        # Size always FLOORED and >=1:
        if size < 1:
            return 1
        else:
            return floor(size)

class COT_Breakout(DaniPortfolio):
    SYMBOLS = [
            'CC', 'KC', 'C', 'CT', 'W',
            'SB', 'BO', 'S', 'SM'
        ]

    def __init__(self, 
                symbols=SYMBOLS, intraday=True,
                breakout_period=20, n_entries=3,
                cot_component_name=None, cot_component_period=52, cot_threshold=70) -> None:
        super(COT_Breakout, self).__init__(symbols, intraday, breakout_period, n_entries)
        
        self.cot_component_name=cot_component_name
        self.cot_component_period=cot_component_period
        self.cot_threshold=cot_threshold

        for symbol in self.symbols:
            if not self.cot_component_name:
                raise ValueError("The COT component name must be passed as a parameter with keyword 'cot_component_name'")

            # COT Signal
            data = self.getdatabyname(self._get_cot_dataname(symbol))
            cot_signal_factory = COTSignalFactory(data)
            self.inds[symbol]['cot_signal'] = cot_signal_factory.create_COT_signal(name=self.cot_component_name,
                                                                                period=self.cot_component_period, 
                                                                                threshold=self.cot_threshold)
    
    def prenext_open(self):
        self.next_open()
    
    def next_open(self):
        orders = self.dict_of_dicts(*self.symbols, market=None, stop=None)
        for symbol in self.symbols:
            if self._is_active(symbol=symbol):
                donchian = self.inds[symbol]['donchian']
                data = self.getdatabyname(symbol)
                position = self.broker.getposition(data)
                stop = donchian.mband[0]

                if position:

                    # LONG STOP
                    if position.size > 0:
                        orders[symbol]['stop'] = self._update_stop_order(data=data, stop=stop, size=abs(position.size), isbuy=True)
                    
                    # SHORT STOP
                    else:
                        orders[symbol]['stop'] = self._update_stop_order(data=data, stop=stop, size=abs(position.size), isbuy=False)

    def prenext(self):
        self.next()

    def next(self):
        orders = self.dict_of_dicts(*self.symbols, market=None, stop=None)
        for symbol in self.symbols:
            if self._is_active(symbol=symbol):
                data = self.getdatabyname(symbol)
                position = self.broker.getposition(data)
                breakout_signal = self.inds[symbol]['breakout_signal']
                donchian = self.inds[symbol]['donchian']

                #- NEW POSITIONS
                if not position:
                    cot_signal = self.inds[symbol]['cot_signal'].signal[0]

                    # LONG side
                    if cot_signal > 0 and breakout_signal.long_signal[0] > 1:
                        if self.intraday:
                            exec_price = donchian.hband[0]
                        else:
                            exec_price = data.close[0]
                        stop = donchian.mband[0]
                        
                        size = self._get_position_sizing(data=data, exec_price=exec_price, stop=stop)
                        orders[symbol]['market'] = self._create_market_order(data=data, price=exec_price, size=size, isbuy=True)
                        orders[symbol]['stop'] = self._create_stop_order(data=data, stop=stop, size=size, isbuy=False)

                    # SHORT side
                    elif cot_signal < 0 and breakout_signal.short_signal[0] < -1:
                        if self.intraday:
                            exec_price = donchian.lband[0]
                        else:
                            exec_price = data.close[0]
                        stop = donchian.mband[0]
                        
                        size = self._get_position_sizing(data=data, exec_price=exec_price, stop=stop)
                        orders[symbol]['market'] = self._create_market_order(data=data, price=exec_price, size=size, isbuy=False)
                        orders[symbol]['stop'] = self._create_stop_order(data=data, stop=stop, size=size, isbuy=False)

                #- ONGOING POSITIONS
                else:

                    # LONG
                    if position.size > 0:

                        # INCREASE position after CONSECUTIVE BREAKOUTS
                        if breakout_signal.long_signal[0] > 1 and self.entries_count[symbol] < self.p.n_entries:
                            if self.intraday:
                                exec_price = donchian.hband[0]
                            else:
                                exec_price = data.close[0]
                            size = self._get_position_sizing(data=data, exec_price=exec_price, stop=stop)
                            orders[symbol]['market'] = self._create_market_order(data=data, price=exec_price, size=size, isbuy=True)

                    # SHORT
                    else:

                        # INCREASE position after CONSECUTIVE BREAKOUTS
                        if breakout_signal.short_signal[0] < -1 and self.entries_count[symbol] < self.p.n_entries:
                            if self.intraday:
                                exec_price = donchian.lband[0]
                            else:
                                exec_price = data.close[0]
                            size = self._get_position_sizing(data=data, exec_price=exec_price, stop=stop)
                            orders[symbol]['market'] = self._create_market_order(data=data, price=exec_price, size=size, isbuy=False)
            
        self.submit_orders(orders=orders)
    
    def stop(self):
        print(f'AuM início: USD {self.broker.getvalue()}')
    
    @staticmethod
    def _get_cot_dataname(symbol: str) -> str:
        return f'{symbol}_cot'

