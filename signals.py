import backtrader as bt
from backtrader.indicators import Lowest, Highest
from indicators import DonchianChannel

class BreakoutSignalBase(bt.Indicator):
    params = (
        ('period', 20),
    )

    def __init__(self, *args):
        super(BreakoutSignalBase, self).__init__(*args)
        self.donchian = DonchianChannel(self.data, period=self.p.period)

    @staticmethod
    def get_breakout_signal(price, mband, wband):
        breakout_signal = 2*(price - mband) / wband

        if breakout_signal >= 2:
            breakout_signal = 2
        elif breakout_signal <= -2:
            breakout_signal = -2
        
        return breakout_signal

class IntradayBreakoutSignal(BreakoutSignalBase):
    lines = ('long_signal', 'short_signal', )

    def next(self):
        mband = self.donchian.mband[0]
        wband = self.donchian.wband[0]
        self.l.long_signal[0] = self.get_breakout_signal(price=self.data.high[0], mband=mband, wband=wband)
        self.l.short_signal[0] = self.get_breakout_signal(price=self.data.low[0], mband=mband, wband=wband)

class CloseBreakoutSignal(BreakoutSignalBase):
    lines = ('signal', )

    def next(self):
        mband = self.donchian.mband[0]
        wband = self.donchian.wband[0]
        self.l.signal[0] = self.get_breakout_signal(price=self.data.close[0], mband=mband, wband=wband)

class COTSignalBase(bt.Indicator):
    lines = ('signal', )

    params = (
        ('period', 52),
        ('threshold', 70),
    )

class MMConcentration_Signal(COTSignalBase):
    def __init__(self, *args):
        super(MMConcentration_Signal, self).__init__(*args)

        period = self.p.period

        # Money Managers Long (MML) Concentration
        mml_conc = self.data.mml_concentration
        self._stoch_mml_conc = 100*(mml_conc - Lowest(mml_conc, period=period)) / (Highest(mml_conc, period=period) - Lowest(mml_conc, period=period))

        # Money Managers Short (MMS) Concentration
        mms_conc = self.data.mms_concentration
        self._stoch_mms_conc = 100*(mms_conc - Lowest(mms_conc, period=period)) / (Highest(mms_conc, period=period) - Lowest(mms_conc, period=period))
    
    def next(self):
        threshold = self.p.threshold
        ulim = threshold
        llim = 100 - threshold

        mml_conc = self._stoch_mml_conc[0]
        mms_conc = self._stoch_mms_conc[0]
        
        if mml_conc > ulim:
            # mml -> overbought
            
            if mms_conc < llim:
                # mms -> undersold
                cot_signal = -2 # strong sell
            elif mms_conc < ulim:
                # mms -> neutral
                cot_signal = -1 # sell
            else:
                # mms -> oversold
                cot_signal = 0 # neutral

        elif mms_conc > ulim:
            # mms -> oversold

            if mml_conc < llim:
                # mml -> underbought
                cot_signal = 2 # strong buy
            elif mml_conc < ulim:
                # mml -> neutral
                cot_signal = 1 # buy
            else:
                # mml -> overbought
                cot_signal = 0 # neutral
        
        else:
            cot_signal = 0   # neutral
        
        self.l.signal[0] = cot_signal
        

class MMClusteringPosSize_Signal(COTSignalBase):
    def __init__(self, *args):
        super(MMClusteringPosSize_Signal, self).__init__(*args)

        period = self.p.period

        # Money Managers Long (MML) Clustering & Position Size
        mml_clus = self.data.mml_clustering
        mml_possize = self.data.mml_possize
        self._stoch_mml_clus = 100*(mml_clus - Lowest(mml_clus, period=period)) / (Highest(mml_clus, period=period) + Lowest(mml_clus, period=period))
        self._stoch_mml_possize = 100*(mml_possize - Lowest(mml_possize, period=period)) / (Highest(mml_possize, period=period) + Lowest(mml_possize, period=period))

        # Money Managers Short (MMS) Clustering  & Position Size
        mms_clus = self.data.mms_clustering
        mms_possize = self.data.mms_possize
        self._stoch_mms_clus = 100*(mms_clus - Lowest(mms_clus, period=period)) / (Highest(mms_clus, period=period) + Lowest(mms_clus, period=period))
        self._stoch_mms_possize = 100*(mms_possize - Lowest(mms_possize, period=period)) / (Highest(mms_possize, period=period) + Lowest(mms_possize, period=period))
    
    def next(self):
        threshold = self.p.threshold
        ulim = threshold
        llim = 100 - threshold

        if self._stoch_mml_clus[0] > ulim:
            # mml clustering -> overbought

            if self._stoch_mms_clus[0] < llim:
                # mms clustering -> undersold
                
                if self._stoch_mml_possize[0] > ulim and self._stoch_mms_possize[0] < llim:
                    # mml possize -> overbought AND mms possize -> undersold
                    cot_signal = -2 # strong sell
                elif self._stoch_mml_possize[0] > ulim or self._stoch_mms_possize[0] < llim:
                    # mml possize -> overbought OR mms possize -> undersold
                    cot_signal = -1.5 # semi-strong sell
                else:
                    cot_signal = -1 # sell
                    
            elif self._stoch_mms_clus[0] < ulim:
                # mms clustering -> neutral
                
                if self._stoch_mml_possize[0] > ulim:
                    # mml possize -> overbought
                    cot_signal = -1 # sell
                else:
                    # mml possize NOT overbought
                    cot_signal = -0.5 # weak sell

            else:
                # mms clustering -> oversold
                cot_signal = 0
            
        elif self._stoch_mms_clus[0] > ulim:
            # mms clustering -> oversold

            if self._stoch_mml_clus[0] < llim:
                # mml clustering -> underbought
                
                if self._stoch_mms_possize[0] > ulim and self._stoch_mml_possize[0] < llim:
                    # mms possize -> oversold AND mml possize -> underbought
                    cot_signal = 2 # strong buy
                elif self._stoch_mms_possize[0] > ulim or self._stoch_mml_possize[0] < llim:
                    # mms possize -> oversold OR mml possize -> underbought
                    cot_signal = 1.5 # semi-strong buy
                else:
                    cot_signal = -1 # buy

            elif self._stoch_mms_clus[0] < ulim:
                # mml clustering -> neutral
                
                if self._stoch_mms_possize[0] > ulim:
                    # mms possize -> oversold
                    cot_signal = 1 # buy
                else:
                    # mms possize NOT oversol
                    cot_signal = -0.5 # weak sell

            else:
                # mml clustering -> overbought
                cot_signal = 0

        else:
            # mml -> NOT overbought AND mms -> NOT overbought
            cot_signal = 0

        self.l.signal[0] = cot_signal


class PMPUNet_SignalBase(COTSignalBase):
    def next(self):
        threshold = self.p.threshold
        ulim = threshold
        llim = 100 - threshold

        if self._stoch_pmpu_net[0] > ulim:
            # if pmpu net -> long:
            cot_signal = 1        # long/buy

        elif self._stoch_pmpu_net[0] < llim:
            # if pmpu net -> long:
            cot_signal = -1        # short/sell

        else:
            cot_signal = 0         # neutral
        
        self.l.signal[0] = cot_signal

class PMPUNetOI_Signal(PMPUNet_SignalBase):
    def __init__(self, *args):
        super(PMPUNetOI_Signal, self).__init__(*args)
        period = self.p.period
        pmpu_net = self.data.pmpu_netoi
        
        self._stoch_pmpu_net = 100*(pmpu_net - Lowest(pmpu_net, period=period)) / (Highest(pmpu_net, period=period) + Lowest(pmpu_net, period=period))
    
class PMPUNetT_Signal(PMPUNet_SignalBase):
    def __init__(self, *args):
        super(PMPUNetT_Signal, self).__init__(*args)
        period = self.p.period
        pmpu_net = self.data.pmpu_nett
        
        self._stoch_pmpu_net = 100*(pmpu_net - Lowest(pmpu_net, period=period)) / (Highest(pmpu_net, period=period) + Lowest(pmpu_net, period=period))

class PMPUNetPosSize_Signal(PMPUNet_SignalBase):
    def __init__(self, *args):
        super(PMPUNetPosSize_Signal, self).__init__(*args)
        period = self.p.period
        pmpu_net = self.data.pmpu_netpossize
        
        self._stoch_pmpu_net = 100*(pmpu_net - Lowest(pmpu_net, period=period)) / (Highest(pmpu_net, period=period) + Lowest(pmpu_net, period=period))
