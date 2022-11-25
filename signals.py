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
        self._stoch_mml_conc = 100*(mml_conc - Lowest(mml_conc, period=period)) / (Highest(mml_conc, period=period) + Lowest(mml_conc, period=period))

        # Money Managers Short (MMS) Concentration
        mms_conc = self.data.mms_concentration
        self._stoch_mms_conc = 100*(mms_conc - Lowest(mms_conc, period=period)) / (Highest(mms_conc, period=period) + Lowest(mms_conc, period=period))
    
    def next(self):
        threshold = self.p.threshold
        
        if self._stoch_mml_conc[0] > threshold and self._stoch_mms_conc[0] < (1- threshold):
            # if mml -> overbought and mms -> undersold:
            cot_signal = -2  # extremely overbought (strong sell)

        elif self._stoch_mml_conc[0] > threshold and self._stoch_mms_conc[0] < threshold:
            # if mml -> overbought and mms -> neutral:
            cot_signal = -1  # overbought (sell)
        
        elif self._stoch_mml_conc[0] < (1 - threshold) and self._stoch_mms_conc[0] > threshold:
            # if mml -> underbought and mms -> oversold:
            cot_signal = 2   # extremely oversold (strong buy)

        elif not self._stoch_mml_conc[0] < threshold and self._stoch_mms_conc[0] > threshold:
            # if mml -> neutral and mms -> oversold:
            cot_signal = 1   # oversold (buy)
        
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
        
        #- LONG SIGNAL
        if self._stoch_mml_clus[0] > threshold and self._stoch_mml_possize[0] > threshold:
            # if clustering -> overbought and pos size -> overbought:
            long_cot_signal = -1        # overbought (sell)

        elif self._stoch_mml_clus[0] > threshold and self._stoch_mml_possize[0] < threshold:
            # if clustering -> overbought and pos size -> not overbought:
            long_cot_signal = -0.5      # slightly overbought (weak sell)
        
        elif self._stoch_mml_clus[0] < (1 - threshold) and self._stoch_mml_possize[0] > (1 - threshold):
            # if clustering -> underbought and pos size -> not underbought:
            long_cot_signal = 0.5       # slightly underbought (weak buy)
        
        elif self._stoch_mml_clus[0] < (1 - threshold) and self._stoch_mml_possize[0] < (1 - threshold):
            # if clustering -> underbought and pos size -> underbought:
            long_cot_signal = 1         # underbought (buy)
        
        else:
            long_cot_signal = 0         # neutral
        

        #- SHORT SIGNAL
        if self._stoch_mms_clus[0] > threshold and self._stoch_mms_possize[0] > threshold:
            # if clustering -> oversold and pos size -> oversold:
            short_cot_signal = 1        # oversold (buy)

        elif self._stoch_mms_clus[0] > threshold and self._stoch_mms_possize[0] < threshold:
            # if clustering -> oversold and pos size -> not oversold:
            short_cot_signal = 0.5      # slightly oversold (weak buy)
        
        elif self._stoch_mms_clus[0] < (1 - threshold) and self._stoch_mms_possize[0] > (1 - threshold):
            # if clustering -> undersold and pos size -> not undersold:
            short_cot_signal = -0.5     # slightly undersold (weak sell)
        
        elif self._stoch_mms_clus[0] < (1 - threshold) and self._stoch_mms_possize[0] < (1 - threshold):
            # if clustering -> undersold and pos size -> undersold:
            short_cot_signal = -1       # undersold (sell)
        
        else:
            short_cot_signal = 0        # neutral
        

        self.l.signal[0] = long_cot_signal + short_cot_signal


class PMPUNet_SignalBase(COTSignalBase):
    def next(self):
        threshold = self.p.threshold

        if self._stoch_pmpu_net[0] > threshold:
            # if pmpu net -> long:
            cot_signal = 1        # long/buy

        elif self._stoch_pmpu_net[0] < (1 - threshold):
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
