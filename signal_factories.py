from signals import COTSignalBase
from signals import MMClusteringPosSize_Signal, MMConcentration_Signal
from signals import PMPUNetT_Signal, PMPUNetOI_Signal, PMPUNetPosSize_Signal

from backtrader import Indicator
from indicators import DonchianChannel

class BreakoutSignalBase(Indicator):
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

class SignalFactoryBase():

    def __init__(self, data):
        self.data = data

class COTSignalFactory(SignalFactoryBase):

    ALL_NAMES = ['mm_concentration', 'mm_clustering&possize', 'pmpu_oi', 'pmpu_t', 'pmpu_possize']

    def create_COT_signal(self, period=1, threshold=70, name=None) -> COTSignalBase:
        if name in self.ALL_NAMES:
            if name == 'mm_concentration':
                cot_signal =  MMConcentration_Signal(self.data, period=period, threshold=threshold)
            elif name == 'mm_clustering&possize':
                cot_signal =  MMClusteringPosSize_Signal(self.data, period=period, threshold=threshold)
            elif name == 'pmpu_oi':
                cot_signal =  PMPUNetOI_Signal(self.data, period=period, threshold=threshold)
            elif name == 'pmpu_oi':
                cot_signal =  PMPUNetOI_Signal(self.data, period=period, threshold=threshold)
            elif name == 'pmpu_t':
                cot_signal =  PMPUNetT_Signal(self.data, period=period, threshold=threshold)
            elif name == 'pmpu_possize':
                cot_signal =  PMPUNetPosSize_Signal(self.data, period=period, threshold=threshold)
        else:
            raise ValueError(f"The COT component passed ({name}) isn't a valid one. Please choose one of the following: {self.ALL_NAMES}")
        return cot_signal
    
class BreakoutSignalFactory(SignalFactoryBase):

    def create_breakout_signal(self, period=20, intraday=True) -> BreakoutSignalBase:
        if intraday:
            breakout_signal = IntradayBreakoutSignal(self.data, period=period)
        else:
            breakout_signal = CloseBreakoutSignal(self.data, period=period)
        return breakout_signal



