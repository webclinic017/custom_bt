import backtrader as bt
from backtrader.indicators import Highest, Lowest

class DonchianChannel(bt.Indicator):
    lines = ('hband','lband','mband', 'wband', )
    params = (
        ('period', 20),
    )

    def __init__(self, *args):
        super(DonchianChannel, self).__init__(*args)
        period = self.params.period
        close = self.data.close
        max = Highest(close, period=period)
        min = Lowest(close, period=period)

        self.l.hband = max(-1)
        self.l.lband = min(-1)
        self.l.mband = (max(-1) + min(-1))/2
        self.l.wband = max(-1) - min(-1)