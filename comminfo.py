from backtrader import CommInfoBase

class CommInfo_Futures_Fixed(CommInfoBase):
    params = (
        ('stocklike', False),
        ('commtype', CommInfoBase.COMM_FIXED),
    )