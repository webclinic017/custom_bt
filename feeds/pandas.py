from backtrader import feeds

class COT_PandasData(feeds.PandasData):

    lines = (
        'datetime',
        'mml_concentration', 'mml_clustering', 'mml_possize', 
        'mms_concentration', 'mms_clustering', 'mms_possize', 
        'pmpu_netoi', 'pmpu_nett', 'pmpu_netpossize'
    )

    params = (
        # COT data parameters
        ('datetime', 'Date'),
        ('mml_concentration', 'MML_Concentration'),
        ('mml_clustering', 'MML_Clustering'),
        ('mml_possize', 'MML_PosSize'),
        ('mms_concentration', 'MMS_Concentration'),
        ('mms_clustering', 'MMS_Clustering'),
        ('mms_possize', 'MMS_PosSize'),
        ('pmpu_netoi', 'PMPU_Net_OI'), 
        ('pmpu_nett', 'PMPU_Net_T'), 
        ('pmpu_netpossize', 'PMPU_Net_PosSize'),
    )


class Price_PandasData(feeds.PandasData):

    linesoverride = True

    lines = ('datetime', 'close', 'open', 'high', 'low')

    params = (
        ('datetime', 'Date'),
        ('close', 'Close'),
        ('open', 'Open'),
        ('high', 'High'),
        ('low', 'Low'),
        ('openinterest', 'OI'),
        ('volume', 'Volume'),
    )