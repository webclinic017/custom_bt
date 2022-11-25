from backtrader import CommInfoBase

class FuturesCommFactory():

    def create_comminfo(self, instrument_symbol: str) -> CommInfoBase:
        if instrument_symbol == 'CC': # Cocoa
            comminfo = CommInfoBase(
                        commission=2.82+10,
                        mult=10,
                        margin=None
            )
        elif instrument_symbol == 'KC': # Coffee
            comminfo = CommInfoBase(
                        commission=2.82+18.75,
                        mult=375,
                        margin=None
            )
        elif instrument_symbol == 'C': # Corn
            comminfo = CommInfoBase(
                        commission=2.82+12.5,
                        mult=50,
                        margin=2250
            )
        elif instrument_symbol == 'CT': # Cotton
            comminfo = CommInfoBase(
                        commission=2.82+10,
                        mult=500,
                        margin=1500
            )
        elif instrument_symbol == 'W': # Wheat
            comminfo = CommInfoBase(
                        commission=2.82+15.32,
                        mult=50,
                        margin=3300
            )
        elif instrument_symbol == 'SB': # Sugar
            comminfo = CommInfoBase(
                        commission=2.82+11.2,
                        mult=1120,
                        margin=2850
            )
        elif instrument_symbol == 'BO': # Soybean Oil
            comminfo = CommInfoBase(
                        commission=2.82+12,
                        mult=600,
                        margin=2700
            )
        elif instrument_symbol == 'S': # Soybean
            comminfo = CommInfoBase(
                        commission=2.82+12.5,
                        mult=50,
                        margin=4000
            )
        elif instrument_symbol == 'SM': # Soybean Meal
            comminfo = CommInfoBase(
                        commission=2.82+10,
                        mult=100,
                        margin=2500
            )
        else:
            return ValueError(f'The symbol passed ({instrument_symbol}) does not match any registered futures instrument')
        
        return comminfo
