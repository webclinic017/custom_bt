def get_instrument(symbol: str) -> dict:
    instrument = dict()
    if symbol == 'CL':
        instrument['name'] = 'Crude Oil (WTI)'
        instrument['csi_symbol'] = 'CL'
        instrument['exchange_name'] = 'CFTC'
        instrument['cot_report_code'] = '067651'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Energy'
    elif symbol == 'CO':
        instrument['name'] = 'Brent'
        instrument['csi_symbol'] = None
        instrument['exchange_name'] = 'ICE'
        instrument['cot_report_code'] = 'B'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Energy'
    elif symbol == 'CC':
        instrument['name'] = 'Cocoa'
        instrument['csi_symbol'] = 'CC'
        instrument['exchange_name'] = 'ICE'
        instrument['cot_report_code'] = 'Cocoa'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Food & Fiber'
    elif symbol == 'KC':
        instrument['name'] = 'Coffee'
        instrument['csi_symbol'] = 'KC'
        instrument['exchange_name'] = 'ICE'
        instrument['cot_report_code'] = 'RC'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Food & Fiber'
    elif symbol == 'HG':
        instrument['name'] = 'Copper'
        instrument['csi_symbol'] = 'HG'
        instrument['exchange_name'] = 'CFTC'
        instrument['cot_report_code'] = '085692'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Metals'
    elif symbol == 'C':
        instrument['name'] = 'Corn'
        instrument['csi_symbol'] = 'C'
        instrument['exchange_name'] = 'CFTC'
        instrument['cot_report_code'] = '002602'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Grains & Oilseeds'
    elif symbol == 'CT':
        instrument['name'] = 'Cotton'
        instrument['csi_symbol'] = 'CT'
        instrument['exchange_name'] = 'CFTC'
        instrument['cot_report_code'] = '033661'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Food & Fiber'
    elif symbol == 'FC':
        instrument['name'] = 'Cattle (Feeder)'
        instrument['csi_symbol'] = 'FC'
        instrument['exchange_name'] = 'CFTC'
        instrument['cot_report_code'] = '057642'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Livestock & Meats'
    elif symbol == 'LC':
        instrument['name'] = 'Cattle (Live)'
        instrument['csi_symbol'] = 'LC'
        instrument['exchange_name'] = 'CFTC'
        instrument['cot_report_code'] = '061641'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Livestock & Meats'
    elif symbol == 'QS':
        instrument['name'] = 'Gasoil'
        instrument['csi_symbol'] = 'F7N'
        instrument['exchange_name'] = 'ICE'
        instrument['cot_report_code'] = 'G'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Energy'
    elif symbol == 'XB':
        instrument['name'] = 'Gasoline (RBOB)'
        instrument['csi_symbol'] = 'RB'
        instrument['exchange_name'] = 'CFTC'
        instrument['cot_report_code'] = '111659'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Energy'
    elif symbol == 'GC':
        instrument['name'] = 'Gold'
        instrument['csi_symbol'] = 'GC'
        instrument['exchange_name'] = 'CFTC'
        instrument['cot_report_code'] = '088691'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Metals'
    elif symbol == 'HO':
        instrument['name'] = 'Heating Oil'
        instrument['csi_symbol'] = 'HO'
        instrument['exchange_name'] = 'CFTC'
        instrument['cot_report_code'] = '022651'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Energy'
    elif symbol == 'KW':
        instrument['name'] = 'Wheat (Kansas)'
        instrument['csi_symbol'] = 'KW'
        instrument['exchange_name'] = 'CFTC'
        instrument['cot_report_code'] = '001612'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Grains & Oilseeds'
    elif symbol == 'W':
        instrument['name'] = 'Wheat'
        instrument['csi_symbol'] = 'W'
        instrument['exchange_name'] = 'CFTC'
        instrument['cot_report_code'] = '001602'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Grains & Oilseeds'
    elif symbol == 'LH':
        instrument['name'] = 'Lean Hogs'
        instrument['csi_symbol'] = 'LH'
        instrument['exchange_name'] = 'CFTC'
        instrument['cot_report_code'] = '054642'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Livestock & Meats'
    elif symbol == 'NG':
        instrument['name'] = 'Natural Gas'
        instrument['csi_symbol'] = 'NG'
        instrument['exchange_name'] = 'CFTC'
        instrument['cot_report_code'] = '023651'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Energy'
    elif symbol == 'PA':
        instrument['name'] = 'Palladium'
        instrument['csi_symbol'] = 'PA'
        instrument['exchange_name'] = 'CFTC'
        instrument['cot_report_code'] = '075651'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Metals'
    elif symbol == 'PL':
        instrument['name'] = 'Platinum'
        instrument['csi_symbol'] = 'PL'
        instrument['exchange_name'] = 'CFTC'
        instrument['cot_report_code'] = '076651'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Metals'
    elif symbol == 'SI':
        instrument['name'] = 'Silver'
        instrument['csi_symbol'] = 'SI'
        instrument['exchange_name'] = 'CFTC'
        instrument['cot_report_code'] = '084691'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Metals'
    elif symbol == 'BO':
        instrument['name'] = 'Soybean Oil'
        instrument['csi_symbol'] = 'BO'
        instrument['exchange_name'] = 'CFTC'
        instrument['cot_report_code'] = '007601'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Grains & Oilseeds'
    elif symbol == 'S':
        instrument['name'] = 'Soybeans'
        instrument['csi_symbol'] = 'S'
        instrument['exchange_name'] = 'CFTC'
        instrument['cot_report_code'] = '005602'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Grains & Oilseeds'
    elif symbol == 'SM':
        instrument['name'] = 'Soybean Meal'
        instrument['csi_symbol'] = 'SM'
        instrument['exchange_name'] = 'CFTC'
        instrument['cot_report_code'] = '026603'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Grains & Oilseeds'
    elif symbol == 'SB':
        instrument['name'] = 'Sugar'
        instrument['csi_symbol'] = 'SB'
        instrument['exchange_name'] = 'ICE'
        instrument['cot_report_code'] = 'W'
        instrument['asset_class'] = 'Commodities'
        instrument['category'] = 'Food & Fiber'
    else:
        instrument = None
    
    return instrument

def futures_contract_code(year, month: int):
    month_code = int2str_month_decoder(month)
    return str(year) + month_code

def int2str_month_decoder(month: int):
    month_code = ''
    if month == 1:
        month_code = 'F'
    elif month == 2:
        month_code = 'G'
    elif month == 3:
        month_code = 'H'
    elif month == 4:
        month_code = 'J'
    elif month == 5:
        month_code = 'K'
    elif month == 6:
        month_code = 'M'
    elif month == 7:
        month_code = 'N'
    elif month == 8:
        month_code = 'Q'
    elif month == 9:
        month_code = 'U'
    elif month == 10:
        month_code = 'V'
    elif month == 11:
        month_code = 'X'
    elif month == 12:
        month_code = 'Z'
    
    return month_code