from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
from tqdm import tqdm
from instruments import get_instrument

def list2str(start_list:list, separator='_') -> str:
    return separator.join([str(item) for item in start_list])

def cot_add_lag(cot_df:pd.DataFrame, price_dates: list, date_label='Date', date_format='%Y-%m-%d') -> pd.DataFrame:
    release_weekday = 0 # All CoT data will only be available to trade on the following Monday
    df = cot_df.copy()
    df[date_label] = df[date_label].apply(lambda x: get_following_date_by_weekday(x, release_weekday, date_format, price_dates=price_dates))

    return df

def get_following_date_by_weekday(date:str, following_weekday:int, date_format:str, price_dates: list) -> str:
    current_date = datetime.strptime(date, date_format)
    current_weekday = current_date.weekday()

    if current_weekday == following_weekday:
        delta_days = 7
    else:
        delta_days = (7-current_weekday) + following_weekday
    following_date = current_date + timedelta(days=delta_days)

    while True:
        if following_date.strftime(date_format) not in price_dates:
            following_date = following_date + timedelta(days=1)
        else:
            break

    return following_date.strftime(date_format)

def cot_week2day(cot_df:pd.DataFrame, daily_dates:list, consolidated_path, date_label='Date', date_format='%Y-%m-%d') -> pd.DataFrame:
    cot_raw_inputs = [str(column) for column in cot_df.columns.to_list() if (column != date_label)]
    daily_dates_dt = [datetime.strptime(dd, date_format) for dd in daily_dates]
    weekly_dates_dt = [datetime.strptime(wd, date_format) for wd in cot_df[date_label].to_list()]
    df = pd.DataFrame(columns=cot_raw_inputs, index=daily_dates)
    last_weekly_date_dt = weekly_dates_dt[0]
    for daily_date_dt in daily_dates_dt:
        foward_weekly_dates_dt = [fwd for fwd in weekly_dates_dt if fwd > daily_date_dt]
        if (len(foward_weekly_dates_dt)>0) and (last_weekly_date_dt != foward_weekly_dates_dt[0]):
            last_weekly_date_dt = foward_weekly_dates_dt[0]
            cot_values = cot_df[cot_df[date_label] == last_weekly_date_dt.strftime(date_format)]
            cot_values = cot_values[cot_raw_inputs].iloc[0].to_dict()
        elif len(foward_weekly_dates_dt) == 0:
            if daily_date_dt < last_weekly_date_dt + timedelta(days=6):
                cot_values = cot_df[cot_df[date_label] == last_weekly_date_dt.strftime(date_format)]
                cot_values = cot_values[cot_raw_inputs].iloc[0].to_dict()
            else:
                cot_values = [np.nan]*len(cot_raw_inputs)
        else:
            cot_values = cot_df[cot_df[date_label] == last_weekly_date_dt.strftime(date_format)]
            cot_values = cot_values[cot_raw_inputs].iloc[0].to_dict()

        daily_date = daily_date_dt.strftime(date_format)
        df.loc[daily_date] = cot_values
    
    df.index.name = date_label
    df.reset_index(inplace=True)
    return df

def get_cot_report(exchange_name:str):

    cot_report = dict()
    if exchange_name == 'CFTC':
        cot_report['available_years'] = ['2006_2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022']
        cot_report['columns'] = ['Report_Date_as_MM_DD_YYYY', 'CFTC_Contract_Market_Code', 'Open_Interest_All', 'Prod_Merc_Positions_Long_All', 'Prod_Merc_Positions_Short_All', 'Swap_Positions_Long_All',
                                'Swap_Positions_Short_All', 'Swap_Positions_Spread_All', 'M_Money_Positions_Long_All', 'M_Money_Positions_Short_All', 'M_Money_Positions_Spread_All',
                                'Other_Rept_Positions_Long_All', 'Other_Rept_Positions_Short_All', 'Other_Rept_Positions_Spread_All', 'NonRept_Positions_Long_All', 'NonRept_Positions_Short_All',
                                'Open_Interest_Old', 'Prod_Merc_Positions_Long_Old', 'Prod_Merc_Positions_Short_Old', 'Swap_Positions_Long_Old', 'Swap_Positions_Short_Old',
                                'Swap_Positions_Spread_Old', 'M_Money_Positions_Long_Old', 'M_Money_Positions_Short_Old', 'M_Money_Positions_Spread_Old', 'Other_Rept_Positions_Long_Old',
                                'Other_Rept_Positions_Short_Old', 'Other_Rept_Positions_Spread_Old', 'NonRept_Positions_Long_Old', 'NonRept_Positions_Short_Old', 'Pct_of_OI_Prod_Merc_Long_All',
                                'Pct_of_OI_Prod_Merc_Short_All', 'Pct_of_OI_Swap_Long_All', 'Pct_of_OI_Swap_Short_All', 'Pct_of_OI_Swap_Spread_All', 'Pct_of_OI_M_Money_Long_All',
                                'Pct_of_OI_M_Money_Short_All', 'Pct_of_OI_M_Money_Spread_All', 'Pct_of_OI_Other_Rept_Long_All', 'Pct_of_OI_Other_Rept_Short_All', 'Pct_of_OI_Other_Rept_Spread_All',
                                'Pct_of_OI_NonRept_Long_All', 'Pct_of_OI_NonRept_Short_All', 'Traders_Tot_All', 'Traders_Prod_Merc_Long_All', 'Traders_Prod_Merc_Short_All', 'Traders_Swap_Long_All',
                                'Traders_Swap_Short_All', 'Traders_Swap_Spread_All', 'Traders_M_Money_Long_All', 'Traders_M_Money_Short_All', 'Traders_M_Money_Spread_All', 'Traders_Other_Rept_Long_All',
                                'Traders_Other_Rept_Short_All', 'Traders_Other_Rept_Spread_All', 'Conc_Gross_LE_4_TDR_Long_All', 'Conc_Gross_LE_4_TDR_Short_All', 'Conc_Gross_LE_8_TDR_Long_All',
                                'Conc_Gross_LE_8_TDR_Short_All', 'Conc_Net_LE_4_TDR_Long_All', 'Conc_Net_LE_4_TDR_Short_All', 'Conc_Net_LE_8_TDR_Long_All', 'Conc_Net_LE_8_TDR_Short_All', 'FutOnly_or_Combined']
    elif exchange_name == 'ICE':
        cot_report['available_years'] = ['2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022']
        cot_report['columns'] = ['As_of_Date_Form_MM/DD/YYYY', 'CFTC_Commodity_Code', 'Open_Interest_All', 'Prod_Merc_Positions_Long_All', 'Prod_Merc_Positions_Short_All', 'Swap_Positions_Long_All',
                                'Swap_Positions_Short_All', 'Swap_Positions_Spread_All', 'M_Money_Positions_Long_All', 'M_Money_Positions_Short_All', 'M_Money_Positions_Spread_All',
                                'Other_Rept_Positions_Long_All', 'Other_Rept_Positions_Short_All', 'Other_Rept_Positions_Spread_All', 'NonRept_Positions_Long_All', 'NonRept_Positions_Short_All',
                                'Open_Interest_Old', 'Prod_Merc_Positions_Long_Old', 'Prod_Merc_Positions_Short_Old', 'Swap_Positions_Long_Old', 'Swap_Positions_Short_Old',
                                'Swap_Positions_Spread_Old', 'M_Money_Positions_Long_Old', 'M_Money_Positions_Short_Old', 'M_Money_Positions_Spread_Old', 'Other_Rept_Positions_Long_Old',
                                'Other_Rept_Positions_Short_Old', 'Other_Rept_Positions_Spread_Old', 'NonRept_Positions_Long_Old', 'NonRept_Positions_Short_Old', 'Pct_of_OI_Prod_Merc_Long_All',
                                'Pct_of_OI_Prod_Merc_Short_All', 'Pct_of_OI_Swap_Long_All', 'Pct_of_OI_Swap_Short_All', 'Pct_of_OI_Swap_Spread_All', 'Pct_of_OI_M_Money_Long_All',
                                'Pct_of_OI_M_Money_Short_All', 'Pct_of_OI_M_Money_Spread_All', 'Pct_of_OI_Other_Rept_Long_All', 'Pct_of_OI_Other_Rept_Short_All', 'Pct_of_OI_Other_Rept_Spread_All',
                                'Pct_of_OI_NonRept_Long_All', 'Pct_of_OI_NonRept_Short_All', 'Traders_Tot_All', 'Traders_Prod_Merc_Long_All', 'Traders_Prod_Merc_Short_All', 'Traders_Swap_Long_All',
                                'Traders_Swap_Short_All', 'Traders_Swap_Spread_All', 'Traders_M_Money_Long_All', 'Traders_M_Money_Short_All', 'Traders_M_Money_Spread_All', 'Traders_Other_Rept_Long_All',
                                'Traders_Other_Rept_Short_All', 'Traders_Other_Rept_Spread_All', 'Conc_Gross_LE_4_TDR_Long_All', 'Conc_Gross_LE_4_TDR_Short_All', 'Conc_Gross_LE_8_TDR_Long_All',
                                'Conc_Gross_LE_8_TDR_Short_All', 'Conc_Net_LE_4_TDR_Long_All', 'Conc_Net_LE_4_TDR_Short_All', 'Conc_Net_LE_8_TDR_Long_All', 'Conc_Net_LE_8_TDR_Short_All', 'FutOnly_or_Combined']
    else:
        cot_report = None
    
    return cot_report

def clean_and_export_cot_data(symbol:str, raw_path: str, consolidated_path: str):
    ''' Function that cleans and exports COT database for a single instrument
    :param symbol: Instrument's symbol
    '''
    # Producer/Merchant/Processor/User, Money Managers, Swap Dealers, Other Reportables, Non-reportables
    trader_groups = ['PMPU', 'MM', 'SD', 'OR', 'NR']

    # Long, Short or Spread
    suffixes = ['L', 'S', 'D']

    # Instrument info
    instrument = get_instrument(symbol)
    if instrument:
        name = instrument['name']
        exchange_name = instrument['exchange_name']
        cot_report_code = instrument['cot_report_code']

        # CoT Report info
        cot_report = get_cot_report(exchange_name)
        available_years = cot_report['available_years']
        columns = cot_report['columns']

        desired_columns = ['Date', 'Code', 'TotalOI', 'PMPUL_OI', 'PMPUS_OI', 'SDL_OI', 'SDS_OI', 'SDD_OI', 'MML_OI', 'MMS_OI', 'MMD_OI', 'ORL_OI', 'ORS_OI', 'ORD_OI', 'NRL_OI', 'NRS_OI', 'TotalOI_Old', 'PMPUL_Old', 'PMPUS_Old', 'SDL_Old', 'SDS_Old',
                        'SDD_Old', 'MML_Old', 'MMS_Old', 'MMD_Old', 'ORL_Old', 'ORS_Old', 'ORD_Old', 'NRL_Old', 'NRS_Old', 'PMPUL_Concentration', 'PMPUS_Concentration', 'SDL_Concentration', 'SDS_Concentration',
                        'SDD_Concentration', 'MML_Concentration', 'MMS_Concentration', 'MMD_Concentration', 'ORL_Concentration', 'ORS_Concentration', 'ORD_Concentration', 'NRL_Concentration', 'NRS_Concentration',
                        'TotalT', 'PMPUL_T', 'PMPUS_T', 'SDL_T', 'SDS_T', 'SDD_T', 'MML_T', 'MMS_T', 'MMD_T', 'ORL_T', 'ORS_T', 'ORD_T', 'Gross_Long_Top4', 'Gross_Short_Top4', 'Gross_Long_Top8', 'Gross_Short_Top8',
                        'Net_Long_Top4', 'Net_Short_Top4', 'Net_Long_Top8', 'Net_Short_Top8', 'FutOnly_or_Combined']

        columns_mapper = dict()
        for i in range(len(columns)):
            if i < len(columns) - 1:
                columns_mapper.update({columns[i]: desired_columns[i]})

        instrument_cot_df = pd.DataFrame(columns=desired_columns)
        print("({} - CoT)".format(name))
        for year in tqdm(available_years):
            file_name = exchange_name + '_' + year
            file_path = raw_path + r'\cot' + '\\' + file_name + '.csv'

            path_doesnt_exists = False
            if os.path.isfile(file_path):
                df = pd.read_csv(file_path, header=0)
            else:
                path_doesnt_exists = True

            if not path_doesnt_exists:
                df = df[columns]
                df = df.rename(columns=columns_mapper)
            
                filtered_df = df.loc[(df['Code'] == cot_report_code) & (df['FutOnly_or_Combined'] == 'FutOnly')]

                if not filtered_df.empty:
                    instrument_cot_df = pd.concat([instrument_cot_df, filtered_df], ignore_index=True, sort=False)
        
        instrument_cot_df.drop(['Code', 'FutOnly_or_Combined'], inplace=True, axis=1)

        for trader_group in trader_groups:
            for suffix in suffixes:
                if not ((trader_group == 'PMPU' or trader_group == 'OR') and ('D' in suffix)):
                    category_name = trader_group + suffix
                    if trader_group != 'NR':
                        instrument_cot_df[category_name + '_Clustering'] = instrument_cot_df[category_name + '_T'] / instrument_cot_df['TotalT'].mask(instrument_cot_df['TotalT'] == 0, np.inf)
                        instrument_cot_df[category_name + '_Clustering'] = instrument_cot_df[category_name + '_Clustering'].apply(lambda x: round(100*x, 2))

                        instrument_cot_df[category_name + '_PosSize'] = instrument_cot_df[category_name + '_OI'] / instrument_cot_df[category_name + '_T'].mask(instrument_cot_df[category_name + '_T'] == 0, np.inf)
                        instrument_cot_df[category_name + '_PosSize'] = instrument_cot_df[category_name + '_PosSize'].apply(lambda x: round(x, 2))

        if not os.path.isdir(consolidated_path + '\\cot'):
            os.mkdir(consolidated_path + '\\cot')
        
        instrument_cot_df['Name'] = name
        instrument_cot_df['Date'] = pd.to_datetime(instrument_cot_df['Date'], format="%m/%d/%Y")
        instrument_cot_df.sort_values('Date', inplace=True)
        instrument_cot_df['Date'] = instrument_cot_df['Date'].dt.strftime(date_format='%Y-%m-%d')
        
        instrument_cot_path = get_cot_path(symbol=symbol)
        instrument_cot_df.to_csv(instrument_cot_path, index=False)

def get_cot_path(symbol:str, consolidated_path) -> str:
    return f"{consolidated_path}\\cot\\{symbol}.csv"