import pandas as pd
import os
from instruments import get_instrument, futures_contract_code
from tqdm import tqdm

def clean_df_columns(df:pd.DataFrame):
    columns = [str(c) for c in df.columns]
    cleaned_df = df.copy()

    # Drops any Unnamed columns
    cleaned_df = cleaned_df.loc[:, ~cleaned_df.columns.str.contains('^Unnamed')]

    # Trims unwanted blank spaces from the columns' name
    columns_mapper = dict()
    for column in columns:
        columns_mapper[column] = column.strip()
    cleaned_df.rename(columns=columns_mapper, inplace=True)

    return cleaned_df

def clean_and_export_price_data(symbol:str, raw_path: str, consolidated_path: str):
    ''' Function that cleans and exports price database for some instrument '''
    
    mapper = {
        'CO': {
            'suffix': {
                'perpetual': '',
                'single_contract': ''
            },
            'available_years': [],
            'available_months': []
        },
        'CL': {
            'suffix': {
                'perpetual': '2_P',
                'single_contract': '2'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023'],
            'available_months': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        },
        'CC': {
            'suffix': {
                'perpetual': '2_P',
                'single_contract': '2'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023', '2024'],
            'available_months': [3, 5, 7, 9, 12]
        },
        'KC': {
            'suffix': {
                'perpetual': '2_P',
                'single_contract': '2'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023', '2024',
                                '2025'],
            'available_months': [3, 5, 7, 9, 12]
        },
        'HG': {
            'suffix': {
                'perpetual': '2_P',
                'single_contract': '2'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023', '2024',
                                '2025', '2026', '2027'],
            'available_months': [3, 5, 7, 9, 12]
        },
        'C': {
            'suffix': {
                'perpetual': '2_P',
                'single_contract': '2_'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023', '2024',
                                '2025'],
            'available_months': [3, 5, 7, 9, 12]
        },
        'CT': {
            'suffix': {
                'perpetual': '2_P',
                'single_contract': '_'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023', '2024',
                                '2025'],
            'available_months': [3, 5, 7, 10, 12]
        },
        'FC': {
            'suffix': {
                'perpetual': '_P',
                'single_contract': '_'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023'],
            'available_months': [1, 3, 4, 5, 8, 9, 10, 11]
        },
        'LC': {
            'suffix': {
                'perpetual': '_P',
                'single_contract': '_'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023'],
            'available_months': [2, 4, 6, 8, 10, 12]
        },
        'QS': {
            'suffix': {
                'perpetual': '_P',
                'single_contract': ''
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023', '2024'],
            'available_months': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        },
        'XB': {
            'suffix': {
                'perpetual': '2_P',
                'single_contract': '2'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023'],
            'available_months': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        },
        'GC': {
            'suffix': {
                'perpetual': '2_P',
                'single_contract': '2'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023', '2024',
                                '2025', '2026'],
            'available_months': [2, 4, 6, 8, 10, 12]
        },
        'HO': {
            'suffix': {
                'perpetual': '2_P',
                'single_contract': '2'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023'],
            'available_months': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        },
        'KW': {
            'suffix': {
                'perpetual': '2_P',
                'single_contract': '2'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023', '2024',
                                '2025'],
            'available_months': [3, 5, 7, 9, 12]
        },
        'W': {
            'suffix': {
                'perpetual': '2_P',
                'single_contract': '2_'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023', '2024',
                                '2025'],
            'available_months': [3, 5, 7, 9, 12]
        },
        'LH': {
            'suffix': {
                'perpetual': '_P',
                'single_contract': '_'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023'],
            'available_months': [3, 5, 7, 9, 12]
        },
        'NG': {
            'suffix': {
                'perpetual': '2_P',
                'single_contract': '2'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023', '2024'],
            'available_months': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        },
        'PA': {
            'suffix': {
                'perpetual': '2_P',
                'single_contract': '2'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023'],
            'available_months': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        },
        'PL': {
            'suffix': {
                'perpetual': '2_P',
                'single_contract': '2'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023'],
            'available_months': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        },
        'SI': {
            'suffix': {
                'perpetual': '2_P',
                'single_contract': '2'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023', '2024',
                                '2025', '2026'],
            'available_months': [1, 3, 5, 7, 9, 12]
        },
        'BO': {
            'suffix': {
                'perpetual': '2_P',
                'single_contract': '2'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023', '2024',
                                '2025'],
            'available_months': [1, 3, 5, 7, 8, 9, 10, 12]
        },
        'S': {
            'suffix': {
                'perpetual': '2_P',
                'single_contract': '2_'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023', '2024',
                                '2025'],
            'available_months': [1, 3, 5, 7, 8, 9, 11]
        },
        'SM': {
            'suffix': {
                'perpetual': '2_P',
                'single_contract': '2'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023', '2024',
                                '2025'],
            'available_months': [1, 3, 5, 7, 8, 9, 10, 12]
        },
        'SB': {
            'suffix': {
                'perpetual': '2_P',
                'single_contract': '2'
            },
            'available_years': ['2006', '2007', '2008', '2007', '2008', '2009', '2010',
                                '2011', '2012', '2013', '2014', '2015', '2016', '2017', 
                                '2018', '2019', '2020', '2021', '2022', '2023', '2024',
                                '2025'],
            'available_months': [3, 5, 7, 10]
        }
    }

    raw_folder_path = f"{raw_path}\\price"
    consolidated_folder_path = f"{consolidated_path}\\price"
    if not os.path.isdir(consolidated_folder_path):
        os.mkdir(consolidated_folder_path)
    
    instrument = get_instrument(symbol)
    if instrument:
        csi_symbol = instrument['csi_symbol']
        name = instrument['name']
        instrument_consolidated_folder_path = f"{consolidated_folder_path}\\{symbol}"

        raw_perpetual_file_name = csi_symbol + mapper[symbol]['suffix']['perpetual']
        raw_perpetual_file_path = raw_folder_path + '\\' + raw_perpetual_file_name + '.csv'
        if os.path.isdir(consolidated_folder_path):
            raw_df = pd.read_csv(raw_perpetual_file_path, header=0)
            perpetual_df = clean_df_columns(df=raw_df)
            perpetual_df['Date'] = pd.to_datetime(perpetual_df['Date'], format='%Y/%m/%d')
            perpetual_df.sort_values('Date', inplace=True)
            perpetual_df['Date'] = perpetual_df['Date'].dt.strftime('%Y-%m-%d')
            if not os.path.isdir(instrument_consolidated_folder_path):
                os.mkdir(instrument_consolidated_folder_path)
            perpetual_df.to_csv(instrument_consolidated_folder_path + '\\' + 'perpetual_OI.csv', index=False)

        print("({} - Price)".format(name))
        for year in tqdm(mapper[symbol]['available_years']):
            for month in mapper[symbol]['available_months']:
                raw_single_contract_file_name = csi_symbol + mapper[symbol]['suffix']['single_contract'] + futures_contract_code(year, month)
                raw_single_contract_file_path = raw_folder_path + '\\' + raw_single_contract_file_name + '.csv'

                if os.path.isfile(raw_single_contract_file_path):
                    raw_df = pd.read_csv(raw_single_contract_file_path, header=0)
                    single_contract_df = clean_df_columns(df=raw_df)
                    single_contract_df['Date'] = pd.to_datetime(single_contract_df['Date'], format='%Y/%m/%d')
                    single_contract_df.sort_values('Date', inplace=True)
                    single_contract_df['Date'] = single_contract_df['Date'].dt.strftime('%Y-%m-%d')
                    if not os.path.isdir(consolidated_folder_path):
                        os.mkdir(consolidated_folder_path)
                    single_contract_df.to_csv(instrument_consolidated_folder_path + '\\' + futures_contract_code(year, month) + '.csv', index=False)

def get_consolidated_path(symbol:str, data_path: str, is_single_contract=False, contract_code=None) -> str:
    instrument_price_path = f"{data_path}\\price\\{symbol}\\"
    if not is_single_contract:
        instrument_price_path += 'perpetual_OI.csv'
    else:
        instrument_price_path +=  f'{contract_code}.csv'
    
    return instrument_price_path