from abc import ABC, abstractclassmethod
import pandas as pd
import os

class DataServiceInterface(ABC):
    
    @abstractclassmethod
    def get_data_from_csv(cls):
        pass

class COTDataService(DataServiceInterface):
    __base_path = '.\\data\\cot'
    
    @classmethod
    def get_data_from_csv(cls, instrument_symbol: str) -> pd.DataFrame:
        cot_path = f"{cls.__base_path}\\{instrument_symbol}.csv"

        cot_df = pd.DataFrame()
        if os.path.isfile(cot_path):
            cot_df = pd.read_csv(cot_path, header=0)
        else:
            raise Warning(f"There isn't a CoT .csv file for the following symbol: {instrument_symbol}")

        return cot_df
    
class PriceDataService(DataServiceInterface):
    __base_path = '.\\data\\price'
    
    @classmethod
    def get_data_from_csv(cls, instrument_symbol: str, is_single_contract=False, contract_code=None) -> pd.DataFrame:
        price_path = f"{cls.__base_path}\\{instrument_symbol}\\"
        if not is_single_contract:
            price_path += 'perpetual_OI.csv'
        else:
            price_path +=  f'{contract_code}.csv'

        price_df = pd.DataFrame()
        if os.path.isfile(price_path):
            price_df = pd.read_csv(price_path, header=0)
        else:
            raise Warning(f"There isn't a price .csv file for the following symbol: {instrument_symbol}")

        return price_df