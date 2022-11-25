# Basic utils
import pandas as pd
import numpy as np
from cot import cot_add_lag

# Data services
from services import PriceDataService, COTDataService

# Custom Backtrader
import backtrader as bt
from strategies import COT_Breakout
from comm_factories import FuturesCommFactory
from feeds.pandas import COT_PandasData, Price_PandasData

def btrun():
    # Simbolos das commodities a serem testadas
    symbols_list = [
        'CC', 'KC', 'C', 'CT', 'W',
        'SB', 'BO', 'S', 'SM'
    ]

    # Dados do COT a serem analisados
    cot_raw_inputs = ['TotalOI', 'TotalT', 
                    'MML_OI', 'MML_T',
                    'MMS_OI', 'MMS_T',
                    'PMPUL_OI','PMPUL_T',
                    'PMPUS_OI', 'PMPUS_T']

    price_raw_inputs = ['Close', 'Open', 'High', 'Low', 'OI', 'Volume']

    # Preparando o dataset do COT
    price_data = pd.DataFrame()
    cot_data = pd.DataFrame()
    for symbol in symbols_list:

        #- Importing price data
        price_df = PriceDataService.get_data_from_csv(symbol)
        price_df = price_df[['Date'] + price_raw_inputs]

        #- Importing CoT data
        cot_df = COTDataService.get_data_from_csv(symbol)
        cot_df = cot_df[['Date']+cot_raw_inputs]

        oi_total = cot_df['TotalOI']
        n_traders_total = cot_df['TotalT']
        for cot_category in ['MML', 'MMS', 'PMPUL', 'PMPUS']:
            oi = cot_df[f'{cot_category}_OI']
            n_traders = cot_df[f'{cot_category}_T']

            conc = np.where(cot_df[['TotalOI']].eq(0).all(1), 0, oi / oi_total)
            clus = np.where(cot_df[['TotalT']].eq(0).all(1), 0, n_traders / n_traders_total)

            cot_df[f'{cot_category}_Concentration'] = conc
            cot_df[f'{cot_category}_Clustering'] = clus
            cot_df[f'{cot_category}_PosSize'] = np.where(cot_df[[f'{cot_category}_T']].eq(0).all(1), 0, oi / n_traders)
        cot_df['PMPU_Net_OI'] = cot_df['PMPUL_OI'] - cot_df['PMPUS_OI']
        cot_df['PMPU_Net_T'] = cot_df['PMPUL_T'] - cot_df['PMPUS_T']
        cot_df['PMPU_Net_PosSize'] = cot_df['PMPUL_PosSize'] - cot_df['PMPUS_PosSize']
        cot_df.fillna(method='ffill', inplace=True)

        cot_features = ['MML_Concentration', 'MML_Clustering', 'MML_PosSize',
                        'MMS_Concentration', 'MMS_Clustering', 'MMS_PosSize',
                        'PMPU_Net_OI', 'PMPU_Net_T', 'PMPU_Net_PosSize']
        
        cot_df = cot_df[['Date']+cot_features]
        cot_df.drop(cot_df.tail(1).index, inplace=True)

        # Adding report lag to CoT data
        cot_df = cot_add_lag(cot_df=cot_df, price_dates=price_df['Date'].to_list())

        # Concatenating COT and price data on date
        first_cot_date = cot_df['Date'][0]
        price_df = price_df.set_index('Date')[first_cot_date:]
        price_df.reset_index(inplace=True)
        price_df['symbol'] = symbol
        cot_df['symbol'] = symbol

        # Merging data for all instruments
        price_data = pd.concat([price_data, price_df], ignore_index=True, sort=False)
        cot_data = pd.concat([cot_data, cot_df], ignore_index=True, sort=False)

    price_data['Date'] = pd.to_datetime(price_data['Date'], format='%Y-%m-%d')
    cot_data['Date'] = pd.to_datetime(cot_data['Date'], format='%Y-%m-%d')

    # Instanciating cerebro
    cerebro = bt.Cerebro(runonce=True)

    # Setting fixed (percentage-based) SLIPPAGE (0.5%), CHEAT-ON-CLOSE and INITIAL CASH (1.5M USD)
    cerebro.broker = bt.brokers.BackBroker(slip_perc=0.005, coc=True)
    cerebro.broker.set_cash(1.5e6)

    # Adding strategy to cerebro and setting the initial cash
    cerebro.addstrategy(COT_Breakout, cot_component_name='mm_concentration')
    
    # Adding Price DataFeed
    comminfo_facory = FuturesCommFactory()
    for symbol, df in price_data.groupby(by='symbol'):
        price_btdata = Price_PandasData(dataname=df.drop(labels=['symbol'], axis=1))
        cerebro.adddata(data=price_btdata, name=f"{symbol}")
        comminfo_bt = comminfo_facory.create_comminfo(instrument_symbol=symbol)
        cerebro.broker.addcommissioninfo(comminfo=comminfo_bt, name=f'{symbol}_price')

    # Adding COT DataFeed
    for symbol, df in cot_data.groupby(by='symbol'):
        cot_btdata = COT_PandasData(dataname=df.drop(labels=['symbol'], axis=1))
        cerebro.adddata(data=cot_btdata, name=f"{symbol}_cot")
    
    cerebro.run()

if __name__ == "__main__":
    btrun()