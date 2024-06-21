import os
import numpy as np
import pandas as pd
from typing import List, Optional, Callable

from aiq_strategy_robot.data.data_accessor import DAL
from aiq_strategy_robot.data.FINNHUB import load_finnhub_symbol_lookup, load_finnhub_equity_data, load_finnhub_fundamental
from .path import DEFAULT_DIR


sdh = DAL()


def filter_to_latest_releases(
    df: pd.DataFrame
) -> pd.DataFrame:
    df.set_index(['ticker', 'datetime', 'variable', 'SMOOTH', 'release_timestamp'], inplace=True)
    df = df.xs(0, level='SMOOTH').drop(['backfill'], axis=1)
    df = df.sort_index()
    df.reset_index('release_timestamp', drop=False, inplace=True)
    df = df.loc[~df.index.duplicated(keep='last')]
    df.drop(['release_timestamp'], axis=1, inplace=True)
    return df


def read_pos_retailer(dir_path=DEFAULT_DIR):
    # Reading from CSV file
    path_to_gen1_csv = os.path.join(dir_path, '20240312_pos_retailer_stack.csv')
    assert path_to_gen1_csv!='', 'Please provide the path to the CSV file'
    df_inc1 = pd.read_csv(path_to_gen1_csv, dtype={'ticker': str, 'SMOOTH': int}, parse_dates=['datetime', 'release_timestamp'])
    df_inc1 = df_inc1.drop('Unnamed: 0', axis=1)
    dfpos = filter_to_latest_releases(df_inc1)
    dfpos

    # unstack dfpos
    dfpos = dfpos.unstack('variable')['values']
    dfpos = dfpos.reset_index()
    dfpos['ticker'] = dfpos['ticker'] + ' JP'
    dfpos = dfpos.set_index(['ticker', 'datetime'])
    return dfpos

    


def get_finnhub_symbol(exchange_code='T'):
    dffinn_symbol = load_finnhub_symbol_lookup(
        DAL(),
        exchange_code=exchange_code
    ).retrieve()
    dffinn_symbol.head()
    return dffinn_symbol



def laod_finnhub_prices(sdh, dffinn_sym, start_datetime, end_datetime):
    return load_finnhub_equity_data(
        sdh,
        symbols=dffinn_sym.symbol.to_list(),
        freq='D',
        start_datetime=start_datetime, 
        end_datetime=end_datetime, 
    ).retrieve()
    # dfmkt = sdh.get_raw_data(1)
    # sdh.get_raw_data(1).to_parquet('dffinnhub_mkt')

def load_finhub_funda(sdh, dffinn_sym, start_datetime, end_datetime, dir_path):

    if dir_path:
        dffunda = pd.read_parquet(os.path.join(dir_path, 'aiq_pos_csmr_retailer_funda.parquet'))
        sdh.set_raw_data(dfraw=dffunda, data_source='finnhub', source='fundamental')
        return dffunda
    else:


        return load_finnhub_fundamental(
            sdh,
            symbols=dffinn_sym.symbol.to_list(),
            st_type='ic', 
            freq='quarterly', 
            start_datetime=start_datetime, 
            end_datetime=end_datetime,
            with_calendar=True
        ).retrieve()


    
def load_sample_dataset(
        sdh,
        dir_path=DEFAULT_DIR,
        f_ticker_cvt: Optional[Callable[[str], str]] = None
        ):

    dfpos = read_pos_retailer(dir_path)
    dffinn_sym = get_finnhub_symbol(exchange_code='T')

    start_datetime = dfpos.index.get_level_values('datetime').min().strftime('%Y-%m-%d')
    end_datetime = dfpos.index.get_level_values('datetime').max().strftime('%Y-%m-%d')
    pos_ticker = dfpos.index.get_level_values('ticker').unique().to_list()

    dfprices = pd.read_parquet(os.path.join(dir_path, 'aiq_pos_csmr_retailer_mkt.parquet'))
    dfprices = dfprices.reset_index()
    dfprices['ticker'] = dfprices['ticker'] + ' JP'
    dfprices = dfprices.set_index(['ticker', 'datetime'])


    universe = dfprices.index.get_level_values('ticker').unique().to_list()

    dfpos = dfpos[dfpos.index.get_level_values('ticker').isin(universe)]
    if f_ticker_cvt is not None:
        dfpos.index = pd.MultiIndex.from_tuples(
            [(f_ticker_cvt(i[0]), i[1]) for i in dfpos.index],
            names=dfpos.index.names)
    sdh.set_raw_data(dfraw=dfpos, data_source='aiq_pos_elec')
    sdh.set_raw_data(dfraw=dfprices, data_source='mkt')

    dffinn_sym = dffinn_sym.loc[dffinn_sym.ticker.isin(pos_ticker)]

    # laod_finnhub_prices(sdh, dffinn_sym, start_datetime, end_datetime)
    load_finhub_funda(sdh, dffinn_sym, start_datetime, end_datetime, dir_path)

