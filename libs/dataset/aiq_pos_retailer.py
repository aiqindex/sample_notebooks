import os
import numpy as np
import pandas as pd
from typing import List, Optional, Callable

from aiq_strategy_robot.data.data_accessor import DAL
from aiq_strategy_robot.data.ALTERNATIVE import load_alternative_aiq_pos_retailer_data


from .utils import format_pos, filter_to_latest_releases
from ..path import DEFAULT_DIR


FILE_NAME = 'pos_retailer_stack.parquet'

# Load Alternative Data
def register_retailer_data(
        sdh,
        data_dir=DEFAULT_DIR,
        f_ticker_cvt: Optional[Callable[[str], str]] = None,
        db_name: str = None,
        schema_name: str = None,
        reload=False,
        end_date=None,
) -> int:

    if reload:
        print('extract pos_retailer by loader..')
        df_pos = read_by_laoder(
            end_date=end_date, 
            db_name=db_name, schema_name=schema_name)
        df_pos.to_parquet(os.path.join(data_dir, FILE_NAME))
    else:
        try:
            # loading from csv to save time for this demo
            df_pos = read_by_file(data_dir)
        except:
            # ファイルが存在しない場合はloaderから取得
            print('extract pos_retailer by loader..')
            df_pos = read_by_laoder(end_date=end_date, db_name=db_name, schema_name=schema_name)

    if f_ticker_cvt is not None:
        df_pos.index = pd.MultiIndex.from_tuples([(f_ticker_cvt(i[0]), i[1]) for i in df_pos.index], names=df_pos.index.names)

    data_id = sdh.set_raw_data(
        data_source='external',
        dfraw=df_pos,
        source='sample'
    )

    sdh.set_alias({data_id: 'pos_retailer'})
    return data_id

def read_by_file(dir_path=DEFAULT_DIR):
    # Reading from CSV file
    path_to_gen = os.path.join(dir_path, FILE_NAME)
    dfpos = pd.read_parquet(path_to_gen)
    # dfpos = filter_to_latest_releases(df_inc1)
    # index_to_upper(dfpos)
    return dfpos


def read_by_laoder(end_date=None, db_name=None, schema_name=None):
    temp_sdh = DAL()
    dfpos = format_pos(
        load_alternative_aiq_pos_retailer_data(
            temp_sdh, load_all_tickers=True,
            end_datetime=end_date, db_name=db_name, schema_name=schema_name).retrieve()
    )
    dfpos.sort_index(inplace=True)
    return dfpos


# def get_finnhub_symbol(exchange_code='T'):
#     dffinn_symbol = load_finnhub_symbol_lookup(
#         DAL(),
#         exchange_code=exchange_code
#     ).retrieve()
#     dffinn_symbol.head()
#     return dffinn_symbol


    
# def load_sample_dataset(
#         sdh,
#         dir_path=DEFAULT_DIR,
#         f_ticker_cvt: Optional[Callable[[str], str]] = None
#         ):
#     dfprices = pd.read_parquet(os.path.join(DEFAULT_DIR, 'aiq_pos_retailer_mkt.parquet'))
#     dfprices.index.names = ['TICKER', 'DATETIME']
#     dfprices = dfprices.reset_index()
#     dfprices['TICKER'] = dfprices['TICKER'] + ' JP'
#     dfprices = dfprices.set_index(['TICKER', 'DATETIME'])
#     universe = dfprices.index.get_level_values('TICKER').unique().to_list()


#     dfpos = read_pos_retailer(dir_path)
#     start_datetime = dfpos.index.get_level_values('DATETIME').min().strftime('%Y-%m-%d')
#     end_datetime = dfpos.index.get_level_values('DATETIME').max().strftime('%Y-%m-%d')
#     pos_ticker = dfpos.index.get_level_values('TICKER').unique().to_list()

#     dfpos = dfpos[dfpos.index.get_level_values('TICKER').isin(universe)]
#     if f_ticker_cvt is not None:
#         dfpos.index = pd.MultiIndex.from_tuples(
#             [(f_ticker_cvt(i[0]), i[1]) for i in dfpos.index],
#             names=dfpos.index.names)
        
#     sdh.set_raw_data(dfraw=dfpos, data_source='aiq_pos_retailer')
#     sdh.set_raw_data(dfraw=dfprices, data_source='mkt')

#     dffinn_sym = get_finnhub_symbol(exchange_code='T')
#     dffinn_sym = dffinn_sym.loc[dffinn_sym.TICKER.isin(pos_ticker)]
#     load_finhub_funda(sdh, dffinn_sym, start_datetime, end_datetime, dir_path)

