import os
from typing import Optional, Callable

import pandas as pd
from aiq_strategy_robot.data.data_accessor import DAL
from aiq_strategy_robot.data.ALTERNATIVE import load_alternative_aiq_pos_csmr_goods_data

from ..path import DEFAULT_DIR
from .utils import format_pos
from ..s3 import to_s3, read_s3, DEFAULT_BUCKET

FILE_NAME = 'pos_csmr_goods_stack.parquet'
FILE_NAME_GEN1 = 'pos_csmr_goods_stack_gen1.parquet'
FILE_NAME_GEN2 = 'pos_csmr_goods_stack_gen2.parquet'
ENV_DATABSE = 'TRIAL_SNOWFLAKE_DATABASE_AIQ_POS_CSMR_GOODS'


# Load Alternative Data
def register_csmr_goods_data(
        sdh,
        data_dir=DEFAULT_DIR,
        f_ticker_cvt: Optional[Callable[[str], str]] = None,
        db_name: str = None,
        schema_name: str = None,
        start_date=None,
        end_date=None,
) -> int:
    try:
        # loading from csv to save time for this demo
        df_inc1, df_inc2 = read_file(data_dir)
    except:
        # ファイルが存在しない場合はloaderから取得
        print('extract pos_csmr_goods by loader..')
        df_inc1, df_inc2 = read_by_laoder(start_date, end_date, db_name=db_name, schema_name=schema_name)

    df_inc1 = format_pos(df_inc1)
    df_inc2 = format_pos(df_inc2)
    df_pos = pd.concat([df_inc1, df_inc2.loc[~df_inc2.index.isin(df_inc1.index)]], axis=0)
    df_pos.sort_index(inplace=True)

    if f_ticker_cvt is not None:
        df_pos.index = pd.MultiIndex.from_tuples([(f_ticker_cvt(i[0]), i[1]) for i in df_pos.index], names=df_pos.index.names)

    data_id = sdh.set_raw_data(
        data_source='external',
        dfraw=df_pos,
        source='sample'
    )

    sdh.set_alias({data_id: 'pos_csmr_goods'})
    return data_id


def read_by_laoder(start_date=None, end_date=None, db_name=None, schema_name=None) -> pd.DataFrame:

    if not db_name:
        db_name = os.environ.get(ENV_DATABSE)

    temp_sdh = DAL()
    df_inc1 = load_alternative_aiq_pos_csmr_goods_data(
            temp_sdh, generation=1, 
            start_datetime=start_date, end_datetime=end_date,
            db_name=db_name, load_all_tickers=True, 
            load_only_latest=False,
            schema_name=schema_name).retrieve()
    df_inc2 = load_alternative_aiq_pos_csmr_goods_data(
            temp_sdh, generation=2, end_datetime=end_date,
            db_name=db_name, load_all_tickers=True,
            load_only_latest=False,
            schema_name=schema_name).retrieve()
    
    return df_inc1, df_inc2


def reload(data_dir=DEFAULT_DIR, start_date=None, end_date=None, db_name=None, schema_name=None):
    df_inc1, df_inc2 = read_by_laoder(
        start_date, end_date, 
        db_name=db_name, schema_name=schema_name
    )
    df_inc1.to_parquet(os.path.join(data_dir, FILE_NAME_GEN1))
    df_inc2.to_parquet(os.path.join(data_dir, FILE_NAME_GEN2))
    dfpos_csmr = pd.concat([df_inc1, df_inc2], axis=0).reset_index(drop=True)
    dfpos_csmr.sort_index(inplace=True)
    return dfpos_csmr


def read_file(data_dir=DEFAULT_DIR):
    # loading from csv to save time for this demo
    df_inc1 = pd.read_parquet(os.path.join(data_dir, FILE_NAME_GEN1), engine='pyarrow')
    df_inc2 = pd.read_parquet(os.path.join(data_dir, FILE_NAME_GEN2), engine='pyarrow')
    return df_inc1, df_inc2


def read_pos_csmr_goods_plus_sales_share():

    share_ts = read_s3(DEFAULT_BUCKET, 'common/pos_csmr_goods_plus_sales_share_ts.csv')

    # POSのシェア率(時系列)
    # share_ts = pd.read_excel('20241003_pos_csmr_goods_plus_sales_share_ts.xlsx')
    share_ts['seccode'] = share_ts['seccode'].map(str)
    share_ts['datetime'] = pd.to_datetime(share_ts['datetime'])
    share_ts = share_ts.set_index(['seccode', 'datetime']).sort_index()
    return share_ts

