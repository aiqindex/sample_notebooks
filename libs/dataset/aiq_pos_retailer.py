import os
import numpy as np
import pandas as pd
from typing import List, Optional, Callable

from aiq_strategy_robot.data.data_accessor import DAL
from aiq_strategy_robot.data.ALTERNATIVE import load_alternative_aiq_pos_retailer_data


from .utils import format_pos
from ..path import DEFAULT_DIR


FILE_NAME = 'pos_retailer_stack.parquet'
ENV_DATABSE = 'TRIAL_SNOWFLAKE_DATABASE_AIQ_POS_RETAILER'

# Load Alternative Data
def register_retailer_data(
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
        df_pos = read_file(data_dir)
    except:
        # ファイルが存在しない場合はloaderから取得
        print('extract pos_retailer by loader..')
        df_pos = read_by_laoder(start_date, end_date, db_name=db_name, schema_name=schema_name)

    if f_ticker_cvt is not None:
        df_pos.index = pd.MultiIndex.from_tuples([(f_ticker_cvt(i[0]), i[1]) for i in df_pos.index], names=df_pos.index.names)

    data_id = sdh.set_raw_data(
        data_source='external',
        dfraw=df_pos,
        source='sample'
    )

    sdh.set_alias({data_id: 'pos_retailer'})
    return data_id


def read_by_laoder(start_date=None, end_date=None, db_name=None, schema_name=None):

    if not db_name:
        db_name = os.environ.get(ENV_DATABSE)

    temp_sdh = DAL()
    dfpos = format_pos(
        load_alternative_aiq_pos_retailer_data(
            temp_sdh, load_all_tickers=True,
            start_datetime=start_date, end_datetime=end_date, 
            db_name=db_name, schema_name=schema_name).retrieve()
    )
    dfpos.sort_index(inplace=True)
    return dfpos



def reload(data_dir=DEFAULT_DIR, start_date=None, end_date=None, db_name=None, schema_name=None):
    df_pos = read_by_laoder(
        start_date, end_date, 
        db_name=db_name, schema_name=schema_name
    )
    df_pos.to_parquet(os.path.join(data_dir, FILE_NAME))
    return df_pos

def read_file(data_dir=DEFAULT_DIR):
    # loading from csv to save time for this demo
    return pd.read_parquet(os.path.join(data_dir, FILE_NAME), engine='pyarrow')