import os
from typing import Optional, Callable
from pathlib import Path

import pandas as pd

from aiq_strategy_robot.data.data_accessor import DAL
from aiq_strategy_robot.data.ALTERNATIVE import load_alternative_aiq_pos_elec_goods_data

from .utils import format_pos, filter_to_latest_releases
from ..path import DEFAULT_DIR


FILE_NAME = 'pos_elec_goods_stack.parquet'
ENV_DATABSE = 'TRIAL_SNOWFLAKE_DATABASE_AIQ_POS_ELEC_GOODS'


def register_elec_goods_data(
        sdh,
        data_dir: str = DEFAULT_DIR,
        f_ticker_cvt: Optional[Callable[[str], str]] = None,
        db_name: str = None,
        schema_name: str = None,
        end_date=None,
) -> int:
    try:
        # loading from csv to save time for this demo
        df_pos = read_by_file(data_dir)
    except:
        # ファイルが存在しない場合はloaderから取得
        print('extract pos_elec_goods by loader..')
        df_pos = read_by_laoder(end_date=end_date, db_name=db_name, schema_name=schema_name)

    if f_ticker_cvt is not None:
        df_pos.index = pd.MultiIndex.from_tuples([(f_ticker_cvt(i[0]), i[1]) for i in df_pos.index], names=df_pos.index.names)

    data_id = sdh.set_raw_data(
        data_source='external',
        dfraw=df_pos,
        source='sample'
    )

    sdh.set_alias({data_id: 'pos_elec_goods'})
    return data_id


def read_by_file(data_dir: str = DEFAULT_DIR) -> pd.DataFrame:
    path_to_csv = Path(data_dir) / FILE_NAME
    dfpos = pd.read_parquet(path_to_csv)
    return dfpos


def read_by_laoder(end_date=None, db_name=None, schema_name=None) -> pd.DataFrame:

    if not db_name:
        db_name = os.environ.get(ENV_DATABSE)

    temp_sdh = DAL()
    dfpos = format_pos(
        load_alternative_aiq_pos_elec_goods_data(
            temp_sdh, load_all_tickers=True, 
            end_datetime=end_date, db_name=db_name, schema_name=schema_name).retrieve()
    )
    dfpos.sort_index(inplace=True)
    return dfpos



def reload(data_dir, end_date=None, db_name=None, schema_name=None):
    df_pos = read_by_laoder(
        end_date=end_date, 
        db_name=db_name, schema_name=schema_name
    )
    df_pos.to_parquet(os.path.join(data_dir, FILE_NAME))
    return df_pos