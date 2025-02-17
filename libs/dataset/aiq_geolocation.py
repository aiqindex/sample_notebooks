import os

import pandas as pd
import yfinance as yf
from tqdm import tqdm

from aiq_strategy_robot.data.ALTERNATIVE import *
from aiq_strategy_robot.data.data_accessor import DAL
from ..path import DEFAULT_DIR as DEFAULT_DIR_EFS

from ..s3 import read_s3, to_s3, DEFAULT_BUCKET


GEO_FILE_NAME = 'pos_geolocation.parquet'
ENV_DATABSE = 'TRIAL_SNOWFLAKE_DATABASE_AIQ_GEOLOCATION'

FUNDA_FILE_NAME = 'dfsales_transportation.parquet'


def read_foot_traffic_place(
    tickers,
    data_dir=DEFAULT_DIR_EFS,
    start_date=None,
    end_date=None,
    db_name: str = None,
    schema_name: str = None,
):
    try:
        geo_car = pd.read_parquet(os.path.join(data_dir, GEO_FILE_NAME), engine='pyarrow')
    except:
        # ファイルが存在しない場合はloaderから取得
        print('extract pos_geolocation by loader..')
        geo_car = read_geo_by_laoder(
            tickers=tickers, start_date=start_date, end_date=end_date,
            db_name=db_name, schema_name=schema_name
        )

    # 集約
    geo_car['DATETIME'] = pd.to_datetime(geo_car['DATETIME'])
    dfgeo = geo_car.groupby(['TICKER', 'DATETIME', 'VARIABLE'])['VALUE'].sum()
    dfgeo = dfgeo.unstack('VARIABLE')
    if tickers:
        dfgeo = dfgeo.loc[dfgeo.index.get_level_values('TICKER').isin(tickers)]
    return dfgeo


def read_geo_by_laoder(
    tickers, 
    start_date=None, 
    end_date=None, 
    db_name=None, 
    schema_name=None
) -> pd.DataFrame:
    
    if not db_name:
        db_name = os.environ.get(ENV_DATABSE)

    dfdata = load_alternative_aiq_geolocation_data(
        DAL(), ticker=tickers, start_datetime=start_date, end_datetime=end_date,
        db_name=db_name, schema_name=schema_name).retrieve()
    return dfdata


def reload_geolocation(
    tickers: list, 
    start_date=None,
    end_date=None,
    db_name=None, 
    schema_name=None,
    data_dir=DEFAULT_DIR_EFS,
):
    dfdata = read_geo_by_laoder(
        tickers=tickers, start_date=start_date, end_date=end_date,
        db_name=db_name, schema_name=schema_name
    )
    dfdata.to_parquet(os.path.join(data_dir, GEO_FILE_NAME))
    return dfdata


def read_fundamental(sec_list):
    car_filename = os.path.join('geo_transportation', FUNDA_FILE_NAME)
    dfsales = read_s3(DEFAULT_BUCKET, car_filename)
    dfsales = dfsales.loc[dfsales.index.get_level_values('TICKER').isin(sec_list)]
    return dfsales

def reload_fundamental(sec_list, start_date=None, end_date=None):
    car_filename = os.path.join('geo_transportation', FUNDA_FILE_NAME)
    dfsales = pd.DataFrame()
    sdh = DAL()
    dfsales = sdh.load(
                    'finnhub', 
                    data_type='fundamental',
                    symbols=[ticker + '.T' for ticker in sec_list], 
                    st_type='ic', 
                    freq='quarterly', 
                    start_datetime=start_date, 
                    end_datetime=end_date, 
                    with_calendar=True
                ).retrieve()

    data_id = sdh.extract_definition.iloc[-1].name
    fields = ['revenue', 'grossIncome']    
    yoy_id_sales = sdh.transform.resample(data_id=data_id, fields=fields, rule='Q', func='sum'
                                            ).log_diff(4, names=[c + '_yoy' for c in fields]).variable_ids
    
    dfsales = sdh.get_variables(yoy_id_sales).sort_index()
    dfsales = dfsales.reset_index().rename({'ticker': 'TICKER', 'datetime': 'DATETIME'}, axis=1)
    dfsales['TICKER'] = dfsales['TICKER'].str.slice(0, 4)
    dfsales.set_index(['TICKER', 'DATETIME'], inplace=True)

    # dfsales.to_parquet(os.path.join(dir_path, f'{filename}.parquet'))
    to_s3(dfsales, DEFAULT_BUCKET, car_filename)