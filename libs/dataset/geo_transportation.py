import os

import pandas as pd
import yfinance as yf
from tqdm import tqdm

from aiq_strategy_robot.data.data_accessor import DAL
from ..path import DEFAULT_DIR, efs_path

from ..s3 import read_s3, to_s3, DEFAULT_BUCKET, DEFAULT_DIR

DEFAULT_DIR = os.path.join(DEFAULT_DIR, 'geo_transportation')

ORIGINAL_DIR = '/efs/share/delivery/test/foot_traffic_place/'
if not os.path.exists(efs_path):
    # aiQ hub上の異なるマウントポジションに対応
    ORIGINAL_DIR = '~/share/delivery/test/foot_traffic_place/'



def read_foot_traffic_place(
    sec_list, 
    filename='foot_traffic_place_stack', 
    reload=False
):
    car_filename = DEFAULT_DIR + '/' + f'{filename}_car.parquet'
    if reload:
        import polars

        ori_dir = ORIGINAL_DIR
        geo_all = polars.read_parquet(os.path.join(ori_dir, f'{filename}.parquet'))
        geo_all = geo_all.with_columns(TICKER=geo_all['place_id'].str.slice(-4))
        geo_car = geo_all.filter((geo_all['TICKER'].is_in(sec_list)) & (geo_all['SMOOTH'] == 0))
        geo_car = geo_car.to_pandas()
        geo_car.rename(columns={'datetime': 'DATETIME'}, inplace=True)
        # geo_car.to_parquet(os.path.join(dir_path, f'{filename}_car.parquet'))

        to_s3(geo_car, DEFAULT_BUCKET, car_filename)
    else:
        # geo_car = pd.read_parquet(os.path.join(dir_path, f'{filename}_car.parquet'))
        geo_car = read_s3(DEFAULT_BUCKET, car_filename)

    # aiQLab用に列名変換
    geo_car['DATETIME'] = pd.to_datetime(geo_car['DATETIME'])

    # 全日、夜間それぞれで日別に集計
    dfgeo = geo_car.groupby(['TICKER', 'DATETIME', 'variable'])['values'].sum()
    dfgeo = dfgeo.unstack('variable')
    dfgeo = dfgeo.loc[dfgeo.index.get_level_values('TICKER').isin(sec_list)]

    return dfgeo

def read_fundamental(
    sec_list, 
    filename='dfsales_transportation', 
    reload=False
):
    car_filename = os.path.join(DEFAULT_DIR, f'{filename}.parquet')
    if reload:
        dfsales = pd.DataFrame()
        sdh = DAL()
        dfsales = sdh.load(
                        'finnhub', 
                        data_type='fundamental',
                        symbols=[ticker + '.T' for ticker in sec_list], 
                        st_type='ic', 
                        freq='quarterly', 
                        start_datetime='2014-01-01', 
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
    else:
        # dfsales = pd.read_parquet(os.path.join(dir_path, f'{filename}.parquet'))
        dfsales = read_s3(DEFAULT_BUCKET, car_filename)

    dfsales = dfsales.loc[dfsales.index.get_level_values('TICKER').isin(sec_list)]
    return dfsales