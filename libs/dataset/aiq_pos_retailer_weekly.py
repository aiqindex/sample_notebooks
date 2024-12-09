import os
from glob import glob
from typing import List, Optional, Callable

import numpy as np
import pandas as pd
from tqdm.notebook import tqdm

from aiq_strategy_robot.data.data_accessor import DAL
from aiq_strategy_robot.data.ALTERNATIVE import load_alternative_aiq_pos_retailer_data

from ..path import DEFAULT_DIR
from ..s3 import read_s3, DEFAULT_BUCKET

FILE_NAME = 'common/pos_csmr_retailer_weekly_formatted.parquet'


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

    # loading from csv to save time for this demo
    df_pos = read_s3(DEFAULT_BUCKET, FILE_NAME)

    # TODO: loaderから取得
    # print('extract pos_retailer_weekly by loader..')
    # df_pos = read_by_laoder(start_date, end_date, db_name=db_name, schema_name=schema_name)

    if f_ticker_cvt is not None:
        df_pos.index = pd.MultiIndex.from_tuples([(f_ticker_cvt(i[0]), i[1]) for i in df_pos.index], names=df_pos.index.names)

    data_id = sdh.set_raw_data(
        data_source='external',
        dfraw=df_pos,
        source='sample'
    )

    sdh.set_alias({data_id: 'SCI'})
    return data_id




# *******************************************************************
# s3::pos_csmr_retailer_weekly_formatted.parquet
# 上記ファイルを作成するにあたって使用した関数群（→loader実装後は要置き換え）
# 👇👇👇👇👇👇👇👇👇👇👇👇👇👇👇👇👇👇👇👇👇👇👇👇👇👇👇

def load_sci_csv2(tardir):
    path = os.path.join(tardir, 'pos_retailer_weekly_stack.csv.gz')
    dfsci = pd.read_csv(path, encoding='Shift-JIS')
    dfsci.rename({'datetime':'DATETIME', 'ticker': 'TICKER'}, axis=1, inplace=True)
    dfsci['DATETIME'] = pd.to_datetime(dfsci['DATETIME'])
    dfsci['TICKER'] = dfsci['TICKER'].astype(str)

    dfsci = dfsci.groupby(['TICKER', 'DATETIME', 'SMOOTH', 'variable'])[['values']].sum()

    return dfsci


def transform_dfsci(dfsci):
    dftx = dfsci['values'].unstack(['variable', 'SMOOTH'])
    pos_sales_WoW = np.log(dftx[['pos_sales']]).groupby('TICKER').diff(1).rename({'pos_sales':'pos_sales_WoW'}, axis=1)
    pos_sales_MoM = np.log(dftx[['pos_sales']]).groupby('TICKER').diff(4).rename({'pos_sales':'pos_sales_MoM'}, axis=1)
    pos_sales_QoQ = np.log(dftx[['pos_sales']]).groupby('TICKER').diff(13).rename({'pos_sales':'pos_sales_QoQ'}, axis=1)
    pos_sales_SYoSY = np.log(dftx[['pos_sales']]).groupby('TICKER').diff(26).rename({'pos_sales':'pos_sales_SYoSY'}, axis=1)
    pos_sales_YoY = np.log(dftx[['pos_sales']]).groupby('TICKER').diff(52).rename({'pos_sales':'pos_sales_YoY'}, axis=1)
    share_WoW = dftx[['share']].groupby('TICKER').diff(13).rename({'share':'share_WoW'}, axis=1)
    share_MoM = dftx[['share']].groupby('TICKER').diff(13).rename({'share':'share_MoM'}, axis=1)
    share_QoQ = dftx[['share']].groupby('TICKER').diff(13).rename({'share':'share_QoQ'}, axis=1)
    share_SYoSY = dftx[['share']].groupby('TICKER').diff(26).rename({'share':'share_SYoSY'}, axis=1)
    share_YoY = dftx[['share']].groupby('TICKER').diff(52).rename({'share':'share_YoY'}, axis=1)
    dftx = pd.concat([dftx, pos_sales_WoW, pos_sales_MoM, pos_sales_QoQ, pos_sales_SYoSY, pos_sales_YoY, share_WoW, share_MoM, share_QoQ, share_SYoSY, share_YoY], axis=1)
    return dftx


def load_sci_weekly_data(baseline='20241016'):
    basedir = '/efs/share/BItool/pos_retailer_weekly/bulk/%s'
    tardir = basedir % baseline
    dfsci_latest = load_sci_csv2(tardir)
    dfsci_latest = transform_dfsci(dfsci_latest)
    dfsci_latest['first_dir'] = pd.to_datetime(tardir.split('/')[-1])

    tardir_list = glob(basedir % '*')
    tardir_list.sort()
    dfsci = dfsci_latest.copy()
    for tardir in tqdm(tardir_list[::-1]):
        dfsci_tmp = load_sci_csv2(tardir)
        dfsci_tmp = transform_dfsci(dfsci_tmp)
        dfsci_tmp['first_dir'] = pd.to_datetime(tardir.split('/')[-1])
        dfsci.update(dfsci_tmp)


    tmp = dfsci.drop('first_dir', axis=1).xs(0, axis=1, level='SMOOTH')
    colfunc = {'pos_sales': 'sum'}
    colfunc.update({cl: 'mean' for cl in tmp.columns.drop('pos_sales')})
    dfsci = tmp.groupby('TICKER').resample('W', level='DATETIME').apply(colfunc)

    return dfsci