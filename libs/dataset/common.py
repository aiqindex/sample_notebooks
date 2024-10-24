from typing import Optional, Union, List
from pathlib import Path

import pandas as pd
import yfinance as yf
from tqdm import tqdm

from aiq_strategy_robot.data.data_accessor import DAL
from aiq_strategy_robot.data.data_accessor import StdDataHandler
from asr_protected.data_transformer.variable_libs import log_diff

from ..downloader.fundamental import download_fundamental
from ..downloader.market import download_market_from_influx
from ..utils import index_to_upper
from ..path import DEFAULT_DIR
from ..s3 import to_s3, read_s3, DEFAULT_BUCKET


def extract_tickers(sdh, data_ids):

    tickers = []
    start_dates = []
    for data_id in data_ids:
        df_base = sdh.get_raw_data(data_id)
        tickers_ = df_base.index.get_level_values('TICKER').unique().to_list()
        tickers.extend(tickers_)
        start_date = df_base.index.get_level_values('DATETIME').min().strftime('%Y-%m-%d')
        start_dates.append(start_date) 

    return list(set(tickers))

def register_market(
    sdh: StdDataHandler,
    filename: str = "common/market_return.parquet"
) -> int:
    """
    Register market data into the handler.

    Parameters
    ----------
    sdh : StdDataHandler
        Data handler for managing the dataset.
    filename : str, optional
        S3 upload file name, by default "common/market_return.parquet".
    base_data_id : Optional[Union[int, list]], optional
        Data ID(s) for the universe when yf_switch is True, by default None.
    start_date : str, optional
        Start date for the reloaded data, by default '2007-01-04'.
    end_date : str, optional
        End date for the reloaded data, by default '2024-07-11'.

    Returns
    -------
    int
        The data ID registered in the handler.
    """

    alias = 'market_returns'
    # print('extract mkt data from s3..')
    df_mkt = read_s3(DEFAULT_BUCKET, filename)

    df_mkt = index_to_upper(df_mkt)
    data_id = sdh.set_raw_data(df_mkt)
        
    sdh.set_alias({data_id: alias})
    return data_id

def read_market_data_from_yfinance(tickers, start_date):
    dfstock = pd.DataFrame()
    not_list = []
    for ticker in tqdm(tickers):
        stock = yf.Ticker(ticker + '.T')
        hist = stock.history(start=start_date)
        if len(hist) > 0:
            hist['TICKER'] = ticker
            hist.index = hist.index.tz_localize(None)
            hist.index.name = 'DATETIME'
            # yfinanceのローソンデータの最終日が正しくないため、それ以前を使う
            if ticker == '2651':
                hist = hist[hist.index < '2024-07-23']
            hist.set_index('TICKER', append=True, inplace=True)
            dfstock = pd.concat([dfstock, hist], axis=0)
        else:
            not_list.append(ticker)

    print('no: ', ','.join(not_list))
    dfstock = dfstock.swaplevel().sort_index()
    dfstock.columns = [c.lower() for c in dfstock.columns]
    return dfstock


def register_fundamental(
    sdh: StdDataHandler, 
    filename="common/fundamental_yoy_on_mongo.parquet", 
) -> int:
    """
    Retrieve financial data from an S3 directory.

    Parameters
    ----------
    sdh : StdDataHandler
        Data handler for managing the dataset.
    filename : str, optional
        Filename on the S3 directory, by default "common/fundamental_yoy_on_mongo.parquet".
    mongo_conn_str : str, optional
        MongoDB connection string. If specified, the data is reloaded, by default None.

    Returns
    -------
    int
        The data ID registered in the handler.
    """

    dfsales = read_s3(DEFAULT_BUCKET, filename)

    dfsales = index_to_upper(dfsales)
    data_id = sdh.set_raw_data(dfsales)
    sdh.set_alias({data_id: 'funda'})
    return data_id

# ************************** reload ************************

import glob
import os

import pandas as pd
from os import listdir
from os.path import isfile, join

def get_matching_files(directory, pattern):
    # パターンに基づいて条件に一致するファイルを取得
    search_pattern = os.path.join(directory, pattern)
    matching_files = glob.glob(search_pattern)
    return matching_files


def get_adj_close(
    dfinput: pd.DataFrame,
    list_tickers: list
) -> pd.DataFrame:
    dfclose = dfinput[['Ticker', 'DATE', 'Close Price', 'Split Factor', 'Div Factor']].copy()
    dfclose = dfclose.rename(columns={
        'Ticker': 'ticker',
        'DATE': 'datetime',
        'Close Price': 'close',
        'Split Factor': 'split_ratio',
        'Div Factor': 'div_ratio'
    })
    dfclose['ticker'] = dfclose['ticker'].apply(lambda x: x[:4])
    dfclose = dfclose.loc[dfclose['ticker'].isin(list_tickers)]
    dfclose = dfclose.set_index(['ticker', 'datetime'])
    dfclose['adj_close'] = dfclose['close'] * dfclose['split_ratio'] * dfclose['div_ratio']
    return dfclose[['adj_close', 'close']]


def reload_market_to_s3(
    tickers: List[str],
    s3filename: str = "common/market_return.parquet",
):

    extract_dir = '/efs/share/data/extract/'
    list_extracts = [join(extract_dir, f) for f in listdir(extract_dir) if isfile(join(extract_dir, f))]
    list_extracts.sort()
    list_extracts

    # files with market data are only required.
    list_data_extracts = [x for x in list_extracts if '_cae_' not in x]

    # # we need only after 2019 for SCI+
    # list_data_sci = [x for x in list_data_extracts if target_file_str in x]
    # list_data_sci

    list_data = []
    for fl in list_extracts:
        dftmp = pd.read_parquet(fl, engine='pyarrow')
        list_data.append(dftmp)
    dfdata = pd.concat(list_data, axis=0, sort=False)
    dfdata.head() 

    df_mkt_raw = get_adj_close(dfdata, tickers)
    df_mkt_raw.index.names = ['TICKER', 'DATETIME']
    df_mkt_raw = df_mkt_raw.loc[~df_mkt_raw.index.duplicated()]

    # transformation to returns
    tmpsdh = DAL()
    tmp_data_id = tmpsdh.set_raw_data(df_mkt_raw)

    # logs = tmpsdh.transform.log(data_id=tmp_data_id, fields=['close', 'open'], names=['log_close', 'log_open']).variable_ids
    # logs_prev = tmpsdh.transform.shift(1, fields=['log_close', 'log_open'], names=['log_close_prev', 'log_open_prev']).variable_ids

    returns = tmpsdh.transform.log_diff(1, data_id=tmp_data_id, fields='adj_close', names='returns').variable_ids[0]  # close(t2) - close(t1) 
    # returns_oo = tmpsdh.transform.log_diff(1, data_id=tmp_data_id, fields='open', names='returns_oo').variable_ids[0] # open(t2) - open(t1) 
    # returns_id = tmpsdh.transform.sub(x1field='log_close', x2field='log_open', name='returns_id').variable_ids[0] # close(t1) - open(t1) 
    # return_on = tmpsdh.transform.sub(x1field='log_open', x2field='log_close_prev', name='returns_on').variable_ids[0] # open(t2) - close(t1) 

    # df_mkt = tmpsdh.get_variables([returns, returns_oo, returns_id, return_on])
    df_mkt = tmpsdh.get_variables([returns])
    to_s3(df_mkt, DEFAULT_BUCKET, s3filename)

    # if efsfilename:
    #     df_mkt_raw.to_parquet(efsfilename)


def reload_fundamental_to_s3(
    mongo_conn_str,
    tickers: list,
    from_year: int = 2008,
    end_date: str = None,
    fields: List[str] = ['sales'],
    filename="common/fundamental_yoy_on_mongo.parquet"
):
    print('extract fundamental from monogo db..')
    dfsales = download_fundamental(
        mongo_conn_str, tickers, from_year, fields
    )
    dfsales = pd.read_parquet('sales_new.parquet').sort_index()
    dfsales.index.names = ['TICKER', 'DATETIME']

    if end_date:
        dfsales = dfsales[dfsales.index.get_level_values('DATETIME') <= end_date]

    # conv yoy
    dfsales = log_diff(dfsales, periods=4)
    dfsales.columns = [c + '_yoy' for c in dfsales.columns]
    to_s3(dfsales, DEFAULT_BUCKET, filename)



"""
#### Reload the data in this way. ##########

from libs.dataset.reload import get_alt_tickers
from libs.dataset.common import reload_market_to_s3, download_fundamental

# Pass to `aiqb`
sys.path.append('../../../modules')

tickers = get_alt_tickers()
reload_market_to_s3(tickers)
reload_fundamental_to_s3(tickers, 'ssss-sss-ss.sss-index.com:11111')
"""