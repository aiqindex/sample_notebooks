from typing import Optional, Union
from pathlib import Path

import pandas as pd
import yfinance as yf
from tqdm import tqdm


from asr_protected.utils.myfunctools import compose
from aiq_strategy_robot.data.data_accessor import StdDataHandler
from asr_protected.data_transformer.variable_libs import log_diff

from ..utils import index_to_upper
from ..path import DEFAULT_DIR
from ..s3 import to_s3, read_s3, DEFAULT_BUCKET



f_add = lambda sdh: compose(sdh.set_raw_data, index_to_upper, pd.read_parquet)



def register_market(
        sdh: StdDataHandler, 
        data_dir: Path=DEFAULT_DIR, 
        yf_switch: bool = False,
        base_data_id: Optional[Union[int, list]] = None
) -> int:
    """
    Register market data into the handler.

    Parameters
    ----------
    sdh : StdDataHandler
        Data handler for managing the dataset.
    data_dir : Path, optional
        Directory path for the price data files, by default DEFAULT_DIR.
    yf_switch : bool, optional
        Whether to retrieve data using the yfinance API, by default False.
    base_data_id : Optional[Union[int, list]], optional
        Data ID(s) for the universe when yf_switch is True, by default None.

    Returns
    -------
    int
        The data ID registered in the handler.
    """

    if not yf_switch:
        df_mkt = pd.read_parquet(Path(data_dir) / "market_on_mongo.parquet")
        
    else:
        assert base_data_id, "If `yf_switch`=True, specify the data_id that will be the universe."
        
        base_data_id = base_data_id if isinstance(base_data_id, list) else [base_data_id]

        tickers = []
        start_dates = []
        for data_id in base_data_id:
            df_base = sdh.get_raw_data(data_id)
            tickers_ = df_base.index.get_level_values('TICKER').unique().to_list()
            tickers.extend(tickers_)
            start_date = df_base.index.get_level_values('DATETIME').min().strftime('%Y-%m-%d')
            start_dates.append(start_date) 

        tickers = list(set(tickers))
        start_date = min(start_dates)

        print('extract mkt data from yfinance..')
        df_mkt = read_market_data_from_yfinance(tickers, start_date)

    df_mkt = index_to_upper(df_mkt)
    data_id = sdh.set_raw_data(df_mkt)
    sdh.set_alias({data_id: 'market'})
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
    mongo_conn_str: str = None, 
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

    if mongo_conn_str:
        from ..downloader.fundamental import download_fundamental_all
        print('extract fundamental from monogo db..')
        dfsales = download_fundamental_all(mongo_conn_str)
        # conv yoy
        dfsales = log_diff(dfsales, periods=4)
        dfsales.columns = [c + '_yoy' for c in dfsales.columns]
        to_s3(dfsales, DEFAULT_BUCKET, filename)
    else:
        dfsales = read_s3(DEFAULT_BUCKET, filename)

    dfsales = index_to_upper(dfsales)
    data_id = sdh.set_raw_data(dfsales)
    sdh.set_alias({data_id: 'funda'})
    return data_id
