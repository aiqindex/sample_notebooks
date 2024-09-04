import os

import pandas as pd
from typing import List


current_file_path = os.path.abspath(__file__)
current_dir, filename = os.path.split(current_file_path)


def download_market_from_mongo(
        mongo_conn_str: str,    # e.g. xxxxx.com:1234,
        tickers: List[str]) -> pd.DataFrame:

    from pymongo import MongoClient
    conn = MongoClient(mongo_conn_str)
    col = conn['plugatrade']['market_data']
    df = pd.DataFrame(col.find(
            {'seccode': {'$in': tickers}},
            {'_id': 0, 'seccode': 1, 'datetime': 1, 'close': 1}
        ))
    df = df.rename({'seccode': 'ticker'}, axis=1)
    df['datetime'] = df['datetime'].apply(
        lambda x: x.tz_localize('UTC').tz_convert('Japan').tz_localize(None))
    df = df.set_index(["ticker", "datetime"])
    return df




def download_market_from_influx(tickers: list, start_date=None, end_date=None):

    list_tickers = list(tickers)

    # 価格データの更新を行う場合は、README
    from market_price_data_handler.TRIN_price_data_handler import TRINPriceLoader as TRIN

    sample_instance = TRIN({
            'config_file' : os.path.join(current_dir, 'config/db_config.yml'),
            'start': start_date,
            'end': end_date,
            'timebase': 'daily',
            'timezone': 'Japan',
            'market_hour_definition': os.path.join(current_dir, 'config/market_hours.yml')
        })

    list_outputs = []
    for tk in list_tickers:
        sample_instance.set_symbol([tk])
        sample_instance.run(['open', 'close'], switch_split=True, take_from_prod=True)
        retval = sample_instance.retrieve('all', start_date, end_date, 'raw')

        if len(retval) == 0:
            continue

        df = retval[0]
        df.index = df.index.tz_localize(None)
        df.index.names = ['DATETIME']
        df['TICKER'] = tk
        df = df.set_index('TICKER', append=True).swaplevel(0, 1)
        list_outputs.append(df)
    dfout = pd.concat(list_outputs, axis=0, sort=False)
    
    return dfout