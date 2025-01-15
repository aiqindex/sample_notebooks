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




def download_market_from_influx(
        conf_path: str, 
        tickers: list, 
        start_date=None, 
        end_date=None, 
):
    # 価格データの更新を行う場合は、README
    from load_data_from_influxdb.TRIN_price_data_handler import TRINPriceLoader as TRIN

    list_tickers = list(tickers)
    sample_instance = TRIN({
            'config_file' : os.path.join(conf_path, 'influx_config.yml'),
            'start': start_date,
            'end': end_date,
            'timebase': 'daily',
            'timezone': 'Japan',
            'market_hour_definition': os.path.join(conf_path, 'market_hours.yml')
        })

    sample_instance.run(list_tickers, ['open', 'high', 'low', 'close', 'volume'], switch_split=True)
    retval = sample_instance.retrieve('all', start_date, end_date, 'raw')

    list_outputs = []
    empty_list = []
    for tk, df in retval.items():

        if df.empty:
            empty_list.append(tk)
            continue

        df.index = df.index.tz_localize(None)
        df.index.names = ['DATETIME']
        df['TICKER'] = tk
        df = df.set_index('TICKER', append=True).swaplevel(0, 1)
        list_outputs.append(df)
    dfout = pd.concat(list_outputs, axis=0, sort=False)

    if empty_list:
        print(f'empty list is {", ".join(empty_list)}.')
    
    return dfout