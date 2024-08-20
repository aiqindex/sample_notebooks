import pandas as pd
from typing import List
from pymongo import MongoClient


def download_market(
        mongo_conn_str: str,    # e.g. xxxxx.com:1234,
        tickers: List[str]) -> pd.DataFrame:
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
