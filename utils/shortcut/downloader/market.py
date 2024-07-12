import pandas as pd
from typing import List
from pymongo import MongoClient
from asr_protected.utils.myfunctools import compose


def download_market(tickers: List[str]) -> pd.DataFrame:
    conn = MongoClient('prod1-modelstore-ndeco.aiq-index.com:27017')
    col = conn['plugatrade']['market_data']
    df = pd.DataFrame(col.find(
            {'seccode': {'$in': tickers}},
            {'_id': 0, 'seccode': 1, 'datetime': 1, 'close': 1}
        ))
    df = df.rename({'seccode': 'ticker'}, axis=1)
    df['datetime'] = df['datetime'].apply(
        lambda x: x.tz_localize('UTC').tz_convert('Japan').tz_localize(None))
    df = df.set_index(["ticker", "datetime"])
    df.index = pd.MultiIndex.from_tuples(
        [(i[0]+"-JP", i[1]) for i in df.index],
        names=df.index.names)
    return df
