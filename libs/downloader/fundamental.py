import pandas as pd
from typing import List

from asr_protected.utils.myfunctools import compose

from .utils import download_alt

def download_fundamental(
        mongo_conn_str: str,    # e.g. xxxxx.com:1234
        tickers: List[str],
        from_year: int = 2008,
        fields: List[str] = ['sales'],
) -> pd.DataFrame:
    from pymongo import MongoClient
    
    conn = MongoClient(mongo_conn_str)
    col = conn['TRDB']['QUARTERLY_FIN_STMT']

    proj = {'_id': 0, 'seccode': 1, 'fiscal_quarter_last_date': 1}
    for fld in fields:
        proj[fld] = 1

    df = compose(pd.DataFrame, list, col.find)(
        {
            'seccode': {'$in': tickers},
            'fiscal_year': {'$gte': from_year},
            'is_latest': 1,
            'num_month': 3,
            'consolidated_flag': 1
        },
        proj
    )
    if not df.empty:
        df.rename(
            columns={
                'fiscal_quarter_last_date': 'datetime',
                'seccode': 'ticker'},
            inplace=True)
        df['datetime'] = pd.to_datetime(df['datetime'], format='%Y%m%d')
    df = df.set_index(['ticker', 'datetime'])
    return df




def download_fundamental_all(
    mongo_conn_str: str,    # e.g. xxxxx.com:1234
    from_year: int = 2008,
    fields: List[str] = ['sales'],
):
    tickers = download_alt()
    return download_fundamental(
        mongo_conn_str, tickers, from_year, fields
    )



if __name__ == "__main__":
    r = download_fundamental(['8035', '9983'], 2022, ['sales'])
    print(r)
