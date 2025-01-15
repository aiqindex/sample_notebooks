import os
from glob import glob
from typing import List, Optional, Callable

import numpy as np
import pandas as pd
from tqdm.notebook import tqdm

from aiq_strategy_robot.data.data_accessor import DAL
from aiq_strategy_robot.data.ALTERNATIVE import load_alternative_aiq_retailer_weekly_data

ENV_DATABSE = 'TRIAL_SNOWFLAKE_DATABASE_AIQ_RETAILER_WEEKLY'

# Load Alternative Data
def register_retailer_data(
    sdh,
    f_ticker_cvt: Optional[Callable[[str], str]] = None,
    db_name: str = None,
    schema_name: str = None,
    start_date=None,
    end_date=None,
) -> int:

    print('extract retailer weekly by loader..')
    df_pos = read_by_laoder(start_date, end_date, db_name=db_name, schema_name=schema_name)

    if f_ticker_cvt is not None:
        df_pos.index = pd.MultiIndex.from_tuples([(f_ticker_cvt(i[0]), i[1]) for i in df_pos.index], names=df_pos.index.names)

    data_id = sdh.set_raw_data(
        data_source='external',
        dfraw=df_pos,
        source='sample'
    )

    sdh.set_alias({data_id: 'retailer_weekly'})
    return data_id

def read_by_laoder(start_date=None, end_date=None, db_name=None, schema_name=None):

    if not db_name:
        db_name = os.environ.get(ENV_DATABSE)

    temp_sdh = DAL()
    dfpos = load_alternative_aiq_retailer_weekly_data(
            temp_sdh, 
            select_timestamp='OLDEST',
            load_all_tickers=True,
            start_datetime=start_date, end_datetime=end_date, 
            db_name=db_name, schema_name=schema_name).retrieve()
    
    dfpos.sort_index(inplace=True)
    dfpos['DATETIME'] = pd.to_datetime(dfpos['DATETIME'])

    dfpos = dfpos[dfpos['COMPANY_ID'].str.startswith('all_')]
    dfpos = aggregate_dfsci(dfpos)
    dfpos = transform_dfsci(dfpos)

    dfpos = dfpos.xs(0, axis=1, level='SMOOTH')

    colfunc = {'sales': 'sum'}
    colfunc.update({cl: 'mean' for cl in dfpos.columns.drop('sales')})
    dfpos = dfpos.groupby('TICKER').resample('W', level='DATETIME').apply(colfunc)
    return dfpos


def aggregate_dfsci(dfsci):
    return dfsci.groupby(['TICKER', 'DATETIME', 'SMOOTH', 'VARIABLE'])[['VALUE']].sum()

def transform_dfsci(dfsci):
    dftx = dfsci['VALUE'].unstack(['VARIABLE', 'SMOOTH'])
    return dftx
