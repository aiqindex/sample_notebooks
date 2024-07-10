import os
import numpy as np
import pandas as pd

from .path import DEFAULT_DIR

# Load Fundamental Data
def register_fundamental_data(sdh, data_dir=DEFAULT_DIR) -> int:
    df_fundamental = pd.read_parquet(os.path.join(data_dir, 'aiq_pos_csmr_goods_fundamental.parquet'), engine='pyarrow')
    df_fundamental.index.names = ['TICKER', 'DATETIME']

    return sdh.set_raw_data(
        data_source='external',
        dfraw=df_fundamental,
        source='sample'
    )

def register_market_prices(sdh, data_dir=DEFAULT_DIR):
    # again we load the existing data for reducing the demo duration.
    prices_df = pd.read_parquet(os.path.join(data_dir, 'aiq_pos_csmr_goods_mkt_long.parquet'), engine='pyarrow')
    prices_df.index.names = ['TICKER', 'DATETIME']
    prices_df = prices_df[~prices_df.index.get_level_values('TICKER').isnull()]
    data_id_price = sdh.set_raw_data(prices_df, data_source='FACTSET', source='gpd_prices')
    return data_id_price

def register_tv(sdh, data_dir=DEFAULT_DIR):

    merged_tv = pd.read_parquet(os.path.join(data_dir, 'aiq_pos_csmr_goods_tv.parquet'), engine='pyarrow')
    merged_tv.index.names = ['TICKER', 'DATETIME']
    merged_tv = merged_tv[~merged_tv.index.get_level_values('TICKER').isnull()]
    data_id_tv = sdh.set_raw_data(merged_tv, data_source='FACTSET', source='TrueValue')
    return data_id_tv


