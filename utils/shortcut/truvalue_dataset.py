import os
import numpy as np
import pandas as pd

from aiq_strategy_robot.data.data_accessor import DAL
from aiq_strategy_robot.data.ALTERNATIVE import load_alternative_aiq_pos_csmr_goods_universe
from aiq_strategy_robot.data.ALTERNATIVE import load_alternative_aiq_pos_csmr_goods_data
from aiq_strategy_robot.data.FACTSET import load_factset_symbol_lookup
from .path import DEFAULT_DIR

sdh = DAL()
sdh = load_alternative_aiq_pos_csmr_goods_universe(sdh)
dfsyms = sdh.retrieve()
dfsyms = dfsyms.rename({'TICKER': 'seccode'}, axis=1)


# Load Fundamental Data
def register_fundamental_data(sdh, factset_symbols: pd.DataFrame=None, use_dump=True, data_dir=DEFAULT_DIR) -> int:
    if use_dump:
        df_fundamental = pd.read_parquet(os.path.join(data_dir, 'aiq_pos_csmr_goods_fundamental.parquet'), engine='pyarrow')
    else:
        start_datetime = sdh.extract_definition.loc[data_id_alt]['start_datetime'].split('T')[0]
        sdh = sdh.load(
            'FACTSET',
            data_type='fundamental',
            symbols=factset_symbols['FSYM_ID'].unique().tolist(),
            freq=3,
            start_datetime=start_datetime
        )
        df_fundamental = sdh.retrieve(pick_cols=['FSYM_ID', 'DATE', 'FF_FPNC', 'FF_SALES'])
        df_fundamental = df_fundamental.merge(factset_symbols[['FSYM_ID', 'TICKER']], on='FSYM_ID', how='left')
        df_fundamental = df_fundamental.sort_values(['TICKER', 'DATE'], ascending=[True, True])
        df_fundamental = df_fundamental[['TICKER', 'DATE', 'FF_SALES']].rename(columns={'TICKER': 'ticker', 'DATE': 'datetime', 'FF_SALES': 'sales'})
        df_fundamental['datetime'] = pd.to_datetime(df_fundamental['datetime'])
        df_fundamental = df_fundamental.set_index(['ticker', 'datetime'])
        df_fundamental.to_parquet('aiq_pos_csmr_goods_fundamental.parquet', engine='pyarrow')

    return sdh.set_raw_data(
        data_source='external',
        dfraw=df_fundamental,
        source='sample'
    )

def register_market_prices(sdh, list_tickers=None, use_dump=True, data_dir=DEFAULT_DIR):
    # again we load the existing data for reducing the demo duration.
    if use_dump:
        prices_df = pd.read_parquet(os.path.join(data_dir, 'aiq_pos_csmr_goods_mkt_long.parquet'), engine='pyarrow')
    else:
        prices_df = load_market_prices(list_tickers, '2016-01-01')
        
    prices_df = prices_df[~prices_df.index.get_level_values('ticker').isnull()]
    data_id_price = sdh.set_raw_data(prices_df, data_source='FACTSET', source='gpd_prices')
    return data_id_price

def register_tv(sdh, list_tickers=None, use_dump=True, data_dir=DEFAULT_DIR):
    if use_dump:
        merged_tv = pd.read_parquet(os.path.join(data_dir, 'aiq_pos_csmr_goods_tv.parquet'), engine='pyarrow')
    else:
        tv_features = [
            ('TV_ESG_RANKS', 'MATERIALITY_ADJ_INSIGHT'),
            ('TV_ESG_RANKS', 'MATERIALITY_IND_PCTL'),
            ('TV_ESG_RANKS', 'ALL_CATEGORIES_ADJ_INSIGHT'),
            ('TV_INSIGHT', 'ALL_CATEGORIES_INSIGHT'),
            ('TV_MOMENTUM', 'ALL_CATEGORIES_MOMENTUM'),
            ('TV_PULSE', 'ALL_CATEGORIES_PULSE'),
        ]
        merged_tv = load_tv_for_tickers(list_tickers, tv_features)

    merged_tv = merged_tv[~merged_tv.index.get_level_values('ticker').isnull()]
    data_id_tv = sdh.set_raw_data(merged_tv, data_source='FACTSET', source='TrueValue')
    return data_id_tv


