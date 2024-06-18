import os
import numpy as np
import pandas as pd

from aiq_strategy_robot.data.data_accessor import DAL
from aiq_strategy_robot.data.ALTERNATIVE import load_alternative_aiq_pos_csmr_goods_universe
from aiq_strategy_robot.data.ALTERNATIVE import load_alternative_aiq_pos_csmr_goods_data
from aiq_strategy_robot.data.FACTSET import load_factset_symbol_lookup

from aiq_strategy_robot.data.FINNHUB import load_finnhub_symbol_lookup, load_finnhub_equity_data, load_finnhub_fundamental
from aiq_strategy_robot.data.ALTERNATIVE import load_alternative_aiq_pos_csmr_goods_data, load_alternative_aiq_pos_csmr_goods_universe



DEFAULT_DIR = '/efs/share/factset/pattaya/sample/jupyter/'


#########################################################################
# データファイルによるデータ取得 
# markt        = factset
# fundamental  = factset
# alternative  = aiQ csmr Googds
#########################################################################

sdh = DAL()
sdh = load_alternative_aiq_pos_csmr_goods_universe(sdh)
dfsyms = sdh.retrieve()
dfsyms = dfsyms.rename({'TICKER': 'seccode'}, axis=1)


def get_tickers():
    return dfsyms['seccode'].unique().tolist()

def get_figis():
    return dfsyms['FIGI'].unique().tolist()

# Find FACTSET IDs
def get_factset_symbols(sdh, list_figis):
    sdh = load_factset_symbol_lookup(sdh, 
        figi=list_figis,
        pick_only_primary=True
    )
    
    dfsyms2 = sdh.retrieve(pick_cols=['FIGI', 'TICKER', 'FSYM_ID', 'FSYM_SECURITY_ID']).sort_values('TICKER', ascending=True)
    dfsyms2 = dfsyms2.drop_duplicates(keep='last')
    dfsyms2 = dfsyms2.merge(dfsyms, on='FIGI', how='left')
    return dfsyms2

# Load Alternative Data
def register_alt_data(sdh, data_dir=DEFAULT_DIR) -> int:
    # loading from csv to save time for this demo
    df_pos = pd.read_parquet(os.path.join(data_dir, 'aiq_pos_csmr_goods_sample_index.parquet'), engine='pyarrow')
      
    return sdh.set_raw_data(
        data_source='external',
        dfraw=df_pos,
        source='sample'
    )

# Load Fundamental Data
def register_fundamental_data(sdh, data_dir=DEFAULT_DIR) -> int:
    df_fundamental = pd.read_parquet(os.path.join(data_dir, 'aiq_pos_csmr_goods_fundamental.parquet'), engine='pyarrow')
    return sdh.set_raw_data(
        data_source='external',
        dfraw=df_fundamental,
        source='sample'
    )

# Load market data
def register_market_data(sdh, data_dir=DEFAULT_DIR) -> int:
    dfmkt = pd.read_parquet(os.path.join(data_dir, 'aiq_pos_csmr_goods_mkt.parquet'), engine='pyarrow')
    return sdh.set_raw_data(
        data_source='external',
        dfraw=dfmkt,
        source='sample'
    )
