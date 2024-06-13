import os
import numpy as np
import pandas as pd

from aiq_strategy_robot.data.data_accessor import DAL
from aiq_strategy_robot.data.ALTERNATIVE import load_alternative_aiq_pos_csmr_goods_universe
from aiq_strategy_robot.data.ALTERNATIVE import load_alternative_aiq_pos_csmr_goods_data
from aiq_strategy_robot.data.FACTSET import load_factset_symbol_lookup

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
def register_alt_data(sdh, list_figis=None, factset_symbols: pd.DataFrame=None, use_dump=True, data_dir='./data') -> int:
    # loading from csv to save time for this demo
    if use_dump:
        df_pos = pd.read_parquet(os.path.join(data_dir, 'aiq_pos_csmr_goods_sample_index.parquet'))
    else:
        assert factset_symbols is not None, '`factset_symbols` must be set with `list_figis`.'
        sdh = load_alternative_aiq_pos_csmr_goods_data(
            sdh,
            generation=2,
            figi=list_figis,
            load_only_raw=True,
            load_only_latest=True
        )
        df_pos = sdh.retrieve()
        df_pos = df_pos.rename(columns = {'TICKER':'seccode'})
        df_pos = df_pos.merge(factset_symbols[['seccode', 'TICKER']], on='seccode', how='inner').drop(['seccode'], axis=1)
        df_pos = df_pos.rename(columns={'TICKER': 'ticker', 'DATETIME': 'datetime'})
        df_pos['datetime'] = pd.to_datetime(df_pos['datetime'])
        df_pos = df_pos.set_index(['ticker', 'datetime'])
        df_pos = df_pos.pivot(columns='VARIABLE', values='VALUE')
        df_pos.columns.name = ''
      
    return sdh.set_raw_data(
        data_source='external',
        dfraw=df_pos,
        source='sample'
    )

# Load Fundamental Data
def register_fundamental_data(sdh, factset_symbols: pd.DataFrame=None, use_dump=True, data_dir='./data') -> int:
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

# Load market data
def register_market_data(sdh, factset_symbols: pd.DataFrame=None, use_dump=True, data_dir='./data') -> int:
    if use_dump:
        dfmkt = pd.read_parquet(os.path.join(data_dir, 'aiq_pos_csmr_goods_mkt.parquet'), engine='pyarrow')
    else:
        dfmkt = sdh.load(
            'FACTSET',
            data_type='gpd_prices',
            ids=factset_symbols['TICKER'].unique().tolist(),
            start_date=start_datetime,
            adjust='SPLIT',
            fields=['price', 'vwap', 'volume', 'turnover']
        ).retrieve()
        dfmkt = dfmkt.reset_index().rename(columns={'DATETIME': 'datetime'})
        dfmkt['datetime'] = pd.to_datetime(dfmkt['datetime'])
        dfmkt = dfmkt.set_index(['ticker', 'datetime'])[['close']]
        dfmkt.to_parquet('aiq_pos_csmr_goods_mkt.parquet', engine='pyarrow')

    return sdh.set_raw_data(
        data_source='external',
        dfraw=dfmkt,
        source='sample'
    )



def transform_mkt_vs_alt(sdh, data_id_mkt, data_id_alt):

    mkt_W_close_id = sdh.transform.resample(data_id=data_id_mkt, rule='W', fields='close', func='last').variable_ids[-1]
    alt_W_shift_0_ids = sdh.transform.resample(data_id=data_id_alt, rule='W', func='mean').variable_ids

    alt_W_shift_1_ids = sdh.transform.shift(data_id=data_id_alt, periods=1).resample(rule='W', func='mean').sma(periods=12).variable_ids
    alt_W_shift_2_ids = sdh.transform.shift(data_id=data_id_alt, periods=2).resample(rule='W', func='mean').sma(periods=12).variable_ids
    alt_W_shift_3_ids = sdh.transform.shift(data_id=data_id_alt, periods=3).resample(rule='W', func='mean').sma(periods=12).variable_ids
    alt_W_shift_4_ids = sdh.transform.shift(data_id=data_id_alt, periods=4).resample(rule='W', func='mean').sma(periods=12).variable_ids
    alt_W_shift_5_ids = sdh.transform.shift(data_id=data_id_alt, periods=5).resample(rule='W', func='mean').sma(periods=12).variable_ids
    alt_W_shift_6_ids = sdh.transform.shift(data_id=data_id_alt, periods=6).resample(rule='W', func='mean').sma(periods=12).variable_ids
    alt_W_shift_7_ids = sdh.transform.shift(data_id=data_id_alt, periods=7).resample(rule='W', func='mean').sma(periods=12).variable_ids
    alt_W_shift_8_ids = sdh.transform.shift(data_id=data_id_alt, periods=8).resample(rule='W', func='mean').sma(periods=12).variable_ids
    
    alt_W_shift_0_sma12_logdiff_ids = sdh.transform.log_diff(fields=alt_W_shift_0_ids, periods=52).variable_ids
    alt_W_shift_1_sma12_logdiff_ids = sdh.transform.log_diff(fields=alt_W_shift_1_ids, periods=52).variable_ids
    alt_W_shift_2_sma12_logdiff_ids = sdh.transform.log_diff(fields=alt_W_shift_2_ids, periods=52).variable_ids
    alt_W_shift_3_sma12_logdiff_ids = sdh.transform.log_diff(fields=alt_W_shift_3_ids, periods=52).variable_ids
    alt_W_shift_4_sma12_logdiff_ids = sdh.transform.log_diff(fields=alt_W_shift_4_ids, periods=52).variable_ids
    alt_W_shift_5_sma12_logdiff_ids = sdh.transform.log_diff(fields=alt_W_shift_5_ids, periods=52).variable_ids
    alt_W_shift_6_sma12_logdiff_ids = sdh.transform.log_diff(fields=alt_W_shift_6_ids, periods=52).variable_ids
    alt_W_shift_7_sma12_logdiff_ids = sdh.transform.log_diff(fields=alt_W_shift_7_ids, periods=52).variable_ids
    alt_W_shift_8_sma12_logdiff_ids = sdh.transform.log_diff(fields=alt_W_shift_8_ids, periods=52).variable_ids
    
    close_ret = sdh.transform.dropna(fields=mkt_W_close_id, how='all').log_diff(periods=1, names='ret').variable_ids[-1]
    return sdh


def register_and_transform_for_sample(sdh, data_dir='./data'):
    
    #  Load Alternative Data
    data_id_alt = register_alt_data(sdh, data_dir=data_dir)
    
    # #  Load Fundamental Data
    # data_id_funda = sc.register_fundamental_data(sdh, data_dir=data_dir)
    
    # Load market data
    data_id_mkt = register_market_data(sdh, data_dir=data_dir)
    
    # Set Alias (Optional)
    sdh.set_alias({
        data_id_alt: 'aiq_pos_csmr_goods',
        # data_id_funda: 'sales',
        data_id_mkt: 'market'
    })

    transform_mkt_vs_alt(sdh, data_id_mkt, data_id_alt)

    

