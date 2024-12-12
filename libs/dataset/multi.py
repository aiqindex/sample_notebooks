import os
import numpy as np
import pandas as pd
from ..path import DEFAULT_DIR


def replace_ns_datetime(df):
    df = df.reset_index()
    df['DATETIME'] = pd.to_datetime(df['DATETIME']).astype('datetime64[ns]')
    df = df.set_index(['TICKER', 'DATETIME'])
    return df

def register_pos_data(sdh, data_dir=DEFAULT_DIR):
    # Using existing data for reducing the amount of time for loading.
    pos_df0 = pd.read_parquet(os.path.join(data_dir, 'aiq_pos_csmr_goods_sample_index_shift.parquet'), engine='pyarrow')
    pos_df0 = pos_df0[~pos_df0.index.get_level_values('ticker').isnull()]
    pos_df0.index.names = ['TICKER', 'DATETIME']
    pos_df0 = replace_ns_datetime(pos_df0)
    data_id_pos = sdh.set_raw_data(pos_df0, data_source='ALTERNATIVE', source='aiq_pos_csmr_goods')
    return data_id_pos
    
def register_market_prices(sdh, data_dir=DEFAULT_DIR):
    # again we load the existing data for reducing the demo duration.
    prices_df = pd.read_parquet(os.path.join(data_dir, 'aiq_pos_csmr_goods_mkt_long.parquet'), engine='pyarrow')
    prices_df = prices_df[~prices_df.index.get_level_values('ticker').isnull()]
    prices_df.index.names = ['TICKER', 'DATETIME']
    prices_df = replace_ns_datetime(prices_df)
    data_id_price = sdh.set_raw_data(prices_df, data_source='FACTSET', source='gpd_prices')
    return data_id_price
    

def register_tv(sdh, data_dir=DEFAULT_DIR):
    merged_tv = pd.read_parquet(os.path.join(data_dir, 'aiq_pos_csmr_goods_tv.parquet'), engine='pyarrow')

    merged_tv = merged_tv[~merged_tv.index.get_level_values('ticker').isnull()]
    merged_tv.index.names = ['TICKER', 'DATETIME']
    merged_tv = replace_ns_datetime(merged_tv)
    data_id_tv = sdh.set_raw_data(merged_tv, data_source='FACTSET', source='TrueValue')
    return data_id_tv

def register_quants_factors(sdh, list_tickers=None, use_dump=True, data_dir=DEFAULT_DIR):
    factors266 = pd.read_parquet(os.path.join(data_dir, 'aiq_pos_csmr_goods_factors.parquet'), engine='pyarrow') 

    factors266 = factors266[~factors266.index.get_level_values('ticker').isnull()]
    factors266.index.names = ['TICKER', 'DATETIME']
    factors266 = replace_ns_datetime(factors266)
    data_id_f266 = sdh.set_raw_data(factors266, data_source='FACTSET', source='Quants factors')
    return data_id_f266
    



def make_baseline(sdh, data_id_price, data_id_f266, data_id_tv, data_id_pos, X_shift=2, resample_term = 'W-FRI'):
    # baselines = []
    baselines = {}
    
    # FTech
    closed_resampled = sdh.transform.resample(data_id=data_id_price, fields='close', rule=resample_term, func='last', label='left', closed='left').dropna().variable_ids
    FTech_Ret001_id = sdh.transform.log_diff(fields=closed_resampled, periods=1
                                            ).mul_val(value=100).shift(periods=X_shift).variable_ids # 1週リターン
    FTech_Ret004_id = sdh.transform.log_diff(fields=closed_resampled, periods=4
                                            ).mul_val(value=100).shift(periods=X_shift).variable_ids # 4週リターン
    FTech_Volatility052_id = sdh.transform.log_diff(fields=closed_resampled, periods=1
                     ).volatility(periods=52).mul_val(value=100).shift(periods=X_shift).variable_ids # 52週ボラティリティ
    
    # baselines.extend(FTech_Ret001_id+FTech_Ret004_id+FTech_Volatility052_id)

    baselines['FTech'] = FTech_Ret001_id+FTech_Ret004_id+FTech_Volatility052_id


    # FFactor
    FFactor_Marketcap_id, *_ = sdh.transform.resample(data_id=data_id_f266, fields=['mktVal'], rule=resample_term, func='last', label='left', closed='left'
                                                     ).log().shift(periods=X_shift).variable_ids
    FFactor_EY, FFactor_BP = sdh.transform.resample(data_id=data_id_f266, fields=['ey', 'bp'], rule=resample_term, func='last', label='left', closed='left'
                                                   ).shift(periods=X_shift).variable_ids
    
    # baselines.extend([FFactor_Marketcap_id, FFactor_EY, FFactor_BP])

    baselines['FFactor'] = [FFactor_Marketcap_id, FFactor_EY, FFactor_BP]

    # FTV
    FTV_IDS = sdh.transform.resample(data_id=data_id_tv, rule=resample_term, func='last', label='left', closed='left').shift(periods=X_shift).variable_ids    
    # baselines.extend(FTV_IDS)
    baselines['FTV'] = FTV_IDS

    # FPos
    sdh.transform.move_biz_day(
        data_id=data_id_pos, fields=['pos_sales', 'share'], bday='Fri', direction='prev').resample(
            rule=resample_term, func='last', label='left', closed='left').sma(periods=12).diff(periods=52).shift(periods=X_shift).variable_ids
    # baselines.extend(sdh.transform.variable_ids)
    baselines['FPos'] = sdh.transform.variable_ids

    return baselines