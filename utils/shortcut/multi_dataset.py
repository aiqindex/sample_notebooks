import numpy as np
import pandas as pd
from aiq_strategy_robot.data.data_accessor import DAL
from asr_common.asr_calendar import shift_dt_index_to_specific_day

sdh = DAL()

# get the list of symbols
def load_symbols() -> pd.DataFrame:
    sdh = DAL()
    sdh = sdh.load(
        'ALTERNATIVE',
        data_type='aiq_pos_csmr_goods',
        meta_data='universe'
    )
    dfsyms = sdh.retrieve()
    dfsyms = dfsyms.rename({'TICKER': 'seccode'}, axis=1)
    list_figis = dfsyms['FIGI'].unique().tolist()

    sdh = sdh.load(
        'Factset',
        data_type='symbol_lookup',
        figi=list_figis,
        pick_only_primary=True
    )
    dfout = sdh.retrieve(pick_cols=['FIGI', 'TICKER', 'FSYM_ID', 'FSYM_SECURITY_ID']).sort_values('TICKER', ascending=True)
    dfout = dfout.drop_duplicates(keep='last')
    dfout = dfout.merge(dfsyms, on='FIGI', how='left')
    return dfout


# Load POS Data 
def load_pos_from_db(list_figis) -> pd.DataFrame:
    sdh = sdh.load(
        'ALTERNATIVE',
        data_type='aiq_pos_csmr_goods',
        meta_data='data',
        generation=2,
        figi=list_figis,
        load_only_raw=True,
        load_only_latest=True
    )
    df_pos = sdh.retrieve()
    df_pos = df_pos.merge(dfsyms2[['seccode', 'TICKER']], left_on='ticker', right_on='seccode', how='inner').drop(['seccode', 'ticker'], axis=1)
    df_pos = df_pos.rename(columns={'TICKER': 'ticker', 'DATETIME': 'datetime'})
    df_pos = df_pos.set_index(['ticker', 'datetime'])

    # shift pos datetime to the next friday.
    custom_mask = 'Mon Tue Wed Thu Fri'
    spec_day = 3
    new_dt_index = shift_dt_index_to_specific_day(pos_df0.index.get_level_values('datetime'), custom_mask, spec_day)
    pos_df0.index = pd.MultiIndex.from_arrays([pos_df0.index.get_level_values('ticker'), new_dt_index], names=['ticker', 'datetime'])
    return pos_df0
    
# Price Data (origin: FactSet HTTP API)
def load_market_prices(list_tickers, start_datetime) -> pd.DataFrame:
    dfmkt = sdh.load(
        'FACTSET',
        data_type='gpd_prices',
        ids=list_tickers,
        start_date=start_datetime,
        adjust='SPLIT',
        fields=['price', 'volume', 'priceOpen', 'priceHigh', 'priceLow', 'currency']
    ).retrieve()
    dfmkt = dfmkt.reset_index().rename(columns={'DATETIME': 'datetime'})
    dfmkt['datetime'] = pd.to_datetime(dfmkt['datetime'])
    dfmkt = dfmkt.set_index(['ticker', 'datetime'])[['close', 'volume', 'open', 'high', 'low', 'currency']]
    return dfmkt

# TruValue data (origin: FactSet Snowflake)
def load_tv_for_tickers(list_tickers, tv_features):
    tv_tables = dict()
    for name in tqdm(['TV_ESG_RANKS', 'TV_INSIGHT', 'TV_MOMENTUM', 'TV_PULSE', 'TV_VOLUME', 'TV_VOLUME_PCTL']):
        tv_table = pd.read_csv(f'./data/POS266_{name}.csv')
        tv_table = tv_table.dropna(subset=['TV_DATE']).drop_duplicates()
        tv_table['datetime'] = pd.to_datetime(tv_table['TV_DATE'])
        tv_table['ticker'] = tv_table['ticker'].astype(str)
        tv_table = tv_table.set_index(['ticker', 'datetime']).sort_index()
        tv_table.drop(columns=['TV_DATE', 'TV_ORG_ID', 'TV_INSTRUMENT_ID'], inplace=True)
        # TV data are delivered on T+1 day
        tv_table = tv_table.groupby('ticker').shift(1)
        tv_tables[name] = tv_table

    merged_tv = None
    for (table, column) in tqdm(tv_features):
        if merged_tv is None:
            merged_tv = tv_tables[table][column].to_frame()
        else:
            merged_tv = merged_tv.merge(tv_tables[table][column].to_frame(), left_index=True, right_index=True, how='outer')
    merged_tv = merged_tv.groupby(level=['ticker', 'datetime']).mean()
    return merged_tv


def register_pos_data(sdh, use_dump=True):
    # Using existing data for reducing the amount of time for loading.
    if use_dump:
        pos_df0 = pd.read_parquet('/efs/share/factset/pattaya/sample/jupyter/aiq_pos_csmr_goods_sample_index_shift.parquet', engine='pyarrow')
    else:
        pos_df0 = load_pos_from_db(list_figis)
    pos_df0 = pos_df0[~pos_df0.index.get_level_values('ticker').isnull()]
    data_id_pos = sdh.set_raw_data(pos_df0, data_source='ALTERNATIVE', source='aiq_pos_csmr_goods')
    return data_id_pos
    
def register_market_prices(sdh, list_tickers=None, use_dump=True):
    # again we load the existing data for reducing the demo duration.
    if use_dump:
        prices_df = pd.read_parquet('/efs/share/factset/pattaya/sample/jupyter/aiq_pos_csmr_goods_mkt_long.parquet', engine='pyarrow')
    else:
        prices_df = load_market_prices(list_tickers, '2016-01-01')
        
    prices_df = prices_df[~prices_df.index.get_level_values('ticker').isnull()]
    data_id_price = sdh.set_raw_data(prices_df, data_source='FACTSET', source='gpd_prices')
    return data_id_price
    

def register_tv(sdh, list_tickers=None, use_dump=True):
    if use_dump:
        merged_tv = pd.read_parquet('/efs/share/factset/pattaya/sample/jupyter/aiq_pos_csmr_goods_tv.parquet', engine='pyarrow')
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

def register_quants_factors(sdh, list_tickers=None, use_dump=True):
    if use_dump:
        factors266 = pd.read_parquet('/efs/share/factset/pattaya/sample/jupyter/aiq_pos_csmr_goods_factors.parquet', engine='pyarrow') 
    else:
        factors266 = load_quants_factors(list_tickers, '2016-01-01')

    factors266 = factors266[~factors266.index.get_level_values('ticker').isnull()]
    data_id_f266 = sdh.set_raw_data(factors266, data_source='FACTSET', source='Quants factors')
    return data_id_f266
    



def make_baseline(sdh, data_id_price, data_id_f266, data_id_tv, data_id_pos, X_shift=2, resample_term = 'W-FRI'):
    # baselines = []
    baselines = {}
    
    # FTech
    closed_resampled = sdh.transform.resample(data_id=data_id_price, fields='close', rule=resample_term, func='last', label='left', closed='left').dropna().variable_ids
    FTech_Ret001_id = sdh.transform.log_diff(fields=closed_resampled, periods=1
                                            ).multiply_by_scalar(value=100).shift(periods=X_shift).variable_ids # 1週リターン
    FTech_Ret004_id = sdh.transform.log_diff(fields=closed_resampled, periods=4
                                            ).multiply_by_scalar(value=100).shift(periods=X_shift).variable_ids # 4週リターン
    FTech_Volatility052_id = sdh.transform.log_diff(fields=closed_resampled, periods=1
                     ).volatility(periods=52).multiply_by_scalar(value=100).shift(periods=X_shift).variable_ids # 52週ボラティリティ
    
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
    FTV_MATERIALITY_ADJ_INSIGHT, FTV_MATERIALITY_IND_PCTL, FTV_ALL_CATEGORIES_ADJ_INSIGHT, \
            FTV_ALL_CATEGORIES_INSIGHT, FTV_ALL_CATEGORIES_MOMENTUM, FTV_ALL_CATEGORIES_PULSE = FTV_IDS
    
    # baselines.extend(FTV_IDS)
    baselines['FTV'] = FTV_IDS

    # FPos
    FPos_sales, FPos_share = sdh.transform.move_biz_day(
        data_id=data_id_pos, fields=['pos_sales', 'share'], bday='Fri', direction='prev').resample(
            rule=resample_term, func='last', label='left', closed='left').sma(periods=12).diff(periods=52).shift(periods=X_shift).variable_ids
    # baselines.extend(sdh.transform.variable_ids)
    baselines['FPos'] = sdh.transform.variable_ids

    return baselines
