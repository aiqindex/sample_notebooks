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
def register_alt_data(sdh, list_figis=None, factset_symbols: pd.DataFrame=None, use_dump=True) -> int:
    # loading from csv to save time for this demo
    if use_dump:
        df_pos = pd.read_parquet('./data/aiq_pos_csmr_goods_sample_index.parquet')
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
        dfraw=df_pos
    )

# Load Fundamental Data
def register_fundamental_data(sdh, factset_symbols: pd.DataFrame=None, use_dump=True) -> int:
    if use_dump:
        df_fundamental = pd.read_parquet('./data/aiq_pos_csmr_goods_fundamental.parquet', engine='pyarrow')
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
        dfraw=df_fundamental
    )

# Load market data
def register_market_data(sdh, factset_symbols: pd.DataFrame=None, use_dump=True) -> int:
    if use_dump:
        dfmkt = pd.read_parquet('./data/aiq_pos_csmr_goods_mkt.parquet', engine='pyarrow')
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
        dfraw=dfmkt
    )


def __filter_to_latest_releases(
    df: pd.DataFrame
) -> pd.DataFrame:
    df.set_index(
        ['ticker', 'datetime', 'variable', 'SMOOTH', 'release_timestamp'],
        inplace=True)
    df = df.xs(0, level='SMOOTH').drop(['backfill'], axis=1)
    df = df.sort_index()
    df.reset_index('release_timestamp', drop=False, inplace=True)
    df = df.loc[~df.index.duplicated(keep='last')]
    df.drop(['release_timestamp'], axis=1, inplace=True)
    df = df.unstack('variable')['values']
    return df


def register_elec_data(sdh) -> int:
    path_to_csv = './data/20240312_pos_elec_goods_stack.csv'
    dfpos = pd.read_csv(
        path_to_csv, dtype={'ticker': str, 'SMOOTH': int},
        parse_dates=['datetime', 'release_timestamp'])
    dfpos = __filter_to_latest_releases(dfpos)
    dfpos.index = pd.MultiIndex.from_tuples(
        [(f"{t}-JP", dt) for t, dt in dfpos.index], names=dfpos.index.names)
    data_id = sdh.set_raw_data(dfpos)
    return data_id


if __name__ == "__main__":
    from aiq_strategy_robot.data.data_accessor import DAL
    sdh = DAL()
    data_id_alt = register_elec_data(sdh)
