import os
import numpy as np
import pandas as pd
from pathlib import Path
from typing import List, Optional, Callable

from aiq_strategy_robot.data.data_accessor import DAL
from aiq_strategy_robot.data.ALTERNATIVE import load_alternative_aiq_pos_csmr_goods_universe
from aiq_strategy_robot.data.ALTERNATIVE import load_alternative_aiq_pos_csmr_goods_data
from aiq_strategy_robot.data.FACTSET import load_factset_symbol_lookup

from aiq_strategy_robot.data.FINNHUB import load_finnhub_symbol_lookup, load_finnhub_equity_data, load_finnhub_fundamental
from aiq_strategy_robot.data.ALTERNATIVE import load_alternative_aiq_pos_csmr_goods_data, load_alternative_aiq_pos_csmr_goods_universe
from .path import DEFAULT_DIR




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
def register_alt_data(
        sdh,
        data_dir=DEFAULT_DIR,
        f_ticker_cvt: Optional[Callable[[str], str]] = None
        ) -> int:
    # loading from csv to save time for this demo
    df_pos = pd.read_parquet(os.path.join(data_dir, 'aiq_pos_csmr_goods_sample_index.parquet'), engine='pyarrow')

    if f_ticker_cvt is not None:
        df_pos.index = pd.MultiIndex.from_tuples(
            [(f_ticker_cvt(i[0]), i[1]) for i in df_pos.index],
            names=df_pos.index.names)

    df_pos.index.names = ['TICKER', 'DATETIME']
    return sdh.set_raw_data(
        data_source='external',
        dfraw=df_pos,
        source='sample'
    )

# Load Fundamental Data
def register_fundamental_data(sdh, data_dir=DEFAULT_DIR) -> int:
    df_fundamental = pd.read_parquet(os.path.join(data_dir, 'aiq_pos_csmr_goods_fundamental.parquet'), engine='pyarrow')

    df_fundamental.index.names = ['TICKER', 'DATETIME']
    return sdh.set_raw_data(
        data_source='external',
        dfraw=df_fundamental,
        source='sample'
    )

# Load market data
def register_market_data(sdh, data_dir=DEFAULT_DIR) -> int:
    dfmkt = pd.read_parquet(os.path.join(data_dir, 'aiq_pos_csmr_goods_mkt.parquet'), engine='pyarrow')

    dfmkt.index.names = ['TICKER', 'DATETIME']
    return sdh.set_raw_data(
        data_source='external',
        dfraw=dfmkt,
        source='sample'
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


def register_elec_data(
        sdh,
        data_dir: str = DEFAULT_DIR
        ) -> int:
    path_to_csv = Path(data_dir) / 'pos_elec_goods_stack.csv'
    dfpos = pd.read_csv(
        path_to_csv, dtype={'ticker': str, 'SMOOTH': int},
        parse_dates=['datetime', 'release_timestamp'])
    dfpos = __filter_to_latest_releases(dfpos)
    dfpos.index = pd.MultiIndex.from_tuples(
        [(f"{t}-JP", dt) for t, dt in dfpos.index], names=dfpos.index.names)
    data_id = sdh.set_raw_data(dfpos)
    return data_id


def load_finnhub_equity_data_fixed_ticker(
            sdh,
            target_stock_ticker: Optional[List[str]] = None,
            freq: Optional[str] = None,
            start_datetime: Optional[str] = None,
            end_datetime: Optional[str] = None,
            data_dir: Optional[str] = None,
        ):
    if data_dir:
        df = pd.read_parquet(
            Path(data_dir) / "market_on_mongo.parquet")
        data_id = sdh.set_raw_data(df)
        return data_id

    df = load_finnhub_equity_data(
        sdh,
        symbols=target_stock_ticker,
        freq=freq,
        start_datetime=start_datetime,
        end_datetime=end_datetime
    ).retrieve()
    data_id = int(
        sdh.extract_definition.index.get_level_values("data_id")[-1])
    df = sdh.get_raw_data(data_id)
    df.index = pd.MultiIndex\
        .from_tuples([
            (t[0].replace(" ", "-"), t[1])for t in df.index],
            names=["ticker", "datetime"])

    data_id = sdh.set_raw_data(df)

    return data_id


def load_data_fields(sdh, data_dir=DEFAULT_DIR):
    def conv(t):
        return t + '-JP'

    #  Load Alternative Data
    data_id_alt = register_alt_data(sdh, data_dir=data_dir, f_ticker_cvt=conv)

    #  Load Fundamental Data
    data_id_funda = register_fundamental_data(sdh, data_dir=data_dir)

    # Load market data
    data_id_mkt = register_market_data(sdh, data_dir=data_dir)


    # Set Alias (Optional)
    sdh.set_alias({
        data_id_alt: 'aiq_pos_csmr_goods',
        data_id_funda: 'sales',
        data_id_mkt: 'market'
    })



#########################################################################
# データローダーによるデータ取得 
# markt        = finnhub
# fundamental  = finnhub
# alternative  = aiQ csmr Googds
#########################################################################

def get_finnhub_symbol(exchange_code='T'):
    dffinn_symbol = load_finnhub_symbol_lookup(
        DAL(),
        exchange_code=exchange_code
    ).retrieve()
    dffinn_symbol.head()
    return dffinn_symbol

def get_pos_symbol(finn_sym):
    dfuniverse = load_alternative_aiq_pos_csmr_goods_universe(
        DAL(), 
        ticker=finn_sym.symbol.str.split('.').str[0].to_list()
    ).retrieve()
    dfuniverse.head()
    return dfuniverse


DEFAULT_SAMPLE = "2281 JP"

def merget_symbols(fin_sym, pos_sym, limit=10):
    pos_sym.columns = ['pos_figi', 'pos_ticker']
    pos_sym['finn_symbol'] = pos_sym.pos_ticker + '.T'
    fin_sym = fin_sym[['symbol', 'ticker']]
    fin_sym.columns = ['finn_symbol', 'finn_ticker']
    dfuniverse = pd.merge(pos_sym, fin_sym, on=['finn_symbol'])
    sample = dfuniverse.loc[dfuniverse.finn_ticker == DEFAULT_SAMPLE]
    dfuniverse = pd.concat([sample, dfuniverse.loc[
        ~dfuniverse.finn_ticker.isin([DEFAULT_SAMPLE])].iloc[:limit-1]])
    return dfuniverse

def load_finnhub_prices(sdh, dfuniverse, start_date, end_date):
    load_finnhub_equity_data(
        sdh,
        symbols=dfuniverse.finn_symbol.to_list(),
        freq='D',
        start_datetime=start_date, 
        end_datetime=end_date
    )

def load_finhub_funda(sdh, dfuniverse, start_date, end_date):
    load_finnhub_fundamental(
        sdh,
        symbols=dfuniverse.finn_symbol.to_list(),
        st_type='ic', 
        freq='quarterly', 
        start_datetime=start_date, 
        end_datetime=end_date,
        with_calendar=True
    ).retrieve()


def load_pos_data(sdh, dfuniverse):
    dfpos = load_alternative_aiq_pos_csmr_goods_data(
        DAL(),
        generation=2,
        ticker=dfuniverse.pos_ticker.to_list(),
        load_only_raw=True,
        load_only_latest=True
    ).retrieve()

    dfpos = dfpos.merge(dfuniverse[['pos_ticker', 'finn_ticker']], left_on='TICKER', right_on='pos_ticker', how='inner'
                         ).drop(['pos_ticker', 'TICKER'], axis=1)
    dfpos = dfpos.rename(columns={'finn_ticker': 'ticker', 'DATETIME': 'datetime'})
    dfpos['datetime'] = pd.to_datetime(dfpos['datetime'])
    dfpos = dfpos.set_index(['ticker', 'datetime'])
    dfpos = dfpos.pivot(columns='VARIABLE', values='VALUE')
    dfpos.columns.name = ''

    sdh.set_raw_data(dfraw=dfpos, data_source='aiq_pos_csmr_goods')



def load_acquirer_data(sdh, start_date, end_date, exchange_code='T', limit=10):

    finn_sym = get_finnhub_symbol(exchange_code)
    pos_sym = get_pos_symbol(finn_sym)

    dfuniverse = merget_symbols(finn_sym, pos_sym, limit=limit)

    load_pos_data(sdh, dfuniverse)
    load_finhub_funda(sdh, dfuniverse, start_date, end_date)
    load_finnhub_prices(sdh, dfuniverse, start_date, end_date)

    # 使用するデータIDを取得
    data_id_alt = 1
    data_id_funda = 2
    data_id_mkt = 3
    
    # Set Alias (Optional)
    sdh.set_alias({
        data_id_alt: 'aiq_pos_csmr_goods',
        data_id_funda: 'revenue',
        data_id_mkt: 'market'
    })



def load_sample_dataset(
        sdh,
        *,
        exchange_code: str = 'T',
        limit: int = 20,
        start_date='2015-09-30',
        end_date='2024-06-01',
        data_dir: str =None,
        ):
    """ サンプルデータのロード

    Parameters
    ----------
    sdh : StdDataHandler
        データハンドラー
    exchange_code : str, optional
        exchange code of finnhub. data_dir=Noneの場合に有効, by default 'T'
    limit : int, optional
        データ取得件数. data_dir=Noneの場合に有効, by default 20
    data_dir : str, optional
        データパス. 指定の場合はデータファイルからの読み込み, by default None

    start_data: str, optional
        データ開始日. data_dir=Noneの場合に有効, by default '2015-09-30'
    end_date: str, optional
        データ終了日. data_dir=Noneの場合に有効, by default '2015-09-30'
    """
    if data_dir:
        return load_data_fields(sdh, data_dir=data_dir)
    else:
        return load_acquirer_data(sdh, start_date, end_date, exchange_code, limit=limit)
