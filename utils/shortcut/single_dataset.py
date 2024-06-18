import numpy as np
import pandas as pd
from typing import List, Optional

from aiq_strategy_robot.data.data_accessor import DAL
from aiq_strategy_robot.data.ALTERNATIVE import load_alternative_aiq_pos_csmr_goods_universe
from aiq_strategy_robot.data.ALTERNATIVE import load_alternative_aiq_pos_csmr_goods_data
from aiq_strategy_robot.data.FACTSET import load_factset_symbol_lookup

from aiq_strategy_robot.data.FINNHUB import load_finnhub_equity_data
from pathlib import Path

DEFAULT_DIR = "./data"

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
def register_market_data(
        sdh,
        factset_symbols: pd.DataFrame = None,
        use_dump: bool = True,
        target_tickers: Optional[List[str]] = None
        ) -> int:
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

    if target_tickers:
        dfmkt = dfmkt.loc[target_tickers]

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
            Path(data_dir) / "finnhub_selected_ticker.parquet")
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


if __name__ == "__main__":
    from aiq_strategy_robot.data.data_accessor import DAL
    sdh = DAL()
    target_tickers = ['2180-JP', '2321-JP', '2323-JP', '2326-JP', '2354-JP', '2388-JP', '2469-JP', '2667-JP', '2706-JP', '2743-JP', '2768-JP', '3020-JP', '3076-JP', '3086-JP', '3140-JP', '3209-JP', '3443-JP', '3635-JP', '3663-JP', '3710-JP', '3715-JP', '3723-JP', '3760-JP', '3765-JP', '3773-JP', '3774-JP', '3777-JP', '3826-JP', '3828-JP', '3843-JP', '3861-JP', '3863-JP', '3864-JP', '3877-JP', '3880-JP', '4188-JP', '4218-JP', '4295-JP', '4312-JP', '4344-JP', '4611-JP', '4631-JP', '4644-JP', '4674-JP', '4686-JP', '4689-JP', '4704-JP', '4722-JP', '4726-JP', '4768-JP', '4776-JP', '4783-JP', '4901-JP', '4902-JP', '4976-JP', '5344-JP', '5801-JP', '5802-JP', '5819-JP', '5856-JP', '5906-JP', '5933-JP', '5940-JP', '5943-JP', '5947-JP', '5992-JP', '6055-JP', '6058-JP', '6436-JP', '6448-JP', '6460-JP', '6479-JP', '6501-JP', '6502-JP', '6503-JP', '6504-JP', '6507-JP', '6516-JP', '6594-JP', '6628-JP', '6629-JP', '6632-JP', '6639-JP', '6645-JP', '6670-JP', '6675-JP', '6676-JP', '6701-JP', '6702-JP', '6703-JP', '6707-JP', '6724-JP', '6727-JP', '6731-JP', '6736-JP', '6737-JP', '6750-JP', '6752-JP', '6753-JP', '6755-JP', '6756-JP', '6758-JP', '6762-JP', '6768-JP', '6770-JP', '6773-JP', '6778-JP', '6784-JP', '6789-JP', '6798-JP', '6803-JP', '6804-JP', '6814-JP', '6815-JP', '6835-JP', '6836-JP', '6839-JP', '6840-JP', '6841-JP', '6861-JP', '6879-JP', '6889-JP', '6897-JP', '6916-JP', '6923-JP', '6924-JP', '6925-JP', '6927-JP', '6929-JP', '6930-JP', '6932-JP', '6942-JP', '6946-JP', '6952-JP', '6955-JP', '6971-JP', '7203-JP', '7228-JP', '7259-JP', '7267-JP', '7272-JP', '7297-JP', '7483-JP', '7485-JP', '7501-JP', '7552-JP', '7590-JP', '7595-JP', '7608-JP', '7718-JP', '7725-JP', '7731-JP', '7733-JP', '7741-JP', '7745-JP', '7751-JP', '7752-JP', '7818-JP', '7832-JP', '7835-JP', '7846-JP', '7862-JP', '7867-JP', '7911-JP', '7912-JP', '7936-JP', '7951-JP', '7952-JP', '7957-JP', '7962-JP', '7970-JP', '7972-JP', '7974-JP', '7975-JP', '7976-JP', '7984-JP', '7987-JP', '7994-JP', '7999-JP', '8001-JP', '8002-JP', '8008-JP', '8020-JP', '8031-JP', '8032-JP', '8050-JP', '8051-JP', '8057-JP', '8060-JP', '8074-JP', '8107-JP', '8130-JP', '8135-JP', '8136-JP', '8144-JP', '8154-JP', '8202-JP', '8219-JP', '9042-JP', '9432-JP', '9433-JP', '9437-JP', '9470-JP', '9477-JP', '9503-JP', '9504-JP', '9600-JP', '9605-JP', '9613-JP', '9629-JP', '9684-JP', '9697-JP', '9746-JP', '9749-JP', '9766-JP', '9830-JP', '9837-JP', '9843-JP', '9880-JP', '9889-JP', '9928-JP']
    # data_id_mkt = register_market_data(sdh, target_tickers=target_tickers)
    # data_id_alt = register_elec_data(sdh)
    id = load_finnhub_equity_data_fixed_ticker(
        sdh, target_tickers,
        freq='D',
        start_datetime='2010-01-01', 
        end_datetime='2024-01-01'
    )
    print(id)
