import pandas as pd
from pathlib import Path
from typing import List, Optional

from aiq_strategy_robot.data.FINNHUB import load_finnhub_equity_data
from .path import DEFAULT_DIR
from utils.shortcut import goods_dataset as sc_goods


def index_to_upper(df: pd.DataFrame) -> None:
    df.index.names = [s.upper() for s in df.index.names]


def register_elec_data(
        sdh,
        data_dir: str = DEFAULT_DIR
        ) -> int:
    path_to_csv = Path(data_dir) / 'pos_elec_goods_stack.csv'
    dfpos = pd.read_csv(
        path_to_csv, dtype={'ticker': str, 'SMOOTH': int},
        parse_dates=['datetime', 'release_timestamp'])
    dfpos = __filter_to_latest_releases(dfpos)
    index_to_upper(dfpos)
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
        index_to_upper(df)
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
    index_to_upper(df)

    data_id = sdh.set_raw_data(df)

    return data_id

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


def load_sample_dataset(sdh) -> None:
    #  Load Alternative Data
    data_id_alt = register_elec_data(sdh, data_dir=DEFAULT_DIR)

    # Load market data
    data_id_mkt = load_finnhub_equity_data_fixed_ticker(
        sdh, data_dir=DEFAULT_DIR)

    #  Load Fundamental Data
    data_id_funda = sc_goods.register_fundamental_data(sdh)

    # Set Alias (Optional)
    sdh.set_alias({
        data_id_alt: 'aiq_pos_elec',
        data_id_mkt: 'market',
        data_id_funda: 'sales',
    })

    return None