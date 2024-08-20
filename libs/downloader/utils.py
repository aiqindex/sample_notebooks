import os
from pathlib import Path 
from typing import Set
from functools import reduce, partial
import pandas as pd
from asr_protected.utils.myfunctools import compose
from datetime import datetime

if __name__ == "__main__":
    import sys
    sys.path.append("./")

from ..path import DEFAULT_DIR
# from utils.shortcut.downloader.fundamental import download_fundamental
# from utils.shortcut.downloader.market import download_market


def download_goods(skip: bool = False) -> Set[str]:
    if skip:
        default_path = Path(DEFAULT_DIR)\
            / "aiq_pos_csmr_goods_sample_index.parquet"
        df = pd.read_parquet(default_path)
        return set(df.index.get_level_values("ticker").map(str))

    return set()


def download_elec(skip: bool = False) -> Set[str]:
    if skip:
        default_path = Path(DEFAULT_DIR)\
            / "pos_elec_goods_stack.csv"
        df = pd.read_csv(default_path)
        return set(df["ticker"].apply(str))
    return set()


def download_retailer(skip: bool = False) -> Set[str]:
    if skip:
        default_path = Path(DEFAULT_DIR)\
            / "pos_retailer_stack.csv"
        df = pd.read_csv(default_path)
        return set(df["ticker"].apply(str))
    return set()


def download_alt(skip: bool = False) -> Set[str]:
    f_alt_l = [
        download_goods,
        download_elec,
        download_retailer
    ]
    alt_result_g = map(lambda f: f(skip=skip), f_alt_l)
    all_ticker = reduce(
        lambda a, b: a | b,
        alt_result_g
    )
    return all_ticker


# def download_all() -> None:
#     date_dir = Path(DEFAULT_DIR)\
#         / f"backup_{datetime.now().strftime('%Y-%m-%d')}"
#     if not date_dir.exists():
#         os.mkdir(date_dir)
#         os.chown(
#                 date_dir,
#                 uid=-1,     # no change
#                 gid=2000    # shared
#             )

#     tickers: Set[str] = download_alt(skip=True)
#     assert all(map(
#         lambda t: len(t) == 4,
#         tickers
#     ))

#     compose(
#         lambda df: df.to_parquet(date_dir / "fundamental_on_mongo.parquet"),
#         download_fundamental,
#         list
#         )(tickers)
#     compose(
#         lambda df: df.to_parquet(date_dir / "market_on_mongo.parquet"),
#         download_market,
#         list
#         )(tickers)


# if __name__ == "__main__":
#     download_all()
