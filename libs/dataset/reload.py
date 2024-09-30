from functools import reduce, partial
import pandas as pd

from . import aiq_pos_csmr_goods as csmr_goods
from . import aiq_pos_elec_goods as elec_goods
from . import aiq_pos_retailer as retailer

def cmr_goods_read_file():
    a, b = csmr_goods.read_file()
    return pd.concat([a, b], axis=0)

def get_alt_tickers():
    f_alt_l = [
        cmr_goods_read_file,
        elec_goods.read_file,
        retailer.read_file,
    ]
    alt_result_g = map(lambda f: set(f()['TICKER'].unique()), f_alt_l)
    all_ticker = reduce(
        lambda a, b: a | b,
        alt_result_g
    )
    return sorted(all_ticker)

