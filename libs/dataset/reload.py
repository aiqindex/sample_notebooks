from functools import reduce, partial

from . import aiq_pos_csmr_goods as csmr_goods
from . import aiq_pos_elec_goods as elec_goods
from . import aiq_pos_retailer as retailer


def get_alt_tickers():
    f_alt_l = [
        csmr_goods.read_file,
        elec_goods.read_file,
        retailer.read_file,
    ]
    alt_result_g = map(lambda f: set(f().index.get_level_values('TICKER').unique()), f_alt_l)
    all_ticker = reduce(
        lambda a, b: a | b,
        alt_result_g
    )
    return sorted(all_ticker)

