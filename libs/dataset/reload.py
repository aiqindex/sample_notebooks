from .aiq_pos_csmr_goods import reload as csmr_goods
from .aiq_pos_elec_goods import reload as elec_goods
from .aiq_pos_retailer import reload as retailer

from .common import reload_market_to_s3, reload_fundamental_to_s3


def reload_aiq_pos(data_dir, end_date=None, db_name=None):
    dfcsmr = csmr_goods(data_dir, end_date, db_name)
    dfelec = elec_goods(data_dir, end_date, db_name)
    dfretail = retailer(data_dir, end_date, db_name)
    tickers = []
    for df in [dfcsmr, dfelec, dfretail]:

        tickers_ = df.index.get_level_values('TICKER').unique().to_list()
        tickers.extend(tickers_)
    return list(set(tickers))


