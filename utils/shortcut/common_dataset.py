import pandas as pd
from pathlib import Path
from utils.shortcut.utils import index_to_upper
from asr_protected.utils.myfunctools import compose

f_add = lambda sdh: compose(sdh.set_raw_data, index_to_upper, pd.read_parquet)

def register_market(sdh, data_dir: Path) -> int:
    data_id = f_add(sdh)(
        Path(data_dir) / "market_on_mongo.parquet")
    sdh.set_alias({data_id: 'market'})
    return data_id


def register_fundamental(sdh, data_dir: Path) -> int:
    data_id = f_add(sdh)(
        Path(data_dir) / "fundamental_on_mongo.parquet")
    sdh.set_alias({data_id: 'sales'})
    return data_id
