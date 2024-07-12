import pandas as pd


# TODO: Remove duplicated function on utils/shortcut/elec_dataset.py.
def index_to_upper(df: pd.DataFrame) -> pd.DataFrame:
    df.index.names = [s.upper() for s in df.index.names]
    return df
