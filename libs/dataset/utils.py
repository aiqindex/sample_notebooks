import pandas as pd


# For formatting data taken from Snowflake database
def format_pos(df):
    df['DATETIME'] = pd.to_datetime(df['DATETIME'])
    df['RELEASE_TIMESTAMP'] = pd.to_datetime(df['RELEASE_TIMESTAMP'])
    df.set_index(['TICKER', 'DATETIME', 'VARIABLE', 'SMOOTH', 'RELEASE_TIMESTAMP'], inplace=True)
    df = df.xs(0, level='SMOOTH').drop(['BACKFILL'], axis=1)
    df = df.sort_index()
    df.reset_index('RELEASE_TIMESTAMP', drop=False, inplace=True)
    df = df.loc[~df.index.duplicated(keep='last')]
    df.drop(['RELEASE_TIMESTAMP'], axis=1, inplace=True)
    return df.unstack('VARIABLE')['VALUE'].sort_index()
