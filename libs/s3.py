import boto3
import pandas as pd
from io import BytesIO


from asr_protected.data_accessor.s3_accessor import read_s3_file
from asr_common.strage.output import output_df_to_s3


s3 = boto3.client('s3')

def read_s3(bucket, filename):
    return read_s3_file(s3, bucket, filename)


def to_s3(df, bucket, filename):
    output_df_to_s3(df, bucket=bucket, filename=filename, pkeys=['TICKER', 'DATETIME'])


DEFAULT_BUCKET = 'aiq-trial-data'
DEFAULT_DIR = ''