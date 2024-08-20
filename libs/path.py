
import os

DEFAULT_DIR = '/efs/share/factset/pattaya/sample/jupyter/'

efs_path = '/efs'

# aiQ hub上の異なるマウントポジションに対応
if not os.path.exists(efs_path):
    DEFAULT_DIR = '~/share/factset/pattaya/sample/jupyter/'
