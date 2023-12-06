import numpy as np
import pandas as pd

from src.prod.system.database import engine_sync

tn_ved = pd.read_sql_table('tn_ved', con=engine_sync)
tn_ved['gr'] = tn_ved['code'].str[:2]
tn_ved = tn_ved[['gr', 'code', ]]

tn_ved = tn_ved.groupby('gr')['code'].agg(list).reset_index(name='code')
tn_ved = tn_ved.astype(str)
tn_ved['code'] = tn_ved['code'].str.replace(' ', '')
tn_ved['code'] = tn_ved['code'].str.replace('[', '')
tn_ved['code'] = tn_ved['code'].str.replace(']', '')
tn_ved['code'] = tn_ved['code'].str.replace("'", '')

# tn_ved['ostat'] = tn_ved['gr']
# tn_ved = tn_ved[['code', 'ostat']]
# tn_ved['code'] = tn_ved['code'].map(str)
# tn_ved = tn_ved.groupby('ostat').code.agg([('count', 'count'), ('code', ','.join)])
# tn_ved = tn_ved[['code']]
# tn_ved.reset_index(drop=True, inplace=True)
