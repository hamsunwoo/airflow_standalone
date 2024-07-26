import pandas as pd
import subprocess
import sys

READ = sys.argv[1]
SAVE = sys.argv[2]

df = pd.read_csv(READ,on_bad_lines='skip',names=['dt', 'cmd', 'cnt'], encoding = "latin")

df['dt'] = df['dt'].str.replace('^', '')
df['cmd'] = df['cmd'].str.replace('^', '')
df['cnt'] = df['cnt'].str.replace('^', '')

#'coerce'는 변환할 수 없는 데이터를 만나면 그 값을 강제로 NaN으로 바꾼다. 
df['cnt'] = pd.to_numeric(df['cnt'],errors='coerce')
#NaN값을 원하는 방식으로 처리한다.(예: 0으로 채우기) 
df['cnt'] = df['cnt'].fillna(0).astype(int)

df.to_parquet(f'{SAVE}', partition_cols=['dt'])
