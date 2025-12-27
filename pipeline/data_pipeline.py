import pandas as pd
import sys

df = pd.DataFrame({
    'num_days': [1, 2, 3], 'num_passengers': [100, 150, 200]
})
month = sys.argv[1]
df['month'] = month

df.to_parquet(f'output_{month}.parquet', index=False)
print(df)
print(f"argument ", sys.argv[1])



