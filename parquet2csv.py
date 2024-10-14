import pandas as pd
from collections import Counter

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)     # Show all rows
pd.set_option('display.max_colwidth', None) # Prevent truncating column contents

filename = 'data/ot_files/baselineExpression'
df = pd.read_parquet(filename)
print(df.shape)
print(df.head(1).to_csv(index=False))
# ENSG00000020219