import pyreadstat
import pandas as pd
import datacompy

# Load both files
sas_df, _ = pyreadstat.read_sas7bdat("/path/to/production/bt.sas7bdat")
py_df, _  = pyreadstat.read_sas7bdat("/path/to/python/bt.sas7bdat")

# Normalize the key
for df in (sas_df, py_df):
    df['BNMCODE'] = df['BNMCODE'].astype(str).str.strip()

# Build comparison
compare = datacompy.Compare(
    sas_df,
    py_df,
    join_columns='BNMCODE',      # unique key
    abs_tol=0.01,                # treat amounts within 1 cent as equal
    rel_tol=1e-6,
    df1_name='SAS_PROD',
    df2_name='PYTHON',
)

print(compare.report())
