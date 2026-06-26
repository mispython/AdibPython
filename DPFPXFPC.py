PROCESSING CONVENTIONAL BANKING FLOAT DATA
==================================================
Columns in fdmthly.sas7bdat: ['state', 'custcode', 'bic', 'lstmatdt', 'branch', 'acctno', 'purpose', 'name', 'openind', 'curbal', 'orgdate', 'matdate', 'rate', 'accttype', 'term', 'intplan', 'renewal', 'intpay', 'intdate', 'lastactv', 'amtind', 'forate']
FDMTHLY available columns: ['state', 'custcode', 'bic', 'lstmatdt', 'branch', 'acctno', 'purpose', 'name', 'openind', 'curbal', 'orgdate', 'matdate', 'rate', 'accttype', 'term', 'intplan', 'renewal', 'intpay', 'intdate', 'lastactv', 'amtind', 'forate']
Columns in curn124.sas7bdat: ['custcd', 'prodcd', 'statecd', 'amtind', 'branch', 'acctno', 'name', 'purpose', 'sector', 'ledgbal', 'lasttran', 'avgamt', 'product', 'race', 'deptype', 'curbal', 'chqfloat', 'odintacc', 'curcode', 'costctr', 'intpaybl', 'openmh', 'closemh', 'accytd', 'forate', 'range', 'avgrnge', 'cabal', 'sabal']
Columns in savg124.sas7bdat: ['custcd', 'prodcd', 'statecd', 'amtind', 'bankno', 'branch', 'acctno', 'name', 'ledgbal', 'lasttran', 'product', 'race', 'deptype', 'curbal', 'chqfloat', 'bdate', 'schind', 'costctr', 'intpaybl', 'openmh', 'closemh', 'accytd', 'range', 'opendate', 'age']
SAVG124 available columns: ['custcd', 'prodcd', 'statecd', 'amtind', 'bankno', 'branch', 'acctno', 'name', 'ledgbal', 'lasttran', 'product', 'race', 'deptype', 'curbal', 'chqfloat', 'bdate', 'schind', 'costctr', 'intpaybl', 'openmh', 'closemh', 'accytd', 'range', 'opendate', 'age']
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBFLOAT_FLOAT.py", line 714, in <module>
    conventional_result = process_conventional_float()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBFLOAT_FLOAT.py", line 202, in process_conventional_float
    deposit_combined = pl.concat(standardized_datasets, how="vertical")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.ShapeError: unable to vstack, column names don't match: "product" and "branch"
