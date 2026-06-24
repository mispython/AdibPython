============================================================
EIFLTEXP PROCESSING STARTED
============================================================
MNI Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/MNI
IMNI Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/IMNI
PIDM Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/PIDM
Output Path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFLTEXP
============================================================

[STEP 1] Loading FDMTHLY data...
  - MNI FDMTHLY columns: ['state', 'custcode', 'bic', 'lstmatdt', 'branch', 'acctno', 'purpose', 'name', 'openind', 'curbal', 'orgdate', 'matdate', 'rate', 'accttype', 'term', 'intplan', 'renewal', 'intpay', 'intdate', 'lastactv', 'amtind', 'forate']
  - MNI FDMTHLY loaded: 2756145 records
  - IMNI FDMTHLY columns: ['lstmatdt', 'branch', 'acctno', 'purpose', 'name', 'openind', 'curbal', 'orgdate', 'matdate', 'rate', 'accttype', 'term', 'intplan', 'renewal', 'intpay', 'intdate', 'lastactv', 'amtind', 'forate', 'state', 'bic', 'custcode']
  - IMNI FDMTHLY loaded: 431257 records
  - Combined FDMTHLY: 3187402 records
  - FDMTHLY saved to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFLTEXP/FDMTHLY.parquet

[STEP 2] Loading CURN data...
  - MNI CURN124 columns: ['custcd', 'prodcd', 'statecd', 'amtind', 'branch', 'acctno', 'name', 'purpose', 'sector', 'ledgbal']
  - MNI CURN124 loaded: 915692 records
  - IMNI CURN124 loaded: 154763 records
  - Combined CURN: 1070455 records
  - CURN filtered (removed PRODUCT=139): 1070184 records

[STEP 3] Loading SAVG data...
  - MNI SAVG124 columns: ['custcd', 'prodcd', 'statecd', 'amtind', 'bankno', 'branch', 'acctno', 'name', 'ledgbal', 'lasttran']
  - MNI SAVG124 loaded: 4241108 records
    - Selected columns: ['acctno', 'product', 'curbal', 'ledgbal', 'amtind', 'intpaybl', 'branch']
  - IMNI SAVG124 loaded: 2262899 records
    - Selected columns: ['acctno', 'product', 'curbal', 'ledgbal', 'amtind', 'intpaybl', 'branch']

[STEP 4] Adding CURN to dataset list...
  - CURN added with 1070184 records
    - Selected columns: ['acctno', 'product', 'curbal', 'ledgbal', 'amtind', 'intpaybl', 'branch']

[STEP 5] Adding FDMTHLY to dataset list...
  - FDMTHLY added with 3187402 records
    - Columns: ['acctno', 'branch', 'curbal', 'amtind', 'ledgbal', 'product', 'progcd', 'intpaybl']

[STEP 6] Combining all datasets...
  - Total datasets to combine: 4
    1. MNI SAVG124: 4241108 records
    2. IMNI SAVG124: 2262899 records
    3. CURN: 1070184 records
    4. FDMTHLY: 3187402 records
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIFLTEXP_FLOAT_EXPOSURE.py", line 288, in <module>
    deposit_combined = pl.concat(datasets_to_combine, how="diagonal")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 247, in concat
    out = wrap_df(plr.concat_df_diagonal(elems))
polars.exceptions.SchemaError: type Int64 is incompatible with expected type Float64
