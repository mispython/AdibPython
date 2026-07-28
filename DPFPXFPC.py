Processing Bad Debt Write-Off List (Conventional Banking)
Report Date: 27/07/2026
Week: 3, Previous Month: 06
Reading LNNOTE (optimized, single pass)...
Successfully read 6232608 records from LNNOTE
Step 1: Creating NPLA...
Deriving HPD loan rows from the already-loaded LNNOTE frame...
HPD loan records: 182575
Step 2: Reading IIS and SP data...
  Warning: could not read iis.sas7bdat cleanly (Column(s) ['sp', 'marketvl'] not found in /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1/iis.sas7bdat.
Available columns in file: ['ACCRUAL', 'ACCTNO', 'BORSTAT', 'BRANCH', 'COSTCTR', 'CURBAL', 'DAYS', 'EXIST', 'IIS', 'IISP', 'IISPCUM', 'IISPW', 'LOANTYP', 'LOANTYPE', 'NAME', 'NETBAL', 'NETPROC', 'NOTENO', 'NTBRCH', 'OI', 'OIP', 'OIPCUM', 'OIRECC', 'OIRECV', 'OISUSP', 'OIW', 'PAIDIND', 'PENDBRH', 'POI', 'RECC', 'RECOVER', 'RESCHEIND', 'RISK', 'SUSPEND', 'TOTIIS', 'UHC', 'USER5', 'WDOWNIND']); using empty frame.
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIFFTXT1.py:559: FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated. In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes. To retain the old behavior, exclude the relevant entries before the concat operation.
  df_npl = pd.concat([df_npla, df_npl_data], ignore_index=True)
NPL records: 135895
Step 3: Reading CREDMSUBAC...
Warning: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1/credmsubac0726.sas7bdat not found.
Merging NPL, CREDSUB, and LOAN data...
Merged loan records: 135895
Step 5: Calculating derived fields (vectorized)...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIFFTXT1.py:622: FutureWarning: Downcasting object dtype arrays on .fillna, .ffill, .bfill is deprecated and will change in a future version. Call result.infer_objects(copy=False) instead. To opt-in to the future behavior, set `pd.set_option('future.no_silent_downcasting', True)`
  df_loan['days'] = df_loan['days'].fillna(0).astype(int) if 'days' in df_loan.columns else 0
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIFFTXT1.py", line 654, in <module>
    ((df_loan['orgbal'] - df_loan['curbal']) / df_loan['payamt']).astype(int),
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/generic.py", line 6643, in astype
    new_data = self._mgr.astype(dtype=dtype, copy=copy, errors=errors)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/internals/managers.py", line 430, in astype
    return self.apply(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/internals/managers.py", line 363, in apply
    applied = getattr(b, f)(**kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/internals/blocks.py", line 758, in astype
    new_values = astype_array_safe(values, dtype, copy=copy, errors=errors)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/dtypes/astype.py", line 237, in astype_array_safe
    new_values = astype_array(values, dtype, copy=copy)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/dtypes/astype.py", line 182, in astype_array
    values = _astype_nansafe(values, dtype, copy=copy)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/dtypes/astype.py", line 101, in _astype_nansafe
    return _astype_float_to_int_nansafe(arr, dtype, copy)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/dtypes/astype.py", line 145, in _astype_float_to_int_nansafe
    raise IntCastingNaNError(
pandas.errors.IntCastingNaNError: Cannot convert non-finite values (NA or inf) to integer
