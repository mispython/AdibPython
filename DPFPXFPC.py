PROCESSING CONVENTIONAL BANKING FLOAT DATA
==================================================
Columns in fdmthly.sas7bdat: ['state', 'custcode', 'bic', 'lstmatdt', 'branch', 'acctno', 'purpose', 'name', 'openind', 'curbal', 'orgdate', 'matdate', 'rate', 'accttype', 'term', 'intplan', 'renewal', 'intpay', 'intdate', 'lastactv', 'amtind', 'forate']
FDMTHLY processed columns: ['acctno', 'branch', 'curbal', 'intplan', 'bic', 'amtind', 'intpay', 'ledgbal', 'product', 'prodcd', 'intpaybl']
Columns in curn124.sas7bdat: ['custcd', 'prodcd', 'statecd', 'amtind', 'branch', 'acctno', 'name', 'purpose', 'sector', 'ledgbal', 'lasttran', 'avgamt', 'product', 'race', 'deptype', 'curbal', 'chqfloat', 'odintacc', 'curcode', 'costctr', 'intpaybl', 'openmh', 'closemh', 'accytd', 'forate', 'range', 'avgrnge', 'cabal', 'sabal']
CURN processed columns: ['acctno', 'branch', 'curbal', 'prodcd', 'product', 'amtind', 'intpaybl', 'ledgbal']
Columns in savg124.sas7bdat: ['custcd', 'prodcd', 'statecd', 'amtind', 'bankno', 'branch', 'acctno', 'name', 'ledgbal', 'lasttran', 'product', 'race', 'deptype', 'curbal', 'chqfloat', 'bdate', 'schind', 'costctr', 'intpaybl', 'openmh', 'closemh', 'accytd', 'range', 'opendate', 'age']
SAVG processed columns: ['acctno', 'branch', 'curbal', 'prodcd', 'product', 'amtind', 'intpaybl', 'ledgbal']
Conventional DEPOSIT records: 7836142
Columns in float.sas7bdat: ['acctno', 'float', 'branch']
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBFLOAT_FLOAT.py:174: DeprecationWarning: use of `how='outer'` should be replaced with `how='full'`.
(Deprecated in version 0.20.29)
  deposit_merged = deposit_sorted.join(
Parquet file created: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBFLOAT/DEPOSIT_CONVENTIONAL.parquet
Parquet file created: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBFLOAT/EXCEPT_CONVENTIONAL.parquet
Conventional DEPOSIT final records: 15949
Conventional EXCEPT records: 2978
Conventional FLOAT file created: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBFLOAT/FLOAT.txt

PROCESSING ISLAMIC BANKING FLOAT DATA
==================================================
Columns in fdmthly.sas7bdat: ['lstmatdt', 'branch', 'acctno', 'purpose', 'name', 'openind', 'curbal', 'orgdate', 'matdate', 'rate', 'accttype', 'term', 'intplan', 'renewal', 'intpay', 'intdate', 'lastactv', 'amtind', 'forate', 'state', 'bic', 'custcode']
FDMTHLY processed columns: ['acctno', 'branch', 'curbal', 'intplan', 'bic', 'amtind', 'intpay', 'ledgbal', 'product', 'prodcd', 'intpaybl']
Columns in curn124.sas7bdat: ['custcd', 'prodcd', 'statecd', 'amtind', 'branch', 'acctno', 'name', 'purpose', 'sector', 'ledgbal', 'lasttran', 'avgamt', 'product', 'race', 'deptype', 'curbal', 'chqfloat', 'odintacc', 'costctr', 'intpaybl', 'openmh', 'closemh', 'accytd', 'range', 'avgrnge', 'cabal', 'sabal']
CURN processed columns: ['acctno', 'branch', 'curbal', 'prodcd', 'product', 'amtind', 'intpaybl', 'ledgbal']
Columns in savg124.sas7bdat: ['custcd', 'prodcd', 'statecd', 'amtind', 'bankno', 'branch', 'acctno', 'name', 'ledgbal', 'lasttran', 'product', 'race', 'deptype', 'curbal', 'chqfloat', 'bdate', 'schind', 'costctr', 'intpaybl', 'openmh', 'closemh', 'accytd', 'range', 'opendate', 'age']
SAVG processed columns: ['acctno', 'branch', 'curbal', 'prodcd', 'product', 'amtind', 'intpaybl', 'ledgbal']
Islamic DEPOSIT records: 2848614
Columns in float.sas7bdat: ['acctno', 'float', 'branch']
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBFLOAT_FLOAT.py:401: DeprecationWarning: use of `how='outer'` should be replaced with `how='full'`.
(Deprecated in version 0.20.29)
  deposit_merged = deposit_sorted.join(
Parquet file created: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBFLOAT/IDEPOSIT_ISLAMIC.parquet
Parquet file created: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBFLOAT/EXCEPT_ISLAMIC.parquet
Islamic IDEPOSIT final records: 2976
Islamic EXCEPT records: 15951
Islamic IFLOAT file created: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBFLOAT/IFLOAT.txt

================================================================================
PROCESSING COMPLETED SUCCESSFULLY
================================================================================
Conventional records processed: 15949
Islamic records processed: 2976
