Reading SAS datasets...
CURRENT dataset: 1132084 rows before filtering
CURRENT dataset: 967521 rows after filtering (ENTITY_CD != 'PIBB')
LIMIT dataset: 1093140 rows before filtering
LIMIT dataset: 937196 rows after filtering (ENTITY_CD != 'PIBB')
NPLA dataset: 28 rows
Reading CISDP dataset in chunks...
CISDP columns: ['ACCTNO', 'INDORG', 'CUSTNO', 'GENDER', 'CACCCODE', 'SECCUST', 'CITIZEN', 'OCCUPAT', 'HOBBIES', 'RELIGION', 'EDUCATN', 'INCOME', 'MARITAL', 'BIRTHDAT', 'OPENDAT', 'RACE', 'CUSTCONSENT', 'BGC', 'FILLER', 'CONSENT_OLD', 'CISCUSTCD1', 'CISCUSTCD2', 'CISCUSTCD3', 'CISCUSTCD4', 'CISCUSTCD5', 'CISCUSTCD6', 'CISCUSTCD7', 'CISCUSTCD8', 'CISCUSTCD9', 'CISCUSTCD10', 'CISCUSTCD11', 'CISCUSTCD12', 'CISCUSTCD13', 'CISCUSTCD14', 'CISCUSTCD15', 'CONSDD', 'CONSMM', 'CONSCC', 'CONSYY', 'SECTOR_CODE', 'INSTSECT', 'RESTATUS', 'SIC_ISS', 'OCCUPAT_MASCO_CD', 'CUST_CODE', 'CONSENTEFFDT', 'OLDIC', 'NEWICIND', 'NEWIC', 'BUSSIND', 'BUSSREG', 'BRANCH', 'RHOLD_IND', 'CUSTNAME', 'ADDREF', 'PRIPHONE', 'SECPHONE', 'CUSTNAM1', 'MOBIPHON', 'LONGNAME', 'EMAILADD', 'UPD_DT_SALE', 'TURNOVER2', 'TURNOVER', 'UPD_DT_EMPLOYEE', 'NOEMPLO2', 'NOEMPLO', 'NEW_BUSS_REG_ID_TYPE', 'NEW_BUSS_REG_ID', 'LARGECO', 'LARGECO_IND', 'LARGE_CORP_FLG', 'MMTOH', 'MM2H_FLG', 'ADDRLN1', 'ADDRLN2', 'ADDRLN3', 'ADDRLN4', 'ADDRLN5', 'MAILCODE', 'MAILSTAT']
Processed 10 chunks, 1000000 rows total
Processed 20 chunks, 2000000 rows total
Processed 30 chunks, 3000000 rows total
Processed 40 chunks, 4000000 rows total
Processed 50 chunks, 5000000 rows total
Processed 60 chunks, 6000000 rows total
Processed 70 chunks, 7000000 rows total
Processed 80 chunks, 8000000 rows total
Processed 90 chunks, 9000000 rows total
Processed 100 chunks, 10000000 rows total
Processed 110 chunks, 10935758 rows total
CISDP dataset: 8666482 rows after filtering (SECCUST == '901')
CA after SCH mapping: 65 rows
Processing LIMIT data...
LMTSTART dtype: float64
LMTSTART sample values: [nan, nan, nan, nan, nan, nan, nan, nan, nan, nan]
LMTSTART null count: 889649
LIMIT processed: 930567 unique records
Reading COLL file (EBCDIC)...
Reading DESC file (EBCDIC)...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDNPGS.py", line 380, in <module>
    dep[['ARREARS','NPLDATE_CALC']] = dep.apply(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/frame.py", line 4299, in __setitem__
    self._setitem_array(key, value)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/frame.py", line 4341, in _setitem_array
    check_key_length(self.columns, key, value)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexers/utils.py", line 390, in check_key_length
    raise ValueError("Columns must be same length as key")
ValueError: Columns must be same length as key
