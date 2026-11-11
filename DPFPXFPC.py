Islamic Banking Statistics - 10/11/2026
Processing data for date: 2026-11-10

Input files:
  CURRENT: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDISLM/intg_dp_acct_current_d10.sas7bdat
  SAVING:  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDISLM/intg_dp_acct_saving_d10.sas7bdat

================================================================================
INSPECTING INPUT DATASETS
================================================================================

SAVING dataset columns (first 20):
  BRANCH, ACCTNO, NAME, TAXNO, DEBIT, CREDIT, CLOSEDT, REOPENDT, CUSTCODE, ORGCODE, ORGTYPE, INTYTD, FEEPD, PURPOSE, SECTOR, USER2, USER3, RISKCODE, LEDGBAL, OPENIND

CURRENT dataset columns (first 20):
  BRANCH, ACCTNO, NAME, TAXNO, DEBIT, CREDIT, CLOSEDT, REOPENDT, CUSTCODE, ODPLAN, RATE1, RATE2, RATE3, RATE4, RATE5, TODRATE, FLATRATE, BASERATE, ODSTAT, ORGCODE

================================================================================
SECTION 1: DAILY ISLAMIC BALANCE SUMMARY (DYIBU)
================================================================================
Loaded CURRENT: 1108836 rows, 149 columns
Loaded SAVING:  6639092 rows, 87 columns

After ENTITY_CD = 'PIBB' filter:
  CURRENT: 159938 rows
  SAVING:  2285493 rows

Using columns:
  BRANCH:  BRANCH
  PRODUCT: PRODUCT
  CURBAL:  CURBAL
  OPENIND: OPENIND
Combined raw data: 2388145 rows
SAS Connection established. Subprocess id is 3686531


80   
81   libname outlib base  '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM'  ;
NOTE: Libref OUTLIB was successfully assigned as follows: 
      Engine:        BASE 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM
82   
  ✓ Saved SAS dataset: dyibu11.sas7bdat
SAS Connection terminated. Subprocess id was 3686531
Section 1: DYIBU - 267 branches

================================================================================
SECTION 2: PROCESS SAVINGS & CURRENT ACCOUNTS
================================================================================
After ENTITY_CD = 'PIBB' filter:
  SAVING:  2,285,493 rows
  CURRENT: 159,938 rows
Total accounts to process: 2,388,145
Processing accounts using vectorized operations...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDISLM.py", line 445, in <module>
    chunk['sadenom'] = chunk['product'].apply(apply_sadenom)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/series.py", line 4917, in apply
    return SeriesApply(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/apply.py", line 1427, in apply
    return self.apply_standard()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/apply.py", line 1507, in apply_standard
    mapped = obj._map_values(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/base.py", line 921, in _map_values
    return algorithms.map_array(arr, mapper, na_action=na_action, convert=convert)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/algorithms.py", line 1743, in map_array
    return lib.map_infer(values, mapper, convert=convert)
  File "lib.pyx", line 2972, in pandas._libs.lib.map_infer
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDISLM.py", line 93, in apply_sadenom
    if PBBDPFMT is not None:
NameError: name 'PBBDPFMT' is not defined
