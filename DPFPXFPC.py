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
SAS Connection established. Subprocess id is 680347


80   
81   libname outlib base  '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM'  ;
NOTE: Libref OUTLIB was successfully assigned as follows: 
      Engine:        BASE 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM
82   
  ✓ Saved SAS dataset: dyibu11.sas7bdat
SAS Connection terminated. Subprocess id was 680347
Section 1: DYIBU - 267 branches

================================================================================
SECTION 2: PROCESS SAVINGS & CURRENT ACCOUNTS
================================================================================
After ENTITY_CD = 'PIBB' filter:
  SAVING:  2,285,493 rows
  CURRENT: 159,938 rows
Total accounts to process: 2,388,145
Processing accounts using vectorized operations...
  Processed 500,000 accounts...
  Processed 1,000,000 accounts...
  Processed 1,500,000 accounts...
  Processed 2,000,000 accounts...
✓ Processed 2,388,145 accounts

================================================================================
GENERATING OUTPUT DATASETS (SAS7BDAT)
================================================================================

Generating awsa11 (Products 204,215 (Regular Savings))...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDISLM.py", line 490, in <module>
    result = generate_dataset(
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDISLM.py", line 401, in generate_dataset
    result = filtered.groupby(groupby_cols).agg(agg_dict).reset_index()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/groupby/generic.py", line 1432, in aggregate
    result = op.agg()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/apply.py", line 190, in agg
    return self.agg_dict_like()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/apply.py", line 423, in agg_dict_like
    return self.agg_or_apply_dict_like(op_name="agg")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/apply.py", line 1608, in agg_or_apply_dict_like
    result_index, result_data = self.compute_dict_like(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/apply.py", line 462, in compute_dict_like
    func = self.normalize_dictlike_arg(op_name, selected_obj, func)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/apply.py", line 663, in normalize_dictlike_arg
    raise KeyError(f"Column(s) {list(cols)} do not exist")
KeyError: "Column(s) ['avgacct', 'noacct'] do not exist"
