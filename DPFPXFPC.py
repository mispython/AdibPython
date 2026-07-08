Islamic Banking Statistics - 07/07/2026
Processing data for date: 2026-07-07

================================================================================
INSPECTING INPUT DATASETS
================================================================================

SAVING dataset columns (first 20):
  BANKNO, FMTCODE, BRANCH, ACCTNO, NAME, TAXNO, DEBIT, CREDIT, CLOSEDT, REOPENDT, CUSTCODE, ORGCODE, ORGTYPE, INTYTD, FEEPD, PURPOSE, SECTOR, USER2, USER3, RISKCODE

CURRENT dataset columns (first 20):
  FMTCODE, BRANCH, ACCTNO, NAME, TAXNO, DEBIT, CREDIT, CLOSEDT, REOPENDT, CUSTCODE, ODPLAN, RATE1, RATE2, RATE3, RATE4, RATE5, TODRATE, FLATRATE, BASERATE, ODSTAT

================================================================================
SECTION 1: DAILY ISLAMIC BALANCE SUMMARY (DYIBU)
================================================================================
Loaded CURRENT: 162640 rows, 147 columns
Loaded SAVING: 2298576 rows, 88 columns

Using columns:
  BRANCH: BRANCH
  PRODUCT: PRODUCT
  CURBAL: CURBAL
  OPENIND: OPENIND
Combined raw data: 2394211 rows

Saving dyibu07...
  ✓ Saved Parquet file: dyibu07.parquet
  Creating SAS dataset: dyibu07
SAS Connection established. Subprocess id is 201591


80   
81   libname outlib base  '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM'  ;
NOTE: Libref OUTLIB was successfully assigned as follows: 
      Engine:        BASE 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM
82   
  ✓ SAS dataset created: dyibu07.sas7bdat
SAS Connection terminated. Subprocess id was 201591
Section 1: DYIBU - 267 branches

================================================================================
SECTION 2: PROCESS SAVINGS & CURRENT ACCOUNTS
================================================================================
Total accounts to process: 2,394,211
Processing accounts using vectorized operations...
  Processed 500,000 accounts...
  Processed 1,000,000 accounts...
  Processed 1,500,000 accounts...
  Processed 2,000,000 accounts...
✓ Processed 2,394,211 accounts

================================================================================
GENERATING OUTPUT DATASETS (SAS7BDAT + PARQUET)
================================================================================

Generating awsa07 (Products 204,215 (Regular Savings))...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDAWSA.py", line 530, in <module>
    result = generate_dataset(
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDAWSA.py", line 438, in generate_dataset
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
