SAS Connection established. Subprocess id is 196106

SAS Connection established successfully
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
  ✗ Error saving SAS dataset dyibu07: 'DataFrame' object has no attribute 'strip'
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
  ✓ Saved SAS dataset (alt method): dyibu07.sas7bdat
  ✓ Saved Parquet file: dyibu07.parquet
Section 1: DYIBU - 267 branches

================================================================================
SECTION 2: PROCESS SAVINGS & CURRENT ACCOUNTS
================================================================================
