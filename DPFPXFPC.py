SAS Connection established. Subprocess id is 197334

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
  Attempting alternative method...
  ✗ Error saving SAS dataset dyibu07: 'DataFrame' object has no attribute 'strip'
  ✓ Saved Parquet file: dyibu07.parquet
Section 1: DYIBU - 267 branches
