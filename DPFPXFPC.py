============================================================
Bad Debt Write-Off List Generation
============================================================
Report Date: 20/07/2026
Week: 4, Previous Month: 06
*** TEST MODE: Limiting to 500000 rows ***
============================================================

STEP 1: DEBUG - Checking BORSTAT values...
  Reading: lnnote.sas7bdat - 500000 rows, 1.4s

  BORSTAT column info:
  Data type: String
  Unique values (first 1000 rows):
    'W' : 4862 rows
    '' : 493191 rows
    'X' : 1636 rows
    'P' : 44 rows
    'F' : 169 rows
    'Y' : 1 rows
    'K' : 63 rows
    'C' : 11 rows
    'M' : 1 rows
    'I' : 9 rows
    '0' : 3 rows
    'R' : 6 rows
    'D' : 1 rows
    'A' : 3 rows
  Found 'A' values in sample

STEP 2: Reading NPLA data...
  Reading: lnnote.sas7bdat - 500000 rows, 2.8s

  Checking BORSTAT values in full dataset...
  BORSTAT distribution:
    'W' : 4862 rows
    '' : 493191 rows
    'X' : 1636 rows
    'P' : 44 rows
    'F' : 169 rows
    'Y' : 1 rows
    'K' : 63 rows
    'C' : 11 rows
    'M' : 1 rows
    'I' : 9 rows
    '0' : 3 rows
    'R' : 6 rows
    'D' : 1 rows
    'A' : 3 rows

  Exact match 'A': 3 rows
  After strip 'A': 3 rows

  Using stripped 'A' filtering...

  NPLA rows: 3

STEP 3: Reading IIS and SP data...
  Reading: iis.sas7bdat - 135725 rows, 0.2s
  Reading: sp2.sas7bdat - 135725 rows, 0.2s
  IIS rows: 135725
  SP rows: 135725


STEP 4: Combining NPL data...
  NPL combined rows: 135728

STEP 5: Reading CCRIS data...
  Looking for: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/icredmsubac0726.sas7bdat
  Reading: icredmsubac0726.sas7bdat - 500000 rows, 15.1s
  Found CCRIS file: icredmsubac0726.sas7bdat
  Columns in CCRIS file: ['micr', 'acctnum', 'NOTENO', 'COSTCTR', 'FISSPURP', 'CUSTFIX', 'SECTFIX', 'FICODE', 'micrn', 'MASTACC', 'DATE', 'BALANCE', 'MTHARR', 'INSTARR', 'UNDRAWN', 'ACSTATUS', 'DAYSARR', 'NOTECHAR', 'BRANCH', 'BNM_SUBMISSION_DATE']...
  Found columns - ACCT: acctnum, DAYS: DAYSARR, FACILITY: FACILITY
  CCRIS rows: 29

STEP 6: Reading HPD loan data...
  Reading: lnnote.sas7bdat - 500000 rows, 1.4s
  HPD loan rows: 2093

STEP 7: Merging data...
  Merged loan rows: 135728

STEP 8: Calculating derived fields...
  Reading: lnnote.sas7bdat - 500000 rows, 4.4s
  Warning: Could not calculate MTHPDUE: unexpected value while building Series of type Int64; found value of type String: "24"
  Calculations completed in 5.4s
  Loan records: 135728

STEP 9: Reading customer names...
  Reading: loan.sas7bdat - 500000 rows, 1.1s
  Found customer name file: loan.sas7bdat
  Customer names: 314388

STEP 10: Reading guarantor information...
  Reading: lnliab07226.sas7bdat - 500000 rows, 0.4s
  Guarantor entries: 289622

STEP 11: Reading previous balance...
  Reading: loan064.sas7bdat - 500000 rows, 0.5s
  SASLN rows: 20505

STEP 12: Final merge and filtering...
  WOFF before filter: 135728
  WOFF after filter: 0


No accounts identified for write-off

============================================================
COMPLETED IN 54.5 SECONDS
============================================================

Output files generated:
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT1/wofftext.txt (Final formatted output)
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT1/wofftex1.txt (Intermediate output)
