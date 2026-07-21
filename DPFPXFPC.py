============================================================
Bad Debt Write-Off List Generation
============================================================
Report Date: 20/07/2026
Week: 4, Previous Month: 06
*** TEST MODE: Limiting to 500000 rows ***
============================================================

STEP 1: DEBUG - Checking BORSTAT values...
  Reading: lnnote.sas7bdat - 500000 rows, 1.7s

  BORSTAT column info:
  Data type: String
  Unique values (first 1000 rows):
    '0' : 3 rows
    'F' : 169 rows
    'Y' : 1 rows
    'K' : 63 rows
    'I' : 9 rows
    'C' : 11 rows
    'P' : 44 rows
    'R' : 6 rows
    'W' : 4862 rows
    'D' : 1 rows
    '' : 493191 rows
    'X' : 1636 rows
    'M' : 1 rows
    'A' : 3 rows
  Found 'A' values in sample

STEP 2: Reading NPLA data...
  Reading: lnnote.sas7bdat - 500000 rows, 2.8s

  Checking BORSTAT values in full dataset...
  BORSTAT distribution:
    'X' : 1636 rows
    '0' : 3 rows
    'Y' : 1 rows
    '' : 493191 rows
    'A' : 3 rows
    'C' : 11 rows
    'K' : 63 rows
    'M' : 1 rows
    'I' : 9 rows
    'W' : 4862 rows
    'F' : 169 rows
    'R' : 6 rows
    'D' : 1 rows
    'P' : 44 rows

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
  Reading: icredmsubac0726.sas7bdat - 500000 rows, 21.3s
  Found CCRIS file: icredmsubac0726.sas7bdat
  Columns in CCRIS file: ['micr', 'acctnum', 'NOTENO', 'COSTCTR', 'FISSPURP', 'CUSTFIX', 'SECTFIX', 'FICODE', 'micrn', 'MASTACC', 'DATE', 'BALANCE', 'MTHARR', 'INSTARR', 'UNDRAWN', 'ACSTATUS', 'DAYSARR', 'NOTECHAR', 'BRANCH', 'BNM_SUBMISSION_DATE']...
  Found columns - ACCT: acctnum, DAYS: DAYSARR, FACILITY: FACILITY
  CCRIS rows: 29

STEP 6: Reading HPD loan data...
  Reading: lnnote.sas7bdat - 500000 rows, 1.7s
  HPD loan rows: 2093

STEP 7: Merging data...
  Merged loan rows: 135728

STEP 8: Calculating derived fields...
  Reading: lnnote.sas7bdat - 500000 rows, 5.2s
  Calculations completed in 6.4s
  Loan records: 135728

STEP 9: Reading customer names...
  Reading: loan.sas7bdat - 500000 rows, 7.1s
  Customer names: 314388

STEP 10: Reading guarantor information...
  Reading: lnliab07226.sas7bdat - 500000 rows, 0.5s
  Guarantor entries: 289622

STEP 11: Reading previous balance...
  Reading: loan064.sas7bdat - 500000 rows, 0.6s
  SASLN rows: 20505

STEP 12: Final merge and filtering...
  WOFF before filter: 135728

  ==================================================
  DEBUG - Checking filter conditions...
  ==================================================

  BORSTAT values in WOFF:
    'None' : 135725 rows
    'A' : 3 rows

  DAYS statistics:
    Min: 0.0
    Max: 2630.0
    Mean: 876.67
    Non-null rows: 3
    Rows with DAYS >= 334: 1

  Active accounts (BORSTAT='A'): 3
  LOANTYPE distribution for active accounts:
    None : 3 rows
  Active with PAIDIND != 'P': 2
  Active with excluded LOANTYPE: 0

  TOTAL statistics:
    Min: -5.684341886080802e-14
    Max: 233881.01385321098
    Mean: 184.17
    Rows with TOTAL != 0: 2070

  Individual filter conditions:
    1. (BORSTAT in ['F','I'] and DAYS >= 334): 0
    2. (DAYS >= 334): 1
    3. (BORSTAT='A' & LOANTYPE not in excluded & PAIDIND != 'P'): 0
    4. (TOTAL != 0): 2070

  Combined (all criteria): 0 rows
  ==================================================

  WOFF after filter: 0


No accounts identified for write-off

============================================================
COMPLETED IN 72.6 SECONDS
============================================================

Output files generated:
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT1/wofftext.txt (Final formatted output)
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT1/wofftex1.txt (Intermediate output)
