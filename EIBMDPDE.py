EIBMDPDE - Deposit Position Date Extraction
============================================================

Reading DPPSTRGS file...
  ✓ DPPSTRGS: Record read (665 bytes)

Extracting TBDATE (packed decimal)...
  Raw bytes: 30 38 9C 9C 41 00
  All digits extracted: 3038912912410 (length: 13)
  Warning: Extracted 13 digits, truncating to 11 for Z11. format
  TBDATE numeric value: 30389129124
  TBDATE Z11.: 30389129124
  SUBSTR (1,8): 30389129
  Parsed: Month=30, Day=38, Year=9129
  Trying YYYYMMDD: Year=3038, Month=91, Day=29
  ✗ ERROR: Invalid date components
    MMDDYYYY: Month=30, Day=38, Year=9129
    YYYYMMDD: Year=3038, Month=91, Day=29
