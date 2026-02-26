EIBMDPDE - Deposit Position Date Extraction
============================================================

Reading DPPSTRGS file...
  ✓ DPPSTRGS: Record read (665 bytes)

Extracting TBDATE (packed decimal)...
  Raw bytes: 30 38 9C 9C 41 00
  TBDATE value: 3038912912410
  TBDATE Z11.: 3038912912410
  SUBSTR (1,8): 30389129
  Parsed: Month=30, Day=38, Year=9129
  ✗ ERROR: Invalid date components - Month must be 1-12, Day must be valid
    Got Month=30, Day=38, Year=9129
