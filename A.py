============================================================
STEP 1: Processing RPVBDATA dates
============================================================
DEBUG: RPVBDATA first line: '0 20250401'
DEBUG: Extracted TBDATE from positions 3-10: '20250401'
✓ TBDATE from RPVBDATA: 20250401
  TBDATE as date: 2025-04-01
  REPTDATE (end of prev month from TBDATE): 2025-03-31
  PREVDATE (end of prev month from REPTDATE): 2025-02-28
  REPTDT (MMYY): 0325
  PREVDT (MMYY): 0225

============================================================
STEP 2: Processing SRSDATA dates
============================================================
DEBUG: SRSDATA first line: '20251103N201'
DEBUG: Extracted TBDATE (first 8 chars): '20251103'
✓ TBDATE from SRSDATA: 20251103
  SRS TBDATE as date: 2025-11-03
  SRS REPTDATE: 2025-11-03
  SRSTDT (MMYY): 1125

============================================================
STEP 3: Macro guard validation
============================================================
Comparing: REPTDT=0325 vs SRSTDT=1125
✗ THE SAP.PBB.RPVB.TEXT IS NOT DATED (MMYY:1125)
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/EIBMREPO_UAT.py", line 219, in <module>
    raise RuntimeError(error_msg)
RuntimeError: THE SAP.PBB.RPVB.TEXT IS NOT DATED (MMYY:1125)
