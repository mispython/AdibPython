============================================================
EIMAR301 / EIMIR301 SAS to Python Conversion
============================================================

1. Processing REPTDATE with previous month...
   Current Date: 24/07/26
   Previous Month Date: 2026-06-01

2. Building LNTEMP (filtered loans + branch lookup)...
   LNTEMP rows: 389357

3. Building LOAN (arrears / new-loan population, duplicates preserved)...
   LOAN rows: 55359

4. Building LOAN1 (category assignment, duplicates preserved, no default cat)...
   LOAN1 rows: 55359

5. Generating Report A (EIMAR301-A, non-CAC branches only)...
✓ Report A saved: 26755 accounts, 54 branch pages

6. Generating Report B (EIMAR301-B, all branches)...
✓ Report B saved: 1482 accounts, 41 branch pages

7. Generating Report C (EIMAR301-C, new releases payment summary)...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR301.py:640: DeprecationWarning: `pl.count()` is deprecated. Please use `pl.len()` instead.
(Deprecated in version 0.20.5)
  pl.count().alias("NOACCT"),
✓ REPORT_C saved: 50654 accounts across 71 branches

8. Generating Report D (EIMAR301-D, exactly 2 installments paid)...
✓ REPORT_D saved: 262 accounts across 33 branches

============================================================
CONVERSION COMPLETE
============================================================
Output saved to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMIR301
