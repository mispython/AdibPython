============================================================
EIMAR301 / EIMIR301 SAS to Python Conversion
============================================================

1. Processing REPTDATE with previous month...
   Current Date: 30/06/26
   Previous Month Date: 2026-05-01

2. Building LNTEMP (filtered loans + branch lookup)...
   LNTEMP rows: 389357

3. Building LOAN (arrears / new-loan population, duplicates preserved)...
   [debug] cond1 (arrears/borstat) accounts: 4704
   [debug] cond2 (new loan) accounts: 50700
   [debug] accounts satisfying BOTH (will print twice): 4413
   [debug] sample overlapping ACCTNO(s): [2970278519.0, 8700017436.0, 8700030905.0, 8700035104.0, 8700037131.0]
   LOAN rows: 55407

4. Building LOAN1 (category assignment, duplicates preserved, no default cat)...
   LOAN1 rows: 55407

5. Generating Report A (EIMAR301-A, non-CAC branches only)...
✓ Report A saved: 26774 accounts, 54 branch pages

6. Generating Report B (EIMAR301-B, all branches)...
✓ Report B saved: 1493 accounts, 41 branch pages

7. Generating Report C (EIMAR301-C, new releases payment summary)...
✓ REPORT_C saved: 50702 accounts across 71 branches

8. Generating Report D (EIMAR301-D, exactly 2 installments paid)...
✓ REPORT_D saved: 262 accounts across 33 branches

============================================================
CONVERSION COMPLETE
============================================================
Output saved to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMIR301
You have mail in /var/spool/mail/sas_edw_dev
