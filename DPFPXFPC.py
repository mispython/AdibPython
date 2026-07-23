============================================================
EIMAR301 SAS to Python Conversion - Multi-Report System
============================================================

1. Processing REPTDATE with previous month (datetime/timedelta)...
   Current Date: 230726
   Previous Month Date: 2026-06-01

2. Loading and filtering HP Direct loans...
   [debug] raw LOANTEMP rows: 663747
   [debug] BALANCE>0 rows: 661107
   [debug] BORSTAT != 'Z' rows: 663747
   [debug] sample PRODUCT values: [102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 116.0, 120.0, 127.0]
   [debug] sample BORSTAT values: ['N', 'I', 'M', 'C', 'D', 'P', 'R', 'K', '', 'G', 'X', 'A', 'Y', 'T', 'S']
   [debug] HPD filter numbers: [110, 115, 700, 705]
   [debug] PRODUCT in HPD rows: 117
   [debug] rows after all three filters (pre-join): 117
   [debug] LKP_BRANCH rows loaded: 376
   [debug] sample branch_df BRANCH values: [None]
   [debug] sample loan BRANCH values: [2, 4, 5, 10, 13, 16, 17, 21, 25, 26, 29, 30, 33, 35, 38]
   [debug] rows after branch join: 0
   Filtered HP Direct loans: 0

3. Generating Report A (EIMAR301-A)...
   Report A accounts: 0
   No data for non-CAC branches in Report A

4. Generating Report B (EIMAR301-B)...
   Report B accounts: 0
   No data for Report B

5. Generating Report C (EIMAR301-C)...
   Report C accounts: 0
   No data for Report C

6. Generating Report D (EIMAR301-D)...
   Report D accounts: 0
   No data for Report D

7. Creating combined analysis...

8. Creating detailed data extracts...

9. Creating branch performance analysis...
✓ Branch performance analysis saved: 0 branches

============================================================
CONVERSION COMPLETE
============================================================
Total HP Direct loans processed: 0
Report A (2+ months arrears): 0
Report B (3-8 months arrears): 0
Report C (New releases): 0
Report D (2 installments paid): 0
Previous month date: 2026-06-01
Output saved to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMIR301
