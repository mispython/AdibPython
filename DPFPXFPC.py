============================================================
EIMAR301 SAS to Python Conversion - Multi-Report System
============================================================
   PBBLNFMT : PBBLNFMT [/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/PBBLNFMT.py]
   PBBELF   : PBBELF [/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/PBBELF.py]
   HPD      bound via: PBBLNFMT.HPD = [128, 130, 131, 132, 380, 381, 700, 705, 720, 725]
   ARRCLASS bound via: PBBLNFMT.put(v,'ARRCLASS')
     ARRCLASS( 2) = '1 - < 2 MTH   '   (expected '1 - < 2 MTH   ')
     ARRCLASS( 3) = '2 - < 3 MTH   '   (expected '2 - < 3 MTH   ')
     ARRCLASS(15) = 'DEFICIT       '  (expected 'DEFICIT       ')
   CACBRCH  bound via: PBBELF.format_cacbrch(v)
     CACBRCH(  2) = '911'   (expected '911' - KL)
     CACBRCH( 13) = '000'  (expected '000' - non-CAC)
     CACBRCH( 31) = '000'  (expected '000' - non-CAC)

1. Processing REPTDATE with previous month (datetime/timedelta)...
   Current Date: 20/10/26
   Previous Month Date: 2026-09-01

2. Loading and filtering HP Direct loans...
   [debug] raw LOANTEMP rows: 631802
   [debug] BALANCE>0 rows: 629109
   [debug] BORSTAT != 'Z' rows: 631802
   [debug] sample PRODUCT values: [102, 103, 104, 105, 106, 107, 110, 111, 112, 113, 114, 115, 116, 120, 127]
   [debug] sample BORSTAT values: ['W', 'D', 'M', 'A', 'K', 'C', 'X', '', 'G', 'N', 'B', 'P', 'R', 'F']
   [debug] HPD filter numbers (from PBBLNFMT.HPD): [128, 130, 131, 132, 380, 381, 700, 705, 720, 725]
   [debug] PRODUCT in HPD rows: 404864
   [debug] rows after all three filters (pre-join): 404862
   [debug] LKP_BRANCH first 8 raw lines (repr):
   [debug]   'B001 PCS   BANK-ATMC                             C                              '
   [debug]   'B002 JSS   JALAN SULTAN SULAIMAN            W    O                              '
   [debug]   'B003 JRC   JALAN RAJA CHULAN                W    O                              '
   [debug]   'B004 MLK   MELAKA                           M    O                              '
   [debug]   'B005 IMO   IPOH MAIN OFFICE                 A    O                              '
   [debug]   'B006 PPG   PULAU PINANG                     P    O                              '
   [debug]   'B007 JBU   JOHOR BAHRU                      J    O                              '
   [debug]   'B008 KTN   KUANTAN                          C    O                              '
   [debug] first 8 parsed rows: [{'BRANCH': 1, 'BRHCODE': 'PCS'}, {'BRANCH': 2, 'BRHCODE': 'JSS'}, {'BRANCH': 3, 'BRHCODE': 'JRC'}, {'BRANCH': 4, 'BRHCODE': 'MLK'}, {'BRANCH': 5, 'BRHCODE': 'IMO'}, {'BRANCH': 6, 'BRHCODE': 'PPG'},{'BRANCH': 7, 'BRHCODE': 'JBU'}, {'BRANCH': 8, 'BRHCODE': 'KTN'}]
   [debug] LKP_BRANCH rows loaded: 376
   [debug] sample branch_df BRANCH values: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
   [debug] sample loan BRANCH values: [13, 14, 17, 27, 29, 31, 34, 48, 49, 52, 55, 59, 63, 65, 69]
   [debug] rows after branch join: 404862
   Filtered HP Direct loans: 404862

3. Generating Report A (EIMAR301-A)...
   Report A accounts: 5946
✓ Report A summary saved: 270 records

4. Generating Report B (EIMAR301-B)...
   Report B accounts: 1061
✓ Report B summary saved: 222 records

5. Generating Report C (EIMAR301-C)...
   Report C accounts: 0
   No data for Report C

6. Generating Report D (EIMAR301-D)...
   Report D accounts: 192
✓ Report D summary saved: 34 records

7. Creating combined analysis...

8. Creating detailed data extracts...

9. Creating branch performance analysis...
✓ Branch performance analysis saved: 111 branches

10. Saving run variables...

============================================================
CONVERSION COMPLETE
============================================================
Total HP Direct loans processed: 404862
Report A (2+ months arrears):    5946
Report B (3-8 months arrears):   1061
Report C (New releases):         0
Report D (2 installments paid):  192
Previous month date:             2026-09-01
Output saved to:                 /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMIR301
