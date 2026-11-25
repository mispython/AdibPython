
============================================================
Bad Debt Write-Off List (Filtered by NPL.LIST)
============================================================
Report Date: 24/11/2026  Week: 4  Prev Month: 10
Output Dir : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT2
============================================================

STEP 1: Reading NPLA (BORSTAT='A')...
  Reading FULL enrh_ln_note.sas7bdat (43 cols)...
  Loaded 8,149,154 rows in 946.9s  RSS 5062MB
  Converted to Polars, RSS 4206MB
  Filtered to 10,917 rows, RSS 4344MB
  NPLA survivors: 10,917
  NPLA ready: 10,917 rows

STEP 2: IIS / SP2...
  Reading: iis.sas7bdat - 135,725 rows, 5.0s
  Reading: sp2.sas7bdat - 135,725 rows, 5.6s
  IIS: 135,725  SP2: 135,725
  WARNING: IIS and SP2 have identical row counts (135,725). Verify: iis.sas7bdat vs sp2.sas7bdat

STEP 3: Combine NPL...
  NPL combined: 146,029

STEP 4: CCRIS...
  Reading: icredmsubac1126.sas7bdat - 1,737,385 rows, 993.9s
  Found: icredmsubac1126.sas7bdat
  cols -> ACCT:acctnum DAYS:DAYSARR FAC:FACILITY
  CCRIS: 1,100,791

STEP 5: Filtering HPD loan types...
  HPD rows: 1,755

STEP 6: MERGE NPL / CREDSUB / LOAN...
STEP 7: Derived fields...
  LOAN ready: 146,029

STEP 8: Customer names...
  Reading: loan.sas7bdat - 13,557,834 rows, 1777.9s
  CNAME: 12,580,070

STEP 9: Guarantors...
  Reading: stg_ln_liabfile.sas7bdat - 2,504,468 rows, 17.3s
  Guarantor keys: 2,180,257

STEP 10: Previous balance...
  Reading: loan104.sas7bdat - 623,910 rows, 25.1s
  SASLN: 20,995

STEP 11: Final merge...
  WOFF pre-filter: 20,995

STEP 12: Filtering by NPL.LIST...
  Reading: list.sas7bdat - 511 rows, 0.0s
  Found LIST: list.sas7bdat
  NPL.LIST accounts: 511
  WOFF after NPL.LIST filter: 177

STEP 13: Writing outputs...
SAS Connection established. Subprocess id is 287349

  SAS session ready: Access Method         = STDIO
SAS Config name       = default
SAS Config file       = /sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sascfg.py
WORK Path             = /sas/saswork/SAS_workDC9300046275_svdwh004/
SAS Version           = 9.04.01M9P06052025
SASPy Version         = 5.102.0
Teach me SAS          = False
Batch                 = False
Results               = Pandas
SAS Session Encoding  = latin1
Python Encoding value = latin_1
SAS process Pid value = 287349
SASsession started    = Wed Nov 25 19:05:44 2026



21   
22   libname NPLOUT    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT2'  ;
NOTE: Libref NPLOUT was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT2
23   
  libref NPLOUT -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT2
  NPLOUT.WOFFTXT written via SASPy

============================================================
SUMMARY
============================================================
Accounts in filtered write-off list: 177
Total exposure: RM 3,923,462.21
  wofftex1.txt: 141,246 bytes
  wofftex1 line lengths: min=797 max=797 expected=797
  wofftext.txt: 139,830 bytes
  NPLOUT.WOFFTXT_FORMATTED written via SASPy

============================================================
COMPLETED IN 4658.3s
============================================================
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT2/wofftext.txt
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT2/wofftex1.txt
  NPLOUT.WOFFTXT
  NPLOUT.WOFFTXT_FORMATTED
SAS Connection terminated. Subprocess id was 287349
