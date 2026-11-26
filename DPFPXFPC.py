
============================================================
ESMR 2009-16 (SEND FILE TO LNS) - 3RD EXTRACTION
============================================================
Report Date: 25/11/2026  Week: 4  Prev Month: 10  RIND: I
Output Dir : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT3
============================================================

STEP 1: IIS / SP2...
  Reading: iis.sas7bdat - 135,725 rows, 2.0s
  Reading: sp2.sas7bdat - 135,725 rows, 1.0s
  IIS: 135,725  SP2: 135,725
  WARNING: IIS and SP2 have identical row counts (135,725). Verify: iis.sas7bdat vs sp2.sas7bdat
  NPL: 135,725 rows

STEP 2: LNNOTE (LOANTYPE IN &HPD)...
  Reading FULL enrh_ln_note.sas7bdat (33 cols)...
  Loaded 8,149,154 rows in 308.1s  RSS 3838MB
  Converted to Polars, RSS 2983MB
  Filtered to 5,501,509 rows, RSS 4545MB
  LNNOTE: 5,501,509 rows

STEP 3: MERGE NPL(IN=AA) LOAN...
  LOAN after merge: 135,725

STEP 4: Derived fields...
  LOAN ready: 135,725

STEP 5: Customer names...
  Reading: loan.sas7bdat - 13,557,834 rows, 386.8s
  CNAME: 12,580,070

STEP 6: Guarantors...
  Reading: stg_ln_liabfile.sas7bdat - 2,504,468 rows, 6.9s
  Guarantor keys: 2,180,257

STEP 7: Previous balance...
  Reading: loan104.sas7bdat - 623,910 rows, 25.1s
  SASLN: 20,827

STEP 8: MERGE SASLN LOAN NPL -> WOFF...
  WOFF pre-LIST: 20,827
  Reading: list.sas7bdat - 511 rows, 0.0s
  NPL.LIST accounts: 511
  WOFF after NPL.LIST filter: 189

STEP 9: Writing outputs...
SAS Connection established. Subprocess id is 340252

  SAS session ready: Access Method         = STDIO
SAS Config name       = default
SAS Config file       = /sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sascfg.py
WORK Path             = /sas/saswork/SAS_work203A0005311C_svdwh004/
SAS Version           = 9.04.01M9P06052025
SASPy Version         = 5.102.0
Teach me SAS          = False
Batch                 = False
Results               = Pandas
SAS Session Encoding  = latin1
Python Encoding value = latin_1
SAS process Pid value = 340252
SASsession started    = Thu Nov 26 11:43:22 2026



21   
22   libname NPLOUT    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT3'  ;
NOTE: Libref NPLOUT was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT3
23   
  libref NPLOUT -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT3
  NPLOUT.WOFFTXT written via SASPy

============================================================
SUMMARY
============================================================
Accounts: 189
Total exposure: RM 3,934,901.21
  wofftex1.txt: 141,939 bytes
  wofftex1 line lengths: min=750 max=750 expected=750
  TEXT parsed: 189 records
  wofftext.txt: 141,939 bytes
  NPLOUT.WOFFTEXT_FORMATTED written via SASPy
  EIIFTXT3_PRINT.txt: 79,295 bytes

============================================================
COMPLETED
============================================================
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT3/wofftext.txt
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT3/wofftex1.txt
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT3/EIIFTXT3_PRINT.txt
  NPLOUT.WOFFTXT
  NPLOUT.WOFFTEXT_FORMATTED
