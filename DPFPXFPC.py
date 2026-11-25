
============================================================
Bad Debt Write-Off List Generation
============================================================
Report Date: 24/11/2026  Week: 4  Prev Month: 10
============================================================

STEP 1+2: Reading enrh_ln_note (ENTITY_CD='PIBB', BORSTAT='A')...
  Reading FULL enrh_ln_note.sas7bdat (44 cols)...
  Loaded 8,149,154 rows in 467.9s  RSS 5262MB
  Converted to Polars, RSS 4270MB
  Filtered to 297 rows, RSS 4536MB
  NPLA survivors: 297
  NPLA ready: 297 rows

STEP 3: IIS / SP2...
  Reading: iis.sas7bdat - 135,725 rows, 2.1s
  Reading: sp2.sas7bdat - 135,725 rows, 1.8s
  IIS: 135,725  SP2: 135,725
  WARNING: IIS and SP2 have identical row counts (135,725). Verify: iis.sas7bdat vs sp2.sas7bdat

STEP 4: Combine NPL...
  NPL combined: 135,924

STEP 5: CCRIS...
  Reading: icredmsubac1126.sas7bdat - 1,737,385 rows, 241.4s
  Found: icredmsubac1126.sas7bdat
  cols -> ACCT:acctnum DAYS:DAYSARR FAC:FACILITY
  CCRIS: 1,100,791

STEP 6+7: MERGE NPL/CREDSUB/LOAN + derived fields...
  LOAN ready: 135,924

STEP 9: Customer names...
  Reading: loan.sas7bdat - 13,557,834 rows, 993.5s
  CNAME: 12,580,070

STEP 10: Guarantors...
  Reading: stg_ln_liabfile.sas7bdat - 2,504,468 rows, 10.4s
  Guarantor keys: 2,180,257

STEP 11: Previous balance...
  Reading: loan104.sas7bdat - 623,910 rows, 15.9s
  SASLN: 20,995

STEP 12: Final merge + filter...
  WOFF pre-filter: 20,995
  WOFF after filter: 691

STEP 13: Writing outputs...
SAS Connection established. Subprocess id is 2050519

  SAS session ready: Access Method         = STDIO
SAS Config name       = default
SAS Config file       = /sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sascfg.py
WORK Path             = /sas/saswork/SAS_work406E001F49D7_svdwh004/
SAS Version           = 9.04.01M9P06052025
SASPy Version         = 5.102.0
Teach me SAS          = False
Batch                 = False
Results               = Pandas
SAS Session Encoding  = latin1
Python Encoding value = latin_1
SAS process Pid value = 2050519
SASsession started    = Wed Nov 25 11:58:28 2026



21   
22   libname NPLOUT    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT1'  ;
NOTE: Libref NPLOUT was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT1
23   
  libref NPLOUT -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT1
  NPLOUT.LIST written via SASPy

============================================================
SUMMARY
============================================================
Accounts: 691
Total exposure: RM 10,489,215.82
  wofftex1.txt: 522,396 bytes
  wofftex1 line lengths: min=755 max=755 expected=789
  WARNING: short lines detected — offsets may misalign
  wofftext.txt: 540,362 bytes
  NPLOUT.WOFFTXT written via SASPy

============================================================
COMPLETED IN 2258.0s
============================================================
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT1/wofftext.txt
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT1/wofftex1.txt
  NPLOUT.LIST
  NPLOUT.WOFFTXT
SAS Connection terminated. Subprocess id was 2050519
