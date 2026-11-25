
============================================================
Bad Debt Write-Off List Generation
============================================================
Report Date: 24/11/2026  Week: 4  Prev Month: 10
============================================================

STEP 1+2: Reading enrh_ln_note (ENTITY_CD='PIBB', BORSTAT='A')...
  Reading FULL enrh_ln_note.sas7bdat (44 cols)...
  Loaded 8,149,154 rows in 369.5s  RSS 5322MB
  Converted to Polars, RSS 4392MB
  Filtered to 297 rows, RSS 4662MB
  NPLA survivors: 297
  NPLA ready: 297 rows

STEP 3: IIS / SP2...
  Reading: iis.sas7bdat - 135,725 rows, 4.3s
  Reading: sp2.sas7bdat - 135,725 rows, 2.9s
  IIS: 135,725  SP2: 135,725
  WARNING: IIS and SP2 have identical row counts (135,725). Verify paths: iis.sas7bdat vs sp2.sas7bdat

STEP 4: Combine NPL...
  NPL combined: 135,924

STEP 5: CCRIS...
  Reading: icredmsubac1126.sas7bdat - 1,737,385 rows, 220.8s
  Found: icredmsubac1126.sas7bdat
  cols -> ACCT:acctnum DAYS:DAYSARR FAC:FACILITY
  CCRIS: 1,100,791

STEP 6+7: MERGE NPL/CREDSUB/LOAN + derived fields...
  LOAN ready: 135,924

STEP 9: Customer names...
  Reading: loan.sas7bdat - 13,557,834 rows, 468.5s
  CNAME: 12,580,070

STEP 10: Guarantors...
  Reading: stg_ln_liabfile.sas7bdat - 2,504,468 rows, 9.0s
  Guarantor keys: 2,180,257

STEP 11: Previous balance...
  Reading: loan104.sas7bdat - 623,910 rows, 24.0s
  SASLN: 20,995

STEP 12: Final merge + filter...
  WOFF pre-filter: 20,995
  WOFF after filter: 691

STEP 13: Writing outputs...
  LIST write skipped: module 'pyreadstat' has no attribute 'write_sas7bdat'

============================================================
SUMMARY
============================================================
Accounts: 691
Total exposure: RM 10,489,215.82
  wofftex1.txt: 522,396 bytes
  wofftext.txt: 540,362 bytes
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIFTXT1.py", line 736, in <module>
    'NOTENO': float(L[60:65]) if L[60:65].strip() else 0.0,
ValueError: could not convert string to float: '10A  '
