
============================================================
ESMR 2009-16 (SEND FILE TO LNS) - 3RD EXTRACTION
============================================================
Report Date: 25/11/2026  Week: 4  Prev Month: 10  RIND: I
Output Dir : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT3
============================================================

STEP 1: IIS / SP2...
  Reading: iis.sas7bdat - 135,725 rows, 0.9s
  Reading: sp2.sas7bdat - 135,725 rows, 1.1s
  IIS: 135,725  SP2: 135,725
  WARNING: IIS and SP2 have identical row counts (135,725). Verify: iis.sas7bdat vs sp2.sas7bdat
  NPL: 135,725 rows

STEP 2: LNNOTE (LOANTYPE IN &HPD)...
  Reading FULL enrh_ln_note.sas7bdat (33 cols)...
  Loaded 8,149,154 rows in 814.2s  RSS 3765MB
  Converted to Polars, RSS 2905MB
  Filtered to 5,501,509 rows, RSS 4459MB
  LNNOTE: 5,501,509 rows

STEP 3: MERGE NPL(IN=AA) LOAN...
  LOAN after merge: 135,725

STEP 4: Derived fields...
  LOAN ready: 135,725

STEP 5: Customer names...
  Reading: loan.sas7bdat - 13,557,834 rows, 746.0s
  CNAME: 12,580,070

STEP 6: Guarantors...
  Reading: stg_ln_liabfile.sas7bdat - 2,504,468 rows, 8.2s
  Guarantor keys: 2,180,257

STEP 7: Previous balance...
  Reading: loan104.sas7bdat - 623,910 rows, 10.3s
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIFTXT3.py", line 507, in <module>
    gd = guarantor_data.get((r['ACCTNO'], r['NOTENO']),
NameError: name 'guarantor_data' is not defined
