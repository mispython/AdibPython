Using report date: 31/03/26
Report month: 03-2026

======================================================================
Starting EIQPROM2 processing...
Report Date: 31/03/26
Report Month: 03
======================================================================

Step 1: Loading and filtering PROMOTE.LOAN data...
Loading file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIEMCRLS/loan03.sas7bdat
Total records in LOAN file: 180643

Analyzing key fields...

FLAG1 value counts (top 10):
  F: 180643

BORSTAT value counts (top 10):
  : 180640
  P: 2
  I: 1

DELQCD value counts (top 10):
  : 180201
  21: 435
  08: 7

EXCL value counts (top 10):
  : 180643

REINPROD value counts (top 10):
  N: 167172
  : 12319
  A: 1152

NEW value counts (top 10):
  : 177641
  Y: 3002

NEWIC statistics:
  Null: 0
  Empty: 0
  Non-empty: 180643

Records after REPAID > 100000: 62203
Records after removing empty GUAREND: 62144
Records after requiring NEWIC: 62144
Records after deduplication by GUAREND: 54275

Final records in RLSLIST: 54275

======================================================================
Step 2: Processing PBB data...
======================================================================
Total records in LN.LNNAME: 5534955
Records in PBBNAME after merge: 48953
Records in PBBNAME (non-email): 38763
Records in MAILPBB (email): 10190

Writing EMCPBB file...
EMCPBB records written: 38763

Processing MAILPBB email statements...
Writing EMLPBB file...
EMLPBB records written: 10190
Writing EMXPBB index file...
EMXPBB records written: 10190

======================================================================
Step 3: Processing PIB data...
======================================================================
Total records in LNI.LNNAME: 1661817
Records in PIBNAME after merge: 5323
Records in PIBNAME (non-email): 4242
Records in MAILPIB (email): 1081

Writing EMCPIB file...
EMCPIB records written: 4242

Processing MAILPIB email statements...
Writing EMLPIB file...
EMLPIB records written: 1081
Writing EMXPIB index file...
EMXPIB records written: 1081

======================================================================
Step 4: Generating report...
======================================================================
Total records for report: 43005
Report written to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIEMCRLS/eiqprom2_report.txt

======================================================================
EIEMCRLS processing completed successfully!
======================================================================
Report Date: 31/03/26
Data Month: 03-2026
Total non-email records (PBB + PIB): 43005
Total email records (PBB + PIB): 11271
======================================================================
