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

REPAID statistics:
  Type: Float64
  Min: -1681335.0300019996
  Max: 15003207.6379664
  Count > 100000: 62203

GUAREND statistics:
  Null count: 0
  Empty string count: 70
  Unique values: 157637

Records after REPAID > 100000 filter: 62203
Records after deduplication by GUAREND: 54276
Final records in RLSLIST: 54276

Date columns in LOAN file: ['EXPRDATE', 'ISSDTE', 'EXPDT']

EXPRDATE statistics:
  Min: 24197.0
  Max: 44879.0
  Null count: 0

ISSDTE statistics:
  Min: 13053.0
  Max: 23282.0
  Null count: 0

BLDAT statistics:
  Min: None
  Max: None
  Null count: 180643

BLPDDAT statistics:
  Min: None
  Max: None
  Null count: 180643

EXPDT statistics:
  Min: 35592.0
  Max: 744730.0
  Null count: 125801

HOLDEXPD statistics:
  Min: 
  Max: L
  Null count: 0

======================================================================
Step 2: Processing PBB data...
======================================================================
Total records in LN.LNNAME: 5534955
Unique ACCTNO in LN.LNNAME: 5534955

ACCTNO type in RLSLIST: Float64
ACCTNO type in LNNAME: Float64
Overlapping ACCTNO between RLSLIST and LNNAME: 48954

Records in PBBNAME after merge: 48954
Records in PBBNAME (non-email): 38764
Records in MAILPBB (email): 10190

Writing EMCPBB file...
EMCPBB records written: 38764

Processing MAILPBB email statements...
Writing EMLPBB file...
EMLPBB records written: 10190
Writing EMXPBB index file...
EMXPBB records written: 10190

======================================================================
Step 3: Processing PIB data...
======================================================================
Total records in LNI.LNNAME: 1661817
Unique ACCTNO in LNI.LNNAME: 1661817
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
Total records for report: 43006
Report written to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIEMCRLS/eiqprom2_report.txt

======================================================================
EIEMCRLS processing completed successfully!
======================================================================
Report Date: 31/03/26
Data Month: 03-2026
Total non-email records (PBB + PIB): 43006
Total email records (PBB + PIB): 11271
======================================================================
