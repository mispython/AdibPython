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

Records after REPAID > 100000: 62203

REINPROD distribution after REPAID filter:
  REINPROD='N': 58687 records
  REINPROD='': 2893 records
  REINPROD='A': 623 records

Records after filtering empty REINPROD: 59310
Records after removing empty GUAREND: 59257
Records after deduplication by GUAREND: 51730

Final records in RLSLIST: 51730

======================================================================
Step 2: Processing PBB data...
======================================================================
Total records in LN.LNNAME: 5534955
Records in PBBNAME after merge: 46435
Records in PBBNAME (non-email): 36608
Records in MAILPBB (email): 9827

Writing EMCPBB file...
EMCPBB records written: 36608

Processing MAILPBB email statements...
Writing EMLPBB file...
EMLPBB records written: 9827
Writing EMXPBB index file...
EMXPBB records written: 9827

======================================================================
Step 3: Processing PIB data...
======================================================================
Total records in LNI.LNNAME: 1661817
Records in PIBNAME after merge: 5296
Records in PIBNAME (non-email): 4220
Records in MAILPIB (email): 1076

Writing EMCPIB file...
EMCPIB records written: 4220

Processing MAILPIB email statements...
Writing EMLPIB file...
EMLPIB records written: 1076
Writing EMXPIB index file...
EMXPIB records written: 1076

======================================================================
Step 4: Generating report...
======================================================================
Total records for report: 40828
Report written to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIEMCRLS/eiqprom2_report.txt

======================================================================
EIEMCRLS processing completed successfully!
======================================================================
Report Date: 31/03/26
Data Month: 03-2026
Total non-email records (PBB + PIB): 40828
Total email records (PBB + PIB): 10903
======================================================================
