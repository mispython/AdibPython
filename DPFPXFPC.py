Report Date: 30-Jun-2026

Loading SAS datasets...
  Loading PBB_ALM_CR...
    ✓ Loaded 244,956 records with 4 columns
  Loading PBB_MAST_BR...
    ✓ Loaded 3,757 records with 3 columns
  Loading PIBB_ALM_CR...
    ✓ Loaded 35,156 records with 4 columns
  Loading PIBB_MAST_BR...
    ✓ Loaded 411 records with 3 columns

Preparing PBB data...
  Combined: 248,713 records
  After filter: 192,548 records

Preparing PIBB data...
  Combined: 35,567 records
  After filter: 34,329 records

Combining datasets...
  Total combined: 226,877 records
  Final columns: ACCTNO, NOTENO, PRODESC, REPTDATE

Writing Parquet output to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBNMMFR/MFRS.parquet
  ✓ Parquet file size: 1.51 MB

Creating SAS dataset using saspy...
  Starting SAS session...
Using SAS Config named: default
SAS Connection established. Subprocess id is 2873024

  Writing 226,877 records to SAS dataset...
  ✗ SAS dataset not found at: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBNMMFR/MFRS.sas7bdat
  Checking for alternative filenames...
    Found: mfrs.sas7bdat
    Renaming mfrs.sas7bdat to MFRS.sas7bdat...
  ✓ Renamed to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBNMMFR/MFRS.sas7bdat
SAS Connection terminated. Subprocess id was 2873024

======================================================================
EIBNMMFR job completed successfully!
======================================================================
Report Date    : 30-Jun-2026
Total records  : 226,877
Columns        : ACCTNO, NOTENO, PRODESC, REPTDATE
----------------------------------------------------------------------
Output files:
  ✓ Parquet : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBNMMFR/MFRS.parquet
    Size: 1.51 MB
  ✓ SAS     : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBNMMFR/MFRS.sas7bdat
    Size: 10.56 MB
======================================================================
