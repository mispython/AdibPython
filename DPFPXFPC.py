Loading SAS datasets...
Preparing PBB data...
Preparing PIBB data...
Combining datasets...
Using saspy to write SAS dataset...
Using SAS Config named: default
SAS Connection established. Subprocess id is 2872535

SAS Connection terminated. Subprocess id was 2872535
ERROR: SAS dataset not found at /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBNMMFR/MFRS.sas7bdat

Writing Parquet output to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBNMMFR/MFRS.parquet

Attempting alternative method: CSV + SAS script...
Created SAS conversion script: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBNMMFR/convert_csv_to_sas.sas
To create SAS dataset, run: sas /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBNMMFR/convert_csv_to_sas.sas
Or manually import the CSV file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBNMMFR/temp_dtlfpt.csv

============================================================
EIBNMMFR job completed successfully!
============================================================
Report Date    : 30-Jun-2026
Total records  : 226,877
Columns        : ACCTNO, NOTENO, PRODESC, REPTDATE
✗ SAS output     : Not created (see alternative method above)
✓ Parquet output : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBNMMFR/MFRS.parquet
============================================================
