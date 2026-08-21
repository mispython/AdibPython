Report Date: 200826 (20-08-26)
DPDATE: 200826 EQDATE: 200826
DPST records: 90326
EQTN records: 532
DCID records after merge and filter: 266
Reading reference files...
Reference data records: 5229286
Final records: 266
✓ Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDCIA/dcid0820.parquet
✓ Saved CSV: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDCIA/dcid0820.csv
Connecting to SAS...
Using SAS Config named: default
SAS Connection established. Subprocess id is 1300319

Uploading data to SAS...
Saving to DCI library...
✓ Saved SAS dataset: DCI.DCID0820
Saving to TEMP library...
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
⚠ Warning: Errors found in SAS LOG for TEMP library save
SAS LOG: 
137  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
137! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
138  
139  
140              LIBNAME TEMP '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDCIA/temp';
NOTE: Library TEMP does not exist.
141              DATA TEMP.DCID260820;
142                  SET work_dcid0820;
143              RUN;
ERROR: Library TEMP does not exist.
NOTE: The SAS System stopped processing this step because of errors.
NOTE: DATA statement used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
144  
145  
146  ods html5 (id=saspy_internal) close;ods listing;

SAS Connection terminated. Subprocess id was 1300319
SAS session closed.

✅ Processing completed successfully!
Output files:
  - /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDCIA/dcid0820.parquet
  - /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDCIA/dcid0820.csv


no need the csv file. only sas and parquet files. sas dataset is important
