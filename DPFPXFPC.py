REPTMON: 07, RDATE: 310726
Read 771 records from lntrrf07.sas7bdat
Available columns: ['curbal', 'costctr', 'accrual', 'balance', 'product', 'censust', 'cinstcl', 'natguar', 'cgcgur', 'tranche', 'sch', 'cvar02', 'cvar01', 'cvar06', 'cvar03', 'cvar04', 'cvar14', 'cvar13', 'cvar08', 'cvar09', 'cvar10', 'cvar17', 'cvar11', 'cvar05', 'cvar07', 'cvar12', 'cvar15', 'branch', 'cvar16']
TRRF dataset created with 31 records
NPGS dataset processed with 771 records
Processing 576 records for SCH=7q
  CSV written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRTRRF/sc7t.csv
  Text file written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRTRRF/sc7t.txt
  Parquet written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRTRRF/sc7t.parquet
SAS Connection established. Subprocess id is 3211773

SAS Connection terminated. Subprocess id was 3211773
  SAS dataset written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRTRRF/sc7t.sas7bdat
============================================================
PUBLIC BANK BERHAD
DETAIL OF ACCTS (SCH=7Q) FOR SUBMISSION TO CGC @ 310726
============================================================
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRTRRF.py", line 345, in <module>
    eibrtrrf()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRTRRF.py", line 113, in eibrtrrf
    process_scheme(npgs_df, "7q", rdate, output)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRTRRF.py", line 225, in process_scheme
    npgs5_report(npgs5_df, rdate, report_file)
TypeError: npgs5_report() missing 1 required positional argument: 'title2'
