============================================================
EIBMRNID - EXACT SAS OUTPUT
============================================================
Report Date: 30/11/2025
Start Date: 01/11/2025

📂 Processing data...
  Original NID records: 580
  Columns: ['nid_acctno', 'nid_refno', 'acctno', 'nid_cdno', 'custname', 'newic', 'branch', 'custcode', 'trancheno', 'startdt', 'matdt', 'intrate', 'intpltdrate', 'curbal', 'lstintpaydt', 'nxtintpaydt', 'intfrq', 'nidstat', 'accint', 'cdstat', 'poststat', 'early_wddt', 'early_wdproc', 'early_telrid', 'early_offid1', 'early_offid2', 'cancdt', 'canc_telrid', 'canc_offid1', 'canc_offid2', 'appldt', 'appldttm', 'appl_telrid', 'appl_offid1', 'appl_offid2', 'printdt', 'print_telrid', 'print_offid1', 'print_offid2', 'maintdt', 'maint_telrid', 'maint_offid1', 'maint_offid2', 'pledgestat', 'nid_odacctno', 'ccollno', 'pledgedt', 'pledge_telrid', 'pledge_offid1', 'pledge_offid2', 'cpledgedt', 'cpledge_telrid', 'cpledge_offid1', 'cpledge_offid2', 'term', 'suitability_assessment_cd', 'sales_staff_id', 'termid', 'intpd', 'costctr', 'product', 'brabbr', 'curcode', 'custcd']
  Merged with TRNCH

🔍 Checking data quality...
  NIDSTAT distribution:
    M: 403
    N: 28
    E: 25
    O: 44
    W: 50
    C: 30
  CDSTAT distribution:
    C: 241
    A: 339
  Converted date columns: ['matdt', 'startdt', 'early_wddt']
  Records with positive balance: 580 (from 580)

💾 Saving processed data to Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/Job/Output/EIBMRNID_DATA.parquet
  Saved 580 records to Parquet

📊 Creating Table 1...
  Table 1 filtered records: 0
  Table 1 records: 0

📊 Creating Table 2...
  Table 2 - NID Count: 0, Volume: 0.00

📊 Creating Table 3...
  Table 3 filtered records: 0
  Warning: No records found for Table 3 yield calculation

💾 Writing SAS-format output to: /sas/python/virt_edw/Data_Warehouse/MIS/Job/Output/EIBMRNID_REPORT.TXT

✅ SAS report generated: /sas/python/virt_edw/Data_Warehouse/MIS/Job/Output/EIBMRNID_REPORT.TXT
✅ Parquet data saved: /sas/python/virt_edw/Data_Warehouse/MIS/Job/Output/EIBMRNID_DATA.parquet

📊 Final Summary:
  Total processed records: 580
  Table 1 (Outstanding): 0 records
  Table 2 (Trading): 0 NIDs, RM 0.00
  Table 3 (Yield): 0.0000% overall

📁 Output folder: /sas/python/virt_edw/Data_Warehouse/MIS/Job/Output
