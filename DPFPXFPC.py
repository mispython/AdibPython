KALMLIQ loaded from: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/KALMLIQ.py
============================================================
EIIDLCRM - BNM LCR Reporting (Islamic Banking)
============================================================
SAS Connection established. Subprocess id is 3997317


Date: 05/10/2026 Week:1 Mon:10
Template: 70 items
  CIS file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/cis/CIS_CUST_DAILY.parquet
CIS: 17611 records

Treasury...
  k1tbl: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/bnmk/k1tbl101.sas7bdat
  k3tbl: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/bnmk/k3tbl101.sas7bdat
  k1tbl exists: True
  k3tbl exists: True
    [_build_k1tbl] reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/bnmk/k1tbl101.sas7bdat
    [_build_k1tbl] columns: ['REPTDATE', 'GWAB', 'GWAN', 'GWAS', 'GWAPP', 'GWACS', 'GWBALA', 'GWBALC', 'GWPAIA', 'GWPAIC', 'GWSHN', 'GWCTP', 'GWACT', 'GWACD', 'GWSAC', 'GWNANC', 'GWCNAL', 'GWCCY', 'GWCNAR', 'GWCNAP', 'GWDIAA', 'GWDIAC', 'GWCIAA', 'GWCIAC', 'GWRATD', 'GWRATC', 'GWDIPA', 'GWDIPC', 'GWCIPA', 'GWCIPC', 'GWPL1D', 'GWPL2D', 'GWPL1C', 'GWPL2C', 'GWPALA', 'GWPALC', 'GWDLP', 'GWDLR', 'GWSDT', 'GWRDT', 'GWRRT', 'GWPDT', 'GWPRT', 'GWPCM', 'GWMOTC', 'GWMRTC', 'GWMRT', 'GWMDT', 'GWMCM', 'GWMWM', 'GWMVT', 'GWMVTS', 'GWSRC', 'GWUC1', 'GWUC2', 'GWC2R', 'GWAMAP', 'GWEXR', 'GWOPT', 'GWOCY', 'GWCBD']
    [_build_k1tbl] rows: 753
    [_build_k1tbl] GWMDT sample: [None, 24471.0, 24441.0]
    [_build_k1tbl] GWMDT dtype: Float64
    [_build_k1tbl] after filter GWMVT='P': 752
    [_build_k1tbl] emitted rows: 741
    [_build_k3tbl] reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/bnmk/k3tbl101.sas7bdat
    [_build_k3tbl] columns: ['REPTDATE', 'UTSTY', 'UTREF', 'UTBRNM', 'UTDLP', 'UTDLR', 'UTSMN', 'UTCUS', 'UTCLC', 'UTCTP', 'UTFCV', 'UTIDT', 'UTLCD', 'UTNCD', 'UTMDT', 'UTCBD', 'UTCPR', 'UTQDS', 'UTPCP', 'UTAMOC', 'UTDPF', 'UTAICT', 'UTAICY', 'UTAIT', 'UTDPET', 'UTDPEY', 'UTDPE', 'UTASN', 'UTOSD', 'UTCA2', 'UTSAC', 'UTCNAP', 'UTCNAR', 'UTCNAL', 'UTCCY', 'UTAMTS', 'UTMM1', 'MATDT', 'ISSDT', 'DDATE', 'XDATE']
    [_build_k3tbl] rows: 2297
    [_build_k3tbl] MATDT sample: [None, 24997.0, 25056.0]
    [_build_k3tbl] MATDT dtype: Float64
    [_build_k3tbl] emitted rows: 501
    [build_kalmliq] ktbl rows: 2484
  Raw k_records: 2484
  UTSAS records: 3003
  Treasury: 2484 records

Banking...
  fd: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcr/fd05.sas7bdat
  sa: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcr/sa05.sas7bdat
  ca: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcr/ca05.sas7bdat
  fcyca: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcr/fcyca05.sas7bdat
  Banking: 2783004 records

Insurance split...
Total: 2991741 records
Summary: 41 codes
============================================================
SAS LOG -- write_sas7bdat -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLCRM/lcr05.sas7bdat:

76   ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
76 ! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
77   
78   
79           libname _outdir "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLCRM";
NOTE: Libref _OUTDIR was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLCRM
80   
81           data _outdir.lcr05;
82               set WORK._tmp_out;
83           run;
NOTE: There were 70 observations read from the data set WORK._TMP_OUT.
NOTE: The data set _OUTDIR.LCR05 has 70 observations and 6 variables.
NOTE: DATA statement used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
84   
85           libname _outdir clear;
NOTE: Libref _OUTDIR has been deassigned.
86   
87   
88   ods html5 (id=saspy_internal) close;ods listing;

============================================================
Report (sas7bdat): lcr05.sas7bdat
============================================================
SAS LOG -- write_text_file -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLCRM/lcr05.txt:

145  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
145! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
146  
147  
148          data _null_;
149              file "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLCRM/lcr05.txt";
150                  put "PUBLIC ISLAMIC BANK BERHAD";
151      put "LIQUIDITY COVERAGE RATIO (LCR) AS AT 051026";
152      put "";
153              set WORK._tmp_txt;
154              put _all_;
155          run;
NOTE: The file "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLCRM/lcr05.txt" is:
      Filename=/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLCRM/lcr05.txt,
      Owner Name=sas_edw_dev,
      Group Name=sas_edw_dev_grp,
      Access Permission=-rw-rw-r--,
      Last Modified=06Oct2026:18:43:16

NOTE: 283 records were written to the file "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLCRM/lcr05.txt".
      The minimum record length was 1.
      The maximum record length was 194.
NOTE: There were 70 observations read from the data set WORK._TMP_TXT.
NOTE: DATA statement used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
156  
157  
158  ods html5 (id=saspy_internal) close;ods listing;

============================================================
Report (text): lcr05.txt

Total: RM 142,009,936K
============================================================
EIIDLCRM Complete
SAS Connection terminated. Subprocess id was 3997317
