KALMLIQ loaded from: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/KALMLIQ.py
============================================================
EIIDLCRM - BNM LCR Reporting (Islamic Banking)
============================================================
SAS Connection established. Subprocess id is 4178711


Date: 06/10/2026 Week:1 Mon:10
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
    [build_kalmliq] WARNING: 1242 MATDT values unparseable (defaulted to 0.1)
    [build_kalmliq] ktbl rows: 2484
  UTSAS warning: File /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/equa/iutms261006.sas7bdat does not exist!
  UTSAS records: 0
  Raw k_records: 2484
  Treasury: 2484 records

Banking...
  fd: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcr/fd06.sas7bdat
  fd warning: File /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcr/fd06.sas7bdat does not exist!
  sa: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcr/sa06.sas7bdat
  sa warning: File /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcr/sa06.sas7bdat does not exist!
  ca: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcr/ca06.sas7bdat
  ca warning: File /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcr/ca06.sas7bdat does not exist!
  fcyca: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcr/fcyca06.sas7bdat
  fcyca warning: File /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcr/fcyca06.sas7bdat does not exist!
  Banking: 0 records

Insurance split...
Total: 2484 records
Summary: 22 codes

  report_data rows: 6
  distinct col values: ['IBB9X810V1', 'NID95840V1', 'STD95830V1']
  out_df shape: (70, 11)
  out_df columns: ['item', 'idesc', 'IBB9X810V1', 'STD95830V1', 'NID95840V1', 'STD95830', 'NID95840', 'IBB9X810', 'TOTALV1', 'TOTALDP', 'OTHSOURCE']
============================================================
SAS LOG -- write_sas7bdat -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLCRM/lcr06.sas7bdat:

81   ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
81 ! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
82   
83   
84           libname _outdir "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLCRM";
NOTE: Libref _OUTDIR was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLCRM
85           data _outdir.lcr06;
86               set WORK._tmp_out;
87           run;
NOTE: There were 70 observations read from the data set WORK._TMP_OUT.
NOTE: The data set _OUTDIR.LCR06 has 70 observations and 11 variables.
NOTE: DATA statement used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
88           libname _outdir clear;
NOTE: Libref _OUTDIR has been deassigned.
89   
90   
91   ods html5 (id=saspy_internal) close;ods listing;

============================================================
Report (sas7bdat): lcr06.sas7bdat
============================================================
SAS LOG -- write_text_file -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLCRM/lcr06.txt:

153  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
153! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
154  
155  
156          data _null_;
157              file "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLCRM/lcr06.txt";
158                  put "PUBLIC ISLAMIC BANK BERHAD";
159      put "LIQUIDITY COVERAGE RATIO (LCR) AS AT 061026";
160      put "";
161              set WORK._tmp_txt;
162              put _all_;
163          run;
NOTE: The file "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLCRM/lcr06.txt" is:
      Filename=/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLCRM/lcr06.txt,
      Owner Name=sas_edw_dev,
      Group Name=sas_edw_dev_grp,
      Access Permission=-rw-rw-r--,
      Last Modified=07Oct2026:10:12:30

NOTE: 283 records were written to the file "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLCRM/lcr06.txt".
      The minimum record length was 1.
      The maximum record length was 271.
NOTE: There were 70 observations read from the data set WORK._TMP_TXT.
NOTE: DATA statement used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
164  
165  
166  ods html5 (id=saspy_internal) close;ods listing;

============================================================
Report (text): lcr06.txt

Total: RM 58,791,681K
============================================================
EIIDLCRM Complete
SAS Connection terminated. Subprocess id was 4178711
