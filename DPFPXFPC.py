REPTMON=09, NOWK=4, REPTYEAR=2026, RDATE=30/09/26, SDATE=23/09/26, SUFFIX=094
L124PBBD DEBUG: reptmon='09' nowk='4' sfx='094'
L124PBBD: reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1/loan094.sas7bdat ...
WARNING: 'l124094' has 0 rows — writing empty dataset.
SAS Connection established. Subprocess id is 3427937

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1839: UserWarning: Note that Indexes are not transferred over as columns. Only actual columns are transferred
  warnings.warn("Note that Indexes are not transferred over as columns. Only actual columns are transferred")
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS Connection terminated. Subprocess id was 3427937
L124 written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/l124094.sas7bdat  (0 rows)
L124PBBD: reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1/uloan094.sas7bdat ...
WARNING: 'ul124094' has 0 rows — writing empty dataset.
SAS Connection established. Subprocess id is 3427985

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1839: UserWarning: Note that Indexes are not transferred over as columns. Only actual columns are transferred
  warnings.warn("Note that Indexes are not transferred over as columns. Only actual columns are transferred")
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS Connection terminated. Subprocess id was 3427985
UL124 written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/ul124094.sas7bdat  (0 rows)
SAS Connection established. Subprocess id is 3428015

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS Connection terminated. Subprocess id was 3428015
LALW written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/lalw094.sas7bdat  (1 rows)
SAS Connection established. Subprocess id is 3428053

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS Connection terminated. Subprocess id was 3428053
Wrote /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/alw094.sas7bdat and /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/alw094.txt (302 rows)
ALW copied from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnmx/alw094.sas7bdat to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/alw094.sas7bdat (302 rows)
P124RDAL DEBUG: REPTMON='09' NOWK='4' sfx='094'
SAS Connection established. Subprocess id is 3428088

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
=== SAS log: PROC EXPORT sas7bdat ===

70   ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
70 ! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
71   
72   
73               PROC EXPORT DATA=WORK.PBBRDAL
74                   OUTFILE="/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output/PBBRDAL.sas7bdat"
75                   DBMS=SAS7BDAT REPLACE;
ERROR: DBMS type SAS7BDAT not valid for export.
NOTE: The SAS System stopped processing this step because of errors.
NOTE: PROCEDURE EXPORT used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
76               RUN;
77   
78   
79   ods html5 (id=saspy_internal) close;ods listing;

=== SAS log: PROC EXPORT txt ===

81   ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
81 ! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
82   
83   
84               PROC EXPORT DATA=WORK.PBBRDAL
85                   OUTFILE="/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output/PBBRDAL.txt"
86                   DBMS=DLM REPLACE;
87                   DELIMITER=';';
88               RUN;
NOTE: Unable to open parameter catalog: SASUSER.PARMS.PARMS.SLIST in update mode. Temporary parameter values will be saved to 
WORK.PARMS.PARMS.SLIST.
NOTE: Unable to open SASUSER.PROFILE. WORK.PROFILE will be opened instead.
NOTE: All profile changes will be lost at the end of the session.
89    /**********************************************************************
90    *   PRODUCT:   SAS
91    *   VERSION:   9.4
92    *   CREATOR:   External File Interface
93    *   DATE:      02OCT26
94    *   DESC:      Generated SAS Datastep Code
95    *   TEMPLATE SOURCE:  (None Specified.)
96    ***********************************************************************/
97       data _null_;
98       %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
99       %let _EFIREC_ = 0;     /* clear export record count macro variable */
100      file '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output/PBBRDAL.txt' delimiter=';' DSD DROPOVER
100! lrecl=32767;
101      if _n_ = 1 then        /* write column names or labels */
102       do;
103         put
104            "itcode"
105         ';'
106            "amount"
107         ;
108       end;
109     set  WORK.PBBRDAL   end=EFIEOD;
110         format itcode $14. ;
111         format amount best12. ;
112       do;
113         EFIOUT + 1;
114         put itcode $ @;
115         put amount ;
116         ;
117       end;
118      if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
119      if EFIEOD then call symputx('_EFIREC_',EFIOUT);
120      run;
NOTE: The file '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output/PBBRDAL.txt' is:
      Filename=/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output/PBBRDAL.txt,
      Owner Name=sas_edw_dev,
      Group Name=sas_edw_dev_grp,
      Access Permission=-rw-rw-r--,
      Last Modified=02Oct2026:11:05:01

NOTE: 45 records were written to the file '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output/PBBRDAL.txt'.
      The minimum record length was 13.
      The maximum record length was 16.
NOTE: There were 44 observations read from the data set WORK.PBBRDAL.
NOTE: DATA statement used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
44 records created in /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output/PBBRDAL.txt from WORK.PBBRDAL.
  
  
NOTE: "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output/PBBRDAL.txt" file was successfully created.
NOTE: PROCEDURE EXPORT used (Total process time):
      real time           0.03 seconds
      cpu time            0.02 seconds
      
121  
122  
123  ods html5 (id=saspy_internal) close;ods listing;

SAS Connection terminated. Subprocess id was 3428088
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWP124.py", line 223, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWP124.py", line 219, in main
    run_p124rdal()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/P124RDAL.py", line 260, in main
    import PBBMRDLF  # noqa: F401
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/PBBMRDLF.py", line 137, in <module>
    build()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/PBBMRDLF.py", line 128, in build
    raise RuntimeError(
RuntimeError: PBBMRDLF: PROC EXPORT did not create /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output/PBBRDAL.sas7bdat. See the SAS log printed above for the actual ERROR.
