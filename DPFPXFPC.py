REPTMON=09, NOWK=4, REPTYEAR=2026, RDATE=30/09/26, SDATE=23/09/26, SUFFIX=094
L124PBBD DEBUG: reptmon='09' nowk='4' sfx='094'
L124PBBD: reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1/loan094.sas7bdat ...
WARNING: 'l124094' has 0 rows — writing empty dataset.
SAS Connection established. Subprocess id is 3439408

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1839: UserWarning: Note that Indexes are not transferred over as columns. Only actual columns are transferred
  warnings.warn("Note that Indexes are not transferred over as columns. Only actual columns are transferred")
SAS Connection terminated. Subprocess id was 3439408
L124 written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/l124094.sas7bdat  (0 rows)
L124PBBD: reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1/uloan094.sas7bdat ...
WARNING: 'ul124094' has 0 rows — writing empty dataset.
SAS Connection established. Subprocess id is 3439458

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1839: UserWarning: Note that Indexes are not transferred over as columns. Only actual columns are transferred
  warnings.warn("Note that Indexes are not transferred over as columns. Only actual columns are transferred")
SAS Connection terminated. Subprocess id was 3439458
UL124 written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/ul124094.sas7bdat  (0 rows)
WARNING: 'lalw094' has 0 rows — writing empty dataset.
SAS Connection established. Subprocess id is 3439488

SAS Connection terminated. Subprocess id was 3439488
LALW written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/lalw094.sas7bdat  (0 rows)
SAS Connection established. Subprocess id is 3439536

SAS Connection terminated. Subprocess id was 3439536
ALW copied: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnmx/alw094.sas7bdat -> /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/alw094.sas7bdat (302 rows)
P124RDAL DEBUG: REPTMON='09' NOWK='4' sfx='094'
SAS Connection established. Subprocess id is 3439570

=== SAS log: LIBNAME+DATA -> PBBRDAL.sas7bdat ===

70   ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
70 ! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
71   
72   
73               LIBNAME _outlib "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output";
NOTE: Libref _OUTLIB was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output
74               DATA _outlib.PBBRDAL;
75                   SET WORK.PBBRDAL;
76               RUN;
NOTE: There were 44 observations read from the data set WORK.PBBRDAL.
NOTE: The data set _OUTLIB.PBBRDAL has 44 observations and 2 variables.
NOTE: DATA statement used (Total process time):
      real time           0.00 seconds
      cpu time            0.01 seconds
      
77               LIBNAME _outlib CLEAR;
NOTE: Libref _OUTLIB has been deassigned.
78   
79   
80   ods html5 (id=saspy_internal) close;ods listing;

=== SAS log: PROC EXPORT txt -> PBBRDAL.txt ===

82   ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
82 ! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
83   
84   
85               PROC EXPORT DATA=WORK.PBBRDAL
86                   OUTFILE="/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output/PBBRDAL.txt"
87                   DBMS=DLM REPLACE;
88                   DELIMITER=';';
89               RUN;
NOTE: Unable to open parameter catalog: SASUSER.PARMS.PARMS.SLIST in update mode. Temporary parameter values will be saved to 
WORK.PARMS.PARMS.SLIST.
NOTE: Unable to open SASUSER.PROFILE. WORK.PROFILE will be opened instead.
NOTE: All profile changes will be lost at the end of the session.
90    /**********************************************************************
91    *   PRODUCT:   SAS
92    *   VERSION:   9.4
93    *   CREATOR:   External File Interface
94    *   DATE:      02OCT26
95    *   DESC:      Generated SAS Datastep Code
96    *   TEMPLATE SOURCE:  (None Specified.)
97    ***********************************************************************/
98       data _null_;
99       %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
100      %let _EFIREC_ = 0;     /* clear export record count macro variable */
101      file '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output/PBBRDAL.txt' delimiter=';' DSD DROPOVER
101! lrecl=32767;
102      if _n_ = 1 then        /* write column names or labels */
103       do;
104         put
105            "itcode"
106         ';'
107            "amount"
108         ;
109       end;
110     set  WORK.PBBRDAL   end=EFIEOD;
111         format itcode $14. ;
112         format amount best12. ;
113       do;
114         EFIOUT + 1;
115         put itcode $ @;
116         put amount ;
117         ;
118       end;
119      if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
120      if EFIEOD then call symputx('_EFIREC_',EFIOUT);
121      run;
NOTE: The file '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output/PBBRDAL.txt' is:
      Filename=/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output/PBBRDAL.txt,
      Owner Name=sas_edw_dev,
      Group Name=sas_edw_dev_grp,
      Access Permission=-rw-rw-r--,
      Last Modified=02Oct2026:12:11:24

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
      
122  
123  
124  ods html5 (id=saspy_internal) close;ods listing;

SAS Connection terminated. Subprocess id was 3439570
PBBMRDLF: wrote /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output/pbbrdal.sas7bdat (44 records)
Streaming LNNOTE: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m09.sas7bdat  (chunksize=1,000,000)
Cagamas summary rows: 0
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/P124RDAL.py:329: FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated. In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes. To retain the old behavior, exclude the relevant entries before the concat operation.
  rdal_df = pd.concat([rdal_df, cag_summary], ignore_index=True, sort=False)
RDAL output written to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWP124/rdal.txt
