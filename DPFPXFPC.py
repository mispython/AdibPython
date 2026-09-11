Step 1: Setting report date...
Report Date: 2026-09-08 11:07:04.183208, RDATE: 24357
COLL_FILE: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831
DESC_FILE: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831

Step 1b: Converting EBCDIC files to sas7bdat via SAS...
SAS Connection established. Subprocess id is 882027

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS inspection result:

21   ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
21 ! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
22   
23   
24       /* Try reading as variable-length records (RDW) first */
25       filename raw1 "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831" recfm=vb lrecl=32760;
25       filename raw1 "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831" recfm=vb lrecl=32760;
                                                                                                                  --
                                                                                                                  24
ERROR 24-2: Invalid value for the recfm option.

ERROR: Error in the FILENAME statement.
26       data coll_work;
27           infile raw1 obs=1000000;
28           length rec $400;
29           input rec $char400.;
30       run;
ERROR: No logical assign for filename RAW1.
NOTE: The SAS System stopped processing this step because of errors.
WARNING: The data set WORK.COLL_WORK may be incomplete.  When this step was stopped there were 0 observations and 1 variables.
NOTE: DATA statement used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
31   
32       proc contents data=coll_work; run;
NOTE: PROCEDURE CONTENTS used (Total process time):
      real time           0.02 seconds
      cpu time            0.01 seconds
      
33   
34   
35   ods html5 (id=saspy_internal) close;ods listing;

SAS Connection terminated. Subprocess id was 882027
Stopped for inspection - check SAS log above to determine correct RECFM/LRECL
