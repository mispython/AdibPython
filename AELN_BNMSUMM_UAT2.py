BATCH MODE =  D
Daily
Initial record count from Parquet: 7885
NULL values in character columns handled
Using SAS Config named: default
SAS Connection established. Subprocess id is 541985

Libname assignment for bt: 
21   ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
21 ! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
22   
23   libname bt '/dwh/btrade_d';
NOTE: Libref BT was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /dwh/btrade_d
24   
25   ods html5 (id=saspy_internal) close;ods listing;

Libname assignment for ctl: 
27   ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
27 ! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
28   
29   libname ctl '/stgsrcsys/host/uat';
NOTE: Libref CTL was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /stgsrcsys/host/uat
30   
31   ods html5 (id=saspy_internal) close;ods listing;


132  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
132! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
133  
134  
135              proc sql noprint;
136                 create table colmeta as
137                 select name, type, length
138                 from dictionary.columns
139                 where libname = upcase("ctl")
140                       and memname = upcase("billstran_ctl");
NOTE: Table WORK.COLMETA created, with 22 rows and 3 columns.

141              quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.01 seconds
      
142  
143  
144  ods html5 (id=saspy_internal) close;ods listing;


Column Metadata:
           name  type  length
0       RECTYPE  char     2.0
1      TRANSREF  char    10.0
2       COSTCTR   num     8.0
3        ACCTNO  char    15.0
4       SUBACCT  char    13.0
5    GLMNEMONIC  char     5.0
6      LIABCODE  char     3.0
7      TRANDATE   num     8.0
8      EXPRDATE   num     8.0
9       TRANAMT   num     8.0
10     EXCHANGE   num     8.0
11     CURRENCY  char     4.0
12        BTREL  char    13.0
13      RELFROM  char    13.0
14   TRANSREFPG  char    10.0
15  TRANAMT_CCY   num     8.0
16    TRANS_NUM  char    10.0
17    TRANS_IND  char     3.0
18  MNEMONIC_CD  char     5.0
19    ACCT_INFO  char    20.0
20    CR_DR_IND  char     1.0
21  VOUCHER_NUM  char    10.0

Generated casted columns:
substr(coalescec(RECTYPE, ''), 1, 2) as RECTYPE
substr(coalescec(TRANSREF, ''), 1, 10) as TRANSREF
COSTCTR
substr(coalescec(ACCTNO, ''), 1, 15) as ACCTNO
substr(coalescec(SUBACCT, ''), 1, 13) as SUBACCT
Record counts before union: 
209  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
209! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
210  
211  
212          proc sql noprint;
213              select count(*) into :work_count from work.billstran_15;
214              select count(*) into :ctl_count from ctl.billstran_ctl;
215          quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.01 seconds
      
216          %put Work table count: &work_count;
Work table count:     7885
217          %put Control table count: &ctl_count;
Control table count:        0
218  
219  
220  ods html5 (id=saspy_internal) close;ods listing;

After transformation: 
222  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
222! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
223  
224  
225                  proc sql noprint;
226                       create table work.billstran_15_transformed as
227                       select substr(coalescec(RECTYPE, ''), 1, 2) as RECTYPE,
228   substr(coalescec(TRANSREF, ''), 1, 10) as TRANSREF,
229   COSTCTR,
230   substr(coalescec(ACCTNO, ''), 1, 15) as ACCTNO,
231   substr(coalescec(SUBACCT, ''), 1, 13) as SUBACCT,
232   substr(coalescec(GLMNEMONIC, ''), 1, 5) as GLMNEMONIC,
233   substr(coalescec(LIABCODE, ''), 1, 3) as LIABCODE,
234   TRANDATE,
235   EXPRDATE,
236   TRANAMT,
237   EXCHANGE,
238   substr(coalescec(CURRENCY, ''), 1, 4) as CURRENCY,
239   substr(coalescec(BTREL, ''), 1, 13) as BTREL,
240   substr(coalescec(RELFROM, ''), 1, 13) as RELFROM,
241   substr(coalescec(TRANSREFPG, ''), 1, 10) as TRANSREFPG,
242   TRANAMT_CCY,
243   substr(coalescec(TRANS_NUM, ''), 1, 10) as TRANS_NUM,
244   substr(coalescec(TRANS_IND, ''), 1, 3) as TRANS_IND,
245   substr(coalescec(MNEMONIC_CD, ''), 1, 5) as MNEMONIC_CD,
246   substr(coalescec(ACCT_INFO, ''), 1, 20) as ACCT_INFO,
247   substr(coalescec(CR_DR_IND, ''), 1, 1) as CR_DR_IND,
248   substr(coalescec(VOUCHER_NUM, ''), 1, 10) as VOUCHER_NUM from work.billstran_15;
NOTE: Invalid argument 3 to function SUBSTR. Missing values may be generated.
NOTE: Invalid argument 3 to function SUBSTR. Missing values may be generated.
NOTE: Invalid argument 3 to function SUBSTR. Missing values may be generated.
NOTE: Invalid argument 3 to function SUBSTR. Missing values may be generated.
NOTE: Invalid argument 3 to function SUBSTR. Missing values may be generated.
NOTE: Invalid argument 2 to function SUBSTR. Missing values may be generated.
NOTE: Invalid argument 2 to function SUBSTR. Missing values may be generated.
NOTE: Table WORK.BILLSTRAN_15_TRANSFORMED created, with 7885 rows and 22 columns.

249  
250                       select count(*) into :transformed_count from work.billstran_15_transformed;
251                  quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.06 seconds
      cpu time            0.02 seconds
      
252                  %put Transformed table count: &transformed_count;
Transformed table count:     7885
253  
254  
255  ods html5 (id=saspy_internal) close;ods listing;

Final table created: 
257  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
257! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
258  
259  
260                  proc sql noprint;
261                       create table bt.billstran_15 as
262                       select RECTYPE, TRANSREF, COSTCTR, ACCTNO, SUBACCT, GLMNEMONIC, LIABCODE, TRANDATE, EXPRDATE, TRANAMT,
262! EXCHANGE, CURRENCY, BTREL, RELFROM, TRANSREFPG, TRANAMT_CCY, TRANS_NUM, TRANS_IND, MNEMONIC_CD, ACCT_INFO, CR_DR_IND,
262! VOUCHER_NUM from ctl.billstran_ctl
263                       union all corr
264                       select RECTYPE, TRANSREF, COSTCTR, ACCTNO, SUBACCT, GLMNEMONIC, LIABCODE, TRANDATE, EXPRDATE, TRANAMT,
264! EXCHANGE, CURRENCY, BTREL, RELFROM, TRANSREFPG, TRANAMT_CCY, TRANS_NUM, TRANS_IND, MNEMONIC_CD, ACCT_INFO, CR_DR_IND,
264! VOUCHER_NUM from work.billstran_15_transformed;
NOTE: Table BT.BILLSTRAN_15 created, with 7885 rows and 22 columns.

265  
266                       select count(*) into :final_count from bt.billstran_15;
267                  quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.06 seconds
      cpu time            0.03 seconds
      
268                  %put Final table count: &final_count;
Final table count:     7885
269  
270  
271  ods html5 (id=saspy_internal) close;ods listing;

Process completed successfully!
SAS Connection terminated. Subprocess id was 541985
