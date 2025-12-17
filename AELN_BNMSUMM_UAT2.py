BATCH MODE =  D
Today's date: 2025-12-17 10:21:48.266991
Batch date: 2025-12-16 10:21:48.266930
Day string: 16
Daily
Using SAS Config named: default
SAS Connection established. Subprocess id is 612966


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
      real time           0.01 seconds
      cpu time            0.01 seconds
      
142  
143  
144  ods html5 (id=saspy_internal) close;ods listing;

Final table created: 
209  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
209! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
210  
211  
212                  proc sql noprint;
213                       create table bt.billstran_16 as
214                       select RECTYPE, TRANSREF, COSTCTR, ACCTNO, SUBACCT, GLMNEMONIC, LIABCODE, TRANDATE, EXPRDATE, TRANAMT,
214! EXCHANGE, CURRENCY, BTREL, RELFROM, TRANSREFPG, TRANAMT_CCY, TRANS_NUM, TRANS_IND, MNEMONIC_CD, ACCT_INFO, CR_DR_IND,
214! VOUCHER_NUM from ctl.billstran_ctl
215                       union all corr
216                       select input(trim(RECTYPE), $2.) as RECTYPE,
217   input(trim(TRANSREF), $10.) as TRANSREF,
218   COSTCTR,
219   input(trim(ACCTNO), $15.) as ACCTNO,
220   input(trim(SUBACCT), $13.) as SUBACCT,
221   input(trim(GLMNEMONIC), $5.) as GLMNEMONIC,
222   input(trim(LIABCODE), $3.) as LIABCODE,
223   TRANDATE,
224   EXPRDATE,
225   TRANAMT,
226   EXCHANGE,
227   input(trim(CURRENCY), $4.) as CURRENCY,
228   input(trim(BTREL), $13.) as BTREL,
229   input(trim(RELFROM), $13.) as RELFROM,
230   input(trim(TRANSREFPG), $10.) as TRANSREFPG,
231   TRANAMT_CCY,
232   input(trim(TRANS_NUM), $10.) as TRANS_NUM,
233   input(trim(TRANS_IND), $3.) as TRANS_IND,
234   input(trim(MNEMONIC_CD), $5.) as MNEMONIC_CD,
235   input(trim(ACCT_INFO), $20.) as ACCT_INFO,
236   input(trim(CR_DR_IND), $1.) as CR_DR_IND,
237   input(trim(VOUCHER_NUM), $10.) as VOUCHER_NUM from work.billstran_16;
NOTE: Table BT.BILLSTRAN_16 created, with 7885 rows and 22 columns.

238                  quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.02 seconds
      cpu time            0.01 seconds
      
239  
240  
241  ods html5 (id=saspy_internal) close;ods listing;
