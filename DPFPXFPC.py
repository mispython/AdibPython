Using SAS Config named: default
SAS Connection established. Subprocess id is 1437146

SUMM1 row count:  6767
SUMM2 row count:  8932
SUMM2 - AFTER CONVERSION DBIRTH: dtype=int64, sample head=[24071, 6314, 3446], sample tail=[566, 2097, 13144]
SUMM1 - AFTER CONVERSION DTECOMPLETE: dtype=int64, sample head=[24363, 24363, 24363], sample tail=[24363, 24363, 24363]
SUMM2 - AFTER CONVERSION DTECOMPLETE: dtype=int64, sample head=[24363, 24363, 24363], sample tail=[24363, 24363, 24363]
SUMM1 - AFTER CONVERSION DATEXT: dtype=float64, sample head=[24328.0, 24328.0, 24328.0], sample tail=[24363.0, 24342.0, 24351.0]
Before conversion DTCOMPLETE: dtype=object, sample=['14/09/2026 04:55:44 PM', '14/09/2026 04:55:44 PM', '14/09/2026 04:55:44 PM']
After conversion DTCOMPLETE: dtype=float64, sample=[2105024144.0, 2105024144.0, 2105024144.0]
Conversion success rate: 6767/6767
Before conversion DTCOMPLETE: dtype=object, sample=['1656631M', '', '']
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/ccrissummary.py:93: UserWarning: Could not infer format, so each element will be parsed individually, falling back to `dateutil`. To ensure parsing is consistent and as-expected, please specify a format.
  result[remaining_mask] = pd.to_datetime(
[WARN] DTCOMPLETE: 372 values failed to parse. Examples: ['1656631M', '1602798W', '1656631M', '1126371H', '661980P']
After conversion DTCOMPLETE: dtype=float64, sample=[nan, nan, nan]
Conversion success rate: 6140/8932
SQL being sent for ALTER bnmsumm1_ctrl: 'proc sql;\n    alter table ctrl.bnmsumm1_ctrl\n    add EVENT_TYPE char(10);\nquit;'
ALTER bnmsumm1_ctrl: 
33   ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
33 ! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
34   
35   proc sql;
36       alter table ctrl.bnmsumm1_ctrl
37       add EVENT_TYPE char(10);
NOTE: Table CTRL.BNMSUMM1_CTRL has been modified, with 41 columns.
38   quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
39   
40   ods html5 (id=saspy_internal) close;ods listing;


190  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
190! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
191  
192  
193          proc sql noprint;
194             create table colmeta as
195             select name, type, length
196             from dictionary.columns
197             where libname = upcase('ctrl')
198                   and memname = upcase('bnmsumm1_ctrl');
NOTE: Table WORK.COLMETA created, with 41 rows and 3 columns.

199          quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
200  
201  
202  ods html5 (id=saspy_internal) close;ods listing;

Final table created : 
267  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
267! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
268  
269  
270          proc sql noprint;
271               create table summ.bnmsumm1_2609 as
272               select MAANO, STAGE, APPLICATION, DTECOMPLETE, AANO, APPKEY, PRIORITY_SECTOR, DSRISS3, FIN_CONCEPT,
272! LN_UTILISE_LOCAT_CD, SPECIALFUND, ASSET_PURCH_AMT, PURPOSE_LOAN, STRUPCO_3YR, FACICODE, AMTAPPLY, AMOUNT, APPTYPE, REJREASON,
272! DATEXT, ACCTNO, EIR, EREQNO, REFIN_FLG, STATUS, CCPT_CLASS, CIR, PRICING_TYPE, LU_SOURCE, LU_ADD1, LU_ADD2, LU_ADD3, LU_ADD4,
272! LU_TOWN_CITY, LU_POSTCODE, LU_STATE_CD, LU_COUNTRY_CD, PROP_STATUS, DTCOMPLETE, REPTDATE, EVENT_TYPE from
272! ctrl.bnmsumm1_ctrl(obs=0)
273               union corr
274               select input(trim(MAANO), $15.) as MAANO,
275   input(trim(STAGE), $2.) as STAGE,
276   input(trim(APPLICATION), $10.) as APPLICATION,
277   DTECOMPLETE,
278   input(trim(AANO), $50.) as AANO,
279   input(trim(APPKEY), $20.) as APPKEY,
280   input(trim(PRIORITY_SECTOR), $2.) as PRIORITY_SECTOR,
281   DSRISS3,
282   input(trim(FIN_CONCEPT), $3.) as FIN_CONCEPT,
283   input(trim(LN_UTILISE_LOCAT_CD), $10.) as LN_UTILISE_LOCAT_CD,
284   SPECIALFUND,
285   ASSET_PURCH_AMT,
286   PURPOSE_LOAN,
287   input(trim(STRUPCO_3YR), $2.) as STRUPCO_3YR,
288   FACICODE,
289   AMTAPPLY,
290   AMOUNT,
291   input(trim(APPTYPE), $1.) as APPTYPE,
292   input(trim(REJREASON), $5.) as REJREASON,
293   DATEXT,
294   ACCTNO,
295   EIR,
296   input(trim(EREQNO), $15.) as EREQNO,
297   input(trim(REFIN_FLG), $3.) as REFIN_FLG,
298   input(trim(STATUS), $1.) as STATUS,
299   input(trim(CCPT_CLASS), $5.) as CCPT_CLASS,
300   CIR,
301   input(trim(PRICING_TYPE), $5.) as PRICING_TYPE,
302   input(trim(LU_SOURCE), $5.) as LU_SOURCE,
303   input(trim(LU_ADD1), $40.) as LU_ADD1,
304   input(trim(LU_ADD2), $40.) as LU_ADD2,
305   input(trim(LU_ADD3), $40.) as LU_ADD3,
306   input(trim(LU_ADD4), $40.) as LU_ADD4,
307   input(trim(LU_TOWN_CITY), $20.) as LU_TOWN_CITY,
308   input(trim(LU_POSTCODE), $5.) as LU_POSTCODE,
309   input(trim(LU_STATE_CD), $2.) as LU_STATE_CD,
310   input(trim(LU_COUNTRY_CD), $2.) as LU_COUNTRY_CD,
311   input(trim(PROP_STATUS), $5.) as PROP_STATUS,
312   DTCOMPLETE,
313   REPTDATE,
314   input(trim(EVENT_TYPE), $10.) as EVENT_TYPE from work.bnmsumm1_2609;
NOTE: Table SUMM.BNMSUMM1_2609 created, with 6767 rows and 41 columns.

315          quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.08 seconds
      cpu time            0.04 seconds
      
316  
317  
318  ods html5 (id=saspy_internal) close;ods listing;


472  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
472! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
473  
474  
475          proc sql noprint;
476             create table colmeta as
477             select name, type, length
478             from dictionary.columns
479             where libname = upcase('ctrl')
480                   and memname = upcase('bnmsumm2_ctrl');
NOTE: Table WORK.COLMETA created, with 36 rows and 3 columns.

481          quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
482  
483  
484  ods html5 (id=saspy_internal) close;ods listing;

Final table created : 
549  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
549! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
550  
551  
552          proc sql noprint;
553               create table summ.bnmsumm2_2609 as
554               select MAANO, STAGE, APPLICATION, DTECOMPLETE, IDNO, ENTKEY, APPLNAME, COUNTRY, DBIRTH, ENTITY_TYPE,
554! CORP_STATUS_CD, INDUSTRIAL_STATUS, RESIDENCY_STATUS_CD, ANNSUBTSALARY, GENDER, OCCUPATION, EMPNAME, EMPLOY_SECTOR_CD,
554! EMPLOY_TYPE_CD, POSTCODE, STATE_CD, COUNTRY_CD, ROLE, ICPP, MARRIED, CUSTOMER_CODE, CISNUMBER, NO_OF_EMPLOYEE, ANNUAL_TURNOVER,
554!  SMESIZE, RACE, INDUSTRIAL_SECTOR_CD, OCCUPAT_MASCO_CD, EREQNO, DSRISS3, REPTDATE from ctrl.bnmsumm2_ctrl(obs=0)
555               union corr
556               select input(trim(MAANO), $15.) as MAANO,
557   input(trim(STAGE), $2.) as STAGE,
558   input(trim(APPLICATION), $10.) as APPLICATION,
559   DTECOMPLETE,
560   input(trim(IDNO), $30.) as IDNO,
561   input(trim(ENTKEY), $20.) as ENTKEY,
562   input(trim(APPLNAME), $150.) as APPLNAME,
563   input(trim(COUNTRY), $5.) as COUNTRY,
564   DBIRTH,
565   ENTITY_TYPE,
566   input(trim(CORP_STATUS_CD), $2.) as CORP_STATUS_CD,
567   input(trim(INDUSTRIAL_STATUS), $5.) as INDUSTRIAL_STATUS,
568   input(trim(RESIDENCY_STATUS_CD), $1.) as RESIDENCY_STATUS_CD,
569   ANNSUBTSALARY,
570   input(trim(GENDER), $1.) as GENDER,
571   input(trim(OCCUPATION), $5.) as OCCUPATION,
572   input(trim(EMPNAME), $150.) as EMPNAME,
573   input(trim(EMPLOY_SECTOR_CD), $10.) as EMPLOY_SECTOR_CD,
574   input(trim(EMPLOY_TYPE_CD), $5.) as EMPLOY_TYPE_CD,
575   input(trim(POSTCODE), $5.) as POSTCODE,
576   input(trim(STATE_CD), $2.) as STATE_CD,
577   input(trim(COUNTRY_CD), $3.) as COUNTRY_CD,
578   input(trim(ROLE), $1.) as ROLE,
579   input(trim(ICPP), $30.) as ICPP,
580   input(trim(MARRIED), $10.) as MARRIED,
581   input(trim(CUSTOMER_CODE), $5.) as CUSTOMER_CODE,
582   input(trim(CISNUMBER), $20.) as CISNUMBER,
583   input(trim(NO_OF_EMPLOYEE), $10.) as NO_OF_EMPLOYEE,
584   input(trim(ANNUAL_TURNOVER), $20.) as ANNUAL_TURNOVER,
585   input(trim(SMESIZE), $5.) as SMESIZE,
586   input(trim(RACE), $2.) as RACE,
587   input(trim(INDUSTRIAL_SECTOR_CD), $10.) as INDUSTRIAL_SECTOR_CD,
588   input(trim(OCCUPAT_MASCO_CD), $10.) as OCCUPAT_MASCO_CD,
589   input(trim(EREQNO), $15.) as EREQNO,
590   input(trim(DSRISS3), $10.) as DSRISS3,
591   REPTDATE from work.bnmsumm2_2609;
NOTE: Table SUMM.BNMSUMM2_2609 created, with 8931 rows and 36 columns.

592          quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.03 seconds
      cpu time            0.04 seconds
      
593  
594  
595  ods html5 (id=saspy_internal) close;ods listing;

SAS Connection terminated. Subprocess id was 1437146
