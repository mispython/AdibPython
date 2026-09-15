Using SAS Config named: default
SAS Connection established. Subprocess id is 1423290

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
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/ccrissummary.py:92: UserWarning: Could not infer format, so each element will be parsed individually, falling back to `dateutil`. To ensure parsing is consistent and as-expected, please specify a format.
  result[remaining_mask] = pd.to_datetime(df.loc[remaining_mask, col_name], errors='coerce')
After conversion DTCOMPLETE: dtype=float64, sample=[nan, nan, nan]
Conversion success rate: 6140/8932
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
ALTER bnmsumm1_ctrl: 
33   ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
33 ! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
34   
35   
36   proc sql;
36 !          "
37       alter table ctrl.bnmsumm1_ctrl"
36   proc sql;"
              -
              180
ERROR 180-322: Statement is not valid or it is used out of proper order.

38       add EVENT_TYPE char(10);
NOTE: PROC SQL set option NOEXEC and will continue to check the syntax of statements.
38 !                             "
39   quit;"
40   
41   
38       add EVENT_TYPE char(10);"
                                 -------
                                 180
ERROR 180-322: Statement is not valid or it is used out of proper order.

42   ;
42 !  *';*";*/;ods html5 (id=saspy_internal) close;ods listing;

Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/ccrissummary.py", line 171, in <module>
    summ1_df['EVENT_TYPE'] = summ1_df.get('EVENT_TYPE', '').astype(str).str.slice(0, 10)
AttributeError: 'str' object has no attribute 'astype'
SAS Connection terminated. Subprocess id was 1423290
