[WARN] /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWCRMA/forate.sas7bdat: using a rate older than 2026-08-02 for some currencies (most recent available on/before that date): [{'CURCODE': 'LAK', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'NZD', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'KHR', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'THB', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'XAU', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'FRF', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'KRW', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'DEM', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'NLG', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'USD', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'SEK', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'PKR', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'SGD', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'CHF', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'INR', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'AED', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'ESP', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'CAD', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'JPY', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'DKK', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'NOK', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'ATS', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'GBP', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'ZAR', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'PHP', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'ITL', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'BND', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'XAT', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'BDT', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'VND', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'CNY', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'FJD', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'IRR', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'FIM', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'XEU', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'TWD', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'HKD', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'LKR', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'AUD', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'IDR', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'EUR', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'BEF', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'SAR', 'REPTDATE': datetime.date(2026, 8, 1)}]
Using SAS Config named: default
SAS Connection established. Subprocess id is 4148249

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")

105  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
105! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
106  
107  
108          PROC EXPORT DATA=temp_table
109              OUTFILE="/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWCRMA/EXTCRMA084.sas7bdat"
110              DBMS=SAS7BDAT REPLACE;
ERROR: DBMS type SAS7BDAT not valid for export.
NOTE: The SAS System stopped processing this step because of errors.
NOTE: PROCEDURE EXPORT used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
111          RUN;
112  
113  
114  ods html5 (id=saspy_internal) close;ods listing;

SAS Connection terminated. Subprocess id was 4148249
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWCRMA.py", line 523, in <module>
    write_sas7bdat(EXTCRMA, OUT_BEP / f"{base_name}.sas7bdat")
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWCRMA.py", line 133, in write_sas7bdat
    raise RuntimeError(f"SAS export to {path} failed -- see log above")
RuntimeError: SAS export to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWCRMA/EXTCRMA084.sas7bdat failed -- see log above
You have mail in /var/spool/mail/sas_edw_dev



it still process too long and taking time, make it faster, increase the chunks size or reduce it, or whatever possible ways and effective to make it run faster
