[WARN] /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWCRMA/forate.sas7bdat: using a rate older than 2026-08-03 for some currencies (most recent available on/before that date): [{'CURCODE': 'XAU', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'CNY', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'NOK', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'XEU', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'SEK', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'AUD', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'NZD', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'CHF', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'EUR', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'USD', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'ITL', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'VND', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'GBP', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'AED', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'ZAR', 'REPTDATE': datetime.date(2026, 8,1)}, {'CURCODE': 'KHR', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'BEF', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'FIM', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'LKR', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'THB', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'CAD', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'JPY', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'IDR', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'FRF', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'INR', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'PKR', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'ESP', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'FJD', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'IRR', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'BND', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'DKK', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'ATS', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'SGD', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'NLG', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'PHP', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'SAR', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'XAT', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'DEM', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'HKD', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'BDT', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'KRW', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'LAK', 'REPTDATE': datetime.date(2026, 8, 1)}, {'CURCODE': 'TWD', 'REPTDATE': datetime.date(2026, 8, 1)}]
[TIME] build SAVING/CURRENT/FD: 60.1s
[TIME] CIS join: 4241.2s
[TIME] build DEPOSIT: 65.5s
[TIME] read CRMA raw file: 0.7s
[TIME] build EXTCRMA base (join+MATCHIND+INACTIVE): 0.0s
[TIME] load LOAN (lnnote parallel read): 155.5s
[TIME] derive date parts / scaling / PRODTYPE: 2.6s
[TIME] write EXTCRMA txt+parquet: 0.3s
[TIME] write EXTMIS txt+parquet: 0.0s
Using SAS Config named: default
SAS Connection established. Subprocess id is 78327

[TIME] wrote EXTCRMA084.sas7bdat (343 rows) in 0.4s
[TIME] wrote EXTMIS084.sas7bdat (343 rows) in 0.2s
SAS Connection terminated. Subprocess id was 78327
[TIME] write EXTCRMA+EXTMIS sas7bdat (shared SAS session): 4.2s
You have mail in /var/spool/mail/sas_edw_dev
