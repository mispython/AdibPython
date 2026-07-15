Calculating report dates...
Report Date: 30062026, Week: 4
Copying files from DEPOBACK to BNM...
Copied: fdmthly.sas7bdat

Processing FDMTHLY data...
Read 2,676,574 records from fdmthly.sas7bdat
Loaded 2,676,574 records

Calculating REMMTH...
Records with positive REMMTH: 2,676,568

Summarizing data...
Summary records: 7,216

Creating BNM codes...

CUSTCODE distribution in summary:
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFDSP.py:219: DeprecationWarning: `pl.count()` is deprecated. Please use `pl.len()` instead.
(Deprecated in version 0.20.5)
  pl.count().alias("COUNT"),
shape: (41, 3)
┌──────────────┬───────┬──────────────┐
│ CUSTCODE_STR ┆ COUNT ┆ TOTAL_AMOUNT │
│ ---          ┆ ---   ┆ ---          │
│ str          ┆ u32   ┆ f64          │
╞══════════════╪═══════╪══════════════╡
│ 06           ┆ 49    ┆ 2.6929e8     │
│ 12           ┆ 52    ┆ 5.45045e6    │
│ 30           ┆ 277   ┆ 3.3192e9     │
│ 32           ┆ 1     ┆ 108830.42    │
│ 33           ┆ 13    ┆ 6.4953e8     │
│ …            ┆ …     ┆ …            │
│ 84           ┆ 4     ┆ 4.6473e6     │
│ 86           ┆ 52    ┆ 4.9259e8     │
│ 95           ┆ 595   ┆ 2.8629e9     │
│ 96           ┆ 688   ┆ 2.1521e9     │
│ 99           ┆ 11    ┆ 1.7935e7     │
└──────────────┴───────┴──────────────┘
ALMDEPT records: 1,350

Generating reports...
Report saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQFDSP/REPORT_42130_30062026.txt
No data for 42132
Report saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQFDSP/REPORT_42630_30062026.txt

============================================================
SUMMARY STATISTICS
============================================================
Report Date: 30/06/2026
Week: 4, Month: 06, Year: 2026
Total records processed: 2,756,145
Records with positive REMMTH: 2,676,568
ALMDEPT records: 1,350

Amount Distribution by BNMCODE prefix:
  42130:     3,430,332,774.04
  42630:     2,099,909,780.10

Processing complete!
You have mail in /var/spool/mail/sas_edw_dev



output after updated the new fdmthly
