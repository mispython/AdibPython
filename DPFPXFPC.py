Calculating report dates...
Report Date: 30062026, Week: 4
Copying files from DEPOBACK to BNM...
Copied: fdmthly.sas7bdat

Processing FDMTHLY data...
Read 2,756,145 records from fdmthly.sas7bdat
Loaded 2,756,145 records

Calculating REMMTH...
Records with positive REMMTH: 486,837

Summarizing data...
Summary records: 2,916

Creating BNM codes...

CUSTCODE distribution in summary:
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQFDSP.py:219: DeprecationWarning: `pl.count()` is deprecated. Please use `pl.len()` instead.
(Deprecated in version 0.20.5)
  pl.count().alias("COUNT"),
shape: (31, 3)
┌──────────────┬───────┬──────────────┐
│ CUSTCODE_STR ┆ COUNT ┆ TOTAL_AMOUNT │
│ ---          ┆ ---   ┆ ---          │
│ str          ┆ u32   ┆ f64          │
╞══════════════╪═══════╪══════════════╡
│ 06           ┆ 1     ┆ 1e7          │
│ 30           ┆ 112   ┆ 1.8786e8     │
│ 34           ┆ 2     ┆ 157784.2     │
│ 35           ┆ 58    ┆ 5790358.1    │
│ 37           ┆ 1     ┆ 204994.8     │
│ …            ┆ …     ┆ …            │
│ 77           ┆ 206   ┆ 4.9048e7     │
│ 78           ┆ 663   ┆ 1.9857e10    │
│ 79           ┆ 203   ┆ 8.7634e8     │
│ 95           ┆ 267   ┆ 1.2420e9     │
│ 96           ┆ 315   ┆ 4.3828e8     │
└──────────────┴───────┴──────────────┘
ALMDEPT records: 582

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
Records with positive REMMTH: 486,837
ALMDEPT records: 582

Amount Distribution by BNMCODE prefix:
  42130:       765,663,246.69
  42630:       914,591,731.01

Processing complete!
You have mail in /var/spool/mail/sas_edw_dev


can you combine the text file as per production? or is it following sas?
