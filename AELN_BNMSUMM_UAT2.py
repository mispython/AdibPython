============================================================
EIBMRNID - EXACT SAS OUTPUT
============================================================
Report Date: 30/11/2025
Start Date: 01/11/2025

📂 Processing data...
  Original NID records: 580
  Merged with TRNCH
  After merge columns: ['nid_acctno', 'nid_refno', 'acctno', 'nid_cdno', 'custname', 'newic', 'branch', 'custcode', 'trancheno', 'startdt', 'matdt', 'intrate', 'intpltdrate', 'curbal', 'lstintpaydt', 'nxtintpaydt', 'intfrq', 'nidstat', 'accint', 'cdstat', 'poststat', 'early_wddt', 'early_wdproc', 'early_telrid', 'early_offid1', 'early_offid2', 'cancdt', 'canc_telrid', 'canc_offid1', 'canc_offid2', 'appldt', 'appldttm', 'appl_telrid', 'appl_offid1', 'appl_offid2', 'printdt', 'print_telrid', 'print_offid1', 'print_offid2', 'maintdt', 'maint_telrid', 'maint_offid1', 'maint_offid2', 'pledgestat', 'nid_odacctno', 'ccollno', 'pledgedt', 'pledge_telrid', 'pledge_offid1', 'pledge_offid2', 'cpledgedt', 'cpledge_telrid', 'cpledge_offid1', 'cpledge_offid2', 'term', 'suitability_assessment_cd', 'sales_staff_id', 'termid', 'intpd', 'costctr', 'product', 'brabbr', 'curcode', 'custcd', 'trn_stat', 'trn_offerstartdt', 'trn_offerenddt', 'trnstartdt', 'trnmatdt', 'term_right', 'termid_right', 'intfrq_right', 'intrate_right', 'creatdt', 'creatstaffid', 'maintdt_right', 'maintstaffid', 'intplrate_bid', 'intplrate_offer']

🔍 Checking date columns before conversion...
  matdt (first 3 values): [21202.0, 21134.0, 21314.0]
  startdt (first 3 values): [20745.0, 20677.0, 20829.0]
  early_wddt (first 3 values): [None, None, None]

  Converting matdt...
    Original dtype: Float64
    After conversion (first 3): [datetime.date(2018, 1, 18), datetime.date(2017, 11, 11), datetime.date(2018, 5, 10)]

  Converting startdt...
    Original dtype: Float64
    After conversion (first 3): [datetime.date(2016, 10, 18), datetime.date(2016, 8, 11), datetime.date(2017, 1, 10)]

  Converting early_wddt...
    Original dtype: Float64
    After conversion (first 3): [None, None, None]

  Converted date columns: ['matdt', 'startdt', 'early_wddt']

🔍 Checking date ranges...
  matdt: min=2017-01-19, max=2025-08-23
  startdt: min=2016-01-18, max=2024-05-23

  Records with positive balance: 580 (from 580)

🔍 Checking NIDSTAT and CDSTAT combinations...
  NIDSTAT x CDSTAT distribution:
    NIDSTAT=M, CDSTAT=A: 224
    NIDSTAT=O, CDSTAT=A: 44
    NIDSTAT=N, CDSTAT=A: 28
    NIDSTAT=E, CDSTAT=C: 25
    NIDSTAT=M, CDSTAT=C: 179
    NIDSTAT=W, CDSTAT=C: 7
    NIDSTAT=W, CDSTAT=A: 43
    NIDSTAT=C, CDSTAT=C: 30

🔍 Analyzing Table 1 filter conditions...
  Condition 1 (matdt > 2025-11-30): 0
  Condition 2 (startdt <= 2025-11-30): 580
  Condition 3 (nidstat = 'N'): 28
  Condition 4 (cdstat = 'A'): 339
  Conditions 1 & 2: 0
  Conditions 3 & 4: 28

  Calculated remmth for 580 records

💾 Saving processed data to Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/Job/Output/EIBMRNID_DATA.parquet
  Saved 580 records to Parquet

📊 Creating Table 1...

  Debug: Trying different filter combinations...
    Strict filtering: 0 records
    Without date filtering: 28 records
    Only date filtering: 0 records
  ⚠️  No records with strict date filtering, but records exist with N/A
  Showing sample of N/A records...
shape: (5, 5)
┌────────────┬────────────┬─────────┬────────┬──────────┐
│ matdt      ┆ startdt    ┆ nidstat ┆ cdstat ┆ curbal   │
│ ---        ┆ ---        ┆ ---     ┆ ---    ┆ ---      │
│ date       ┆ date       ┆ str     ┆ str    ┆ f64      │
╞════════════╪════════════╪═════════╪════════╪══════════╡
│ 2017-01-19 ┆ 2016-12-19 ┆ N       ┆ A      ┆ 36000.0  │
│ 2017-01-19 ┆ 2016-12-19 ┆ N       ┆ A      ┆ 1.5e6    │
│ 2018-04-06 ┆ 2017-01-06 ┆ N       ┆ A      ┆ 120000.0 │
│ 2018-04-12 ┆ 2017-01-12 ┆ N       ┆ A      ┆ 100000.0 │
│ 2018-04-12 ┆ 2017-01-12 ┆ N       ┆ A      ┆ 100000.0 │
└────────────┴────────────┴─────────┴────────┴──────────┘
    Date ranges for N/A records:
      matdt: 2017-01-19 to 2019-02-17
      startdt: 2016-01-18 to 2017-11-17
  Table 1 records: 0

📊 Creating Table 2...
  Table 2 - NID Count: 0, Volume: 0.00

📊 Creating Table 3...
  Table 3 filtered records: 0
  Warning: No records found for Table 3 yield calculation

💾 Writing SAS-format output to: /sas/python/virt_edw/Data_Warehouse/MIS/Job/Output/EIBMRNID_REPORT.TXT

✅ SAS report generated: /sas/python/virt_edw/Data_Warehouse/MIS/Job/Output/EIBMRNID_REPORT.TXT
✅ Parquet data saved: /sas/python/virt_edw/Data_Warehouse/MIS/Job/Output/EIBMRNID_DATA.parquet

📊 Final Summary:
  Total processed records: 580
  Table 1 (Outstanding): 0 records
  Table 2 (Trading): 0 NIDs, RM 0.00
  Table 3 (Yield): 0.0000% overall

📁 Output folder: /sas/python/virt_edw/Data_Warehouse/MIS/Job/Output

⚠️  DIAGNOSTICS: Table 1 has no records
   Report date: 2025-11-30
   Try adjusting the report date if the data is for a different period
