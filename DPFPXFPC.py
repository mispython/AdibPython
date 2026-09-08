============================================================
Processing date: 2026-08-31 (SAS date: 24349)
============================================================

============================================================
STEP 1: Reading LNNOTE
============================================================
Essential columns available: ['ACCTNO', 'NOTENO', 'LOANTYPE', 'CENSUS', 'COMMNO', 'ENTITY_CD', 'ISSUEDT', 'BLDATE', 'BALANCE', 'CURBAL', 'NAME']
Reading LNNOTE with essential columns only...
Filtered LNNOTE rows: 0
WARNING: No data with LOANTYPE=575 and CENSUS=575.09 found!
This is the August file - loan type 575 might not exist in this month.
Checking what loan types are available...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBLTRRF.py:562: DeprecationWarning: `pl.count()` is deprecated. Please use `pl.len()` instead.
(Deprecated in version 0.20.5)
  combo = sample.group_by(['LOANTYPE', 'CENSUS']).agg(pl.count().alias('COUNT'))
Available LOANTYPE-CENSUS combinations (showing first 20):
shape: (20, 3)
┌──────────┬────────┬───────┐
│ LOANTYPE ┆ CENSUS ┆ COUNT │
│ ---      ┆ ---    ┆ ---   │
│ f64      ┆ f64    ┆ u32   │
╞══════════╪════════╪═══════╡
│ 4.0      ┆ 0.0    ┆ 15    │
│ 5.0      ┆ 0.0    ┆ 40    │
│ 6.0      ┆ 0.0    ┆ 1     │
│ 15.0     ┆ 0.0    ┆ 46    │
│ 15.0     ┆ 111.01 ┆ 3     │
│ …        ┆ …      ┆ …     │
│ 25.0     ┆ 0.0    ┆ 34    │
│ 26.0     ┆ 0.0    ┆ 1     │
│ 31.0     ┆ 0.0    ┆ 3     │
│ 32.0     ┆ 0.0    ┆ 16    │
│ 34.0     ┆ 0.0    ┆ 1     │
└──────────┴────────┴───────┘

Found 5 rows with LOANTYPE between 570-580:
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBLTRRF.py:571: DeprecationWarning: `pl.count()` is deprecated. Please use `pl.len()` instead.
(Deprecated in version 0.20.5)
  print(loantype_575.group_by(['LOANTYPE', 'CENSUS']).agg(pl.count().alias('COUNT')))
shape: (1, 3)
┌──────────┬────────┬───────┐
│ LOANTYPE ┆ CENSUS ┆ COUNT │
│ ---      ┆ ---    ┆ ---   │
│ f64      ┆ f64    ┆ u32   │
╞══════════╪════════╪═══════╡
│ 570.0    ┆ 0.0    ┆ 5     │
└──────────┴────────┴───────┘

============================================================
STEP 2: Reading LNCOMM
============================================================
LNCOMM essential columns available: ['ACCTNO', 'COMMNO', 'CORGAMT']
Reading LNCOMM with essential columns only...
LNCOMM rows after filter: 1066036
WARNING: INTAMT column not found, using 0

============================================================
STEP 6: Reading COLL file (EBCDIC)
============================================================
Parsing EBCDIC COLL file...
COLL rows: 1
First few COLL records: shape: (1, 3)
┌─────────┬────────────┬────────────┐
│ CCOLLNO ┆ ACCTNO     ┆ NOTENO     │
│ ---     ┆ ---        ┆ ---        │
│ i64     ┆ i64        ┆ i64        │
╞═════════╪════════════╪════════════╡
│ 133     ┆ 3078959107 ┆ 3078959107 │
└─────────┴────────────┴────────────┘

============================================================
STEP 7: Reading DESC file (EBCDIC)
============================================================
Parsing EBCDIC DESC file...
WARNING: No DESC records found!

WARNING: Cannot merge COLL and DESC - insufficient data

============================================================
Insufficient data to create NPGS
LOAN1 rows: 0
COLL rows: 1
============================================================

Processing completed with no output generated.
The August file doesn't contain the required loan type 575.
This might be expected if loan type 575 is only present in specific months.

============================================================
Processing complete!
============================================================
