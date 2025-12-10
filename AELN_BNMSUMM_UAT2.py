============================================================
EIBWSDBM - SDBMS File Processor
============================================================
Report Date: 2025-12-09 (Week 2)
Expected SDBMS date (DDMMYY): 091225
Output Path: /host/mis/parquet/year=2025/month=12/day=09
✓ SDBMS file date found: 09122025 (comparing as 091225)
------------------------------------------------------------
✓ SDBMS file date matches expected date
→ Executing EIBWSDB1...
------------------------------------------------------------
============================================================
EIBWSDB1 - SDBMS Data Processor
============================================================
Report Date: 2025-12-09 (Week 2)
Output Path: /host/mis/parquet/year=2025/month=12/day=09
------------------------------------------------------------
✓ Read 0 records from SDBMS.txt
✓ Processed dates and created OPENDT field

Sample Records (first 5 rows):
------------------------------------------------------------
Empty DataFrame
Columns: [BRANCH, NAME, IC, NATIONALITY, BOXNO, HIRERTY, ADDRESS, ACCTHOLDER, ACCTNO, PRIPHONE, MOBILENO, MTHOVERDUE1, MTHOVERDUE2, MTHOVERDUE3, TOTALOVERDUE, OPENDT, RENTALDATE, LASTRENTPAY]
Index: []
------------------------------------------------------------

✓ STATUS dataset saved: SDB.parquet
  Location: /host/mis/parquet/year=2025/month=12/day=09/SDB.parquet
  Records: 0
  Columns: 14

✓ R1STAT dataset saved: R1SDB.parquet
  Location: /host/mis/parquet/year=2025/month=12/day=09/R1SDB.parquet
  Records: 0
  Columns: 5

============================================================
✓ EIBWSDB1 processing completed successfully
============================================================

------------------------------------------------------------
✓ EIBWSDB1 executed successfully
============================================================
✓ Processing completed successfully
============================================================
