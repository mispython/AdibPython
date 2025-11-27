REPTDATE = 2025-11-26, PREVDATE = 2025-11-25
RDATE (SAS format) = 24071
MON=11, DAY=26, YEAR=25
Filter date (reptdte) = 251126
SAVING.csv shape: (8320, 81)
CIS shape: (224992, 100)
PRISEC sample values: [None, 3.0, 25.0, 901.0, 902.0]
Bright accounts found: 0
CIS processed records: 211119
/pythonITD/mis_dev/sas_migration/BRIGHTSTAR/EIBDSTAR_BRIGHTSTAR_SAVINGS_EXTRACTION.py:101: DeprecationWarning: `pl.count()` is deprecated. Please use `pl.len()` instead.
(Deprecated in version 0.20.5)
  joint_dist = cis_processed.group_by("JOINT").agg(pl.count())
Joint account distribution: shape: (2, 2)
┌───────┬────────┐
│ JOINT ┆ count  │
│ ---   ┆ ---    │
│ str   ┆ u32    │
╞═══════╪════════╡
│ Y     ┆ 32175  │
│ N     ┆ 178944 │
└───────┴────────┘
Joined records: 0
Final output records: 0
Output written to /pythonITD/mis_dev/OUTPUT/BRIGHTSTAR_SAVINGS_251126.parquet
Traceback (most recent call last):
  File "/pythonITD/mis_dev/sas_migration/BRIGHTSTAR/EIBDSTAR_BRIGHTSTAR_SAVINGS_EXTRACTION.py", line 139, in <module>
    duckdb.sql("INSTALL parquet; LOAD parquet;")
duckdb.duckdb.IOException: IO Error: Failed to download extension "parquet" at URL "http://extensions.duckdb.org/v1.3.2/linux_amd64/parquet.duckdb_extension.gz"
Extension "parquet" is an existing extension.

For more info, visit https://duckdb.org/docs/stable/extensions/troubleshooting/?version=v1.3.2&platform=linux_amd64&extension=parquet (ERROR Could not establish connection)
