============================================================
STEP 1: Processing RPVBDATA dates
============================================================
DEBUG: RPVBDATA first line: '0 20251201'
DEBUG: Extracted TBDATE from positions 3-10: '20251201'
✓ TBDATE from RPVBDATA: 20251201
  TBDATE as date: 2025-12-01
  REPTDATE (end of prev month from TBDATE): 2025-11-30
  PREVDATE (end of prev month from REPTDATE): 2025-10-31
  REPTDT (MMYY): 1125
  PREVDT (MMYY): 1025

============================================================
STEP 2: Processing SRSDATA dates
============================================================
DEBUG: SRSDATA first line: '20251103N201'
DEBUG: Extracted TBDATE (first 8 chars): '20251103'
✓ TBDATE from SRSDATA: 20251103
  SRS TBDATE as date: 2025-11-03
  SRS REPTDATE: 2025-11-03
  SRSTDT (MMYY): 1125

============================================================
STEP 3: Macro guard validation
============================================================
Comparing: REPTDT=1125 vs SRSTDT=1125
✓ Date validation passed - REPTDT matches SRSTDT

============================================================
STEP 4: Reading RPVBDATA with fixed-width parsing
============================================================

Reading RPVBDATA.txt - Total lines: 1208
DEBUG Line 2: '1 8042194628 90010      36 FROZEN MART SDN. BHD.                           R   700                0 N          R 20250326     750.00 5SNE         0.00       0.00       0.00   54000.00 20250329                       20250327                0.00                                                                                                                                                                                                                                                                                                                                                                     '
DEBUG Line 3: '1 8042229447 90010      ABD RAZAK BIN MD NOOR                              R   700 F N Y          0 N          R 20250310    1200.00 PTDR     30000.00   31000.00   21000.00   21000.00 20250313 0                     20250327                0.00                                                                                                                                                                                                                                                                                                                                                                     '
DEBUG Line 4: '1 8785972221 90010      ABDUL HANIS BIN DAUD                               R   128 F N N          0 N          R 20250311    1100.00 5SNE     10000.00   15000.00   10000.00   10000.00 20250314 0                     20250317                0.00                                                                                                                                                                                                                                                                                                                                                                     '
DEBUG Line 5: '1 8766333414 90010      ABDUL HISYAM BIN MUBIN                             R   128                0 N          R 20250312     750.00 5SNE         0.00       0.00       0.00   20000.00 20250314                       20250313                0.00                                                                                                                                                                                                                                                                                                                                                                     '
DEBUG Line 6: '1 8650989531 90010      ABDUL JABBAR BIN ZAINAL                            D   700 F N N          0 N          R 20250213    1100.00 PTDR     20000.00   22000.00   20000.00   20000.00 20250215 A   18200.00 20250325 20250326 20250321   16000.00                                                                                                                                                                                                                                                                                                                                                                     '
DEBUG Line 7: '2 8650989531 90010                                                                                                                                                                                                                         20000.00                                                                                                                                                                                                                                                                                                                                                                     '
Parsed 1207 data records
✓ RPVB1 records read: 1207
  RPVB1 columns: ['RECID', 'MNIACTNO', 'LOANNOTE', 'NAME', 'ACCTSTA', 'PRODTYPE', 'PRSTCOND', 'REGCARD', 'IGNTKEY', 'REPODIST', 'ACCTWOFF', 'MODEREPO', 'REPOPAID', 'REPOSTAT', 'TKEPRICE', 'MRKTVAL', 'RSVPRICE', 'FTHSCHLD', 'MODEDISP', 'APPVDISP', 'HOPRICE', 'NOAUCT', 'PRIOUT', 'DATEWOFF', 'DATEREPO', 'DATE5TH', 'DATEAPRV', 'DATESTLD', 'DATEHO']

Sample of RPVB1 data (first 3 rows):
shape: (3, 29)
┌───────┬────────────┬──────────┬──────────────────────────┬───┬────────────┬──────────┬────────────┬────────┐
│ RECID ┆ MNIACTNO   ┆ LOANNOTE ┆ NAME                     ┆ … ┆ DATE5TH    ┆ DATEAPRV ┆ DATESTLD   ┆ DATEHO │
│ ---   ┆ ---        ┆ ---      ┆ ---                      ┆   ┆ ---        ┆ ---      ┆ ---        ┆ ---    │
│ str   ┆ str        ┆ str      ┆ str                      ┆   ┆ date       ┆ date     ┆ date       ┆ date   │
╞═══════╪════════════╪══════════╪══════════════════════════╪═══╪════════════╪══════════╪════════════╪════════╡
│ 1     ┆ 8042194628 ┆ 90010    ┆ 36 FROZEN MART SDN. BHD. ┆ … ┆ 2025-03-29 ┆ null     ┆ 2025-03-27 ┆ null   │
│ 1     ┆ 8042229447 ┆ 90010    ┆ ABD RAZAK BIN MD NOOR    ┆ … ┆ 2025-03-13 ┆ null     ┆ 2025-03-27 ┆ null   │
│ 1     ┆ 8785972221 ┆ 90010    ┆ ABDUL HANIS BIN DAUD     ┆ … ┆ 2025-03-14 ┆ null     ┆ 2025-03-17 ┆ null   │
└───────┴────────────┴──────────┴──────────────────────────┴───┴────────────┴──────────┴────────────┴────────┘

============================================================
STEP 5: Creating RPVB2 and RPVB3
============================================================
✓ RPVB2 created: 776 records (ACCTSTA in D,S,R)
✓ RPVB3 created: 776 records (with DATESTLD)

============================================================
STEP 6: Creating REPO.REPS&REPTDT
============================================================
Previous file to load: /sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output/REPO/REPS_1025.parquet
Current output file: /sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output/REPO/REPS_1125.parquet
ℹ No previous REPO data found or error loading: No such file or directory (os error 2): /sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output/REPO/REPS_1025.parquet

This error occurred with the following context stack:
        [1] 'parquet scan'
        [2] 'sink'

  No previous data, using only RPVB3 (776 records)
✓ Saved REPO data: 776 records to /sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output/REPO/REPS_1125.parquet

============================================================
STEP 7: Creating REPOWH.REPS&REPTDT (with NODUPKEY)
============================================================
✓ Removed 0 duplicate MNIACTNO records
✓ Saved REPOWH data: 776 records to /sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output/REPOWH/REPS_1125.parquet

============================================================
SUMMARY
============================================================
TBDATE from RPVBDATA: 20251201
TBDATE from SRSDATA: 20251103
REPTDT: 1125
PREVDT: 1025
SRSTDT: 1125
RPVB1 records: 1207
RPVB2 records: 776
RPVB3 records: 776
REPO_REPS records: 776
REPOWH_REPS records: 776
============================================================
✓ Processing completed successfully!
============================================================
