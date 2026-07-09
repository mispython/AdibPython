============================================================
ISLAMIC GL PROCESSING STARTED (EIIDNLGL)
============================================================
Processing date: 2026-07-08
Store directory: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDNLGL
============================================================
Total lines: 25
GL Date from file: 08/07/26
REPT Date: 08/07/26

============================================================
Preparing base data...
============================================================
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIDNLGL.py:180: PolarsInefficientMapWarning: 
Expr.map_elements is significantly slower than the native expressions API.
Only use if you absolutely CANNOT implement your logic otherwise.
Replace this expression...
  - pl.col("DATEX").map_elements(_parse_DDMMYY8)
with this one instead:
  + pl.col("DATEX").str.to_datetime(format="%d/%m/%y").dt.date()

  DATE = pl.col("DATEX").map_elements(_parse_DDMMYY8, return_dtype=pl.Date),
Base data shape: (24, 13)

============================================================
Processing Islamic GL P1...
============================================================
P1 mapped shape: (0, 13)
No data for P1

============================================================
Processing Islamic GL P2...
============================================================
P2 mapped shape: (0, 13)
No data for P2

============================================================
ISLAMIC PROCESSING COMPLETE!
============================================================

Output files saved to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDNLGL

⚠ No parquet files found in the output directory.

============================================================
