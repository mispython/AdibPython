from __future__ import annotations
from pathlib import Path
from datetime import date, timedelta
import polars as pl
import duckdb  # as requested
import pyarrow.parquet as pq  # as requested

# ---------- SAS-like libs (adjust paths only) ----------
DPAA   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/uat/RBP2.B033.ODPA.EXT.FILE.MIS.txt")  # DPAA(0) -> file: ODPA_EXT_FILE_MIS.parquet
LMTDET = Path("SAP.PBB.DPDET.parquet_lib")                 # target lib
LMTDET.mkdir(parents=True, exist_ok=True)

# ---------- 1) DATA LMTDET.LMTDET ----------
# DPAA Parquet must already expose the SAS fields from INPUT.
req = [
 "AANO","APRVDT","APRVAMT","ACCTNO","TOTLMTAMT","LASTMNTDT","LMTID","LMTAMT",
 "LMTSTARTDT","LMTENDDT","LMTTERM","LMTTERMID","LMTPAIDIND",
 "COLL1","COLL2","COLL3","COLL4","COLL5","COLL6","COLL7","COLL8","COLL9","COLL10"
]
df = pl.read_parquet(DPAA / "ODPA_EXT_FILE_MIS.parquet")
miss = [c for c in req if c not in df.columns]
if miss:
    df = df.with_columns([pl.lit(None).alias(c) for c in miss])
LMTDET_LMTDET = df.select(req)
LMTDET_LMTDET.write_parquet(LMTDET / "LMTDET.parquet")

# ---------- 2) DATA LMTDET.REPTDATE (KEEP=EXTDATE REPTDATE) ----------
today = date.today()
REPTDATE = today - timedelta(days=1)
YYYY = f"{REPTDATE.year:04d}"
MM   = f"{REPTDATE.month:02d}"
DD   = f"{REPTDATE.day:02d}"
DAY1 = date(REPTDATE.year, 1, 1)
DAYS = (today - DAY1).days  # SAS: DAYS = TODAY() - DAY1;
TEMPDATE = f"{MM}{DD}{YYYY}{DAYS}"
EXTDATE = int(TEMPDATE)  # COMPRESS(...)*1

pl.DataFrame({"EXTDATE":[EXTDATE], "REPTDATE":[REPTDATE]}).write_parquet(LMTDET / "REPTDATE.parquet")

print("DONE:")
print(" - LMTDET/LMTDET.parquet")
print(" - LMTDET/REPTDATE.parquet")


Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWDPLE.py", line 20, in <module>
    df = pl.read_parquet(DPAA / "ODPA_EXT_FILE_MIS.parquet")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/io/parquet/functions.py", line 289, in read_parquet
    return lf.collect()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
OSError: Not a directory (os error 20): ...a_Warehouse/MIS/XMIS/input/uat/RBP2.B033.ODPA.EXT.FILE.MIS.txt/ODPA_EXT_FILE_MIS.parquet (set POLARS_VERBOSE=1 to see full path)

This error occurred with the following context stack:
        [1] 'parquet scan'
        [2] 'sink'
