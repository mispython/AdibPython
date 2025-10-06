# BNM_CONVENTIONAL_BASE.PY
# Converted from SAS program EIBMBASE
# Purpose: Initialize base reporting parameters for conventional (PBB) reporting

import polars as pl
from datetime import date
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------
# 1️⃣ Read REPTDATE dataset (from Parquet)
# ---------------------------------------------------------------------
# Assuming DEPOSIT.REPTDATE exists in Parquet format
df_reptdate = pl.read_parquet("DEPOSIT_REPTDATE.parquet")
reptdate_value = df_reptdate["REPTDATE"][0]
reptdate = date.fromisoformat(str(reptdate_value))

# ---------------------------------------------------------------------
# 2️⃣ Determine week group (NOWK)
# ---------------------------------------------------------------------
day_of_month = reptdate.day
if day_of_month == 8:
    nowk = "1"
elif day_of_month == 15:
    nowk = "2"
elif day_of_month == 22:
    nowk = "3"
else:
    nowk = "4"

# ---------------------------------------------------------------------
# 3️⃣ Extract date parts
# ---------------------------------------------------------------------
reptyear = reptdate.year
reptmon = f"{reptdate.month:02d}"
reptday = f"{reptdate.day:02d}"
rdate = reptdate.strftime("%d/%m/%Y")

# ---------------------------------------------------------------------
# 4️⃣ LIBNAME equivalent
# ---------------------------------------------------------------------
# In SAS: LIBNAME WALK "SAP.PBB.D&REPTYEAR" DISP=SHR;
walk_path = f"SAP/PBB/D{reptyear}"
print(f"LIBNAME WALK → {walk_path}")

# ---------------------------------------------------------------------
# 5️⃣ Display results (like PROC PRINT)
# ---------------------------------------------------------------------
print("====== BNM Conventional Base Setup ======")
print(f"REPTDATE   : {rdate}")
print(f"REPTYEAR   : {reptyear}")
print(f"REPTMON    : {reptmon}")
print(f"REPTDAY    : {reptday}")
print(f"NOWK (week): {nowk}")
print("=========================================")

# ---------------------------------------------------------------------
# 6️⃣ Store metadata (for downstream jobs)
# ---------------------------------------------------------------------
meta = pl.DataFrame({
    "REPTDATE": [rdate],
    "REPTYEAR": [reptyear],
    "REPTMON": [reptmon],
    "REPTDAY": [reptday],
    "NOWK": [nowk],
    "LIBPATH": [walk_path]
})

meta.write_csv("BNM_CONVENTIONAL_META.csv")
pq.write_table(pa.Table.from_pandas(meta.to_pandas()), "BNM_CONVENTIONAL_META.parquet")

print(" Metadata saved: BNM_CONVENTIONAL_META.(csv/parquet)")

# ---------------------------------------------------------------------
# 7️⃣ Next Jobs (replacing %INCLUDE)
# ---------------------------------------------------------------------
# You may import and run the downstream scripts:
# from DALWPBBD import main as run_dalwpbbd
# from EIBMRLBA import main as run_eibmrlba
# run_dalwpbbd(meta)
# run_eibmrlba(meta)
