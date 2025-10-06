# BNM_REPORT_BASE.PY
# Converted from SAS program EIIMBASE
# Purpose: Determine reporting date context and current week group

import polars as pl
from datetime import date
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------
# 1️⃣ Read REPTDATE dataset (from Parquet or CSV)
# ---------------------------------------------------------------------
# Assuming DEPOSIT.REPTDATE is stored as Parquet
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
# 3️⃣ Extract other date macros
# ---------------------------------------------------------------------
reptyear = reptdate.year
reptmon = f"{reptdate.month:02d}"
reptday = f"{reptdate.day:02d}"
rdate = reptdate.strftime("%d/%m/%Y")

# ---------------------------------------------------------------------
# 4️⃣ Assign equivalent of LIBNAME
# ---------------------------------------------------------------------
# (In SAS: LIBNAME WALK "SAP.PIBB.D&REPTYEAR" DISP=SHR)
walk_path = f"SAP/PIBB/D{reptyear}"
print(f"LIBNAME WALK → {walk_path}")

# ---------------------------------------------------------------------
# 5️⃣ Log results (acts like PROC PRINT)
# ---------------------------------------------------------------------
print(f"REPTDATE   : {rdate}")
print(f"REPTYEAR   : {reptyear}")
print(f"REPTMON    : {reptmon}")
print(f"REPTDAY    : {reptday}")
print(f"NOWK (week): {nowk}")

# ---------------------------------------------------------------------
# 6️⃣ Save metadata (optional for next scripts)
# ---------------------------------------------------------------------
meta = pl.DataFrame({
    "REPTDATE": [rdate],
    "REPTYEAR": [reptyear],
    "REPTMON": [reptmon],
    "REPTDAY": [reptday],
    "NOWK": [nowk],
    "LIBPATH": [walk_path]
})

# Save outputs for next modules
meta.write_csv("BNM_REPORT_META.csv")
pq.write_table(pa.Table.from_pandas(meta.to_pandas()), "BNM_REPORT_META.parquet")

print(" Base report setup complete → BNM_REPORT_META.(csv/parquet)")

# ---------------------------------------------------------------------
# 7️⃣ Call subsequent programs (replacement for %INCLUDE)
# ---------------------------------------------------------------------
# (You can chain them in Python instead of %INC)
# from DALWPBBD import main as run_dalwpbbd
# from EIBMRLBA import main as run_eibmrlba
# run_dalwpbbd(meta)
# run_eibmrlba(meta)
