# DCP_CONSOLIDATION.PY
# Converted from SAS EIVQDCPR
# Purpose: Consolidate EQU and BOS data, output to CSV, Excel-like TXT, and Parquet (optional)

import polars as pl
from datetime import date
import pyarrow as pa
import pyarrow.parquet as pq

# ------------------------------------------------
# 1️⃣ Create REPTDATE equivalent
# ------------------------------------------------
today = date.today()
reptdate = today.replace(day=1)  # Equivalent of today() - day(today())
print(f"Reporting Date (REPTDATE): {reptdate}")

# ------------------------------------------------
# 2️⃣ Read EQU data (fixed width)
# ------------------------------------------------
EQU_FILE = "EQU.txt"
EQU_schema = {
    "UTRDT": pl.Utf8,
    "UTCIC": pl.Utf8,
    "UTDOB": pl.Utf8,
    "UTDOC": pl.Utf8,
}

df_equ = pl.read_csv(
    EQU_FILE,
    has_header=False,
    new_columns=["UTRDT", "UTCIC", "UTDOB", "UTDOC"],
    infer_schema_length=0,
    separator=None,  # treat as fixed width (manual cleanup if needed)
)
df_equ = df_equ.with_columns(pl.lit("EQU").alias("SOURCE"))

# ------------------------------------------------
# 3️⃣ Read BOS data (CSV)
# ------------------------------------------------
BOS_FILE = "BOS.csv"

df_bos = pl.read_csv(
    BOS_FILE,
    has_header=True,
    new_columns=["FICODE", "UTCIC", "UTDOB", "BRANCH", "UTDOC"],
)
df_bos = df_bos.with_columns(pl.lit("BOS").alias("SOURCE"))

# ------------------------------------------------
# 4️⃣ Consolidate (SET BOS EQU)
# ------------------------------------------------
df_conso = pl.concat([df_bos, df_equ], how="vertical")
df_conso = df_conso.with_columns(
    FICODE=pl.lit(1204),
    BRANCH=pl.lit(14),
)

# ------------------------------------------------
# 5️⃣ Output to CSV
# ------------------------------------------------
df_conso.select(["FICODE", "UTCIC", "UTDOB", "BRANCH", "UTDOC"]).write_csv("DCPCSV.csv")

# ------------------------------------------------
# 6️⃣ Output to Excel-like TXT (DCPXLS)
# ------------------------------------------------
with open("DCPXLS.txt", "w", encoding="utf-8") as f:
    f.write("PUBLIC INVESTMENT BANK BERHAD\n\n")
    f.write("FI Code\x05ID Number\x05Date of Birth\x05Branch\x05Date of Account\n")
    for row in df_conso.iter_rows(named=True):
        f.write(
            f"{row['FICODE']}\x05{row['UTCIC']}\x05{row['UTDOB']}\x05{row['BRANCH']}\x05{row['UTDOC']}\n"
        )

# ------------------------------------------------
# 7️⃣ Optional: Save to Parquet using PyArrow
# ------------------------------------------------
table = pa.Table.from_pandas(df_conso.to_pandas())
pq.write_table(table, "DCPCONSO.parquet")

print("✅ Consolidation complete: DCPCSV.csv, DCPXLS.txt, DCPCONSO.parquet")
