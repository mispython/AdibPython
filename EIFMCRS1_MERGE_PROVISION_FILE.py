import polars as pl
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# === Step 1: Load PAGE03 & PAGE04 parquet inputs ===
page03 = pl.read_parquet("page03.parquet")
page04 = pl.read_parquet("page04.parquet")

# === Step 2: Adjust monetary fields (×100) ===
money_cols_page03 = ["PRINCIPA", "INTEROUT", "TOTBAL"]
money_cols_page04 = [
    "REALISAB", "CLASSAMT", "IISOPBAL", "IISSUAMT", "IISWBAMT", "IISWOAMT",
    "INTEROUT", "SPOPBAL", "SPTRANS", "SPWBAMT", "SPWOAMT", "SP"
]

page03 = page03.with_columns([pl.col(c) * 100 for c in money_cols_page03])
page04 = page04.with_columns([pl.col(c) * 100 for c in money_cols_page04])

# === Step 3: Merge using DuckDB (like SAS MERGE BY) ===
duckdb.register("page03", page03.to_arrow())
duckdb.register("page04", page04.to_arrow())

merged_arrow = duckdb.sql("""
    SELECT 
        COALESCE(a.ACCTNO, b.ACCTNO) AS ACCTNO,
        COALESCE(a.NOTENO, b.NOTENO) AS NOTENO,
        a.*, b.*
    FROM page03 a
    FULL OUTER JOIN page04 b
    ON a.ACCTNO = b.ACCTNO AND a.NOTENO = b.NOTENO
""").arrow()

provisio = pl.from_arrow(merged_arrow)

# === Step 4: Fill missing values with zeros where appropriate ===
provisio = provisio.fill_null(0)

# === Step 5: Derive TREF & ARREARS logic ===
def derive_tref(row):
    classify = row["CLASSIFY"]
    borstat = row["BORSTAT"]
    ficode = row["FICODE"]
    arrears = row["ARREARS"]

    if classify in ("B", "D", "S"):
        tref = "D"
    elif borstat == "F" and classify == "B" and ficode not in (981,982,983,991,992,993):
        return ("A", 999)
    elif borstat == "F" and classify == "B" and ficode in (981,982,983,991,992,993) and arrears == 0:
        return ("B", 999)
    elif borstat != "F" and classify == "B" and ficode in (981,982,983,991,992,993):
        return ("C", 0)
    else:
        return (None, arrears)

# Apply logic
provisio = provisio.with_columns([
    pl.struct(["CLASSIFY", "BORSTAT", "FICODE", "ARREARS"]).map_elements(
        lambda row: derive_tref(row)[0], return_dtype=pl.Utf8
    ).alias("TREF"),
    pl.struct(["CLASSIFY", "BORSTAT", "FICODE", "ARREARS"]).map_elements(
        lambda row: derive_tref(row)[1], return_dtype=pl.Int64
    ).alias("ARREARS")
])

# === Step 6: Save results ===
provisio.write_csv("provision_output.csv")
provisio.write_parquet("provision_output.parquet")

print("✅ Provision file successfully generated.")
