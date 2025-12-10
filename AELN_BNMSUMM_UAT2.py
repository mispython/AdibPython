import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
import datetime as dt
import os

# -------------------------
# CONFIG / PATHS
# -------------------------
depo1_file = "/sasdata/SAP.PBB.MNITB.DAILY.sas7bdat"
depo1x_file = "/sasdata/SAP.PIBB.MNITB.DAILY.sas7bdat"
depo2_file = "/sasdata/SAP.PBB.MNIFD.DAILY.sas7bdat"
depo2x_file = "/sasdata/SAP.PIBB.MNIFD.DAILY.sas7bdat"

# Base output directory for Hive partitioning
output_base = "/sasdata/output"

# -------------------------
# STEP 1: REPTDATE
# -------------------------
# Note: Polars doesn't directly support SAS7BDAT, so we use pyreadstat or pandas
try:
    import pyreadstat
    reptdate_df, meta = pyreadstat.read_sas7bdat(depo1_file, usecols=["TBDATE"], row_limit=1)
    tbd = reptdate_df["TBDATE"].iloc[0]
except ImportError:
    # Fallback to pandas if pyreadstat not available
    import pandas as pd
    reptdate_df = pd.read_sas(depo1_file, encoding='latin1').head(1)[["TBDATE"]]
    tbd = reptdate_df["TBDATE"].iloc[0]

reptdate = dt.datetime.strptime(str(tbd)[:8], "%Y%m%d")

REPTYEAR = reptdate.strftime("%Y")
REPTMON = reptdate.strftime("%m")
REPTDAY = reptdate.strftime("%d")
REPDT = reptdate.strftime("%d%b%Y").upper()  # e.g. 11SEP2025

print("Report Date:", REPDT)
print(f"Partitions: year={REPTYEAR}/month={REPTMON}/day={REPTDAY}")

# -------------------------
# STEP 2: DEPO1 - Read SAS files
# -------------------------
def read_sas_to_polars(filepath, columns=None):
    """Helper function to read SAS7BDAT files into Polars DataFrame"""
    try:
        import pyreadstat
        df, meta = pyreadstat.read_sas7bdat(filepath, usecols=columns)
        return pl.from_pandas(df)
    except ImportError:
        import pandas as pd
        df = pd.read_sas(filepath, encoding='latin1')
        if columns:
            df = df[columns]
        return pl.from_pandas(df)

depo1 = read_sas_to_polars(depo1_file, ["ACCTNO","BRANCH","SECOND","NAME"])
depo1x = read_sas_to_polars(depo1x_file, ["ACCTNO","BRANCH","SECOND","NAME"])
depo1 = pl.concat([depo1, depo1x])

depo1 = depo1.filter(pl.col("SECOND").is_in([99991, 99992]))
depo1 = depo1.with_columns([
    pl.when(pl.col("SECOND")==99991).then("PBB")
     .when(pl.col("SECOND")==99992).then("PIBB")
     .otherwise(None).alias("SECOND2")
])
depo1 = depo1.select(["ACCTNO","BRANCH","SECOND2","NAME"]).sort(["ACCTNO","BRANCH"])

# -------------------------
# STEP 3: DEPO2
# -------------------------
depo2 = read_sas_to_polars(depo2_file, ["ACCTNO","BRANCH","CDNO","MATDATE","TERM","RATE","CURBAL"])
depo2x = read_sas_to_polars(depo2x_file, ["ACCTNO","BRANCH","CDNO","MATDATE","TERM","RATE","CURBAL"])
depo2 = pl.concat([depo2, depo2x])

depo2 = depo2.filter(pl.col("CURBAL")>0).sort(["ACCTNO","BRANCH"])

# -------------------------
# STEP 4: MERGE
# -------------------------
all_df = depo1.join(depo2, on=["ACCTNO","BRANCH"], how="inner").sort("MATDATE")

# -------------------------
# STEP 5: SPLIT FD vs FCYFD
# -------------------------
cond = (
    ((pl.col("ACCTNO")>=1590000000)&(pl.col("ACCTNO")<=1599999999)) |
    ((pl.col("ACCTNO")>=1689999999)&(pl.col("ACCTNO")<=1699999999)) |
    ((pl.col("ACCTNO")>=1789999999)&(pl.col("ACCTNO")<=1799999999))
)
fcyfd = all_df.filter(cond)
fd = all_df.filter(~cond)

# -------------------------
# STEP 6: CREATE HIVE PARTITION DIRECTORIES
# -------------------------
def get_hive_path(base_dir, filename, year, month, day):
    """Create Hive partition path: base/year=YYYY/month=MM/day=DD/filename.parquet"""
    partition_path = os.path.join(
        base_dir, 
        f"year={year}", 
        f"month={month}", 
        f"day={day}"
    )
    os.makedirs(partition_path, exist_ok=True)
    return os.path.join(partition_path, filename)

# -------------------------
# STEP 7: OUTPUT FD → HIVE PARTITIONED PARQUET
# -------------------------
fd_parquet_path = get_hive_path(output_base, "fd_maturity.parquet", REPTYEAR, REPTMON, REPTDAY)
fd_csv_path = get_hive_path(output_base, "fd_maturity.csv", REPTYEAR, REPTMON, REPTDAY)

# Write CSV with header
fd_header = [
    f"DAILY RM FD MATURITY HQ AS AT {REPDT}",
    "BRANCH,PBB/PIBB,NAME,ACCTNO,PRINCIPAL AM(RM),CDNO,MATDATE,PREVIOUS RATE(% P.A),PREVIOUS TERM (MONTH)"
]
with open(fd_csv_path, "w") as f:
    for line in fd_header:
        f.write(line + "\n")
fd.write_csv(fd_csv_path, include_header=False, separator=",", append=True)

# Write Parquet
fd.write_parquet(fd_parquet_path)

# -------------------------
# STEP 8: OUTPUT FCYFD → HIVE PARTITIONED PARQUET
# -------------------------
fcyfd_parquet_path = get_hive_path(output_base, "fcyfd_maturity.parquet", REPTYEAR, REPTMON, REPTDAY)
fcyfd_csv_path = get_hive_path(output_base, "fcyfd_maturity.csv", REPTYEAR, REPTMON, REPTDAY)

# Write CSV with header
fcyfd_header = [
    f"DAILY FCYFD MATURITY HQ AS AT {REPDT}",
    "BRANCH,PBB/PIBB,NAME,ACCTNO,PRINCIPAL AM(RM),CDNO,MATDATE,PREVIOUS RATE(% P.A),PREVIOUS TERM (MONTH)"
]
with open(fcyfd_csv_path, "w") as f:
    for line in fcyfd_header:
        f.write(line + "\n")
fcyfd.write_csv(fcyfd_csv_path, include_header=False, separator=",", append=True)

# Write Parquet
fcyfd.write_parquet(fcyfd_parquet_path)

# -------------------------
# STEP 9: ARROW FORMAT (Optional)
# -------------------------
arrow_fd = fd.to_arrow()
arrow_fcyfd = fcyfd.to_arrow()

fd_arrow_path = get_hive_path(output_base, "fd_maturity_arrow.parquet", REPTYEAR, REPTMON, REPTDAY)
fcyfd_arrow_path = get_hive_path(output_base, "fcyfd_maturity_arrow.parquet", REPTYEAR, REPTMON, REPTDAY)

pq.write_table(arrow_fd, fd_arrow_path)
pq.write_table(arrow_fcyfd, fcyfd_arrow_path)

# -------------------------
# STEP 10: DUCKDB VALIDATION
# -------------------------
con = duckdb.connect()
con.register("fd", arrow_fd)
con.register("fcyfd", arrow_fcyfd)

print("\n" + "="*60)
print("VALIDATION RESULTS")
print("="*60)
print(f"FD count: {con.execute('SELECT COUNT(*) FROM fd').fetchall()[0][0]}")
print(f"FCYFD count: {con.execute('SELECT COUNT(*) FROM fcyfd').fetchall()[0][0]}")
print("\nSample FD records:")
print(con.execute("SELECT * FROM fd LIMIT 5").fetchdf())
print("\nSample FCYFD records:")
print(con.execute("SELECT * FROM fcyfd LIMIT 5").fetchdf())

print("\n" + "="*60)
print("OUTPUT FILES CREATED")
print("="*60)
print(f"FD Parquet: {fd_parquet_path}")
print(f"FD CSV: {fd_csv_path}")
print(f"FCYFD Parquet: {fcyfd_parquet_path}")
print(f"FCYFD CSV: {fcyfd_csv_path}")
print("="*60)
