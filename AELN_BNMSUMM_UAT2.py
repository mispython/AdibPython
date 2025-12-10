import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
import datetime as dt
import os

# -------------------------
# CONFIG / PATHS
# -------------------------
depo1_file = "/sasdata/SAP.PBB.MNITB.DAILY.parquet"
depo1x_file = "/sasdata/SAP.PIBB.MNITB.DAILY.parquet"
depo2_file = "/sasdata/SAP.PBB.MNIFD.DAILY.parquet"
depo2x_file = "/sasdata/SAP.PIBB.MNIFD.DAILY.parquet"

fdtxt_file = "/sasdata/SAP.PBB.FD.FDTXT.csv"
fdtxt1_file = "/sasdata/SAP.PBB.FD.FDTXT1.csv"

# -------------------------
# STEP 1: REPTDATE
# -------------------------
reptdate_df = pl.read_parquet(depo1_file).select("TBDATE").head(1)
tbd = reptdate_df.item()
reptdate = dt.datetime.strptime(str(tbd)[:8], "%Y%m%d")

REPTYEAR = reptdate.strftime("%Y")
REPTMON = reptdate.strftime("%m")
REPTDAY = reptdate.strftime("%d")
REPDT = reptdate.strftime("%d%b%Y").upper()  # e.g. 11SEP2025

print("Report Date:", REPDT)

# -------------------------
# STEP 2: DEPO1
# -------------------------
depo1 = pl.read_parquet(depo1_file).select(["ACCTNO","BRANCH","SECOND","NAME"])
depo1x = pl.read_parquet(depo1x_file).select(["ACCTNO","BRANCH","SECOND","NAME"])
depo1 = pl.concat([depo1,depo1x])

depo1 = depo1.filter(pl.col("SECOND").is_in([99991,99992]))
depo1 = depo1.with_columns([
    pl.when(pl.col("SECOND")==99991).then("PBB")
     .when(pl.col("SECOND")==99992).then("PIBB")
     .otherwise(None).alias("SECOND2")
])
depo1 = depo1.select(["ACCTNO","BRANCH","SECOND2","NAME"]).sort(["ACCTNO","BRANCH"])

# -------------------------
# STEP 3: DEPO2
# -------------------------
depo2 = pl.read_parquet(depo2_file).select(["ACCTNO","BRANCH","CDNO","MATDATE","TERM","RATE","CURBAL"])
depo2x = pl.read_parquet(depo2x_file).select(["ACCTNO","BRANCH","CDNO","MATDATE","TERM","RATE","CURBAL"])
depo2 = pl.concat([depo2,depo2x])

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
# STEP 6: OUTPUT FD → FDTXT
# -------------------------
fd_header = [
    f"DAILY RM FD MATURITY HQ AS AT {REPDT}",
    "BRANCH,PBB/PIBB,NAME,ACCTNO,PRINCIPAL AM(RM),CDNO,MATDATE,PREVIOUS RATE(% P.A),PREVIOUS TERM (MONTH)"
]
with open(fdtxt_file,"w") as f:
    for line in fd_header:
        f.write(line+"\n")
fd.write_csv(fdtxt_file, include_header=False, separator=",", append=True)

# -------------------------
# STEP 7: OUTPUT FCYFD → FDTXT1
# -------------------------
fcyfd_header = [
    f"DAILY FCYFD MATURITY HQ AS AT {REPDT}",
    "BRANCH,PBB/PIBB,NAME,ACCTNO,PRINCIPAL AM(RM),CDNO,MATDATE,PREVIOUS RATE(% P.A),PREVIOUS TERM (MONTH)"
]
with open(fdtxt1_file,"w") as f:
    for line in fcyfd_header:
        f.write(line+"\n")
fcyfd.write_csv(fdtxt1_file, include_header=False, separator=",", append=True)

# -------------------------
# STEP 8: MODERN EXPORTS
# -------------------------
fd.write_parquet(fdtxt_file.replace(".csv",".parquet"))
fcyfd.write_parquet(fdtxt1_file.replace(".csv",".parquet"))

arrow_fd = fd.to_arrow()
arrow_fcyfd = fcyfd.to_arrow()
pq.write_table(arrow_fd, fdtxt_file.replace(".csv","_arrow.parquet"))
pq.write_table(arrow_fcyfd, fdtxt1_file.replace(".csv","_arrow.parquet"))

# -------------------------
# STEP 9: DUCKDB VALIDATION
# -------------------------
con = duckdb.connect()
con.register("fd", arrow_fd)
con.register("fcyfd", arrow_fcyfd)

print("FD count:", con.execute("SELECT COUNT(*) FROM fd").fetchall())
print("FCYFD count:", con.execute("SELECT COUNT(*) FROM fcyfd").fetchall())
print("Sample FD:", con.execute("SELECT * FROM fd LIMIT 5").fetchdf())
print("Sample FCYFD:", con.execute("SELECT * FROM fcyfd LIMIT 5").fetchdf())
