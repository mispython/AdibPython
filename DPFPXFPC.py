import polars as pl
import pyreadstat
import datetime as dt
import os

# -------------------------
# CONFIG
# -------------------------
dpfl_file = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDDCIA/dpfl.txt"
eqfl_file = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDDCIA/eqfl.txt"

ca_file   = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDDCIA/ca{REPTYEAR}{REPTMON}{REPTDAY}.sas7bdat"
sa_file   = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDDCIA/sa{REPTYEAR}{REPTMON}{REPTDAY}.sas7bdat"
fcy_file  = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDDCIA/fcy{REPTYEAR}{REPTMON}{REPTDAY}.sas7bdat"

out_file  = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDDCIA/SAP.PBB.DCIWH.DCID"

# -------------------------
# STEP 1: Report Date (yesterday)
# -------------------------
reptdate = dt.date.today() - dt.timedelta(days=1)
RDATE = reptdate.strftime("%d%b%Y").upper()
REPTYEAR = reptdate.strftime("%y")
REPTMON  = reptdate.strftime("%m")
REPTDAY  = reptdate.strftime("%d")

print(f"Report Date: {RDATE} ({REPTDAY}-{REPTMON}-{REPTYEAR})")

# -------------------------
# STEP 2: Read header dates
# -------------------------
with open(dpfl_file) as f:
    hdr = f.readline()
    yy, mm, dd = int(hdr[0:4]), int(hdr[5:7]), int(hdr[8:10])
    DPDATE = dt.date(yy, mm, dd)

with open(eqfl_file) as f:
    hdr = f.readline()
    dd, mm, yy = int(hdr[19:21]), int(hdr[21:23]), int(hdr[23:27])
    EQDATE = dt.date(yy, mm, dd)

print("DPDATE:", DPDATE, "EQDATE:", EQDATE)

# -------------------------
# Only run if dates match
# -------------------------
if DPDATE != reptdate:
    raise SystemExit(f"❌ DPDATE {DPDATE} does not match RDATE {reptdate}")

# -------------------------
# STEP 3: Parse DPFL fixed-width file → DPST
# -------------------------
dp_schema = [
    ("TICKETNO", (0,7),"str"),
    ("BRANCH", (7,12),"str"),
    ("CUSTNAME", (12,38),"str"),
    ("NEWIC", (38,58),"str"),
    ("SALESID", (58,66),"str"),
    ("CUSTCODE", (66,71),"int"),
    ("INVCURRAC",(71,82),"str"),
    ("ALTCURRAC",(82,93),"str"),
    ("INVCURR",(93,96),"str"),
    ("ALTCURR",(96,99),"str"),
    ("INVAMT",(99,112),"float"),
    ("TRYY",(112,116),"int"),
    ("TRMM",(117,119),"int"),
    ("TRDD",(120,122),"int"),
    ("STYY",(122,126),"int"),
    ("STMM",(127,129),"int"),
    ("STDD",(130,132),"int"),
    ("FIYY",(132,136),"int"),
    ("FIMM",(137,139),"int"),
    ("FIDD",(140,142),"int"),
    ("MTYY",(142,146),"int"),
    ("MTMM",(147,149),"int"),
    ("MTDD",(150,152),"int"),
    ("TENOR",(152,155),"int"),
    ("STRIKERT",(155,168),"float"),
    ("DCIRT",(168,177),"float"),
    ("ACCINT",(177,192),"float"),
    ("ROLLOVER",(192,193),"str"),
    ("CONVERTIND",(193,194),"str"),
    ("DEALERID",(194,202),"str"),
    ("MANAGERID",(202,210),"str")
]

rows = []
with open(dpfl_file) as f:
    next(f)  # skip header
    for line in f:
        row = {}
        for col,(start,end,typ) in dp_schema:
            raw = line[start:end].strip()
            if typ=="int":
                row[col] = int(raw) if raw else None
            elif typ=="float":
                row[col] = float(raw) if raw else None
            else:
                row[col] = raw
        # trade/start/fixing/maturity
        try:
            row["TRADEDT"] = dt.date(row["TRYY"], row["TRMM"], row["TRDD"])
            row["STARTDT"] = dt.date(row["STYY"], row["STMM"], row["STDD"])
            row["FIXINGDT"]= dt.date(row["FIYY"], row["FIMM"], row["FIDD"])
            row["MATDT"]   = dt.date(row["MTYY"], row["MTMM"], row["MTDD"])
        except Exception:
            row["TRADEDT"]=row["STARTDT"]=row["FIXINGDT"]=row["MATDT"]=None
        rows.append(row)

dpst = pl.DataFrame(rows)

# -------------------------
# STEP 4: Parse EQFL (pipe-delimited)
# -------------------------
eqtn = pl.read_csv(eqfl_file, separator="|", has_header=True)

# Map STATIND → STATUSIND
status_map = {
    "New":"N","Outstanding":"OS","Mature":"M","Premature":"P","Cancelled":"C"
}
eqtn = eqtn.with_columns(
    pl.col("STATIND").map_elements(lambda x: status_map.get(x,"")).alias("STATUSIND")
)

# -------------------------
# STEP 5: Merge DPST & EQTN by TICKETNO
# -------------------------
dcid = dpst.join(eqtn, on="TICKETNO", how="inner")
dcid = dcid.filter(pl.col("NEWDEAL").is_in(["O","N"]))

# -------------------------
# STEP 6: Join CA / SA / FCY refs using pyreadstat
# -------------------------
# Read SAS files with pyreadstat
ca_df, ca_meta = pyreadstat.read_sas7bdat(
    ca_file.format(REPTYEAR=REPTYEAR, REPTMON=REPTMON, REPTDAY=REPTDAY)
)
sa_df, sa_meta = pyreadstat.read_sas7bdat(
    sa_file.format(REPTYEAR=REPTYEAR, REPTMON=REPTMON, REPTDAY=REPTDAY)
)
fcy_df, fcy_meta = pyreadstat.read_sas7bdat(
    fcy_file.format(REPTYEAR=REPTYEAR, REPTMON=REPTMON, REPTDAY=REPTDAY)
)

# Convert to Polars DataFrames
ca = pl.from_pandas(ca_df).with_columns(pl.col("CUSTFISS").cast(pl.Int32).alias("CUSTCODE2"))
sa = pl.from_pandas(sa_df).with_columns(pl.col("CUSTCODE").alias("CUSTCODE2"))
fcy = pl.from_pandas(fcy_df).with_columns(pl.col("CUSTCD").cast(pl.Int32).alias("CUSTCODE2"))

# Combine account references
dpdata = pl.concat([
    ca.select(["ACCTNO","CUSTCODE2"]).rename({"ACCTNO":"INVCURRAC2"}),
    sa.select(["ACCTNO","CUSTCODE2"]).rename({"ACCTNO":"INVCURRAC2"}),
    fcy.select(["ACCTNO","CUSTCODE2"]).rename({"ACCTNO":"INVCURRAC2"})
])

# Join with main data
dcid2 = dcid.join(dpdata, left_on="INVCURRAC", right_on="INVCURRAC2", how="left")

# Update CUSTCODE if found in reference tables
dcid2 = dcid2.with_columns(
    pl.when(pl.col("CUSTCODE2").is_not_null())
      .then(pl.col("CUSTCODE2"))
      .otherwise(pl.col("CUSTCODE"))
      .alias("CUSTCODE")
).drop(["INVCURRAC2","CUSTCODE2"])

# -------------------------
# STEP 7: Save results
# -------------------------
# Save as Parquet
dcid2.write_parquet(f"{out_file}.parquet")

# Save as SAS7BDAT using saspy
import saspy

# Initialize SAS session
sas = saspy.SASsession()

# Convert Polars DataFrame to pandas for SAS
dcid2_pd = dcid2.to_pandas()

# Upload to SAS
sas_df = sas.df2sd(dcid2_pd, "dcid2_df")

# Save as SAS7BDAT
sas.submit(f"""
    PROC EXPORT DATA=dcid2_df 
        OUTFILE="{out_file}.sas7bdat" 
        DBMS=SAS7BDAT REPLACE;
    RUN;
""")

# Close SAS session
sas.endsas()

print(f"Output files created:")
print(f"  - {out_file}.parquet")
print(f"  - {out_file}.sas7bdat")
