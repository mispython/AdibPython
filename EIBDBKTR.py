import polars as pl
import pyarrow.parquet as pq
import duckdb
import datetime as dt
import sys

# ----------------------------
# CONFIG / INPUT PATHS
# ----------------------------
extmast_path  = "/sasdata/SAP.PBB.BTRADE.EXTMAST.parquet"
extmast2_path = "/sasdata/SAP.PBB.BTRADE.EXTMAST2.parquet"
extsubac_path = "/sasdata/SAP.PBB.BTRADE.EXTSUBAC.parquet"
excrepos_path = "/sasdata/SAP.PBB.BTRADE.EXCREPOS.parquet"
exprov_path   = "/sasdata/SAP.PBB.BTRADE.EXPROV.parquet"
extcomm_path  = "/sasdata/SAP.PBB.BTRADE.CRFCOMD.parquet"

obtrsa_path   = "/sasdata/SAP.PBB.BTRADE.SASDATA.DAILY.parquet"
btrsa_out     = "/sasdata/SAP.PBB.BTRADE.SASDATA.DAILY_OUT.parquet"
oibtrsa_path  = "/sasdata/SAP.PIBB.BTRADE.SASDATA.DAILY.parquet"
ibtrsa_out    = "/sasdata/SAP.PIBB.BTRADE.SASDATA.DAILY_OUT.parquet"

# ----------------------------
# STEP 1: Compute Report Dates
# ----------------------------
today = dt.date.today()
reptdate = today - dt.timedelta(days=1)   # yesterday
prevdate = reptdate - dt.timedelta(days=1)

RDATE    = reptdate.strftime("%d/%m/%Y")
REPTYEAR = reptdate.strftime("%y")
REPTMON  = reptdate.strftime("%m")
REPTDAY  = reptdate.strftime("%d")
PREVMON  = prevdate.strftime("%m")
PREVDAY  = prevdate.strftime("%d")

print("▶ RDATE:", RDATE)

# ----------------------------
# STEP 2: Get header dates from source datasets
# (in SAS: INFILE ... OBS=1; INPUT @05 REPTDATE YYMMDD8.)
# ----------------------------
def get_first_date(path: str, col="REPTDATE"):
    try:
        df = pl.read_parquet(path, n_rows=1)
        if col in df.columns:
            val = df[col][0]
            if isinstance(val, (dt.date, dt.datetime)):
                return val.date() if isinstance(val, dt.datetime) else val
            # if numeric like YYYYMMDD
            sval = str(val)
            if len(sval) == 8:
                return dt.datetime.strptime(sval, "%Y%m%d").date()
        return None
    except Exception as e:
        print(f"⚠ Cannot read {path}: {e}")
        return None

mast = get_first_date(extmast_path)
suba = get_first_date(extsubac_path)
cred = get_first_date(excrepos_path)
prov = get_first_date(exprov_path)
comm = get_first_date(extcomm_path)

print("MAST:", mast, "SUBA:", suba, "CRED:", cred, "PROV:", prov, "COMM:", comm)

# ----------------------------
# STEP 3: Validation logic (like %MACRO PROCESS)
# ----------------------------
if all([
    mast == reptdate,
    suba == reptdate,
    cred == reptdate,
    prov == reptdate,
    comm == reptdate
]):
    print("✅ All source datasets match RDATE → continue to next step (EIBDBTCC).")
    # In SAS: %INC PGM(EIBDBTCC);
    # In Python: you would import or run EIBDBTCC translation script here
    # Example:
    # import EIBDBTCC
    # EIBDBTCC.run(reptdate, ...)
else:
    if mast != reptdate:
        print(f"❌ BANKTRADE MASTER FILE IS NOT DATED {RDATE}")
    if suba != reptdate:
        print(f"❌ BANKTRADE SUBACCT FILE IS NOT DATED {RDATE}")
    if cred != reptdate:
        print(f"❌ BANKTRADE CREDIT FILE IS NOT DATED {RDATE}")
    if prov != reptdate:
        print(f"❌ BANKTRADE PROVISION FILE IS NOT DATED {RDATE}")
    if comm != reptdate:
        print(f"❌ BANKTRADE CRFCOMM FILE IS NOT DATED {RDATE}")
    print("❌ THE JOB IS NOT DONE !!")
    sys.exit(77)

# ----------------------------
# STEP 4: Save REPTDATE markers (like BTRSA.REPTDATE, IBTRSA.REPTDATE)
# ----------------------------
rept_tbl = pl.DataFrame({"REPTDATE": [reptdate]})
rept_tbl.write_parquet(btrsa_out.replace(".parquet","_REPTDATE.parquet"))
rept_tbl.write_parquet(ibtrsa_out.replace(".parquet","_REPTDATE.parquet"))

print("✔ Wrote REPTDATE markers for BTRSA and IBTRSA")
