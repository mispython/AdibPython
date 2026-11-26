from pathlib import Path

code = r'''"""
EIFFTXT1 - Bad Debt Write-Off List (Conventional Banking)
Polars port following the tested EIIFTXT1 structure and the original
conventional EIFFTXT1 SAS program.

Important SAS behaviour:
- DATA REPTDATE reads LOAN.REPTDATE (not system yesterday).
- MERGE SASLN LOAN NPL; BY ACCTNO is a sequential SAS match-merge.
- Other match-merges retain their original BY keys.
- Intermediate/final fixed-width records use the original SAS @ positions.
"""

import gc
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pyreadstat

try:
    import psutil
    def rss_mb():
        return psutil.Process().memory_info().rss / 1024**2
except ImportError:
    def rss_mb():
        return 0.0

sys.path.insert(0, "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
from PBBLNFMT import (
    HP_ACTIVE, format_mthpass, format_delqdes
)

# ---------------------------------------------------------------------------
# CONFIGURATION - replace these paths with your actual conventional sources
# ---------------------------------------------------------------------------
PATH_REPTDATE = Path("/path/to/loan/reptdate.sas7bdat")
PATH_LNNOTE   = Path("/path/to/loan/lnnote.sas7bdat")
PATH_IIS      = Path("/path/to/npl/iis.sas7bdat")
PATH_SP2      = Path("/path/to/npl/sp2.sas7bdat")
PATH_CCRIS    = Path("/path/to/ccris/credmsubac.sas7bdat")  # dynamic name below
PATH_CNAME    = Path("/path/to/cisname/loan.sas7bdat")
PATH_LIAB     = Path("/path/to/loan/liab.sas7bdat")
PATH_SASLN    = Path("/path/to/sasln/loan.sas7bdat")       # dynamic name below
OUTPUT_DIR    = Path("/path/to/output/EIFFTXT1")
OUTPUT_FILE   = OUTPUT_DIR / "wofftext.txt"
OUTPUT_FILE1  = OUTPUT_DIR / "wofftex1.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

HPD = set(HP_ACTIVE)  # SAS macro &HPD from PBBLNFMT
EXCLUDED_LOANTYPES = [983, 993, 678, 679, 698, 699]
RIND = "D"
BIZTYPE = "C"
EXPECTED_LEN = 797

# These mappings must match the formats in BKCTRL.CISFMT / PBBELF.
# Replace with the official format definitions if they differ.
OCCUPFMT = {
    "001": "PROFESSIONAL", "002": "BUSINESSMAN", "003": "SELF EMPLOYED",
    "004": "EMPLOYEE - PRIVATE", "005": "EMPLOYEE - GOVERNMENT",
    "006": "RETIRED", "999": "OTHERS",
}
BGCFMT = {
    "B": "BUSINESS", "G": "GOVERNMENT", "C": "CORPORATE",
    "I": "INDIVIDUAL", "  ": "NOT SPECIFIED",
}

# BRCHCD. is supplied by PBBLNFMT/PBBELF in SAS. Verify this mapping against
# the production BRCHCD. format before running the job.
BRCHCD = {1: "KL", 2: "PJ", 3: "JB", 4: "PG", 5: "IP"}

# ---------------------------------------------------------------------------
# SAS reader
# ---------------------------------------------------------------------------
def read_sas(path, columns=None, required=None, optional=False):
    path = Path(path)
    if not path.exists():
        if optional:
            print(f"  Optional input missing: {path}")
            return pl.DataFrame()
        raise FileNotFoundError(path)

    _, meta = pyreadstat.read_sas7bdat(str(path), metadataonly=True)
    lookup = {c.lower(): c for c in meta.column_names}
    required = required or []
    missing = [c for c in required if c.lower() not in lookup]
    if missing:
        raise RuntimeError(f"{path.name}: missing required columns {missing}")

    usecols = None
    if columns:
        usecols = [lookup[c.lower()] for c in columns if c.lower() in lookup]
    pdf, _ = pyreadstat.read_sas7bdat(
        str(path), usecols=usecols, disable_datetime_conversion=True
    )
    pdf.columns = [c.upper() for c in pdf.columns]
    return pl.from_pandas(pdf) if len(pdf) else pl.DataFrame()

def empty_like(columns):
    return pl.DataFrame(schema={c: pl.Null for c in columns})

def add_missing(df, columns):
    for c in columns:
        if c not in df.columns:
            df = df.with_columns(pl.lit(None).alias(c))
    return df

def sas_sum(*cols):
    """SAS SUM(): ignore missing values; return 0 when all are missing."""
    return pl.sum_horizontal([pl.col(c).fill_null(0) for c in cols])

def to_num(df, columns):
    exprs = [
        pl.col(c).cast(pl.Float64, strict=False).alias(c)
        for c in columns if c in df.columns
    ]
    return df.with_columns(exprs) if exprs else df

def as_text(value):
    return "" if value is None else str(value).strip()

def safe_num(value, default=0.0):
    try:
        x = float(value)
        return x if x == x and abs(x) != float("inf") else default
    except (TypeError, ValueError):
        return default

def z11_date(value):
    """SAS INPUT(SUBSTR(PUT(value,Z11.),1,8),MMDDYY8.)."""
    if value is None:
        return None
    try:
        v = int(value)
        if v == 0:
            return None
        return datetime.strptime(str(v).zfill(11)[:8], "%m%d%y").date()
    except (ValueError, TypeError, OverflowError):
        return None

def mmddyy(value):
    if isinstance(value, (datetime, date)):
        return value.strftime("%m/%d/%Y")
    return ""

def calc_mthpdue(days):
    if days is None:
        return 0
    try:
        d = int(days)
    except (ValueError, TypeError):
        return 0
    if d <= 0:
        return 0
    result = int(format_mthpass(d) or 0)
    if result == 24:
        result = int((d / 365) * 12)
    return result

def branch_text(code):
    if code is None:
        return "UNK 000"
    try:
        n = int(code)
        return f"{BRCHCD.get(n, 'UNK')} {n:03d}"
    except (ValueError, TypeError):
        return "UNK 000"

# ---------------------------------------------------------------------------
# SAS DATA-step match-merge emulation
# ---------------------------------------------------------------------------
def sas_match_merge(frames, by, in_filter=None):
    """
    Emulate a sorted SAS DATA step MERGE with BY processing.

    For each BY group, SAS outputs the maximum row count among inputs.
    Variables from each input are retained when that input has no further
    observation in the BY group. Later datasets overwrite earlier values
    when they supply a non-missing value for that observation.

    in_filter: (dataset_index, boolean mask column name) to reproduce IF IN=.
    """
    prepared = []
    all_cols = set(by)

    for i, frame in enumerate(frames):
        if frame is None or frame.height == 0:
            prepared.append(None)
            continue
        f = frame.sort(by).with_columns(
            pl.int_range(0, pl.len()).over(by).alias("__SAS_SEQ")
        )
        renamed = {}
        for c in f.columns:
            if c not in by and c != "__SAS_SEQ":
                renamed[c] = f"{c}__SRC{i}"
                all_cols.add(c)
        f = f.rename(renamed)
        f = f.with_columns(pl.lit(True).alias(f"__IN{i}"))
        prepared.append(f)

    nonempty = [f for f in prepared if f is not None]
    if not nonempty:
        return pl.DataFrame()

    out = nonempty[0]
    for f in nonempty[1:]:
        out = out.join(f, on=by + ["__SAS_SEQ"], how="full", coalesce=True)

    out = out.sort(by + ["__SAS_SEQ"])
    source_cols = {}
    for i, f in enumerate(prepared):
        if f is not None:
            for c in f.columns:
                if c.endswith(f"__SRC{i}"):
                    source_cols.setdefault(c[:-len(f"__SRC{i}")], []).append(c)

    fill_exprs = []
    for cols in source_cols.values():
        fill_exprs.extend(pl.col(c).forward_fill().over(by).alias(c) for c in cols)
    if fill_exprs:
        out = out.with_columns(fill_exprs)

    for base, cols in source_cols.items():
        out = out.with_columns(
            pl.coalesce([pl.col(c) for c in reversed(cols)]).alias(base)
        )

    if in_filter is not None:
        idx, flag = in_filter
        out = out.filter(pl.col(f"__IN{idx}").fill_null(False))

    drop = [c for c in out.columns if c.startswith("__")]
    return out.drop(drop)

# ---------------------------------------------------------------------------
# Fixed-width helpers - SAS @ positions are 1-based
# ---------------------------------------------------------------------------
def put_field(buf, position, width, value, align="left", numeric=False, decimals=2):
    start = position - 1
    if numeric:
        if value is None:
            text = ""
        else:
            v = safe_num(value)
            text = f"{v:.{decimals}f}" if decimals is not None else str(int(v))
    else:
        text = as_text(value)
    text = text[:width]
    text = text.rjust(width) if align == "right" else text.ljust(width)
    buf[start:start + width] = text

def make_intermediate(row):
    buf = [" "] * EXPECTED_LEN
    specs = [
        (1,7,"BRANCH","left",False,2),(9,40,"NAME","left",False,2),
        (50,10,"ACCTNO","right",True,0),(61,5,"NOTENO","right",True,0),
        (67,1,"BORSTAT","left",False,2),(69,16,"IIS","right",True,2),
        (85,16,"OI","right",True,2),(101,16,"TOTIIS","right",True,2),
        (117,16,"SP","right",True,2),(149,16,"CURBAL","right",True,2),
        (165,16,"PREVBAL","right",True,2),(181,16,"PAYMENT","right",True,2),
        (197,16,"ECSRRSRV","right",True,2),(213,16,"POSTAMT","right",True,2),
        (229,16,"OTHERAMT","right",True,2),(245,10,"MATDATE","left",False,2),
        (255,3,"LOANTYPE","right",True,0),(258,16,"INTAMT","right",True,2),
        (274,1,"POSTNTRN","left",False,2),(278,16,"MARKETVL","right",True,2),
        (294,16,"INTEARN4","right",True,2),(310,6,"DAYS","right",True,0),
        (317,3,"CUSTCODE","right",True,0),(321,1,"RIND","left",False,2),
        (322,16,"OIFEEAMT","right",True,2),(339,10,"LASTTRA1","left",False,2),
        (350,3,"LSTTRNCD","right",True,0),(354,3,"MTHPDUE","right",True,0),
        (357,16,"BALANCE","right",True,2),(408,20,"GUAREND","left",False,2),
        (429,40,"GUARNAM1","left",False,2),(470,40,"GUARNAM2","left",False,2),
        (511,10,"ISSXDTE","left",False,2),(522,16,"NETPROC","right",True,2),
        (539,70,"COLLDESC","left",False,2),(610,4,"COLLYEAR","right",True,0),
        (615,3,"BILPAID","right",True,0),(619,5,"CRRGRADE","left",False,2),
        (625,16,"MARGINFI","right",True,2),(642,3,"NOTETERM","right",True,0),
        (646,16,"PAYAMT","right",True,2),(663,10,"DOBMNI","left",False,2),
        (674,1,"ECSRIND","left",False,2),(677,2,"DELQCD","left",False,2),
        (713,3,"OCCUPAT","left",False,2),(743,2,"BGC","left",False,2),
        (767,1,"PAY75PCT","left",False,2),(769,10,"NACODATE","left",False,2),
        (780,1,"CP","left",False,2),(782,6,"MODELDES","left",False,2),
        (789,9,"AKPK_STATUS","left",False,2),
    ]
    for p,w,c,a,n,d in specs:
        value = row.get(c)
        if c in ("ISSXDTE", "DOBMNI") and isinstance(value, (date, datetime)):
            value = value.strftime("%m/%d/%Y")
        put_field(buf,p,w,value,a,n,d)
    return "".join(buf)

def get_delq_desc(code):
    return format_delqdes(code) if code else "NO LEGAL ACTION TAKEN"

def get_occup_desc(code):
    return OCCUPFMT.get(as_text(code) or "999", "OTHERS")

def get_bgc_desc(code):
    return BGCFMT.get(as_text(code) or "  ", "NOT SPECIFIED")

def write_final(intermediate, output):
    with open(intermediate, "r", encoding="latin1") as src, \
         open(output, "w", encoding="latin1", newline="") as dst:
        for line in src:
            line = line.rstrip("\r\n").ljust(EXPECTED_LEN)
            totiis = safe_num(line[100:116].strip())
            balance = safe_num(line[356:372].strip())
            oi = safe_num(line[84:100].strip())
            sp = balance - totiis
            total = totiis + sp
            delqcd, occupat, bgc = line[676:678], line[712:715], line[742:744]

            buf = [" "] * EXPECTED_LEN
            # Start with original text; then replace/add fields at SAS @ columns.
            buf[:len(line)] = list(line[:EXPECTED_LEN])
            put_field(buf,117,16,sp,"right",True,2)
            put_field(buf,133,16,total,"right",True,2)
            put_field(buf,374,16,0,"right",True,2)
            put_field(buf,391,16,oi,"right",True,2)
            put_field(buf,680,30,get_delq_desc(delqcd))
            put_field(buf,711,1,"C")
            put_field(buf,717,25,get_occup_desc(occupat))
            put_field(buf,746,20,get_bgc_desc(bgc))
            dst.write("".join(buf) + "\n")

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
print("\nEIFFTXT1 Conventional - starting")
print("Reading report date from LOAN.REPTDATE...")
df_date = read_sas(PATH_REPTDATE, ["REPTDATE"], required=["REPTDATE"])
if df_date.height == 0:
    raise RuntimeError("LOAN.REPTDATE contains no report date")
raw_date = df_date["REPTDATE"][0]
if isinstance(raw_date, datetime):
    reptdate = raw_date.date()
elif isinstance(raw_date, date):
    reptdate = raw_date
else:
    raise RuntimeError(f"Unsupported REPTDATE value: {raw_date!r}")

day = reptdate.day
if day == 8:
    wk, wk1 = "1", "4"
elif day == 15:
    wk, wk1 = "2", "1"
elif day == 22:
    wk, wk1 = "3", "2"
else:
    wk, wk1 = "4", "3"

mm = reptdate.month
mm1 = 12 if mm == 1 else mm - 1
nowk, nowks, nowk1 = wk, "4", wk1
reptmon, reptmon1 = f"{mm:02d}", f"{mm1:02d}"
reptyear = f"{reptdate.year % 100:02d}"
rdate = reptdate.strftime("%d/%m/%y")

PATH_CCRIS = PATH_CCRIS.with_name(f"credmsubac{reptmon}{reptyear}.sas7bdat")
PATH_SASLN = PATH_SASLN.with_name(f"loan{reptmon1}{nowks}.sas7bdat")
print(f"Report date: {reptdate:%d/%m/%Y}; week={nowk}; CCRIS={PATH_CCRIS.name}; SASLN={PATH_SASLN.name}")

# Step 1: NPLA from LOAN.LNNOTE
npl_cols = ["NAME","ACCTNO","NOTENO","IIS","OI","TOTIIS","SP","MARKETVL","BRANCH"]
loan_cols = [
    "BORSTAT","LOANTYPE","FEEDUE","FEEDUEMS","FEEAMT16","NAME","ACCTNO","NOTENO",
    "MARKETVL","NTBRCH","FEETOTAL","NFEEAMT5","FEEAMT3","FEEAMT4","FEETOT2",
    "FEEAMTA","FEEAMT5","ECSRRSRV","MATUREDT","LASTTRAN","SCORE2","CONTRTYPE",
    "NETPROC","APPVALUE","BIRTHDT","ORGBAL","CURBAL","PAYAMT","NACOSPADT",
    "INTAMT","POSTNTRN","INTEARN4","CUSTCODE","LSTTRNCD","BALANCE","GUAREND",
    "ISSXDTE","COLLDESC","COLLYEAR","NOTETERM","DELQCD","CP","MODELDES",
    "AKPK_STATUS","PAIDIND",
]
print("Step 1: Reading LOAN.LNNOTE...")
df_lnnote = read_sas(PATH_LNNOTE, loan_cols, required=["BORSTAT","LOANTYPE","ACCTNO","NOTENO"])
df_lnnote = to_num(df_lnnote, ["ACCTNO","NOTENO","LOANTYPE","FEEDUE","FEEDUEMS","FEEAMT16","NTBRCH","MARKETVL"])
df_npla = df_lnnote.filter(
    (pl.col("BORSTAT").cast(pl.Utf8).str.strip_chars() == "A") &
    (~pl.col("LOANTYPE").is_in(EXCLUDED_LOANTYPES))
)
df_npla = df_npla.with_columns([
    pl.lit(0.0).alias("IIS"),
    (pl.col("FEEDUE").fill_null(0) - pl.col("FEEDUEMS").fill_null(0)).alias("OI"),
    (pl.col("FEEDUE").fill_null(0) - pl.col("FEEDUEMS").fill_null(0)).alias("TOTIIS"),
    (pl.col("FEEDUEMS").fill_null(0) + pl.col("FEEAMT16").fill_null(0)).alias("SP"),
    pl.col("NTBRCH").map_elements(branch_text, return_dtype=pl.Utf8).alias("BRANCH"),
]).select(npl_cols)

# Step 2: MERGE SP IIS BY ACCTNO NOTENO (SAS later dataset IIS wins)
print("Step 2: Reading IIS and SP2...")
df_sp = read_sas(PATH_SP2, npl_cols, required=["ACCTNO","NOTENO"], optional=True)
df_iis = read_sas(PATH_IIS, npl_cols, required=["ACCTNO","NOTENO"], optional=True)
for name, frame in (("SP",df_sp),("IIS",df_iis)):
    if frame.height:
        if frame.select(pl.struct(["ACCTNO","NOTENO"]).is_duplicated().any()).item():
            frame = frame.unique(subset=["ACCTNO","NOTENO"], keep="first")
        if name == "SP": df_sp = frame
        else: df_iis = frame
df_npl_data = sas_match_merge([df_sp,df_iis], ["ACCTNO","NOTENO"])
if df_npl_data.height:
    df_npl_data = add_missing(df_npl_data,npl_cols).select(npl_cols)
else:
    df_npl_data = empty_like(npl_cols)
df_npl = pl.concat([df_npla,df_npl_data], how="diagonal_relaxed")
df_npl = (df_npl.with_columns(pl.col("MARKETVL").round(2))
          .unique(subset=["ACCTNO","NOTENO"], keep="first")
          .sort(["ACCTNO","NOTENO"]))
print(f"  NPL rows: {df_npl.height:,}")

# Step 3: CCRIS CREDMSUBAC
print("Step 3: Reading CCRIS...")
df_creds = read_sas(PATH_CCRIS, ["ACCTNUM","NOTENO","DAYSARR","FACILITY"],
                    required=["ACCTNUM","NOTENO","DAYSARR","FACILITY"])
df_creds = (df_creds.filter(pl.col("FACILITY").cast(pl.Utf8).str.strip_chars().is_in(["34331","34332"]))
            .rename({"ACCTNUM":"ACCTNO","DAYSARR":"DAYS"})
            .sort(["ACCTNO","NOTENO","DAYS"], descending=[False,False,True])
            .unique(subset=["ACCTNO","NOTENO"], keep="first"))

# Step 4: HPD loan records from full LOAN.LNNOTE (SAS PROC SORT NODUPKEY)
print("Step 4: Preparing HPD loan records...")
df_loan_src = (df_lnnote.filter(pl.col("LOANTYPE").is_in(list(HPD)))
               .sort(["ACCTNO","NOTENO"])
               .unique(subset=["ACCTNO","NOTENO"], keep="first"))

# SAS: MERGE NPL(IN=AA) CREDSUB LOAN; BY ACCTNO NOTENO; IF AA;
df_loan = sas_match_merge([df_npl,df_creds,df_loan_src],
                          ["ACCTNO","NOTENO"], in_filter=(0,"__IN0"))
# Derived fields (SAS SUM() ignores missing values)
df_loan = add_missing(df_loan, loan_cols + [
    "DAYS","FEETOTAL","NFEEAMT5","FEEAMT3","FEETOT2","FEEAMTA","FEEAMT5",
    "ECSRRSRV","MATUREDT","LASTTRAN","SCORE2","CONTRTYPE","NETPROC","APPVALUE",
    "BIRTHDT","ORGBAL","CURBAL","PAYAMT","NACOSPADT"
])
df_loan = to_num(df_loan, [
    "FEETOTAL","NFEEAMT5","FEEAMT3","FEETOT2","FEEAMTA","FEEAMT5","ECSRRSRV",
    "MATUREDT","LASTTRAN","NETPROC","APPVALUE","BIRTHDT","ORGBAL","CURBAL",
    "PAYAMT","NACOSPADT","DAYS"
])
df_loan = df_loan.with_columns([
    sas_sum("FEETOTAL","NFEEAMT5").alias("POSTAMT"),
    (pl.col("FEEAMT3").fill_null(0) - sas_sum("FEETOTAL","NFEEAMT5")).alias("OTHERAMT"),
    (pl.col("FEETOT2").fill_null(0) - pl.col("FEEAMTA").fill_null(0) + pl.col("FEEAMT5").fill_null(0)).alias("OIFEEAMT"),
    pl.when(pl.col("ECSRRSRV").is_null() | (pl.col("ECSRRSRV") <= 0)).then(0).otherwise(pl.col("ECSRRSRV")).alias("ECSRRSRV"),
    pl.when(pl.col("APPVALUE").is_not_null() & (pl.col("APPVALUE") != 0))
      .then((pl.col("NETPROC") / pl.col("APPVALUE")).round(2)).otherwise(None).alias("MARGINFI"),
    pl.when(pl.col("ECSRRSRV") > 0).then(pl.lit("Y")).otherwise(pl.lit("N")).alias("ECSRIND"),
    pl.when(pl.col("NACOSPADT").fill_null(0) > 0).then(pl.lit("Y")).otherwise(pl.lit("N")).alias("PAY75PCT"),
])
date_cols = []
for source, target in (("MATUREDT","MATDATE"),("LASTTRAN","LASTTRA1"),("BIRTHDT","DOBMNI"),("NACOSPADT","NACODATE")):
    values = [z11_date(v) for v in df_loan[source].to_list()]
    date_cols.append(pl.Series(target, values, dtype=pl.Date))
df_loan = df_loan.with_columns(date_cols)
df_loan = df_loan.with_columns([
    pl.Series("MTHPDUE",[calc_mthpdue(v) for v in df_loan["DAYS"].to_list()],dtype=pl.Int64),
    pl.Series("CRRGRADE",[
        (as_text(a)+as_text(b))[:5]
        for a,b in zip(df_loan["SCORE2"].to_list(),df_loan["CONTRTYPE"].to_list())
    ],dtype=pl.Utf8),
    pl.Series("BILPAID",[
        int((safe_num(a)-safe_num(b))/safe_num(c)) if safe_num(c) != 0 else None
        for a,b,c in zip(df_loan["ORGBAL"].to_list(),df_loan["CURBAL"].to_list(),df_loan["PAYAMT"].to_list())
    ],dtype=pl.Int64),
])
# SAS stores MATUREDT as an SAS date after conversion; retain MATDATE as formatted text.
df_loan = df_loan.with_columns([
    pl.col("MATDATE").map_elements(mmddyy, return_dtype=pl.Utf8).alias("MATDATE"),
    pl.col("LASTTRA1").map_elements(mmddyy, return_dtype=pl.Utf8).alias("LASTTRA1"),
    pl.col("DOBMNI").map_elements(mmddyy, return_dtype=pl.Utf8).alias("DOBMNI"),
    pl.col("NACODATE").map_elements(mmddyy, return_dtype=pl.Utf8).alias("NACODATE"),
])

# Step 5: CNAME
print("Step 5: Reading CNAME...")
df_cname = read_sas(PATH_CNAME, ["ACCTNO","CUSTNAM1","OCCUPAT","BGC","SECCUST"],
                    required=["ACCTNO","SECCUST"])
df_cname = (df_cname.filter(pl.col("SECCUST").cast(pl.Utf8).str.strip_chars() == "901")
            .sort("ACCTNO").unique(subset=["ACCTNO"],keep="first")
            .select(["ACCTNO","CUSTNAM1","OCCUPAT","BGC"]))

# Step 6: LIAB and PROC TRANSPOSE equivalent
print("Step 6: Reading LOAN.LIAB...")
df_liab = read_sas(PATH_LIAB, ["ACCTNO","NOTENO","LIABACCT","LIABNAME"],
                   required=["ACCTNO","NOTENO","LIABACCT"])
df_liab = sas_match_merge([
    df_liab.sort("LIABACCT"),
    df_cname.select(["ACCTNO","CUSTNAM1"]).rename({"ACCTNO":"LIABACCT","CUSTNAM1":"GNAME"}).sort("LIABACCT")
], ["LIABACCT"], in_filter=(0,"__IN0"))
df_liab = df_liab.with_columns(
    pl.when(pl.col("GNAME").is_null() | (pl.col("GNAME").cast(pl.Utf8).str.strip_chars()==""))
      .then(pl.col("LIABNAME")).otherwise(pl.col("GNAME")).alias("GNAME")
).sort(["ACCTNO","NOTENO"])
guar_rows = []
for key, grp in df_liab.group_by(["ACCTNO","NOTENO"], maintain_order=True):
    names = [as_text(x) for x in grp["GNAME"].to_list()]
    guar_rows.append({"ACCTNO":key[0],"NOTENO":key[1],
                      "GUARNAM1":names[0] if names else "",
                      "GUARNAM2":names[1] if len(names)>1 else ""})
df_guar = pl.DataFrame(guar_rows) if guar_rows else pl.DataFrame(
    schema={"ACCTNO":pl.Float64,"NOTENO":pl.Float64,"GUARNAM1":pl.Utf8,"GUARNAM2":pl.Utf8})

# Step 7: Previous balance + MERGE SASLN NPL(IN=AA) LIAB BY ACCTNO NOTENO
print("Step 7: Previous balance...")
df_sasln = read_sas(PATH_SASLN, ["ACCTNO","NOTENO","CURBAL"], required=["ACCTNO","NOTENO"])
df_sasln = df_sasln.rename({"CURBAL":"PREVBAL"})
df_sasln = sas_match_merge([df_sasln,df_npl,df_guar],["ACCTNO","NOTENO"],in_filter=(1,"__IN1"))
df_sasln = add_missing(df_sasln,["PREVBAL","GUARNAM1","GUARNAM2"])

# Step 8: Final SAS MERGE SASLN LOAN NPL; BY ACCTNO (not account+note!)
print("Step 8: Final SAS match-merge by ACCTNO...")
df_woff = sas_match_merge([df_sasln,df_loan,df_npl],["ACCTNO"])
df_woff = add_missing(df_woff,["CURBAL","PREVBAL","TOTIIS","SP","BORSTAT","DAYS","LOANTYPE","PAIDIND"])
df_woff = df_woff.with_columns([
    (pl.col("CURBAL") - pl.col("PREVBAL")).alias("PAYMENT"),
    (pl.col("TOTIIS") + pl.col("SP")).alias("TOTAL"),
    pl.lit(RIND).alias("RIND"),
])
# Literal SAS filter: first clause is subsumed by DAYS>=334, retained for clarity.
df_woff = df_woff.filter(
    (
        (pl.col("BORSTAT").is_in(["F","I"]) & (pl.col("DAYS") >= 334)) |
        (pl.col("DAYS") >= 334) |
        ((pl.col("BORSTAT") == "A") &
         (~pl.col("LOANTYPE").is_in(EXCLUDED_LOANTYPES)) &
         (pl.col("PAIDIND").is_null() | (pl.col("PAIDIND") != "P")))
    ) & (pl.col("TOTAL").is_not_null()) & (pl.col("TOTAL") != 0)
).with_columns(pl.lit("Y").alias("CONFIRM")).sort("ACCTNO")

# Step 9: MERGE WOFF(IN=A DROP=NAME) CNAME BY ACCTNO
if "NAME" in df_woff.columns:
    df_woff = df_woff.drop("NAME")
df_woff = sas_match_merge([df_woff,df_cname.rename({"CUSTNAM1":"NAME"})],["ACCTNO"],in_filter=(0,"__IN0"))
df_woff = add_missing(df_woff,["BRANCH","NAME","NOTENO","BORSTAT","IIS","OI","TOTIIS","SP",
    "CURBAL","PREVBAL","PAYMENT","ECSRRSRV","POSTAMT","OTHERAMT","MATDATE","LOANTYPE",
    "INTAMT","POSTNTRN","MARKETVL","INTEARN4","DAYS","CUSTCODE","RIND","OIFEEAMT",
    "LASTTRA1","LSTTRNCD","MTHPDUE","BALANCE","GUAREND","GUARNAM1","GUARNAM2","ISSXDTE",
    "NETPROC","COLLDESC","COLLYEAR","BILPAID","CRRGRADE","MARGINFI","NOTETERM","PAYAMT",
    "DOBMNI","ECSRIND","DELQCD","OCCUPAT","BGC","PAY75PCT","NACODATE","CP","MODELDES","AKPK_STATUS"])

# Write intermediate and final fixed-width text
print("Step 10: Writing fixed-width files...")
with open(OUTPUT_FILE1,"w",encoding="latin1",newline="") as f:
    for row in df_woff.iter_rows(named=True):
        f.write(make_intermediate(row)+"\n")
write_final(OUTPUT_FILE1,OUTPUT_FILE)

print(f"\nCompleted: {df_woff.height:,} write-off records")
print(f"Intermediate: {OUTPUT_FILE1}")
print(f"Final:        {OUTPUT_FILE}")
if df_woff.height and "TOTAL" in df_woff.columns:
    print(f"Total exposure: RM {df_woff['TOTAL'].sum():,.2f}")
print(f"Expected fixed-width length: {EXPECTED_LEN}")
'''
path = Path("/mnt/data/EIFFTXT1_conventional_updated.py")
path.write_text(code, encoding="utf-8")
print(f"Created {path} ({len(code.splitlines())} lines, {path.stat().st_size:,} bytes)")
