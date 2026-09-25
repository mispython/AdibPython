import polars as pl
import pyreadstat
import saspy
from datetime import datetime, timedelta
from pathlib import Path

# -----------------------------
# CONFIGURATION
# -----------------------------
input_deposit_dir = "/host_pq/dwh/input"                    # dir containing DPDARPGS_FB_*
input_dyibu_dir   = "/stgsrcsys/host/uat/maa/python/input"  # dir containing dyibu*{mon}.sas7bdat
output_dir        = "/stgsrcsys/host/uat/maa/python/output"

# REPTDATE = yesterday
run_date = datetime.now() - timedelta(days=1)
reptyear = run_date.strftime("%Y")
reptmon  = run_date.strftime("%m")
reptday  = run_date.strftime("%d")
rdate    = run_date.strftime("%Y-%m-%d")

# Deposit flat file: DPDARPGS_FB_{YYYY}{MM}{DD}
deposit_file = f"{input_deposit_dir}/DPDARPGS_FB_{reptyear}{reptmon}{reptday}"

# SAS session for writing SAS7BDAT
sas = saspy.SASsession()

# -----------------------------
# STEP 0: Read existing dyibu*{reptmon}.sas7bdat inputs (lowercase columns)
# -----------------------------
dyibu_names = ["DYIBUF", "DYIBUB", "DYIBUA", "DYIBUN", "DYIBUY"]

existing_dyibu = {}
for name in dyibu_names:
    # file name: dyibu{f,b,a,n,y}{mon}.sas7bdat  -> e.g. dyibuf09.sas7bdat
    fname = f"{name.lower()}{reptmon}.sas7bdat"
    path = Path(input_dyibu_dir) / fname

    if path.exists():
        pdf, meta = pyreadstat.read_sas7bdat(str(path))
        pdf.columns = [c.lower() for c in pdf.columns]
        existing_dyibu[name] = pl.from_pandas(pdf)
        print(f"Loaded {name} from {path.name}: {len(existing_dyibu[name])} rows")
    else:
        existing_dyibu[name] = None
        print(f"No existing {name} at {path} — will create new.")

# -----------------------------
# STEP 1: Read deposit flat file (fixed-width, packed decimals)
# -----------------------------
# SAS: LRECL=1693
# Positions (1-based SAS -> 0-based Python):
#   @3   BANKNO   PD2.   -> bytes 2:4
#   @24  REPTNO   PD3.   -> bytes 23:26
#   @27  FMTCODE  PD2.   -> bytes 26:28
#   @106 BRANCH   PD4.   -> bytes 105:109
#   @110 ACCTNO   PD6.   -> bytes 109:115
#   @155 OPENIND  $1.    -> byte  154:155
#   @156 CURBAL   PD6.2  -> bytes 155:161
#   @208 INTPLAN  PD2.   -> bytes 207:209
#   @392 LMATDATE PD6.   -> bytes 391:397

def read_packed_decimal(raw: bytes, scale: int = 0) -> float:
    """Decode a packed decimal (COMP-3) field."""
    digits = ""
    sign = 0x0C  # default positive
    for i, b in enumerate(raw):
        high = (b >> 4) & 0x0F
        low  = b & 0x0F
        if i == len(raw) - 1:
            digits += str(high)
            sign = low
        else:
            digits += str(high) + str(low)
    val = int(digits) if digits else 0
    if sign in (0x0D, 0x0B):
        val = -val
    return val / (10 ** scale)

def read_flat_file(path: str) -> pl.DataFrame:
    records = []
    with open(path, "rb") as f:
        for line in f:
            if len(line) < 397:
                continue
            records.append({
                "BANKNO":   read_packed_decimal(line[2:4]),
                "REPTNO":   read_packed_decimal(line[23:26]),
                "FMTCODE":  read_packed_decimal(line[26:28]),
                "BRANCH":   read_packed_decimal(line[105:109]),
                "ACCTNO":   read_packed_decimal(line[109:115]),
                "OPENIND":  line[154:155].decode("ascii", errors="ignore"),
                "CURBAL":   read_packed_decimal(line[155:161], scale=2),
                "INTPLAN":  read_packed_decimal(line[207:209]),
                "LMATDATE": read_packed_decimal(line[391:397]),
            })
    return pl.DataFrame(records)

print(f"Reading deposit file: {deposit_file}")
pl_fd = read_flat_file(deposit_file)

# -----------------------------
# STEP 2: Filter valid deposits (same logic as SAS)
# -----------------------------
pl_fd = pl_fd.filter(
    (pl.col("BANKNO") == 33) &
    (pl.col("REPTNO") == 4001) &
    (pl.col("FMTCODE").is_in([1, 2])) &
    (pl.col("OPENIND").is_in(["D", "O"])) &
    (
        ((pl.col("INTPLAN") >= 340) & (pl.col("INTPLAN") <= 359)) |
        ((pl.col("INTPLAN") >= 448) & (pl.col("INTPLAN") <= 459)) |
        ((pl.col("INTPLAN") >= 461) & (pl.col("INTPLAN") <= 469)) |
        ((pl.col("INTPLAN") >= 580) & (pl.col("INTPLAN") <= 599)) |
        ((pl.col("INTPLAN") >= 660) & (pl.col("INTPLAN") <= 740))
    )
)

# Convert LMATDATE (numeric YYYYMMDD or 0) to datetime
def parse_lmatdate(val):
    try:
        if val is None or val == 0:
            return None
        s = str(int(val)).zfill(8)
        return datetime.strptime(s[:8], "%Y%m%d")
    except Exception:
        return None

pl_fd = pl_fd.with_columns([
    pl.col("LMATDATE")
      .map_elements(parse_lmatdate, return_dtype=pl.Datetime)
      .alias("LMATDT")
])

# -----------------------------
# STEP 3: Helper — summarize by period
# -----------------------------
def summarise_period(df: pl.DataFrame, label: str, condition) -> pl.DataFrame:
    subset = df.filter(condition)
    grouped = (
        subset
        .group_by(["BRANCH", "INTPLAN"])
        .agg([
            pl.len().alias("FDINO"),          # count of accounts
            pl.sum("CURBAL").alias("FDI")     # sum of current balance
        ])
        .with_columns(pl.lit(run_date).alias("REPTDATE"))
    )
    print(f"{label}: {len(grouped)} rows summarized.")
    return grouped

# -----------------------------
# STEP 4: Period definitions
# -----------------------------
DYIBUF = summarise_period(pl_fd, "DYIBUF", pl.col("LMATDT").is_not_null())
DYIBUB = summarise_period(pl_fd, "DYIBUB", pl.col("LMATDT") < datetime(2004, 9, 4))
DYIBUA = summarise_period(
    pl_fd, "DYIBUA",
    (pl.col("LMATDT") >= datetime(2004, 9, 4)) &
    (pl.col("LMATDT") <= datetime(2006, 4, 15))
)
DYIBUN = summarise_period(
    pl_fd, "DYIBUN",
    (pl.col("LMATDT") >= datetime(2006, 4, 16)) &
    (pl.col("LMATDT") <= datetime(2008, 9, 15))
)
DYIBUY = summarise_period(pl_fd, "DYIBUY", pl.col("LMATDT") >= datetime(2008, 9, 16))

new_results = {
    "DYIBUF": DYIBUF,
    "DYIBUB": DYIBUB,
    "DYIBUA": DYIBUA,
    "DYIBUN": DYIBUN,
    "DYIBUY": DYIBUY,
}

# -----------------------------
# STEP 5: Merge with existing + write SAS7BDAT via saspy
# -----------------------------
def save_via_saspy(df: pl.DataFrame, name: str):
    """Write a polars DataFrame to a SAS dataset via saspy with lowercase columns."""
    pdf = df.to_pandas()
    pdf.columns = [c.lower() for c in pdf.columns]

    # Convert REPTDATE to SAS-friendly date string
    if "reptdate" in pdf.columns:
        pdf["reptdate"] = pdf["reptdate"].dt.strftime("%Y-%m-%d")

    sas.df2sd(pdf, table=name, libref="WORK")
    print(f"Wrote {name} -> WORK.{name} ({len(pdf)} rows)")

for name, new_df in new_results.items():
    existing = existing_dyibu.get(name)

    if existing is not None and len(existing) > 0:
        if reptday == "01":
            # SAS logic: on the 1st, delete ALL existing and replace
            combined = new_df
            print(f"{name}: REPTDAY=01 → replacing all existing rows.")
        else:
            # SAS logic: delete rows with the same REPTDATE, then append
            if "reptdate" in existing.columns:
                existing = existing.filter(
                    pl.col("reptdate").cast(pl.Utf8) != rdate
                )
            combined = pl.concat([existing, new_df], how="diagonal_relaxed")
            print(f"{name}: appended {len(new_df)} rows to {len(existing)} retained rows.")

        save_via_saspy(combined, name)
    else:
        save_via_saspy(new_df, name)

print("All summaries successfully exported as SAS7BDAT via saspy.")
