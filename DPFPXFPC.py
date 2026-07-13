#!/usr/bin/env python3
"""
Program Name: EIBAABBA.py
"""

import polars as pl
import pyreadstat
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

# ---------------------------------------------------------------------------
# Configuration - Define input and output paths at the beginning
# ---------------------------------------------------------------------------
BASE_PATH = Path.cwd()
INPUT_PATH = BASE_PATH / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/"
OUTPUT_PATH = BASE_PATH / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBAABBA/ABBALST.txt"

# For testing - set to None for full production run
TEST_LIMIT = 1000  # Set to None to disable limit

SAS_BASE_DATE = datetime(1960, 1, 1)

COLLATER_GROUPS = {
    "29": {"001", "006", "007", "014", "016", "024", "025", "026", "046", "048", "049"},
    "70": {
        "000",
        "011",
        "012",
        "013",
        "017",
        "018",
        "019",
        "021",
        "027",
        "028",
        "029",
        "030",
        "031",
        "105",
        "106",
    },
    "90": {
        "002",
        "003",
        "041",
        "042",
        "043",
        "058",
        "059",
        "067",
        "068",
        "069",
        "070",
        "071",
        "072",
        "078",
        "079",
        "084",
        "107",
    },
    "30": {"004", "005"},
    "10": {
        "032",
        "033",
        "034",
        "035",
        "036",
        "037",
        "038",
        "039",
        "040",
        "044",
        "050",
        "051",
        "052",
        "053",
        "054",
        "055",
        "056",
        "057",
        "060",
        "061",
        "062",
    },
    "40": {"065", "066", "075", "076", "082", "083", "093", "094", "095", "096", "097", "098", "101", "102", "103", "104"},
    "60": {"063", "064", "073", "074", "080", "081"},
    "50": {"010", "085", "086", "087", "088", "089", "090", "091", "092"},
    "00": {"009", "022", "023"},
    "21": {"008"},
    "22": {"045", "047"},
    "23": {"015"},
    "80": {"020"},
    "81": {"108", "109"},
    "99": {"077"},
}

CCLASSC_TO_COLLATER = {
    cclassc: collater
    for collater, cclassc_values in COLLATER_GROUPS.items()
    for cclassc in cclassc_values
}

MTHARR_THRESHOLDS = [
    (698, 23),
    (668, 22),
    (638, 21),
    (608, 20),
    (577, 19),
    (547, 18),
    (516, 17),
    (486, 16),
    (456, 15),
    (424, 14),
    (394, 13),
    (364, 12),
    (333, 11),
    (303, 10),
    (273, 9),
    (243, 8),
    (213, 7),
    (182, 6),
    (151, 5),
    (121, 4),
    (89, 3),
    (59, 2),
    (30, 1),
]


def read_sas_dataset(path: Path, file_name: str, limit: Optional[int] = None) -> pl.DataFrame:
    """Read SAS dataset using pyreadstat with optional row limit"""
    try:
        full_path = path / file_name
        if not full_path.exists():
            print(f"File not found: {full_path}")
            return pl.DataFrame()

        # Read with row limit if specified
        if limit is not None:
            df, meta = pyreadstat.read_sas7bdat(str(full_path), row_limit=limit)
        else:
            df, meta = pyreadstat.read_sas7bdat(str(full_path))

        return pl.from_pandas(df)
    except Exception as e:
        print(f"Error reading {file_name}: {e}")
        return pl.DataFrame()


def compute_reporting_context(reptdate: datetime) -> dict:
    week_map = {8: ("1", 1), 15: ("2", 9), 22: ("3", 16)}
    week, sdd = week_map.get(reptdate.day, ("4", 23))
    return {
        "week": week,
        "sdd": sdd,
        "month": f"{reptdate.month:02d}",
        "year": str(reptdate.year),
        "sdate": f"{reptdate.day:02d}{reptdate.month:02d}",
    }


def to_datetime(value):
    if value in (None, 0):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return SAS_BASE_DATE + timedelta(days=int(value))
    try:
        return datetime.strptime(str(value).zfill(8)[:8], "%m%d%Y")
    except (ValueError, TypeError):
        return None


def calculate_age(birthdt, snapshot_date: datetime) -> int:
    try:
        bdate = to_datetime(birthdt)
        if bdate is None:
            return 0
        return int(round((snapshot_date - bdate).days / 365))
    except (TypeError, ValueError):
        return 0


def calculate_mtharr(bldate, snapshot_date: datetime) -> int:
    try:
        bldate_dt = to_datetime(bldate)
        if bldate_dt is None:
            return 0

        days = (snapshot_date - bldate_dt).days + 1
        if days > 729:
            return int((days / 365) * 12)

        for threshold, result in MTHARR_THRESHOLDS:
            if days > threshold:
                return result
        return 0
    except (TypeError, ValueError):
        return 0


def map_collater(cclassc) -> Optional[str]:
    """Map CCLASSC to COLLATER group"""
    if cclassc in (None, ""):
        return None
    return CCLASSC_TO_COLLATER.get(str(cclassc).zfill(3))


def safe_cast_to_int(series: pl.Series, default_value: int = 0) -> pl.Series:
    """
    Safely cast a series to Int64, replacing invalid/unparseable values
    (empty strings, whitespace, non-numeric junk, nulls) with default_value.

    Uses a non-strict cast so values that fail to parse become null instead
    of raising, then fills those nulls with default_value.
    """
    return series.cast(pl.Int64, strict=False).fill_null(default_value)


def process_abba_data(input_path: Path, snapshot_date: datetime, limit: Optional[int] = None) -> pl.DataFrame:
    """Process LNNOTE data"""
    abba_df = read_sas_dataset(input_path, "EIBAABBA/lnnote.sas7bdat", limit)
    if abba_df.is_empty():
        print("LNNOTE dataset is empty")
        return abba_df

    # Check if required columns exist
    required_cols = ["PAIDIND", "LOANTYPE", "RISKRATE", "BIRTHDT", "PENDBRH", "COLLDESC"]
    missing_cols = [col for col in required_cols if col not in abba_df.columns]
    if missing_cols:
        print(f"Missing columns in LNNOTE: {missing_cols}")
        return pl.DataFrame()

    # Convert RISKRATE to numeric safely.
    # NOTE: pl.when/then/otherwise evaluates BOTH branches over the whole
    # column before selecting per-row, so casting the raw string column
    # inside `otherwise(...)` still blows up on empty/non-numeric strings
    # even though those rows are meant to be replaced by `then(0)`.
    # Fix: use a non-strict cast (invalid -> null) then fill nulls with 0.
    if abba_df["RISKRATE"].dtype == pl.Utf8:
        abba_df = abba_df.with_columns(
            pl.col("RISKRATE").cast(pl.Int64, strict=False).fill_null(0).alias("RISKRATE")
        )
    else:
        abba_df = abba_df.with_columns(
            pl.col("RISKRATE").fill_null(0).cast(pl.Int64)
        )

    # Convert LOANTYPE to numeric safely (same fix as RISKRATE above)
    if abba_df["LOANTYPE"].dtype == pl.Utf8:
        abba_df = abba_df.with_columns(
            pl.col("LOANTYPE").cast(pl.Int64, strict=False).fill_null(0).alias("LOANTYPE")
        )
    else:
        abba_df = abba_df.with_columns(
            pl.col("LOANTYPE").fill_null(0).cast(pl.Int64)
        )

    # Convert PAYAMT to Float64 safely
    if "PAYAMT" in abba_df.columns:
        abba_df = abba_df.with_columns(
            pl.col("PAYAMT").fill_null(0).cast(pl.Float64)
        )

    return (
        abba_df.filter(
            (pl.col("PAIDIND") != "P")
            & (((pl.col("LOANTYPE") >= 110) & (pl.col("LOANTYPE") <= 119)) | ((pl.col("LOANTYPE") >= 139) & (pl.col("LOANTYPE") <= 140)))
            & (pl.col("RISKRATE").is_in([2, 3, 4]))
        )
        .with_columns([
            pl.col("BIRTHDT").map_elements(lambda x: calculate_age(x, snapshot_date), return_dtype=pl.Int64).alias("AGE"),
            pl.col("PENDBRH").alias("BRANCH"),
            pl.col("COLLDESC").str.slice(0, 34).alias("COLLD"),
        ])
        .select([
            "ACCTNO",
            "NOTENO",
            "SECTOR",
            "BRANCH",
            "STATE",
            "RISKRATE",
            "BILLCNT",
            "LOANTYPE",
            "AGE",
            "COLLD",
            "PAYAMT",
        ])
        .sort(["ACCTNO", "NOTENO"])
    )


def merge_sasb_data(abba_df: pl.DataFrame, input_path: Path, month: str, week: str, snapshot_date: datetime, limit: Optional[int] = None) -> pl.DataFrame:
    """Merge with SASB loan data"""
    sasb_df = read_sas_dataset(input_path, f"EIMHPTOP/loan{month}{week}.sas7bdat", limit)
    if sasb_df.is_empty():
        print(f"SASB data not found for month {month} week {week}")
        return abba_df.with_columns([
            pl.lit(0).alias("BALANCE"),
            pl.lit(0).alias("MTHARR"),
            pl.lit(0).alias("OVERDUE")
        ])

    # Check if required columns exist
    if "BLDATE" not in sasb_df.columns:
        print("BLDATE column not found in SASB data")
        return abba_df.with_columns([
            pl.lit(0).alias("BALANCE"),
            pl.lit(0).alias("MTHARR"),
            pl.lit(0).alias("OVERDUE")
        ])

    # Ensure ACCTNO and NOTENO are string type for join
    if sasb_df["ACCTNO"].dtype != pl.Utf8:
        sasb_df = sasb_df.with_columns(
            pl.col("ACCTNO").cast(pl.Utf8)
        )
    if sasb_df["NOTENO"].dtype != pl.Utf8:
        sasb_df = sasb_df.with_columns(
            pl.col("NOTENO").cast(pl.Utf8)
        )

    # Fill null values in BALANCE
    if "BALANCE" in sasb_df.columns:
        sasb_df = sasb_df.with_columns(
            pl.col("BALANCE").fill_null(0).cast(pl.Float64)
        )

    sasb_df = (
        sasb_df.with_columns(
            pl.col("BLDATE").map_elements(lambda x: calculate_mtharr(x, snapshot_date), return_dtype=pl.Int64).alias("MTHARR")
        )
        .select(["ACCTNO", "NOTENO", "BALANCE", "MTHARR"])
        .sort(["ACCTNO", "NOTENO"])
    )

    # Ensure numeric columns are properly typed
    abba_df = abba_df.with_columns([
        pl.col("PAYAMT").cast(pl.Float64),
    ])

    # Ensure ACCTNO and NOTENO are string type for join
    if abba_df["ACCTNO"].dtype != pl.Utf8:
        abba_df = abba_df.with_columns(
            pl.col("ACCTNO").cast(pl.Utf8)
        )
    if abba_df["NOTENO"].dtype != pl.Utf8:
        abba_df = abba_df.with_columns(
            pl.col("NOTENO").cast(pl.Utf8)
        )

    result = abba_df.join(sasb_df, on=["ACCTNO", "NOTENO"], how="left")

    # Fill null values and calculate OVERDUE
    result = result.with_columns([
        pl.col("BALANCE").fill_null(0),
        pl.col("MTHARR").fill_null(0),
    ])

    result = result.with_columns(
        (pl.col("PAYAMT") * pl.col("MTHARR")).alias("OVERDUE")
    )

    result = result.with_columns(
        pl.col("OVERDUE").fill_null(0)
    )

    return result


def merge_customer_data(abba_df: pl.DataFrame, input_path: Path, limit: Optional[int] = None) -> pl.DataFrame:
    """Merge with customer data from CIS"""
    cisln_df = read_sas_dataset(input_path, "EIMHPTOP/loan.sas7bdat", limit)
    if cisln_df.is_empty():
        print("Customer data not found")
        return abba_df.with_columns([
            pl.lit("").alias("CUSTNAME"),
            pl.lit("").alias("GENDER"),
            pl.lit("").alias("OCCUPAT"),
            pl.lit("").alias("ADDRLN1"),
            pl.lit("").alias("ADDRLN2"),
            pl.lit("").alias("ADDRLN3"),
            pl.lit("").alias("ADDRLN4"),
            pl.lit("").alias("ADDRLN5"),
        ])

    # Ensure ACCTNO is string type for join
    if cisln_df["ACCTNO"].dtype != pl.Utf8:
        cisln_df = cisln_df.with_columns(
            pl.col("ACCTNO").cast(pl.Utf8)
        )

    # Select only available columns
    customer_cols = ["ACCTNO", "CUSTNAME", "GENDER", "OCCUPAT", "ADDRLN1", "ADDRLN2", "ADDRLN3", "ADDRLN4", "ADDRLN5"]
    available_cols = [col for col in customer_cols if col in cisln_df.columns]

    cisln_df = cisln_df.select(available_cols).sort("ACCTNO")

    # Fill null customer fields
    for col in available_cols:
        if col != "ACCTNO":
            cisln_df = cisln_df.with_columns(
                pl.col(col).fill_null("")
            )

    # Ensure ACCTNO is string type for join in abba_df
    if abba_df["ACCTNO"].dtype != pl.Utf8:
        abba_df = abba_df.with_columns(
            pl.col("ACCTNO").cast(pl.Utf8)
        )

    # Merge
    result = abba_df.join(cisln_df, on="ACCTNO", how="left")

    # Fill missing columns with empty strings
    for col in ["CUSTNAME", "GENDER", "OCCUPAT", "ADDRLN1", "ADDRLN2", "ADDRLN3", "ADDRLN4", "ADDRLN5"]:
        if col not in result.columns:
            result = result.with_columns(
                pl.lit("").alias(col)
            )
        else:
            result = result.with_columns(
                pl.col(col).fill_null("")
            )

    return result


def merge_collateral_data(abba_df: pl.DataFrame, input_path: Path, limit: Optional[int] = None) -> pl.DataFrame:
    """Merge with collateral data"""
    coll_df = read_sas_dataset(input_path, "EIBAABBA/collater.sas7bdat", limit)
    if coll_df.is_empty():
        print("Collateral data not found")
        return abba_df.with_columns(
            pl.lit("").alias("COLLD")
        )

    # Check if required columns exist
    if "CCLASSC" not in coll_df.columns:
        print("CCLASSC column not found in collateral data")
        return abba_df.with_columns(
            pl.lit("").alias("COLLD")
        )

    # Ensure ACCTNO and NOTENO are string type for join
    if coll_df["ACCTNO"].dtype != pl.Utf8:
        coll_df = coll_df.with_columns(
            pl.col("ACCTNO").cast(pl.Utf8)
        )
    if coll_df["NOTENO"].dtype != pl.Utf8:
        coll_df = coll_df.with_columns(
            pl.col("NOTENO").cast(pl.Utf8)
        )

    # Ensure ACCTNO and NOTENO are string type for join in abba_df
    if abba_df["ACCTNO"].dtype != pl.Utf8:
        abba_df = abba_df.with_columns(
            pl.col("ACCTNO").cast(pl.Utf8)
        )
    if abba_df["NOTENO"].dtype != pl.Utf8:
        abba_df = abba_df.with_columns(
            pl.col("NOTENO").cast(pl.Utf8)
        )

    coll_df = (
        coll_df.with_columns(
            pl.col("CCLASSC").map_elements(map_collater, return_dtype=pl.Utf8).alias("COLLATER")
        )
        .select(["ACCTNO", "NOTENO", "COLLATER"])
        .rename({"COLLATER": "COLLD"})
        .sort(["ACCTNO", "NOTENO"])
    )

    result = abba_df.join(
        coll_df.select(["ACCTNO", "NOTENO", "COLLD"]),
        on=["ACCTNO", "NOTENO"],
        how="left",
        suffix="_coll",
    )

    # Coalesce COLLD columns
    if "COLLD_coll" in result.columns:
        result = result.with_columns(
            pl.coalesce(pl.col("COLLD_coll"), pl.col("COLLD")).alias("COLLD")
        ).drop("COLLD_coll")
    else:
        result = result.with_columns(
            pl.col("COLLD").fill_null("")
        )

    return result


def finalize_output(abba_df: pl.DataFrame) -> pl.DataFrame:
    """Finalize output data"""
    # Fill any remaining nulls
    fill_cols = ["COLLD", "SECTOR", "STATE", "CUSTNAME", "GENDER", "OCCUPAT"]
    for col in fill_cols:
        if col in abba_df.columns:
            abba_df = abba_df.with_columns(
                pl.col(col).fill_null("")
            )

    return abba_df.unique(subset=["ACCTNO", "NOTENO"], keep="first").sort(["BRANCH", "ACCTNO", "NOTENO"])


def eibaabba():
    """Main function for EIBAABBA - Account Analysis Report"""
    input_path = INPUT_PATH
    output_path = OUTPUT_PATH
    test_limit = TEST_LIMIT

    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("EIBAABBA - Account Analysis Report")
    print("=" * 60)
    print(f"Input path: {input_path}")
    print(f"Output path: {output_path}")
    if test_limit:
        print(f"*** TEST MODE - Row limit: {test_limit} per dataset ***")
    else:
        print(f"*** PRODUCTION MODE - No row limit ***")
    print()

    # Hardcode REPTDATE (current date - 1 day)
    reptdate = datetime.now().date() - timedelta(days=1)
    context = compute_reporting_context(reptdate)
    snapshot_date = datetime.strptime(context["sdate"] + context["year"][-2:], "%d%m%y")

    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Snapshot Date: {snapshot_date.strftime('%d/%m/%Y')}")
    print(f"Week: {context['week']}, SDD: {context['sdd']}")
    print("-" * 60)

    # Process data
    print("Reading LNNOTE data...")
    abba_df = process_abba_data(input_path, snapshot_date, test_limit)
    if abba_df.is_empty():
        print("No LNNOTE data found")
        return

    print(f"After LNNOTE processing: {len(abba_df):,} records")

    print("Merging with SASB loan data...")
    abba_df = merge_sasb_data(abba_df, input_path, context["month"], context["week"], snapshot_date, test_limit)
    print(f"After SASB merge: {len(abba_df):,} records")

    print("Merging with customer data...")
    abba_df = merge_customer_data(abba_df, input_path, test_limit)
    print(f"After customer merge: {len(abba_df):,} records")

    print("Merging with collateral data...")
    abba_df = merge_collateral_data(abba_df, input_path, test_limit)
    print(f"After collateral merge: {len(abba_df):,} records")

    print("Finalizing output...")
    abba_df = finalize_output(abba_df)
    print(f"Final records: {len(abba_df):,}")
    print("-" * 60)

    # Generate output
    generate_abba_output(abba_df, output_path)
    print(f"\nProcessing complete. Report saved to: {output_path}")
    print("=" * 60)


def generate_abba_output(df: pl.DataFrame, output_path: Path):
    """Generate text file output"""
    if df.is_empty():
        print("No data to output")
        return

    output_columns = [
        "ACCTNO",
        "NOTENO",
        "BRANCH",
        "LOANTYPE",
        "SECTOR",
        "STATE",
        "RISKRATE",
        "COLLD",
        "OVERDUE",
        "BALANCE",
        "MTHARR",
        "BILLCNT",
        "AGE",
        "GENDER",
        "OCCUPAT",
        "CUSTNAME",
        "ADDRLN1",
        "ADDRLN2",
        "ADDRLN3",
        "ADDRLN4",
        "ADDRLN5",
    ]

    # Select only columns that exist
    existing_columns = [col for col in output_columns if col in df.columns]
    output_df = df.select(existing_columns)

    # Write to text file with header
    with open(output_path, 'w') as f:
        # Write header
        f.write("|".join(existing_columns) + "\n")

        # Write data rows
        for row in output_df.iter_rows(named=True):
            row_values = []
            for col in existing_columns:
                val = row.get(col, '')
                if val is None:
                    val = ''
                row_values.append(str(val))
            f.write("|".join(row_values) + "\n")

    print(f"Output file created: {output_path}")
    print(f"Total records: {len(output_df):,}")

    if len(output_df) > 0:
        print("\nSample records (first 3):")
        print("-" * 60)
        for row in output_df.head(3).iter_rows(named=True):
            acctno = str(row.get('ACCTNO', ''))
            custname = str(row.get('CUSTNAME', ''))[:30]
            balance = row.get('BALANCE', 0) or 0
            print(f"  ACCTNO: {acctno:<15} Customer: {custname:<30} Balance: {balance:>15,.2f}")
        print("-" * 60)


if __name__ == "__main__":
    eibaabba()
