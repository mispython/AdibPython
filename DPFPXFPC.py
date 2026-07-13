#!/usr/bin/env python3
"""
Program Name: EIBAABBA.py
"""

import polars as pl
import pyreadstat
from pathlib import Path
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Configuration - Define input and output paths at the beginning
# ---------------------------------------------------------------------------
BASE_PATH = Path.cwd()
INPUT_PATH = BASE_PATH / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/"
OUTPUT_PATH = BASE_PATH / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBAABBA/ABBALST.txt"

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


def read_sas_dataset(path: Path, file_name: str) -> pl.DataFrame:
    """Read SAS dataset using pyreadstat"""
    try:
        df, meta = pyreadstat.read_sas7bdat(str(path / file_name))
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
    return datetime.strptime(str(value).zfill(8)[:8], "%m%d%Y")


def calculate_age(birthdt, snapshot_date: datetime) -> int:
    try:
        bdate = to_datetime(birthdt)
        if bdate is None:
            return 0
        return round((snapshot_date - bdate).days / 365)
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


def map_collater(cclassc) -> str | None:
    if cclassc in (None, ""):
        return None
    return CCLASSC_TO_COLLATER.get(str(cclassc).zfill(3))


def process_abba_data(input_path: Path, snapshot_date: datetime) -> pl.DataFrame:
    abba_df = read_sas_dataset(input_path, "EIBAABBA/lnnote.sas7bdat")
    if abba_df.is_empty():
        return abba_df

    return (
        abba_df.filter(
            (pl.col("PAIDIND") != "P")
            & (((pl.col("LOANTYPE") >= 110) & (pl.col("LOANTYPE") <= 119)) | ((pl.col("LOANTYPE") >= 139) & (pl.col("LOANTYPE") <= 140)))
            & (pl.col("RISKRATE").is_in([2, 3, 4]))
        )
        .with_columns(
            pl.col("BIRTHDT").map_elements(lambda x: calculate_age(x, snapshot_date), return_dtype=pl.Int64).alias("AGE"),
            pl.col("PENDBRH").alias("BRANCH"),
            pl.col("COLLDESC").str.slice(0, 34).alias("COLLD"),
        )
        .select(
            [
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
            ]
        )
        .sort(["ACCTNO", "NOTENO"])
    )


def merge_sasb_data(abba_df: pl.DataFrame, input_path: Path, month: str, week: str, snapshot_date: datetime) -> pl.DataFrame:
    sasb_df = read_sas_dataset(input_path, f"EIMHPTOP/loan{month}{week}.sas7bdat")
    if sasb_df.is_empty():
        return abba_df.with_columns(
            pl.lit(None).alias("BALANCE"),
            pl.lit(None).alias("MTHARR"),
            pl.lit(None).alias("OVERDUE")
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
    
    result = abba_df.join(sasb_df, on=["ACCTNO", "NOTENO"], how="left")
    result = result.with_columns(
        (pl.col("PAYAMT") * pl.col("MTHARR")).alias("OVERDUE")
    )
    
    # Fill null values
    result = result.with_columns([
        pl.col("BALANCE").fill_null(0),
        pl.col("MTHARR").fill_null(0),
        pl.col("OVERDUE").fill_null(0),
    ])
    
    return result


def merge_customer_data(abba_df: pl.DataFrame, input_path: Path) -> pl.DataFrame:
    cisln_df = read_sas_dataset(input_path, "EIMHPTOP/loan.sas7bdat")
    if cisln_df.is_empty():
        return abba_df.with_columns(
            pl.lit("").alias("CUSTNAME"),
            pl.lit("").alias("GENDER"),
            pl.lit("").alias("OCCUPAT"),
            pl.lit("").alias("ADDRLN1"),
            pl.lit("").alias("ADDRLN2"),
            pl.lit("").alias("ADDRLN3"),
            pl.lit("").alias("ADDRLN4"),
            pl.lit("").alias("ADDRLN5"),
        )

    cisln_df = cisln_df.select(
        [
            "ACCTNO",
            "CUSTNAME",
            "GENDER",
            "OCCUPAT",
            "ADDRLN1",
            "ADDRLN2",
            "ADDRLN3",
            "ADDRLN4",
            "ADDRLN5",
        ]
    ).sort("ACCTNO")
    
    # Fill null customer fields
    cisln_df = cisln_df.with_columns([
        pl.col("CUSTNAME").fill_null(""),
        pl.col("GENDER").fill_null(""),
        pl.col("OCCUPAT").fill_null(""),
        pl.col("ADDRLN1").fill_null(""),
        pl.col("ADDRLN2").fill_null(""),
        pl.col("ADDRLN3").fill_null(""),
        pl.col("ADDRLN4").fill_null(""),
        pl.col("ADDRLN5").fill_null(""),
    ])
    
    return abba_df.join(cisln_df, on="ACCTNO", how="left")


def merge_collateral_data(abba_df: pl.DataFrame, input_path: Path) -> pl.DataFrame:
    coll_df = read_sas_dataset(input_path, "EIBAABBA/collater.sas7bdat")
    if coll_df.is_empty():
        return abba_df

    coll_df = (
        coll_df.with_columns(
            pl.col("CCLASSC").map_elements(map_collater, return_dtype=pl.Utf8).alias("COLLATER")
        )
        .select(["ACCTNO", "NOTENO", "COLLATER"])
        .rename({"COLLATER": "COLLD"})
        .sort(["ACCTNO", "NOTENO"])
    )
    
    return (
        abba_df.join(
            coll_df.select(["ACCTNO", "NOTENO", "COLLD"]),
            on=["ACCTNO", "NOTENO"],
            how="left",
            suffix="_coll",
        )
        .with_columns(pl.coalesce(pl.col("COLLD_coll"), pl.col("COLLD")).alias("COLLD"))
        .drop("COLLD_coll")
    )


def finalize_output(abba_df: pl.DataFrame) -> pl.DataFrame:
    # Fill any remaining nulls
    abba_df = abba_df.with_columns([
        pl.col("COLLD").fill_null(""),
        pl.col("SECTOR").fill_null(""),
        pl.col("STATE").fill_null(""),
    ])
    
    return abba_df.unique(subset=["ACCTNO", "NOTENO"], keep="first").sort(["BRANCH", "ACCTNO", "NOTENO"])


def eibaabba():
    """Main function for EIBAABBA - Account Analysis Report"""
    input_path = INPUT_PATH
    output_path = OUTPUT_PATH
    
    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"EIBAABBA - Account Analysis Report")
    print(f"Input path: {input_path}")
    print(f"Output path: {output_path}")
    
    # Hardcode REPTDATE (current date - 1 day)
    reptdate = datetime.now().date() - timedelta(days=1)
    context = compute_reporting_context(reptdate)
    snapshot_date = datetime.strptime(context["sdate"] + context["year"][-2:], "%d%m%y")

    print(f"Date: {context['sdate']}, Week: {context['week']}, SDD: {context['sdd']}")

    # Process data
    abba_df = process_abba_data(input_path, snapshot_date)
    if abba_df.is_empty():
        print("No LNNOTE data found")
        return

    print(f"After LNNOTE processing: {len(abba_df)} records")
    
    abba_df = merge_sasb_data(abba_df, input_path, context["month"], context["week"], snapshot_date)
    print(f"After SASB merge: {len(abba_df)} records")
    
    abba_df = merge_customer_data(abba_df, input_path)
    print(f"After customer merge: {len(abba_df)} records")
    
    abba_df = merge_collateral_data(abba_df, input_path)
    print(f"After collateral merge: {len(abba_df)} records")
    
    abba_df = finalize_output(abba_df)
    print(f"Final records: {len(abba_df)}")

    # Generate output
    generate_abba_output(abba_df, output_path)
    print(f"Processing complete. Report saved to: {output_path}")


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
    if len(output_df) > 0:
        print("\nFirst 3 records:")
        for row in output_df.head(3).iter_rows(named=True):
            print(
                f"  ACCTNO: {row.get('ACCTNO', '')}, "
                f"Customer: {str(row.get('CUSTNAME', ''))[:20]}, "
                f"Balance: {row.get('BALANCE', 0):,.2f}"
            )


if __name__ == "__main__":
    eibaabba()

above is updated code


Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBAABBA.py", line 187, in <module>
    def map_collater(cclassc) -> str | None:
TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'
You have mail in /var/spool/mail/sas_edw_dev
