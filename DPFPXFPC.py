#!/usr/bin/env python3
"""
ELDS BNM Summary Processing
Processes Bank Negara Malaysia summary files with cumulative data loading.

Change from previous version:
  - The previous cumulative file is no longer assumed to be exactly 1 day
    before REPTDATE. The latest earlier partition is used instead, so a
    missed day no longer resets the cumulative history.
  - A missing previous file / date gap only prints a WARNING (no abort).
  - UNION ALL BY NAME, atomic write, row-count assertion, temp-file cleanup.
  - Append failure no longer silently falls back to "today only".
  - Optional --date YYYY-MM-DD to re-run / backfill a specific report date.
"""

import polars as pl
import pyreadstat
import duckdb
from datetime import datetime, timedelta
from pathlib import Path
import argparse
import sys
import re
import traceback

# =============================================================================
# CONFIGURATION PATHS
# =============================================================================
BASE_INPUT_PATH = Path("/host/mis/input/ELDS")
ELDS_DATA_PATH = Path("/parquet/dwh/ELDS")
ELDS_DATA_PATH.mkdir(parents=True, exist_ok=True)

# =============================================================================
# DATE VARIABLES
# =============================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--date", help="Report date YYYY-MM-DD (default: yesterday)")
ARGS, _ = parser.parse_known_args()

if ARGS.date:
    REPTDATE = datetime.strptime(ARGS.date, "%Y-%m-%d")
else:
    REPTDATE = datetime.today() - timedelta(days=1)

FILE_DT = REPTDATE.strftime('%Y%m%d')

# SAS-style numeric RDATE (days since 1960-01-01)
SAS_ORIGIN = datetime(1960, 1, 1)
RDATE = (REPTDATE - SAS_ORIGIN).days

print("=" * 70)
print(f"ELDS BNM Summary Processing - {REPTDATE.strftime('%Y-%m-%d')}")
print("=" * 70)

# =============================================================================
# FILE PATHS
# =============================================================================
BNMSUMM1 = BASE_INPUT_PATH / f"bnmsummary1_{FILE_DT}.csv"
BNMSUMM2 = BASE_INPUT_PATH / f"bnmsummary2_{FILE_DT}.csv"

OUTPUT_DATA_PATH = Path(
    f"{ELDS_DATA_PATH}/year={REPTDATE.year}/month={REPTDATE.month:02d}/day={REPTDATE.day:02d}"
)
OUTPUT_DATA_PATH.mkdir(parents=True, exist_ok=True)

# EHP SAS7BDAT sources
EHP_SRC = '/sas/eHP/intermediate/intg_app_ehp_fs_dwh_bnmsummary1.sas7bdat'
EHP2_SRC = '/sas/eHP/intermediate/intg_app_ehp_fs_dwh_bnmsummary2.sas7bdat'


# ==============================
# PRODUCTION SCHEMA DEFINITION
# ==============================

SUMM1_SCHEMA = {
    'MAANO': ('char', 50),
    'STAGE': ('char', 2),
    'APPLICATION': ('char', 10),
    'DTECOMPLETE': ('char', 10),
    'AANO': ('char', 50),
    'APPKEY': ('char', 10),
    'PRIORITY_SECTOR': ('char', 2),
    'DSRISS3': ('num', 8),
    'FIN_CONCEPT': ('char', 60),
    'LN_UTILISE_LOCAT_CD': ('char', 2),
    'SPECIALFUND': ('num', 8),
    'ASSET_PURCH_AMT': ('num', 8),
    'PURPOSE_LOAN': ('num', 8),
    'STRUPCO_3YR': ('char', 2),
    'FACICODE': ('num', 8),
    'AMTAPPLY': ('num', 8),
    'AMOUNT': ('num', 8),
    'APPTYPE': ('char', 1),
    'REJREASON': ('char', 5),
    'DATEXT': ('char', 10),
    'ACCTNO': ('num', 8),
    'EIR': ('num', 8),
    'EREQNO': ('char', 15),
    'REFIN_FLG': ('char', 3),
    'INDINTERIM': ('char', 1),
    'DATE': ('num', 8),
    'STATUS': ('char', 30),
    'CCPT_TAG': ('char', 5),
    'CIR': ('num', 8),
    'PRICING_TYPE': ('char', 5),
    'LU_ADD1': ('char', 40),
    'LU_ADD2': ('char', 40),
    'LU_ADD3': ('char', 40),
    'LU_ADD4': ('char', 40),
    'LU_TOWN_CITY': ('char', 20),
    'LU_POSTCODE': ('char', 5),
    'LU_STATE_CD': ('char', 2),
    'LU_COUNTRY_CD': ('char', 3),
    'PROP_STATUS': ('char', 5),
    'DTCOMPLETE': ('char', 25),
    'LU_SOURCE': ('char', 5),
    'EVENT_TYPE': ('char', 2),
}

SUMM2_SCHEMA = {
    'MAANO': ('char', 50),
    'STAGE': ('char', 2),
    'APPLICATION': ('char', 10),
    'DTECOMPLETE': ('char', 10),
    'IDNO': ('char', 30),
    'ENTKEY': ('char', 11),
    'APPLNAME': ('char', 150),
    'COUNTRY': ('char', 5),
    'DBIRTH': ('char', 10),
    'ENTITY_TYPE': ('num', 8),
    'CORP_STATUS_CD': ('char', 2),
    'INDUSTRIAL_STATUS': ('char', 5),
    'RESIDENCY_STATUS_CD': ('char', 1),
    'ANNSUBTSALARY': ('num', 8),
    'GENDER': ('char', 1),
    'OCCUPATION': ('char', 5),
    'EMPNAME': ('char', 150),
    'EMPLOY_SECTOR_CD': ('char', 10),
    'EMPLOY_TYPE_CD': ('char', 5),
    'POSTCODE': ('char', 5),
    'STATE_CD': ('char', 2),
    'COUNTRY_CD': ('char', 3),
    'ROLE': ('char', 1),
    'ICPP': ('char', 30),
    'MARRIED': ('char', 10),
    'CUSTOMER_CODE': ('char', 2),
    'CISNUMBER': ('char', 20),
    'NO_OF_EMPLOYEE': ('char', 10),
    'ANNUAL_TURNOVER': ('char', 20),
    'SMESIZE': ('char', 5),
    'RACE': ('char', 2),
    'INDUSTRIAL_SECTOR_CD': ('char', 10),
    'OCCUPAT_MASCO_CD': ('char', 10),
    'EREQNO': ('char', 15),
    'DSRISS3': ('char', 10),
    'INDINTERIM': ('char', 1),
    'DATE': ('num', 8),
    'DTCOMPLETE': ('char', 25),
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def apply_production_schema(df: pl.DataFrame, schema: dict, dataset_name: str) -> pl.DataFrame:
    """Apply exact production schema with proper data types and lengths."""
    print(f"  [schema] Applying production schema to {dataset_name}...")

    expressions = []

    for col_name, (col_type, col_len) in schema.items():
        if col_name not in df.columns:
            if col_type == 'char':
                expressions.append(pl.lit(None).cast(pl.Utf8).alias(col_name))
            else:
                expressions.append(pl.lit(None).cast(pl.Float64).alias(col_name))
            continue

        if col_type == 'char':
            # CUSTOMER_CODE: only pad if value exists
            if col_name == 'CUSTOMER_CODE':
                expr = (
                    pl.when(
                        pl.col(col_name).is_null() |
                        (pl.col(col_name).cast(pl.Utf8, strict=False).str.strip_chars() == '')
                    )
                    .then(None)
                    .otherwise(
                        pl.col(col_name)
                        .cast(pl.Utf8, strict=False)
                        .str.strip_chars()
                        .str.pad_start(col_len, '0')
                    )
                    .alias(col_name)
                )
            # Other zero-padded fields: always pad
            elif col_name in ['POSTCODE', 'STATE_CD',
                              'LN_UTILISE_LOCAT_CD', 'LU_POSTCODE', 'LU_STATE_CD']:
                expr = (
                    pl.col(col_name)
                    .cast(pl.Utf8, strict=False)
                    .str.strip_chars()
                    .str.pad_start(col_len, '0')
                    .alias(col_name)
                )
            else:
                # Standard character field: strip and truncate to max length
                expr = (
                    pl.col(col_name)
                    .cast(pl.Utf8, strict=False)
                    .str.strip_chars()
                    .str.slice(0, col_len)
                    .alias(col_name)
                )
        else:
            # Numeric field: clean and convert
            expr = (
                pl.col(col_name)
                .cast(pl.Utf8, strict=False)
                .str.strip_chars()
                .str.replace_all(r'[^0-9.\-]', '')
                .replace('', None)
                .cast(pl.Float64, strict=False)
                .alias(col_name)
            )

        expressions.append(expr)

    df_transformed = df.select(expressions)
    schema_cols = list(schema.keys())
    df_transformed = df_transformed.select([c for c in schema_cols if c in df_transformed.columns])

    print(f"  [ok] Schema applied: {len(df_transformed.columns)} columns, {df_transformed.height:,} rows")

    return df_transformed


def apply_sas_eof_check(df: pl.DataFrame, source_name: str, is_sas_dataset: bool = False) -> pl.DataFrame:
    """SAS-style file validation with FH/FT checking."""
    if "MAANO" not in df.columns:
        print(f"  [warn] MAANO column missing in {source_name}, skipping validation.")
        return df.with_columns([
            pl.lit("Y").alias("INDINTERIM"),
            pl.lit(RDATE).alias("DATE")
        ])

    if is_sas_dataset:
        print("  [info] SAS dataset - no FH/FT markers expected")
        df = df.with_columns([
            pl.lit("Y").alias("INDINTERIM"),
            pl.lit(RDATE).alias("DATE")
        ])
        return df

    df_str = df.with_columns(pl.col("MAANO").cast(pl.Utf8, strict=False))
    header_mask = df_str["MAANO"].str.starts_with("FH")

    if header_mask.any():
        header_count = header_mask.sum()
        print(f"  [info] Removing {header_count} header row(s) starting with 'FH'")
        df = df.filter(~header_mask)
        df_str = df.with_columns(pl.col("MAANO").cast(pl.Utf8, strict=False))

    if df.height == 0:
        print(f"  [warn] No data rows in {source_name}")
        return df

    control_row = df_str[-1, :]
    ctrl_val = str(control_row["MAANO"][0]).strip()

    match = re.match(r"FT0*(\d+)", ctrl_val)
    if not match:
        print(f"  [info] No FT control footer found in {source_name}")
        df = df.with_columns([
            pl.lit("Y").alias("INDINTERIM"),
            pl.lit(RDATE).alias("DATE")
        ])
        return df

    control_count = int(match.group(1))
    print(f"  [info] {source_name}: Control count = {control_count}")

    df_data = df.slice(0, df.height - 1)
    actual_count = df_data.height

    print(f"  [info] {source_name}: Actual data rows = {actual_count}")

    if control_count != actual_count:
        print(f"  [FAIL] ABORT 77: Row count mismatch in {source_name}")
        print(f"     Expected: {control_count}, Got: {actual_count}")
        raise SystemExit(77)
    else:
        print(f"  [ok] Record count matches control ({actual_count})")

    df_data = df_data.with_columns([
        pl.lit("Y").alias("INDINTERIM"),
        pl.lit(RDATE).alias("DATE")
    ])
    return df_data


def find_latest_prev(data_path: Path, dataset_name: str, before: datetime):
    """
    Find the most recent cumulative parquet whose partition date is strictly
    before `before`. Returns (partition_date, path) or None.
    """
    cutoff = before.replace(hour=0, minute=0, second=0, microsecond=0)
    best = None
    for p in data_path.glob(f"year=*/month=*/day=*/{dataset_name}.parquet"):
        try:
            y, m, d = (int(x.split("=")[1]) for x in p.parts[-4:-1])
            dt = datetime(y, m, d)
        except (ValueError, IndexError):
            continue
        if dt < cutoff and (best is None or dt > best[0]):
            best = (dt, p)
    return best


def save_with_cumulative_append(df: pl.DataFrame, dataset_name: str, reptdate: datetime,
                                output_path: Path, data_path: Path):
    """
    Build today's cumulative = latest previous cumulative + today's rows.
    Missing previous file / gap = WARNING only (no abort).
    """
    print(f"\nBuilding cumulative file for {dataset_name}...")
    print(f"  Today's new records: {df.height:,} rows")
    print(f"  Today's date (RDATE): {RDATE}")

    # ---- locate previous cumulative (latest earlier partition) ----
    found = find_latest_prev(data_path, dataset_name, reptdate)
    if found is None:
        prev_dt, prev_parquet = None, None
        print(f"  [WARNING] No previous cumulative found for {dataset_name} under {data_path}")
        print(f"            Treating as initial load: cumulative will contain today's rows only.")
    else:
        prev_dt, prev_parquet = found
        gap = (reptdate.date() - prev_dt.date()).days
        print(f"  Previous cumulative: {prev_dt:%Y-%m-%d} ({prev_parquet})")
        if gap > 1:
            print(f"  [WARNING] Gap of {gap} days since last cumulative "
                  f"({prev_dt:%Y-%m-%d}). Data for the missing day(s) is not in the file.")

    output_file = output_path / f"{dataset_name}.parquet"
    tmp_output = output_path / f"{dataset_name}.parquet.tmp"
    temp_today = output_path / f"{dataset_name}_today_temp.parquet"

    # ---- save today's data to temp ----
    print("  Saving today's data to temp...")
    duckdb.sql(f"""
        COPY (SELECT * FROM df)
        TO '{temp_today}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
    """)
    print(f"  [ok] Temp file created: {df.height:,} rows")

    # ---- initial load ----
    if prev_parquet is None:
        if output_file.exists():
            output_file.unlink()
        temp_today.rename(output_file)
        final_count = df.height
        print(f"  [ok] Initial cumulative: {final_count:,} rows")
        return final_count

    # ---- append ----
    try:
        prev_count = duckdb.query(
            f"SELECT COUNT(*) FROM read_parquet('{prev_parquet}', hive_partitioning=false)"
        ).fetchone()[0]
        print(f"  [ok] Previous cumulative rows: {prev_count:,}")
        print(f"  Streaming append via DuckDB: {prev_count:,} (previous) + {df.height:,} (today)")

        if tmp_output.exists():
            tmp_output.unlink()

        duckdb.sql(f"""
            COPY (
                SELECT * FROM read_parquet('{prev_parquet}', hive_partitioning=false)
                UNION ALL BY NAME
                SELECT * FROM read_parquet('{temp_today}', hive_partitioning=false)
            )
            TO '{tmp_output}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
        """)

        final_count = duckdb.query(
            f"SELECT COUNT(*) FROM read_parquet('{tmp_output}', hive_partitioning=false)"
        ).fetchone()[0]

        expected = prev_count + df.height
        if final_count != expected:
            raise RuntimeError(
                f"Row count mismatch after append: expected {expected:,}, got {final_count:,}"
            )

        # Atomic replace: only now does the real file appear / get overwritten
        tmp_output.replace(output_file)
        temp_today.unlink(missing_ok=True)

        print(f"  [ok] New cumulative: {final_count:,} rows")
        print(f"       Verified: {prev_count:,} + {df.height:,} = {final_count:,}")

    except Exception as e:
        print(f"  [ERROR] Append failed for {dataset_name}: {e}")
        traceback.print_exc()
        # Clean up partial files, but do NOT write a "today only" cumulative:
        # that would silently wipe the history.
        tmp_output.unlink(missing_ok=True)
        temp_today.unlink(missing_ok=True)
        raise

    return final_count


# =============================================================================
# PROCESS SUMM1 (Loan Applications)
# =============================================================================

print("\n" + "=" * 70)
print("PROCESSING SUMM1 (Loan Applications)")
print("=" * 70)

print(f"\nReading {BNMSUMM1.name}...")
if not BNMSUMM1.exists():
    print(f"[FAIL] ABORT: File not found: {BNMSUMM1}")
    sys.exit(1)

try:
    SUMM1 = pl.read_csv(
        BNMSUMM1,
        separator=",",
        has_header=False,
        ignore_errors=True,
        truncate_ragged_lines=True,
        infer_schema_length=0,
        skip_rows=1
    )

    print(f"  [ok] Raw CSV: {SUMM1.height:,} rows, {SUMM1.width} columns")

    csv_column_names = [
        "MAANO", "STAGE", "APPLICATION", "DTECOMPLETE", "AANO", "APPKEY",
        "PRIORITY_SECTOR", "DSRISS3", "FIN_CONCEPT", "LN_UTILISE_LOCAT_CD",
        "SPECIALFUND", "ASSET_PURCH_AMT", "PURPOSE_LOAN", "STRUPCO_3YR",
        "FACICODE", "AMTAPPLY", "AMOUNT", "APPTYPE", "REJREASON", "DATEXT",
        "ACCTNO", "EIR", "EREQNO", "REFIN_FLG", "STATUS", "CCPT_TAG", "CIR",
        "PRICING_TYPE", "LU_ADD1", "LU_ADD2", "LU_ADD3", "LU_ADD4",
        "LU_TOWN_CITY", "LU_POSTCODE", "LU_STATE_CD", "LU_COUNTRY_CD",
        "PROP_STATUS", "DTCOMPLETE", "LU_SOURCE", "EVENT_TYPE"
    ]

    if SUMM1.width < 40:
        print(f"[FAIL] ABORT: CSV has only {SUMM1.width} columns, need at least 40")
        sys.exit(1)

    SUMM1 = SUMM1.select([pl.col(f"column_{i+1}") for i in range(40)])
    SUMM1.columns = csv_column_names

except Exception as e:
    print(f"[FAIL] ERROR loading CSV: {e}")
    sys.exit(1)

SUMM1 = apply_sas_eof_check(SUMM1, "bnmsummary1_csv", is_sas_dataset=False)

print(f"\nReading {Path(EHP_SRC).name}...")
if not Path(EHP_SRC).exists():
    print(f"[FAIL] ABORT: File not found: {EHP_SRC}")
    sys.exit(1)

SUMM1_EHP_df, _ = pyreadstat.read_sas7bdat(EHP_SRC)
print(f"[ok] SAS7BDAT loaded: {len(SUMM1_EHP_df):,} rows")

SUMM1_EHP = pl.from_pandas(SUMM1_EHP_df)

print("  Mapping EHP column names...")

ehp_column_map = {
    # Core identifiers
    'MAANO': 'MAANO',
    'STAGE': 'STAGE',
    'APPLICATION': 'APPLICATION',
    'APPKEY': 'APPKEY',

    # Dates
    'DATE_COMPLETED': 'DTECOMPLETE',
    'DATE': 'DATEXT',
    'DTCOMPLETE': 'DTCOMPLETE',

    # Classification
    'PRIORITY_SECTOR': 'PRIORITY_SECTOR',
    'APPTYPE': 'APPTYPE',
    'STATUS': 'STATUS',
    'EVENT_TYPE': 'EVENT_TYPE',

    # Financial
    'DEBT_SERVICE_RATIO': 'DSRISS3',
    'SPECIAL_FUND': 'SPECIALFUND',
    'ASSET_PURCHASE': 'ASSET_PURCH_AMT',
    'PURPOSE_LOAN': 'PURPOSE_LOAN',
    'AMOUNT': 'AMTAPPLY',
    'AMT_APPROVED': 'AMOUNT',
    'ACCOUNT_NO': 'ACCTNO',

    # Interest rates
    'EIR': 'EIR',
    'CIR': 'CIR',
    'PRICING_TYPE': 'PRICING_TYPE',

    # Facility
    'STARTUP_FINANCING': 'STRUPCO_3YR',
    'FACILITY_TYPE': 'FACICODE',
    'REJECT_REASON': 'REJREASON',

    # Concept & flags
    'FIN_CONCEPT': 'FIN_CONCEPT',
    'REFINANCING': 'REFIN_FLG',
    'CCPT_TAG': 'CCPT_TAG',

    # Location
    'LOCATION': 'LN_UTILISE_LOCAT_CD',
    'ADDRESS_1': 'LU_ADD1',
    'ADDRESS_2': 'LU_ADD2',
    'CITY': 'LU_TOWN_CITY',
    'POSTCODE': 'LU_POSTCODE',
    'STATECODE': 'LU_STATE_CD',
    'COUNTRY': 'LU_COUNTRY_CD',

    # Request
    'EREQ_NUMBER': 'EREQNO',
}

rename_dict = {k: v for k, v in ehp_column_map.items() if k in SUMM1_EHP.columns}
if rename_dict:
    SUMM1_EHP = SUMM1_EHP.rename(rename_dict)
    print(f"  [ok] Renamed {len(rename_dict)} columns")

SUMM1_EHP = apply_sas_eof_check(SUMM1_EHP, "bnmsummary1_ehp", is_sas_dataset=True)

print("\nApplying production schema...")
SUMM1 = apply_production_schema(SUMM1, SUMM1_SCHEMA, "CSV")
SUMM1_EHP = apply_production_schema(SUMM1_EHP, SUMM1_SCHEMA, "EHP")

print("\nCombining CSV + EHP...")
SUMM1_COMBINED = pl.concat([SUMM1, SUMM1_EHP], how="vertical_relaxed")
print(f"[ok] Combined: {SUMM1_COMBINED.height:,} rows")

final_summ1_count = save_with_cumulative_append(
    SUMM1_COMBINED, "SUMM1", REPTDATE, OUTPUT_DATA_PATH, ELDS_DATA_PATH
)

print(f"\n[ok] SUMM1 Complete: {final_summ1_count:,} total rows")

# =============================================================================
# PROCESS SUMM2 (Borrower Details)
# =============================================================================

print("\n" + "=" * 70)
print("PROCESSING SUMM2 (Borrower Details)")
print("=" * 70)

print(f"\nReading {BNMSUMM2.name}...")
if not BNMSUMM2.exists():
    print(f"[FAIL] ABORT: File not found: {BNMSUMM2}")
    sys.exit(1)

try:
    SUMM2 = pl.read_csv(
        BNMSUMM2,
        separator=",",
        has_header=False,
        ignore_errors=True,
        truncate_ragged_lines=True,
        infer_schema_length=0,
        skip_rows=1
    )

    print(f"  [ok] Raw CSV: {SUMM2.height:,} rows, {SUMM2.width} columns")

    csv_column_names = [
        "MAANO", "STAGE", "APPLICATION", "DTECOMPLETE", "IDNO", "ENTKEY",
        "APPLNAME", "COUNTRY", "DBIRTH", "ENTITY_TYPE", "CORP_STATUS_CD",
        "INDUSTRIAL_STATUS", "RESIDENCY_STATUS_CD", "ANNSUBTSALARY", "GENDER",
        "OCCUPATION", "EMPNAME", "EMPLOY_SECTOR_CD", "EMPLOY_TYPE_CD",
        "POSTCODE", "STATE_CD", "COUNTRY_CD", "ROLE", "ICPP", "MARRIED",
        "CUSTOMER_CODE", "CISNUMBER", "NO_OF_EMPLOYEE", "ANNUAL_TURNOVER",
        "SMESIZE", "RACE", "INDUSTRIAL_SECTOR_CD", "OCCUPAT_MASCO_CD",
        "EREQNO", "DSRISS3", "DTCOMPLETE"
    ]

    if SUMM2.width < 36:
        print(f"[FAIL] ABORT: CSV has only {SUMM2.width} columns, need at least 36")
        sys.exit(1)

    SUMM2 = SUMM2.select([pl.col(f"column_{i+1}") for i in range(36)])
    SUMM2.columns = csv_column_names

except Exception as e:
    print(f"[FAIL] ERROR loading CSV: {e}")
    sys.exit(1)

SUMM2 = apply_sas_eof_check(SUMM2, "bnmsummary2_csv", is_sas_dataset=False)

print(f"\nReading {Path(EHP2_SRC).name}...")
if not Path(EHP2_SRC).exists():
    print(f"[FAIL] ABORT: File not found: {EHP2_SRC}")
    sys.exit(1)

SUMM2_EHP_df, _ = pyreadstat.read_sas7bdat(EHP2_SRC)
print(f"[ok] SAS7BDAT loaded: {len(SUMM2_EHP_df):,} rows")

SUMM2_EHP = pl.from_pandas(SUMM2_EHP_df)

print("  Mapping EHP column names...")
ehp2_column_map = {
    'DATECOMPLETE': 'DTECOMPLETE',
    'NAME': 'APPLNAME',
    'NATIONALITY': 'COUNTRY',
    'DOB': 'DBIRTH',
    'CORP_STATUS': 'CORP_STATUS_CD',
    'INDUSTRIAL_SECTOR': 'INDUSTRIAL_STATUS',
    'RESIDENCY_STATUS': 'RESIDENCY_STATUS_CD',
    'INCOME': 'ANNSUBTSALARY',
    'EMPLOYER_NAME': 'EMPNAME',
    'EMPLOYMENT_SECTOR': 'EMPLOY_SECTOR_CD',
    'EMPLOYMENT_TYPE': 'EMPLOY_TYPE_CD',
    'STATE': 'STATE_CD',
    'COUNTRY': 'COUNTRY_CD',
    'MARITALSTATUS': 'MARRIED',
    'CUSTCODE': 'CUSTOMER_CODE',
    'CISNO': 'CISNUMBER',
    'NUM_EMPLOYEE': 'NO_OF_EMPLOYEE',
    'MSIC': 'INDUSTRIAL_SECTOR_CD',
    'MASCO': 'OCCUPAT_MASCO_CD',
    'DSR': 'DSRISS3'
}

rename_dict = {k: v for k, v in ehp2_column_map.items() if k in SUMM2_EHP.columns}
if rename_dict:
    SUMM2_EHP = SUMM2_EHP.rename(rename_dict)
    print(f"  [ok] Renamed {len(rename_dict)} columns")

SUMM2_EHP = apply_sas_eof_check(SUMM2_EHP, "bnmsummary2_ehp", is_sas_dataset=True)

print("\nApplying production schema...")
SUMM2 = apply_production_schema(SUMM2, SUMM2_SCHEMA, "CSV")
SUMM2_EHP = apply_production_schema(SUMM2_EHP, SUMM2_SCHEMA, "EHP")

print("\nCombining CSV + EHP...")
SUMM2_COMBINED = pl.concat([SUMM2, SUMM2_EHP], how="vertical_relaxed")
print(f"[ok] Combined: {SUMM2_COMBINED.height:,} rows")

final_summ2_count = save_with_cumulative_append(
    SUMM2_COMBINED, "SUMM2", REPTDATE, OUTPUT_DATA_PATH, ELDS_DATA_PATH
)

print(f"\n[ok] SUMM2 Complete: {final_summ2_count:,} rows")

# ====================
# COMPLETION SUMMARY
# ====================

print("\n" + "=" * 70)
print("PROCESSING COMPLETE")
print("=" * 70)
print(f"Date: {REPTDATE.strftime('%Y-%m-%d')}")
print(f"Output Directory: {OUTPUT_DATA_PATH}")
print("\nResults:")
print(f"  SUMM1.parquet: {final_summ1_count:,} rows, {len(SUMM1_SCHEMA)} columns")
print(f"  SUMM2.parquet: {final_summ2_count:,} rows, {len(SUMM2_SCHEMA)} columns")
