import os
import sys
import pandas as pd
import pyreadstat
from pathlib import Path
from datetime import datetime, timedelta

# =====================================================
# CONFIGURATION
# =====================================================

# Input paths (separate for loan and uloan)
LOAN_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP03")
ULOAN_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP03")
OUTPUT_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP03")

JOB_NAME = "EIBWHP03"

# Calculate report date (yesterday)
REPORT_DATE = datetime.now() - timedelta(days=1)
REPORT_MONTH = REPORT_DATE.strftime("%m")              # MM format, e.g. "07"
REPORT_YEAR = REPORT_DATE.strftime("%y")              # YY format, e.g. "26" (NOT 2026)
REPORT_WEEK = str((REPORT_DATE.day - 1) // 7 + 1)     # Week number, e.g. "4"

# Previous week calculation (handle month/year rollover)
if int(REPORT_WEEK) > 1:
    PREV_WEEK = str(int(REPORT_WEEK) - 1)
    PREV_MONTH = REPORT_MONTH
    PREV_YEAR = REPORT_YEAR
else:
    # Roll back to last week of previous month
    first_of_month = REPORT_DATE.replace(day=1)
    prev_month_date = first_of_month - timedelta(days=1)
    PREV_MONTH = prev_month_date.strftime("%m")
    PREV_YEAR = prev_month_date.strftime("%y")
    PREV_WEEK = "4"  # last week of previous month

# Input SAS7BDAT files with dynamic naming
# loan{reptmon}{nowk}.sas7bdat
# uloan{reptmon}{nowk}{reptyear}.sas7bdat
INPUT_DATASETS = {
    "LOAN_CURRENT":  LOAN_DIR / f"loan{REPORT_MONTH}{REPORT_WEEK}.sas7bdat",
    "LOAN_PREVIOUS": LOAN_DIR / f"loan{PREV_MONTH}{PREV_WEEK}.sas7bdat",
    "ULOAN_CURRENT": ULOAN_DIR / f"uloan{REPORT_MONTH}{REPORT_WEEK}{REPORT_YEAR}.sas7bdat",
}

OUTPUT_FILE = OUTPUT_DIR / f"{JOB_NAME}_{REPORT_DATE.strftime('%Y%m%d')}.txt"

# Chunk size for processing large datasets
CHUNK_SIZE = 50000


# =====================================================
# DISP SIMULATION
# =====================================================

def disp_delete(path):
    if path.exists():
        path.unlink()
        print(f"[DELETE] Removed file: {path}")


def disp_shr(path):
    if not path.exists():
        raise FileNotFoundError(f"[DISP ERROR] Missing input dataset: {path}")


def disp_new(path):
    if path.exists():
        raise FileExistsError(f"[DISP ERROR] File already exists: {path}")


# =====================================================
# PRODUCTION OUTPUT FORMATTER - MATCHES SAS OUTPUT
# =====================================================

def format_production_record(acctno, value1=0, value2=0, value3=0, value4=0):
    """
    Format record to match production output format:
    {acctno};{value1};{value2};{value3};{value4}

    Example: 6734061000000Y;0;0;6;0
    """
    acctno_str = str(acctno).strip()

    # If account number doesn't end with Y, ensure Y suffix
    if not acctno_str.endswith('Y'):
        if 'Y' in acctno_str:
            parts = acctno_str.split('Y')
            if len(parts) > 1:
                acctno_str = parts[0] + 'Y'
        else:
            acctno_str = acctno_str + 'Y'

    try:
        v1 = int(value1) if value1 is not None else 0
        v2 = int(value2) if value2 is not None else 0
        v3 = int(value3) if value3 is not None else 0
        v4 = int(value4) if value4 is not None else 0
    except (ValueError, TypeError):
        v1 = v2 = v3 = v4 = 0

    return f"{acctno_str};{v1};{v2};{v3};{v4}"


def format_record_from_sas(row, column_mapping):
    """
    Format a SAS record to production output format.
    """
    acctno = None
    acctno_col = None

    possible_acct_cols = ['ACCTNO', 'BNMCODE', 'ACCOUNT_NO', 'ACCT_NUM', 'CUSTCD']
    for col in possible_acct_cols:
        if col in row.index:
            acctno = row[col]
            acctno_col = col
            break

    if acctno is None:
        acctno = row.iloc[0]
        acctno_col = row.index[0]

    val1 = 0
    val2 = 0
    val3 = 0
    val4 = 0

    for i, col in enumerate(row.index):
        if col == acctno_col:
            continue

        col_lower = col.lower()
        if 'amount' in col_lower or 'balance' in col_lower or 'amt' in col_lower:
            if val1 == 0:
                val1 = row[col] if pd.notna(row[col]) else 0
            elif val2 == 0:
                val2 = row[col] if pd.notna(row[col]) else 0
        elif 'count' in col_lower or 'cnt' in col_lower or 'num' in col_lower:
            if val3 == 0:
                val3 = row[col] if pd.notna(row[col]) else 0
            elif val4 == 0:
                val4 = row[col] if pd.notna(row[col]) else 0
        else:
            if val1 == 0:
                val1 = row[col] if pd.notna(row[col]) else 0
            elif val2 == 0:
                val2 = row[col] if pd.notna(row[col]) else 0
            elif val3 == 0:
                val3 = row[col] if pd.notna(row[col]) else 0
            elif val4 == 0:
                val4 = row[col] if pd.notna(row[col]) else 0

    if val1 == 0 and len(row) > 1:
        for i in range(1, min(5, len(row))):
            if i == 1 and val1 == 0:
                val1 = row.iloc[i] if pd.notna(row.iloc[i]) else 0
            elif i == 2 and val2 == 0:
                val2 = row.iloc[i] if pd.notna(row.iloc[i]) else 0
            elif i == 3 and val3 == 0:
                val3 = row.iloc[i] if pd.notna(row.iloc[i]) else 0
            elif i == 4 and val4 == 0:
                val4 = row.iloc[i] if pd.notna(row.iloc[i]) else 0

    return format_production_record(acctno, val1, val2, val3, val4)


# =====================================================
# TEXT FILE WRITER - CHUNKED
# =====================================================

def write_text_file_chunked(path, records_generator, total_estimate=None):
    count = 0
    with open(path, "w", encoding="utf-8") as f:
        for record in records_generator:
            f.write(record + "\n")
            count += 1
            if count % 100000 == 0:
                print(f"[PROGRESS] Written {count:,} records...")
                f.flush()
    print(f"[WRITE] Text file created: {path}")
    print(f"[INFO] Total records written: {count:,}")
    return count


# =====================================================
# SAS DATA READER - CHUNKED
# =====================================================

def read_sas7bdat_chunked(file_path, chunk_size=CHUNK_SIZE):
    try:
        print(f"[READ] Reading {file_path.name} in chunks of {chunk_size:,} records")
        reader = pd.read_sas(file_path, format='sas7bdat', chunksize=chunk_size)

        chunk_count = 0
        total_processed = 0

        for chunk in reader:
            chunk_count += 1
            total_processed += len(chunk)
            print(f"[PROGRESS] Processing chunk {chunk_count} ({len(chunk):,} records, total: {total_processed:,})")
            yield chunk
            del chunk

        print(f"[READ] Completed reading {file_path.name}")

    except Exception as e:
        raise Exception(f"Error reading SAS7BDAT file {file_path} in chunks: {e}")


# =====================================================
# SAS BUSINESS LOGIC - PRODUCTION FORMAT
# =====================================================

def execute_business_logic(output_file_path):
    print("[EXEC] Executing EIBWHP03 business logic...")

    with open(output_file_path, "w", encoding="utf-8") as output_file:

        total_processed = 0
        spool_lines = []

        spool_lines.append(f"{JOB_NAME} REPORT")
        spool_lines.append(f"Report Date: {REPORT_DATE.strftime('%Y-%m-%d')}")
        spool_lines.append(f"Report Month: {REPORT_MONTH}")
        spool_lines.append(f"Report Week: {REPORT_WEEK}")
        spool_lines.append(f"Report Year (YY): {REPORT_YEAR}")
        spool_lines.append(f"Loan Input Dir: {LOAN_DIR}")
        spool_lines.append(f"ULoan Input Dir: {ULOAN_DIR}")
        spool_lines.append("-" * 80)

        # Process LOAN_CURRENT
        loan_path = INPUT_DATASETS.get("LOAN_CURRENT")
        if loan_path and loan_path.exists():
            print(f"[PROCESS] Processing LOAN_CURRENT")
            try:
                sample_df, meta = pyreadstat.read_sas7bdat(loan_path, row_limit=1)
                column_names = sample_df.columns.tolist()
                print(f"[INFO] LOAN_CURRENT columns: {column_names[:10]}...")
                del sample_df

                for chunk in read_sas7bdat_chunked(loan_path, chunk_size=CHUNK_SIZE):
                    for idx, row in chunk.iterrows():
                        try:
                            formatted_record = format_record_from_sas(row, None)
                            output_file.write(formatted_record + "\n")
                            total_processed += 1
                        except Exception as e:
                            print(f"[WARNING] Error processing row {idx}: {e}")
                            continue
                    del chunk

                print(f"[COMPLETE] Processed {total_processed:,} records from LOAN_CURRENT")

            except Exception as e:
                print(f"[ERROR] Failed to process LOAN_CURRENT: {e}")
                raise

        # Process ULOAN_CURRENT
        uloan_path = INPUT_DATASETS.get("ULOAN_CURRENT")
        if uloan_path and uloan_path.exists():
            print(f"[PROCESS] Processing ULOAN_CURRENT")
            try:
                sample_df, meta = pyreadstat.read_sas7bdat(uloan_path, row_limit=1)
                column_names = sample_df.columns.tolist()
                print(f"[INFO] ULOAN_CURRENT columns: {column_names[:10]}...")
                del sample_df

                for chunk in read_sas7bdat_chunked(uloan_path, chunk_size=CHUNK_SIZE):
                    for idx, row in chunk.iterrows():
                        try:
                            formatted_record = format_record_from_sas(row, None)
                            output_file.write(formatted_record + "\n")
                            total_processed += 1
                        except Exception as e:
                            print(f"[WARNING] Error processing row {idx}: {e}")
                            continue
                    del chunk

                print(f"[COMPLETE] Processed {total_processed:,} total records (including ULOAN)")

            except Exception as e:
                print(f"[ERROR] Failed to process ULOAN_CURRENT: {e}")
                raise

        spool_lines.append("-" * 80)
        spool_lines.append(f"TOTAL RECORDS PROCESSED: {total_processed:,}")
        spool_lines.append(f"END OF {JOB_NAME} REPORT")

        print(f"[SUMMARY] Total records processed: {total_processed:,}")

        spool_file = OUTPUT_DIR / f"{JOB_NAME}_REPORT_{REPORT_DATE.strftime('%Y%m%d')}.txt"
        with open(spool_file, "w", encoding="utf-8") as f:
            for line in spool_lines:
                f.write(line + "\n")
        print(f"[SPOOL] Spool file created: {spool_file}")

        return total_processed


# =====================================================
# JOB EXECUTION
# =====================================================

def run_job():
    print(f"========== START JOB {JOB_NAME} ==========")
    print(f"Report Date: {REPORT_DATE.strftime('%Y-%m-%d')}")
    print(f"Report Month: {REPORT_MONTH}")
    print(f"Report Week: {REPORT_WEEK}")
    print(f"Report Year (YY): {REPORT_YEAR}")
    print(f"Loan Input Dir: {LOAN_DIR}")
    print(f"ULoan Input Dir: {ULOAN_DIR}")
    print(f"Python version: {sys.version}")
    print(f"Chunk size: {CHUNK_SIZE:,} records")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    disp_delete(OUTPUT_FILE)

    for name, path in INPUT_DATASETS.items():
        disp_shr(path)
        print(f"[SHR] Validated input dataset: {name} - {path.name}")

    disp_new(OUTPUT_FILE)

    print("[EXEC] Starting business logic execution...")

    try:
        total_processed = execute_business_logic(OUTPUT_FILE)

        print("-" * 60)
        print("JOB SUMMARY:")
        print(f"Output file: {OUTPUT_FILE}")
        print(f"Total records processed: {total_processed:,}")

        if OUTPUT_FILE.exists():
            file_size = OUTPUT_FILE.stat().st_size
            if file_size > 1024 * 1024:
                print(f"Output file size: {file_size / (1024*1024):.2f} MB")
            else:
                print(f"Output file size: {file_size / 1024:.2f} KB")

        print("\n[VERIFICATION] First 5 output records:")
        with open(OUTPUT_FILE, "r") as f:
            for i, line in enumerate(f):
                if i >= 5:
                    break
                print(f"  {line.strip()}")

        print("-" * 60)
        print(f"========== END JOB {JOB_NAME} ==========")

    except Exception as e:
        print(f"[JOB FAILED] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(8)


# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    try:
        run_job()
    except Exception as e:
        print(f"[JOB FAILED] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(8)
