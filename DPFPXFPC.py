import os
import sys
import pandas as pd
import pyreadstat
from pathlib import Path
from datetime import datetime, timedelta

# =====================================================
# CONFIGURATION
# =====================================================

BASE_DIR = Path(".")
INPUT_DIR = BASE_DIR / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP03"
OUTPUT_DIR = BASE_DIR / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWHP03"

JOB_NAME = "EIBWHP03"

# Calculate report date (yesterday)
REPORT_DATE = datetime.now() - timedelta(days=1)
REPORT_MONTH = REPORT_DATE.strftime("%m")  # Month in MM format
REPORT_WEEK = str((REPORT_DATE.day - 1) // 7 + 1)  # Week number

# Input SAS7BDAT files with dynamic naming
INPUT_DATASETS = {
    "LOAN_CURRENT": INPUT_DIR / f"loan{REPORT_MONTH}{REPORT_WEEK}.sas7bdat",
    "LOAN_PREVIOUS": INPUT_DIR / f"loan{REPORT_MONTH}{int(REPORT_WEEK)-1}.sas7bdat" if int(REPORT_WEEK) > 1 else INPUT_DIR / f"loan{int(REPORT_MONTH)-1:02d}4.sas7bdat",
    "ULOAN_CURRENT": INPUT_DIR / f"uloan{REPORT_MONTH}{REPORT_WEEK}.sas7bdat"
}

OUTPUT_FILE = OUTPUT_DIR / f"{JOB_NAME}_{REPORT_DATE.strftime('%Y%m%d')}.txt"

# Chunk size for processing large datasets (adjust based on available memory)
CHUNK_SIZE = 50000  # Process 50K records at a time


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
# PBBLNFMT.PY - FORMATTING LOGIC (OPTIMIZED)
# =====================================================

def apply_pbblnfmt(value, field_type="amount"):
    """
    Apply PBBLNFMT formatting similar to SAS PBBLNFMT.
    Optimized for performance.
    """
    if pd.isna(value) or value is None:
        return " " * 15
    
    if field_type == "amount":
        try:
            if isinstance(value, (int, float)):
                int_val = int(value)
                # Format as 12 digits with leading zeros, then right justify to 15
                return f"{int_val:012d}"[:15].rjust(15)
            else:
                clean_str = ''.join(filter(str.isdigit, str(value)))
                if clean_str:
                    return f"{int(clean_str):012d}"[:15].rjust(15)
                return " " * 15
        except:
            return str(value)[:15].ljust(15)
    
    elif field_type == "account":
        clean_str = ''.join(filter(str.isdigit, str(value)))
        if clean_str:
            return clean_str[:15].zfill(15)
        return str(value)[:15].rjust(15)
    
    else:
        return str(value)[:15].ljust(15)


def format_record_fast(bnmcode, amount, record_type):
    """
    Fast formatting for a single record.
    """
    formatted_bnm = apply_pbblnfmt(bnmcode, "account")
    formatted_amount = apply_pbblnfmt(amount, "amount")
    return f"{formatted_bnm}|{formatted_amount}"


# =====================================================
# TEXT FILE WRITER - CHUNKED
# =====================================================

def write_text_file_chunked(path, records_generator, total_estimate=None):
    """
    Write records to a text file using a generator to save memory.
    """
    count = 0
    
    with open(path, "w", encoding="utf-8") as f:
        for record in records_generator:
            if isinstance(record, str):
                f.write(record + "\n")
            else:
                f.write(str(record) + "\n")
            
            count += 1
            
            # Progress indicator
            if count % 100000 == 0:
                print(f"[PROGRESS] Written {count:,} records...")
                f.flush()
    
    print(f"[WRITE] Text file created: {path}")
    print(f"[INFO] Total records written: {count:,}")
    return count


def write_records_batch(records, file_handle):
    """
    Write a batch of records to file.
    """
    for record in records:
        file_handle.write(record + "\n")


# =====================================================
# SAS DATA READER - CHUNKED WITH PYREADSTAT
# =====================================================

def read_sas7bdat_chunked(file_path, chunk_size=CHUNK_SIZE, usecols=None):
    """
    Read SAS7BDAT file in chunks using pyreadstat.
    This is memory efficient for large files.
    """
    try:
        print(f"[READ] Reading {file_path.name} in chunks of {chunk_size:,} records")
        
        # First, get metadata without reading data
        meta = pyreadstat.read_sas7bdat(file_path, row_limit=1)[1]
        total_rows = meta.number_rows if hasattr(meta, 'number_rows') else "unknown"
        print(f"[INFO] Total rows in file: {total_rows:,}" if isinstance(total_rows, int) else f"[INFO] Total rows: {total_rows}")
        
        # Read in chunks using pandas read_sas with chunksize
        reader = pd.read_sas(file_path, format='sas7bdat', chunksize=chunk_size)
        
        chunk_count = 0
        total_processed = 0
        
        for chunk in reader:
            chunk_count += 1
            total_processed += len(chunk)
            print(f"[PROGRESS] Processing chunk {chunk_count} ({len(chunk):,} records, total: {total_processed:,})")
            
            yield chunk
            
            # Explicitly delete chunk to free memory
            del chunk
        
        print(f"[READ] Completed reading {file_path.name} in {chunk_count} chunks")
        
    except Exception as e:
        raise Exception(f"Error reading SAS7BDAT file {file_path} in chunks: {e}")


def read_sas7bdat_metadata(file_path):
    """
    Read only metadata from SAS file (no data).
    """
    try:
        df, meta = pyreadstat.read_sas7bdat(file_path, row_limit=1)
        return meta
    except Exception as e:
        print(f"[WARNING] Could not read metadata for {file_path.name}: {e}")
        return None


# =====================================================
# SAS BUSINESS LOGIC - FULLY CHUNKED
# =====================================================

def process_chunk(chunk, record_type, output_file_handle, spool_lines, max_spool=100):
    """
    Process a chunk of data and write directly to file.
    """
    chunk_processed = 0
    chunk_spool = []
    
    # Get column names
    col_names = chunk.columns.tolist()
    
    # Determine which columns to use (first two columns by default)
    # You may need to adjust this based on actual column names
    col1 = col_names[0] if len(col_names) > 0 else None
    col2 = col_names[1] if len(col_names) > 1 else None
    
    if col1 is None or col2 is None:
        print(f"[WARNING] Chunk has insufficient columns: {len(col_names)}")
        return 0, []
    
    # Process each row in the chunk
    for idx, row in chunk.iterrows():
        try:
            # Get values from the first two columns
            bnmcode = str(row[col1]) if pd.notna(row[col1]) else ""
            amount = row[col2] if pd.notna(row[col2]) else 0
            
            # Format the record
            formatted_record = format_record_fast(bnmcode, amount, record_type)
            
            # Write directly to output file
            output_file_handle.write(formatted_record + "\n")
            chunk_processed += 1
            
            # Collect sample for spool report
            if len(spool_lines) + len(chunk_spool) < max_spool:
                status = "PROCESSED" if record_type == "LOAN" else "UNSECURED"
                formatted_bnm = apply_pbblnfmt(bnmcode, "account")
                formatted_amount = apply_pbblnfmt(amount, "amount")
                chunk_spool.append(f"{formatted_bnm}  {formatted_amount}  {status}")
                
        except Exception as e:
            # Skip problematic records but log error
            print(f"[WARNING] Error processing record {idx}: {e}")
            continue
    
    return chunk_processed, chunk_spool


def execute_business_logic(output_file_path):
    """
    Execute the business logic using chunked processing.
    """
    print("[EXEC] Executing EIBWHP03 business logic...")

    # Open output file for writing
    with open(output_file_path, "w", encoding="utf-8") as output_file:
        
        spool_lines = []
        spool_lines.append(f"{JOB_NAME} REPORT")
        spool_lines.append(f"Report Date: {REPORT_DATE.strftime('%Y-%m-%d')}")
        spool_lines.append(f"Report Month: {REPORT_MONTH}")
        spool_lines.append(f"Report Week: {REPORT_WEEK}")
        spool_lines.append("-" * 80)
        spool_lines.append("BNMCODE                    AMOUNT         STATUS")
        spool_lines.append("-" * 80)

        total_processed = 0

        # Process LOAN_CURRENT if available
        loan_path = INPUT_DATASETS.get("LOAN_CURRENT")
        if loan_path and loan_path.exists():
            print(f"[PROCESS] Processing LOAN_CURRENT")
            try:
                # Process in chunks
                for chunk in read_sas7bdat_chunked(loan_path, chunk_size=CHUNK_SIZE):
                    chunk_count, chunk_spool = process_chunk(
                        chunk, "LOAN", output_file, spool_lines
                    )
                    total_processed += chunk_count
                    spool_lines.extend(chunk_spool)
                    
                    # Clear chunk to free memory
                    del chunk
                    
                print(f"[COMPLETE] Processed {total_processed:,} records from LOAN_CURRENT")
                
            except Exception as e:
                print(f"[ERROR] Failed to process LOAN_CURRENT: {e}")
                raise

        # Process ULOAN_CURRENT if available
        uloan_path = INPUT_DATASETS.get("ULOAN_CURRENT")
        if uloan_path and uloan_path.exists():
            print(f"[PROCESS] Processing ULOAN_CURRENT")
            try:
                # Add separator in spool
                if len(spool_lines) < 100:
                    spool_lines.append("-" * 80)
                    spool_lines.append("UNSECURED LOANS")
                    spool_lines.append("-" * 80)
                
                for chunk in read_sas7bdat_chunked(uloan_path, chunk_size=CHUNK_SIZE):
                    chunk_count, chunk_spool = process_chunk(
                        chunk, "ULOAN", output_file, spool_lines
                    )
                    total_processed += chunk_count
                    spool_lines.extend(chunk_spool)
                    
                    del chunk
                    
                print(f"[COMPLETE] Processed {total_processed:,} total records (including ULOAN)")
                
            except Exception as e:
                print(f"[ERROR] Failed to process ULOAN_CURRENT: {e}")
                raise

        # Process LOAN_PREVIOUS for comparison (sample only)
        loan_prev_path = INPUT_DATASETS.get("LOAN_PREVIOUS")
        if loan_prev_path and loan_prev_path.exists():
            print(f"[PROCESS] Processing LOAN_PREVIOUS sample")
            try:
                spool_lines.append("-" * 80)
                spool_lines.append("PREVIOUS WEEK DATA (Sample - first 100 records)")
                spool_lines.append("-" * 80)
                
                # Only read first 100 records
                df_prev, meta = pyreadstat.read_sas7bdat(loan_prev_path, row_limit=100)
                
                col_names = df_prev.columns.tolist()
                col1 = col_names[0] if len(col_names) > 0 else None
                col2 = col_names[1] if len(col_names) > 1 else None
                
                for idx, row in df_prev.iterrows():
                    bnmcode = str(row[col1]) if col1 and pd.notna(row[col1]) else ""
                    amount = row[col2] if col2 and pd.notna(row[col2]) else 0
                    
                    formatted_bnm = apply_pbblnfmt(bnmcode, "account")
                    formatted_amount = apply_pbblnfmt(amount, "amount")
                    spool_lines.append(f"{formatted_bnm}  {formatted_amount}  PREVIOUS")
                
                print(f"[COMPLETE] Added {len(df_prev)} sample records from LOAN_PREVIOUS")
                
            except Exception as e:
                print(f"[ERROR] Failed to process LOAN_PREVIOUS: {e}")

        # Footer
        spool_lines.append("-" * 80)
        spool_lines.append(f"TOTAL RECORDS PROCESSED: {total_processed:,}")
        spool_lines.append(f"END OF {JOB_NAME} REPORT")

        print(f"[SUMMARY] Total records processed: {total_processed:,}")
        print(f"[SUMMARY] Spool lines: {len(spool_lines)}")

        return spool_lines, total_processed


# =====================================================
# JOB EXECUTION
# =====================================================

def run_job():
    print(f"========== START JOB {JOB_NAME} ==========")
    print(f"Report Date: {REPORT_DATE.strftime('%Y-%m-%d')}")
    print(f"Report Month: {REPORT_MONTH}")
    print(f"Report Week: {REPORT_WEEK}")
    print(f"Python version: {sys.version}")
    print(f"Chunk size: {CHUNK_SIZE:,} records")

    # 1️⃣ Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 2️⃣ DELETE old output file
    disp_delete(OUTPUT_FILE)

    # 3️⃣ Validate input datasets (DISP=SHR)
    for name, path in INPUT_DATASETS.items():
        disp_shr(path)
        print(f"[SHR] Validated input dataset: {name} - {path.name}")

    # 4️⃣ Validate NEW output file
    disp_new(OUTPUT_FILE)

    # 5️⃣ Execute business logic with chunked processing
    print("[EXEC] Starting business logic execution...")
    
    try:
        spool_lines, total_processed = execute_business_logic(OUTPUT_FILE)
        
        # 6️⃣ Write spool/report information to a separate file
        spool_file = OUTPUT_DIR / f"{JOB_NAME}_REPORT_{REPORT_DATE.strftime('%Y%m%d')}.txt"
        
        with open(spool_file, "w", encoding="utf-8") as f:
            for line in spool_lines:
                f.write(line + "\n")
        
        print(f"[WRITE] Spool file created: {spool_file}")
        print(f"[INFO] Spool lines: {len(spool_lines):,}")

        # 7️⃣ Print summary
        print("-" * 60)
        print("JOB SUMMARY:")
        print(f"Output file: {OUTPUT_FILE}")
        print(f"Report file: {spool_file}")
        print(f"Total records processed: {total_processed:,}")
        
        if OUTPUT_FILE.exists():
            file_size = OUTPUT_FILE.stat().st_size
            if file_size > 1024 * 1024:
                print(f"Output file size: {file_size / (1024*1024):.2f} MB")
            else:
                print(f"Output file size: {file_size / 1024:.2f} KB")
        
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
        sys.exit(8)  # Simulate ABEND
