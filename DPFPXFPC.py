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
# PBBLNFMT.PY - FORMATTING LOGIC
# =====================================================

def apply_pbblnfmt(value, field_type="amount"):
    """
    Apply PBBLNFMT formatting similar to SAS PBBLNFMT.
    """
    if pd.isna(value) or value is None:
        return " " * 15  # Return blanks for missing values
    
    # Convert to string if not already
    value_str = str(value)
    
    if field_type == "amount":
        # Format amounts: PBBLNFMT typically right-aligns with leading zeros
        # Example: 120000 -> "0000000120000"
        # Remove any decimal points and format as integer
        try:
            # Handle both integer and decimal values
            if isinstance(value, (int, float)):
                # Remove decimal and format as integer
                int_val = int(value)
                formatted = f"{int_val:012d}"  # 12 digits with leading zeros
                return formatted[:15].rjust(15)  # Ensure 15 characters total
            else:
                # Try to clean the string
                clean_str = ''.join(filter(str.isdigit, value_str))
                if clean_str:
                    int_val = int(clean_str)
                    formatted = f"{int_val:012d}"
                    return formatted[:15].rjust(15)
                return " " * 15
        except:
            return value_str[:15].ljust(15)
    
    elif field_type == "code":
        # Format codes: left-justified with trailing blanks
        return value_str[:15].ljust(15)
    
    elif field_type == "account":
        # Format account numbers: right-justified with leading zeros
        clean_str = ''.join(filter(str.isdigit, value_str))
        if clean_str:
            return clean_str[:15].zfill(15)  # Pad with zeros to 15 chars
        return value_str[:15].rjust(15)
    
    else:
        # Default formatting
        return value_str[:15].ljust(15)


def format_record(record, record_type):
    """
    Apply PBBLNFMT formatting to entire record.
    This mimics the SAS PBBLNFMT format.
    """
    formatted_record = []
    
    if record_type == "LOAN":
        # Expected format: BNMCODE, AMOUNT, OTHER_FIELDS
        # Apply formatting based on field type
        if len(record) >= 2:
            # Format BNMCODE (account number)
            formatted_record.append(apply_pbblnfmt(record[0], "account"))
            # Format AMOUNT
            formatted_record.append(apply_pbblnfmt(record[1], "amount"))
            # Format any remaining fields as generic
            for field in record[2:]:
                formatted_record.append(apply_pbblnfmt(field, "code"))
        else:
            # If record doesn't match expected format, apply generic
            for field in record:
                formatted_record.append(apply_pbblnfmt(field, "code"))
    
    elif record_type == "ULOAN":
        # Similar formatting for unsecured loans
        if len(record) >= 2:
            formatted_record.append(apply_pbblnfmt(record[0], "account"))
            formatted_record.append(apply_pbblnfmt(record[1], "amount"))
            for field in record[2:]:
                formatted_record.append(apply_pbblnfmt(field, "code"))
        else:
            for field in record:
                formatted_record.append(apply_pbblnfmt(field, "code"))
    
    else:
        # Default formatting for other record types
        for field in record:
            formatted_record.append(apply_pbblnfmt(field, "code"))
    
    return formatted_record


# =====================================================
# TEXT FILE WRITER
# =====================================================

def write_text_file(path, records, delimiter="|"):
    """
    Write records to a text file with proper formatting.
    """
    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            if isinstance(record, list):
                # Join fields with delimiter
                f.write(delimiter.join(record) + "\n")
            elif isinstance(record, str):
                f.write(record + "\n")
            else:
                f.write(str(record) + "\n")
    
    print(f"[WRITE] Text file created: {path}")
    print(f"[INFO] Total records written: {len(records)}")


# =====================================================
# SAS DATA READER USING PYREADSTAT
# =====================================================

def read_sas7bdat(file_path):
    """
    Read SAS7BDAT file using pyreadstat and return pandas DataFrame.
    pyreadstat is faster and more reliable than sas7bdat library.
    """
    try:
        # Read SAS file with pyreadstat
        df, meta = pyreadstat.read_sas7bdat(file_path)
        
        # Print metadata for debugging
        print(f"[READ] Successfully read: {file_path.name}")
        print(f"[INFO] Records: {len(df)}, Columns: {len(df.columns)}")
        print(f"[INFO] Column names: {', '.join(df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")
        print(f"[INFO] File encoding: {meta.encoding}")
        
        return df
    
    except FileNotFoundError:
        raise FileNotFoundError(f"SAS file not found: {file_path}")
    except Exception as e:
        raise Exception(f"Error reading SAS7BDAT file {file_path} with pyreadstat: {e}")


def read_sas7bdat_with_options(file_path, usecols=None, row_limit=None):
    """
    Read SAS7BDAT file with additional options.
    
    Parameters:
    - file_path: Path to SAS file
    - usecols: List of column names to read (optional)
    - row_limit: Maximum number of rows to read (optional)
    """
    try:
        # Read SAS file with options
        df, meta = pyreadstat.read_sas7bdat(
            file_path,
            usecols=usecols,
            row_limit=row_limit
        )
        
        print(f"[READ] Read {len(df)} rows from {file_path.name}")
        return df, meta
    
    except Exception as e:
        raise Exception(f"Error reading SAS7BDAT file {file_path} with pyreadstat: {e}")


# =====================================================
# SAS BUSINESS LOGIC
# =====================================================

def execute_business_logic():
    """
    Execute the business logic using SAS7BDAT inputs.
    """
    print("[EXEC] Executing EIBWHP03 business logic...")

    # Read SAS datasets using pyreadstat
    data_frames = {}
    for name, path in INPUT_DATASETS.items():
        if path.exists():
            data_frames[name] = read_sas7bdat(path)
        else:
            print(f"[WARNING] Input dataset missing: {name} - {path}")
            data_frames[name] = None

    # Process loan datasets
    processed_records = []
    spool_lines = []

    # Header lines for output
    spool_lines.append(f"{JOB_NAME} REPORT")
    spool_lines.append(f"Report Date: {REPORT_DATE.strftime('%Y-%m-%d')}")
    spool_lines.append(f"Report Month: {REPORT_MONTH}")
    spool_lines.append(f"Report Week: {REPORT_WEEK}")
    spool_lines.append("-" * 80)
    spool_lines.append("BNMCODE                    AMOUNT         STATUS")
    spool_lines.append("-" * 80)

    # Process LOAN_CURRENT if available
    if data_frames.get("LOAN_CURRENT") is not None:
        df = data_frames["LOAN_CURRENT"]
        print(f"[PROCESS] Processing LOAN_CURRENT - {len(df)} records")
        
        # Get column names for reference
        col_names = df.columns.tolist()
        print(f"[INFO] LOAN_CURRENT columns: {col_names}")
        
        for idx, row in df.iterrows():
            # Extract fields from DataFrame - first column is usually BNMCODE, second is AMOUNT
            bnmcode = str(row.iloc[0]) if len(row) > 0 else ""
            amount = row.iloc[1] if len(row) > 1 else 0
            
            # Create record and apply PBBLNFMT
            record = [bnmcode, amount]
            formatted_record = format_record(record, "LOAN")
            
            # Format for output
            output_line = "|".join(formatted_record)
            processed_records.append(output_line)
            
            # Add to spool for reporting (limit to first 100 records for readability)
            if idx < 100:
                spool_lines.append(f"{formatted_record[0]}  {formatted_record[1]}  PROCESSED")

    # Process ULOAN_CURRENT if available
    if data_frames.get("ULOAN_CURRENT") is not None:
        df = data_frames["ULOAN_CURRENT"]
        print(f"[PROCESS] Processing ULOAN_CURRENT - {len(df)} records")
        
        col_names = df.columns.tolist()
        print(f"[INFO] ULOAN_CURRENT columns: {col_names}")
        
        for idx, row in df.iterrows():
            bnmcode = str(row.iloc[0]) if len(row) > 0 else ""
            amount = row.iloc[1] if len(row) > 1 else 0
            
            record = [bnmcode, amount]
            formatted_record = format_record(record, "ULOAN")
            
            output_line = "|".join(formatted_record)
            processed_records.append(output_line)
            
            if idx < 100:
                spool_lines.append(f"{formatted_record[0]}  {formatted_record[1]}  UNSECURED")

    # Process LOAN_PREVIOUS if available (for comparison)
    if data_frames.get("LOAN_PREVIOUS") is not None:
        df = data_frames["LOAN_PREVIOUS"]
        print(f"[PROCESS] Processing LOAN_PREVIOUS - {len(df)} records")
        spool_lines.append("-" * 80)
        spool_lines.append("PREVIOUS WEEK DATA")
        spool_lines.append("-" * 80)
        
        for idx, row in df.iterrows():
            bnmcode = str(row.iloc[0]) if len(row) > 0 else ""
            amount = row.iloc[1] if len(row) > 1 else 0
            
            record = [bnmcode, amount]
            formatted_record = format_record(record, "LOAN")
            
            if idx < 100:
                spool_lines.append(f"{formatted_record[0]}  {formatted_record[1]}  PREVIOUS")

    # Footer
    spool_lines.append("-" * 80)
    spool_lines.append(f"TOTAL RECORDS PROCESSED: {len(processed_records)}")
    spool_lines.append(f"END OF {JOB_NAME} REPORT")

    return processed_records, spool_lines


# =====================================================
# JOB EXECUTION
# =====================================================

def run_job():

    print(f"========== START JOB {JOB_NAME} ==========")
    print(f"Report Date: {REPORT_DATE.strftime('%Y-%m-%d')}")
    print(f"Report Month: {REPORT_MONTH}")
    print(f"Report Week: {REPORT_WEEK}")
    print(f"Python version: {sys.version}")

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

    # 5️⃣ Execute business logic
    processed_records, spool_lines = execute_business_logic()

    # 6️⃣ Write output to text file
    write_text_file(OUTPUT_FILE, processed_records)

    # 7️⃣ Write spool/report information to a separate file
    spool_file = OUTPUT_DIR / f"{JOB_NAME}_REPORT_{REPORT_DATE.strftime('%Y%m%d')}.txt"
    write_text_file(spool_file, spool_lines)

    # 8️⃣ Print summary
    print("-" * 60)
    print("JOB SUMMARY:")
    print(f"Output file: {OUTPUT_FILE}")
    print(f"Report file: {spool_file}")
    print(f"Total records written: {len(processed_records)}")
    print(f"Output file size: {OUTPUT_FILE.stat().st_size / 1024:.2f} KB" if OUTPUT_FILE.exists() else "Output file not created")
    print("-" * 60)

    print(f"========== END JOB {JOB_NAME} ==========")


# =====================================================
# ALTERNATIVE: Read specific columns using pyreadstat
# =====================================================

def read_specific_columns(file_path, columns):
    """
    Read only specific columns from SAS file using pyreadstat.
    This is more efficient for large files.
    """
    try:
        df, meta = pyreadstat.read_sas7bdat(file_path, usecols=columns)
        return df
    except Exception as e:
        print(f"[ERROR] Failed to read specific columns: {e}")
        return None


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
