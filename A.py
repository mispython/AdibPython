import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile

# HP Product codes
HP_PRODUCTS = [
    200, 201, 204, 205, 209, 210, 211, 212, 214, 215, 219, 220,
    225, 226, 227, 228, 230, 233, 234, 235, 236, 237, 238, 239,
    240, 241, 242, 243, 300, 301, 304, 305, 359, 361, 363, 392,
    152, 153, 154, 155, 423, 424, 425, 426, 175, 176, 177, 178,
    400, 401, 402, 406, 407, 408, 409, 410, 411, 412, 413, 414,
    415, 416, 419, 420, 422, 429, 430, 464
]

# SAS date origin
SAS_ORIGIN = datetime(1960, 1, 1)

def process_large_loan_bill_scd(
    input_enrh_path: Path,
    output_dir: Path,
    prev_dir: Path,
    report_date: datetime,
    chunk_size: int = 5_000_000
) -> tuple:
    
    print("="*80)
    print("STEP 1: LOAD_EXDWH_LN_BILL - SCD TYPE 2 PROCESSING (ORIGINAL SAS LOGIC)")
    print("="*80)
    
    # SAS date calculations for processing logic
    REPTDATE = report_date
    PREVDATE = REPTDATE - timedelta(days=1)
    RDATE = (REPTDATE - SAS_ORIGIN).days
    PDATE = (PREVDATE - SAS_ORIGIN).days
    
    # yymmdd format for file naming
    date_str = REPTDATE.strftime("%y%m%d")
    prev_date_str = PREVDATE.strftime("%y%m%d")
    
    print(f"Report Date: {REPTDATE.strftime('%Y-%m-%d')} (SAS: {RDATE}, File: {date_str})")
    print(f"Previous Date: {PREVDATE.strftime('%Y-%m-%d')} (SAS: {PDATE}, File: {prev_date_str})")
    print(f"Processing in chunks of {chunk_size:,} records")
    print("-" * 80)
      
    # Define historical file paths (previous day's output)
    input_ln_bill = prev_dir / f"LOAN_BILL.parquet"
    input_iln_bill = prev_dir / f"ILOAN_BILL.parquet"
    
    # ============================================================================
    # TEMPORARY CHECKING - JUST COUNT RECORDS AND EXIT
    # ============================================================================
    print("TEMPORARY CHECK - COUNTING HISTORICAL RECORDS ONLY")
    
    print(f"Looking for historical data:")
    print(f"  LOAN_BILL: {input_ln_bill} - {'EXISTS' if input_ln_bill.exists() else 'NOT FOUND'}")
    if input_ln_bill.exists():
        temp_con = duckdb.connect()
        ln_bill_count = temp_con.execute(f"SELECT COUNT(*) FROM read_parquet('{input_ln_bill}')").fetchone()[0]
        temp_con.close()
        print(f"    Records: {ln_bill_count:,}")
        
    print(f"  ILOAN_BILL: {input_iln_bill} - {'EXISTS' if input_iln_bill.exists() else 'NOT FOUND'}")
    if input_iln_bill.exists():
        temp_con = duckdb.connect()
        iln_bill_count = temp_con.execute(f"SELECT COUNT(*) FROM read_parquet('{input_iln_bill}')").fetchone()[0]
        temp_con.close()
        print(f"    Records: {iln_bill_count:,}")
    
    print("\n" + "="*80)
    print("TEMPORARY CHECK COMPLETED - EXITING WITHOUT PROCESSING")
    print("="*80)
    return None, None  # Return None to indicate we're just checking
    
    # ============================================================================
    # COMMENT OUT EVERYTHING BELOW THIS LINE FOR TEMPORARY CHECKING
    # ============================================================================
    '''
    # Create temporary directory for chunk processing
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Strategy 1: Use PyArrow for efficient chunked reading
        print("\n1.1: Reading ENRH_LN_BILL in chunks...")
        enrh_file = pq.ParquetFile(input_enrh_path)
        total_records = enrh_file.metadata.num_rows
        print(f"  Total records: {total_records:,}")
        
        # Process chunks
        chunk_files = []
        for i, batch in enumerate(enrh_file.iter_batches(batch_size=chunk_size)):
            print(f"  Processing chunk {i+1}...")
            
            df = pl.from_arrow(batch)
            chunk_file = temp_path / f"enrh_chunk_{i}.parquet"
            df.write_parquet(chunk_file)
            chunk_files.append(chunk_file)
            
            del df, batch  # Free memory
        
        # Strategy 2: Process each chunk separately and combine
        print("\n1.2: Processing chunks with ORIGINAL SAS SCD logic...")
        
        con = duckdb.connect(':memory:')
        
        # Load ALL historical records (not just active ones)
        has_historical_data = False
        hist_count = 0
        
        # Check if both historical files exist
        if input_ln_bill.exists() and input_iln_bill.exists():
            print(f"  Loading ALL historical records...")
            
            # Load ALL records from LOAN_BILL
            con.execute(f"""
                CREATE TABLE ln_bill_hist AS 
                SELECT 
                    ACCTNO, NOTENO, 
                    BILL_DT,
                    BILL_PAID_DT,
                    BILL_AMT, BILL_AMT_PRIN, BILL_AMT_INT, BILL_AMT_ESCROW, BILL_AMT_FEE,
                    BILL_NOT_PAY_AMT, BILL_NOT_PAY_AMT_PRIN, BILL_NOT_PAY_AMT_INT,
                    BILL_NOT_PAY_AMT_ESCROW, BILL_NOT_PAY_AMT_FEE,
                    COSTCTR, PRODUCT,
                    VALID_FROM_DT,
                    VALID_TO_DT
                FROM read_parquet('{input_ln_bill}')
            """)
            
            # Load ALL records from ILOAN_BILL
            con.execute(f"""
                CREATE TABLE iln_bill_hist AS 
                SELECT 
                    ACCTNO, NOTENO, 
                    BILL_DT,
                    BILL_PAID_DT,
                    BILL_AMT, BILL_AMT_PRIN, BILL_AMT_INT, BILL_AMT_ESCROW, BILL_AMT_FEE,
                    BILL_NOT_PAY_AMT, BILL_NOT_PAY_AMT_PRIN, BILL_NOT_PAY_AMT_INT,
                    BILL_NOT_PAY_AMT_ESCROW, BILL_NOT_PAY_AMT_FEE,
                    COSTCTR, PRODUCT,
                    VALID_FROM_DT,
                    VALID_TO_DT
                FROM read_parquet('{input_iln_bill}')
            """)
            
            # Combine ALL historical data
            con.execute(f"""
                CREATE TABLE loan_bill_hist AS
                SELECT * FROM ln_bill_hist
                UNION ALL
                SELECT * FROM iln_bill_hist
            """)
            
            # Count historical records for monitoring
            hist_count = con.execute("SELECT COUNT(*) FROM loan_bill_hist").fetchone()[0]
            
            # Create index for faster joins
            con.execute("""
                CREATE INDEX idx_hist_key ON loan_bill_hist(ACCTNO, NOTENO, BILL_DT)
            """)
            has_historical_data = True
            print(f"  ✓ Loaded {hist_count:,} ALL historical records")
        else:
            # Create empty historical table structure
            con.execute("""
                CREATE TABLE loan_bill_hist AS
                SELECT 
                    CAST(NULL AS BIGINT) AS ACCTNO,
                    CAST(NULL AS BIGINT) AS NOTENO, 
                    CAST(NULL AS DOUBLE) AS BILL_DT,
                    CAST(NULL AS DOUBLE) AS BILL_PAID_DT,
                    CAST(NULL AS DOUBLE) AS BILL_AMT,
                    CAST(NULL AS DOUBLE) AS BILL_AMT_PRIN,
                    CAST(NULL AS DOUBLE) AS BILL_AMT_INT,
                    CAST(NULL AS DOUBLE) AS BILL_AMT_ESCROW,
                    CAST(NULL AS DOUBLE) AS BILL_AMT_FEE,
                    CAST(NULL AS DOUBLE) AS BILL_NOT_PAY_AMT,
                    CAST(NULL AS DOUBLE) AS BILL_NOT_PAY_AMT_PRIN,
                    CAST(NULL AS DOUBLE) AS BILL_NOT_PAY_AMT_INT,
                    CAST(NULL AS DOUBLE) AS BILL_NOT_PAY_AMT_ESCROW,
                    CAST(NULL AS DOUBLE) AS BILL_NOT_PAY_AMT_FEE,
                    CAST(NULL AS BIGINT) AS COSTCTR,
                    CAST(NULL AS BIGINT) AS PRODUCT,
                    CAST(NULL AS DOUBLE) AS VALID_FROM_DT,
                    CAST(NULL AS DOUBLE) AS VALID_TO_DT
                WHERE 1=0
            """)
            print("  ✓ No historical data found - starting fresh")
        
        # Process each chunk
        final_chunks = []
        for i, chunk_file in enumerate(chunk_files):
            print(f"  SCD processing chunk {i+1}/{len(chunk_files)}...")
            
            # Process chunk with ORIGINAL SAS SCD logic
            chunk_result = process_scd_chunk_original_sas(
                con, chunk_file, REPTDATE, PREVDATE, temp_path, f"chunk_{i}", 
                has_historical_data, hist_count
            )
            final_chunks.append(chunk_result)
        
        # Combine all chunks
        print("\n1.3: Combining all chunks...")
        if final_chunks:
            union_chunks = " UNION ALL ".join([f"SELECT * FROM read_parquet('{f}')" for f in final_chunks])
            con.execute(f"""
                CREATE TABLE loan_bill_combined AS
                {union_chunks}
            """)
        else:
            # If no chunks were processed, create empty table
            con.execute("""
                CREATE TABLE loan_bill_combined AS
                SELECT 
                    CAST(NULL AS BIGINT) AS ACCTNO,
                    CAST(NULL AS BIGINT) AS NOTENO, 
                    CAST(NULL AS DATE) AS BILL_DT,
                    CAST(NULL AS DATE) AS BILL_PAID_DT,
                    CAST(NULL AS DOUBLE) AS BILL_AMT,
                    CAST(NULL AS DOUBLE) AS BILL_AMT_PRIN,
                    CAST(NULL AS DOUBLE) AS BILL_AMT_INT,
                    CAST(NULL AS DOUBLE) AS BILL_AMT_ESCROW,
                    CAST(NULL AS DOUBLE) AS BILL_AMT_FEE,
                    CAST(NULL AS DOUBLE) AS BILL_NOT_PAY_AMT,
                    CAST(NULL AS DOUBLE) AS BILL_NOT_PAY_AMT_PRIN,
                    CAST(NULL AS DOUBLE) AS BILL_NOT_PAY_AMT_INT,
                    CAST(NULL AS DOUBLE) AS BILL_NOT_PAY_AMT_ESCROW,
                    CAST(NULL AS DOUBLE) AS BILL_NOT_PAY_AMT_FEE,
                    CAST(NULL AS BIGINT) AS COSTCTR,
                    CAST(NULL AS BIGINT) AS PRODUCT,
                    CAST(NULL AS DATE) AS VALID_FROM_DT,
                    CAST(NULL AS DATE) AS VALID_TO_DT
                WHERE 1=0
            """)
        
        # Add ENTITY_CD (optimized lookup)
        print("\n1.4: Adding ENTITY_CD with optimized lookup...")
        lookup_cost_ctr_path = Path("/sasdata/rawdata/lookup/lkp_cost_ctr.sas7bdat")
        if lookup_cost_ctr_path.exists():
            # Use broadcast join for small lookup table
            if lookup_cost_ctr_path.suffix == '.sas7bdat':
                import pyreadstat
                df, meta = pyreadstat.read_sas7bdat(str(lookup_cost_ctr_path))
                con.execute("CREATE TABLE lkp_cost_ctr AS SELECT * FROM df")
            else:
                con.execute(f"CREATE TABLE lkp_cost_ctr AS SELECT * FROM read_parquet('{lookup_cost_ctr_path}')")
            
            # Create index on lookup table
            con.execute("CREATE INDEX idx_lkp_cost ON lkp_cost_ctr(COSTCTR)")
            
            con.execute("""
                CREATE TABLE with_entity AS
                SELECT L.*, C.ENTITY_CD
                FROM loan_bill_combined L
                LEFT JOIN lkp_cost_ctr C ON L.COSTCTR = C.COSTCTR
            """)
        else:
            con.execute("""
                CREATE TABLE with_entity AS
                SELECT *, CAST(NULL AS VARCHAR) AS ENTITY_CD
                FROM loan_bill_combined
            """)
        
        # Split and save results
        print("\n1.5: Saving final results...")
        ln_bill_path, iln_bill_path = save_final_results(con, output_dir, date_str)
        
        con.close()
        
        return ln_bill_path, iln_bill_path
    '''
    # ============================================================================
    # END OF COMMENTED SECTION
    # ============================================================================

# ... (keep all other functions: process_scd_chunk_original_sas, save_final_results, process_hp_bill_extraction)

def process_scd_chunk_original_sas(con, chunk_file, REPTDATE, PREVDATE, temp_path, chunk_id, has_historical_data, hist_count):
    """Process ORIGINAL SAS SCD logic for a single chunk - compare with ALL historical records"""
    pass  # Placeholder - this won't be called during temporary check

def save_final_results(con, output_dir, date_str):
    """Save the final LN_BILL and ILOAN_BILL files"""
    pass  # Placeholder - this won't be called during temporary check

def process_hp_bill_extraction(
    loan_bill_path: Path,
    iloan_bill_path: Path,
    output_dir: Path,
    report_date: datetime
) -> None:
    """STEP 2: Extract HP_BILL and IHP_BILL from TODAY'S LOAN_BILL and ILOAN_BILL"""
    pass  # Placeholder - this won't be called during temporary check

# Main execution
if __name__ == "__main__":
    print("="*80)
    print("BILL PROCESSING PIPELINE - TEMPORARY RECORD COUNT CHECK")
    print("="*80)
    
    # SAS date calculations for processing logic
    REPTDATE = datetime.today() - timedelta(days=1)  # Yesterday as report date
    PREVDATE = REPTDATE - timedelta(days=1)          # Day before yesterday
    RDATE = (REPTDATE - SAS_ORIGIN).days
    PDATE = (PREVDATE - SAS_ORIGIN).days
    
    # yymmdd format for file naming
    date_str = REPTDATE.strftime("%y%m%d")
    prev_date_str = PREVDATE.strftime("%y%m%d")
    
    print(f"Report Date: {REPTDATE.strftime('%Y-%m-%d')} (SAS: {RDATE}, File: {date_str})")
    print(f"Previous Date: {PREVDATE.strftime('%Y-%m-%d')} (SAS: {PDATE}, File: {prev_date_str})")
    print()
    
    # Input paths
    input_enrh = Path("/parquet/dwh/LOAN/enrichment/ENRH_LN_BILL.parquet")
    
    # Output directory
    output_dir = Path(f"/parquet/dwh/LOAN/year={REPTDATE.year}/month={REPTDATE.month:02d}/day={REPTDATE.day:02d}")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Previous Output directory
    prev_dir = Path(f"/parquet/dwh/LOAN/year={PREVDATE.year}/month={PREVDATE.month:02d}/day={PREVDATE.day:02d}")

    
    # STEP 1: Just count historical records and exit
    loan_bill_path, iloan_bill_path = process_large_loan_bill_scd(
        input_enrh_path=input_enrh,
        output_dir=output_dir,
        prev_dir=prev_dir,
        report_date=REPTDATE,
        chunk_size=5_000_000
    )
    
    # Don't proceed to STEP 2 since we're just checking
    print("\n" + "="*80)
    print("PROGRAM COMPLETED - ONLY RECORD COUNTING WAS PERFORMED")
    print("="*80)
