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
    chunk_size: int = 50_000,  # Reduced chunk size for testing
    test_mode: bool = True     # Added test mode flag
) -> tuple:
    
    print("="*80)
    print("STEP 1: LOAD_EXDWH_LN_BILL - SCD TYPE 2 PROCESSING (OPTIMIZED - TEST MODE)")
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
    print(f"TEST MODE: {test_mode}")
    print("-" * 80)
      
    # Define historical file paths (previous day's output)
    input_ln_bill = prev_dir / f"LOAN_BILL.parquet"
    input_iln_bill = prev_dir / f"ILOAN_BILL.parquet"
    
    print(f"Looking for historical data:")
    print(f"  LOAN_BILL: {input_ln_bill} - {'EXISTS' if input_ln_bill.exists() else 'NOT FOUND'}")
    print(f"  ILOAN_BILL: {input_iln_bill} - {'EXISTS' if input_iln_bill.exists() else 'NOT FOUND'}")
    
    # Create temporary directory for chunk processing
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Strategy 1: Use PyArrow for efficient chunked reading - WITH LIMIT FOR TESTING
        print("\n1.1: Reading ENRH_LN_BILL in chunks (TEST MODE - LIMITED RECORDS)...")
        enrh_file = pq.ParquetFile(input_enrh_path)
        total_records = enrh_file.metadata.num_rows
        print(f"  Total records in file: {total_records:,}")
        
        # TEST MODE: Only read first few chunks or limited rows
        if test_mode:
            # Option 1: Read only first N rows
            test_row_limit = 100_000  # Adjust this number as needed
            print(f"  TEST MODE: Reading only first {test_row_limit:,} records")
            
            # Read the entire file but limit rows
            full_df = pl.read_parquet(input_enrh_path)
            test_df = full_df.head(test_row_limit)
            
            # Split into smaller chunks for processing
            chunk_files = []
            for i in range(0, len(test_df), chunk_size):
                chunk = test_df[i:i + chunk_size]
                chunk_file = temp_path / f"enrh_chunk_{i//chunk_size}.parquet"
                chunk.write_parquet(chunk_file)
                chunk_files.append(chunk_file)
                print(f"  Created chunk {i//chunk_size + 1} with {len(chunk):,} records")
                
            del full_df, test_df  # Free memory
            
        else:
            # Original logic for production
            chunk_files = []
            for i, batch in enumerate(enrh_file.iter_batches(batch_size=chunk_size)):
                print(f"  Processing chunk {i+1}...")
                
                df = pl.from_arrow(batch)
                chunk_file = temp_path / f"enrh_chunk_{i}.parquet"
                df.write_parquet(chunk_file)
                chunk_files.append(chunk_file)
                
                del df, batch  # Free memory
        
        print(f"  Total chunks to process: {len(chunk_files)}")
        
        # Strategy 2: Process each chunk separately and combine
        print("\n1.2: Processing chunks with OPTIMIZED SCD logic...")
        
        con = duckdb.connect(':memory:')
        
        # Load and prepare ONLY ACTIVE historical records from previous day
        has_historical_data = False
        active_hist_count = 0
        
        # Check if both historical files exist
        if input_ln_bill.exists() and input_iln_bill.exists():
            print(f"  Loading only ACTIVE records from previous day (VALID_TO_DT = {PREVDATE.date()})...")
            
            # Convert SAS date to numeric for comparison (since VALID_TO_DT is stored as DOUBLE)
            prev_date_numeric = (PREVDATE - SAS_ORIGIN).days
            
            # TEST MODE: Also limit historical data for faster testing
            if test_mode:
                print(f"  TEST MODE: Limiting historical data to first 10,000 records per file")
                
                # Load limited active records from LOAN_BILL
                con.execute(f"""
                    CREATE TABLE ln_bill_hist_active AS 
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
                    FROM (
                        SELECT * FROM read_parquet('{input_ln_bill}')
                        WHERE VALID_TO_DT = {prev_date_numeric}
                        LIMIT 10000
                    )
                """)
                
                # Load limited active records from ILOAN_BILL
                con.execute(f"""
                    CREATE TABLE iln_bill_hist_active AS 
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
                    FROM (
                        SELECT * FROM read_parquet('{input_iln_bill}')
                        WHERE VALID_TO_DT = {prev_date_numeric}
                        LIMIT 10000
                    )
                """)
            else:
                # Original logic for production
                con.execute(f"""
                    CREATE TABLE ln_bill_hist_active AS 
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
                    WHERE VALID_TO_DT = {prev_date_numeric}
                """)
                
                con.execute(f"""
                    CREATE TABLE iln_bill_hist_active AS 
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
                    WHERE VALID_TO_DT = {prev_date_numeric}
                """)
            
            # Combine active historical data
            con.execute(f"""
                CREATE TABLE loan_bill_hist_active AS
                SELECT * FROM ln_bill_hist_active
                UNION ALL
                SELECT * FROM iln_bill_hist_active
            """)
            
            # Count active records for monitoring
            active_hist_count = con.execute("SELECT COUNT(*) FROM loan_bill_hist_active").fetchone()[0]
            
            # Create index for faster joins
            con.execute("""
                CREATE INDEX idx_hist_active_key ON loan_bill_hist_active(ACCTNO, NOTENO, BILL_DT)
            """)
            has_historical_data = True
            print(f"  ✓ Loaded {active_hist_count:,} ACTIVE historical records (vs full dataset)")
        else:
            # Create empty historical table structure
            con.execute("""
                CREATE TABLE loan_bill_hist_active AS
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
            
            # Process chunk with OPTIMIZED SCD logic
            chunk_result = process_scd_chunk_optimized(
                con, chunk_file, REPTDATE, PREVDATE, temp_path, f"chunk_{i}", 
                has_historical_data, active_hist_count
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

def process_scd_chunk_optimized(con, chunk_file, REPTDATE, PREVDATE, temp_path, chunk_id, has_historical_data, active_hist_count):
    """Process OPTIMIZED SCD logic for a single chunk - only compare with active records"""
    
    # Load chunk as new records - FIXED: Convert to DATE types
    con.execute(f"""
        CREATE TEMP TABLE new_records AS
        SELECT 
            ACCTNO, 
            NOTENO,
            -- Convert BILL_DT from SAS numeric to DATE
            CASE 
                WHEN BILL_DT IS NOT NULL THEN 
                    DATE '{SAS_ORIGIN.date()}' + INTERVAL (CAST(BILL_DT AS INTEGER)) DAYS
                ELSE NULL 
            END AS BILL_DT,
            -- Convert BILL_PAID_DT from SAS numeric to DATE
            CASE 
                WHEN BILL_PAID_DT IS NOT NULL AND CAST(BILL_PAID_DT AS INTEGER) > 0 THEN 
                    DATE '{SAS_ORIGIN.date()}' + INTERVAL (CAST(BILL_PAID_DT AS INTEGER)) DAYS
                ELSE NULL 
            END AS BILL_PAID_DT,
            BILL_AMT, 
            BILL_AMT_PRIN, 
            BILL_AMT_INT, 
            BILL_AMT_ESCROW, 
            BILL_AMT_FEE,
            BILL_NOT_PAY_AMT, 
            BILL_NOT_PAY_AMT_PRIN, 
            BILL_NOT_PAY_AMT_INT,
            BILL_NOT_PAY_AMT_ESCROW, 
            BILL_NOT_PAY_AMT_FEE,
            COSTCTR, 
            PRODUCT
        FROM read_parquet('{chunk_file}')
    """)
    
    # Create index for faster joins
    con.execute("""
        CREATE INDEX idx_new_key ON new_records(ACCTNO, NOTENO, BILL_DT)
    """)
    
    if has_historical_data and active_hist_count > 0:
        print(f"    Comparing with {active_hist_count:,} active historical records...")
        
        # Create temporary table for historical data with converted dates
        con.execute(f"""
            CREATE TEMP TABLE hist_with_dates AS
            SELECT 
                ACCTNO, 
                NOTENO,
                -- Convert BILL_DT from SAS numeric to DATE
                CASE 
                    WHEN BILL_DT IS NOT NULL THEN 
                        DATE '{SAS_ORIGIN.date()}' + INTERVAL (CAST(BILL_DT AS INTEGER)) DAYS
                    ELSE NULL 
                END AS BILL_DT,
                -- Convert BILL_PAID_DT from SAS numeric to DATE
                CASE 
                    WHEN BILL_PAID_DT IS NOT NULL AND CAST(BILL_PAID_DT AS INTEGER) > 0 THEN 
                        DATE '{SAS_ORIGIN.date()}' + INTERVAL (CAST(BILL_PAID_DT AS INTEGER)) DAYS
                    ELSE NULL 
                END AS BILL_PAID_DT,
                BILL_AMT, 
                BILL_AMT_PRIN, 
                BILL_AMT_INT, 
                BILL_AMT_ESCROW, 
                BILL_AMT_FEE,
                BILL_NOT_PAY_AMT, 
                BILL_NOT_PAY_AMT_PRIN, 
                BILL_NOT_PAY_AMT_INT,
                BILL_NOT_PAY_AMT_ESCROW, 
                BILL_NOT_PAY_AMT_FEE,
                COSTCTR, 
                PRODUCT,
                -- Convert VALID_FROM_DT from SAS numeric to DATE
                CASE 
                    WHEN VALID_FROM_DT IS NOT NULL THEN 
                        DATE '{SAS_ORIGIN.date()}' + INTERVAL (CAST(VALID_FROM_DT AS INTEGER)) DAYS
                    ELSE NULL 
                END AS VALID_FROM_DT
            FROM loan_bill_hist_active
        """)
        
        # Create index for historical data
        con.execute("""
            CREATE INDEX idx_hist_dates_key ON hist_with_dates(ACCTNO, NOTENO, BILL_DT)
        """)
        
        # OPTIMIZED SCD logic - only compare with ACTIVE historical records
        con.execute(f"""
            CREATE TEMP TABLE scd_result AS
            WITH matched_records AS (
                -- MATCH: Records that exist in both with same values (ALL FIELDS)
                -- Only comparing with ACTIVE historical records
                SELECT 
                    T1.ACCTNO, T1.NOTENO, T1.BILL_DT, T1.BILL_PAID_DT,
                    T1.BILL_AMT, T1.BILL_AMT_PRIN, T1.BILL_AMT_INT, 
                    T1.BILL_AMT_ESCROW, T1.BILL_AMT_FEE,
                    T1.BILL_NOT_PAY_AMT, T1.BILL_NOT_PAY_AMT_PRIN, 
                    T1.BILL_NOT_PAY_AMT_INT, T1.BILL_NOT_PAY_AMT_ESCROW, 
                    T1.BILL_NOT_PAY_AMT_FEE,
                    T1.COSTCTR, T1.PRODUCT, T1.VALID_FROM_DT,
                    DATE '{REPTDATE.date()}' AS VALID_TO_DT
                FROM hist_with_dates T1
                WHERE EXISTS (
                    SELECT 1 FROM new_records T2 
                    WHERE T1.ACCTNO = T2.ACCTNO 
                    AND T1.NOTENO = T2.NOTENO
                    AND T1.BILL_DT = T2.BILL_DT
                    AND (T1.BILL_PAID_DT = T2.BILL_PAID_DT OR (T1.BILL_PAID_DT IS NULL AND T2.BILL_PAID_DT IS NULL))
                    AND T1.BILL_AMT = T2.BILL_AMT
                    AND T1.BILL_AMT_PRIN = T2.BILL_AMT_PRIN
                    AND T1.BILL_AMT_INT = T2.BILL_AMT_INT
                    AND T1.BILL_AMT_ESCROW = T2.BILL_AMT_ESCROW
                    AND T1.BILL_AMT_FEE = T2.BILL_AMT_FEE
                    AND T1.BILL_NOT_PAY_AMT = T2.BILL_NOT_PAY_AMT
                    AND T1.BILL_NOT_PAY_AMT_PRIN = T2.BILL_NOT_PAY_AMT_PRIN
                    AND T1.BILL_NOT_PAY_AMT_INT = T2.BILL_NOT_PAY_AMT_INT
                    AND T1.BILL_NOT_PAY_AMT_ESCROW = T2.BILL_NOT_PAY_AMT_ESCROW
                    AND T1.BILL_NOT_PAY_AMT_FEE = T2.BILL_NOT_PAY_AMT_FEE
                    AND T1.COSTCTR = T2.COSTCTR
                    AND T1.PRODUCT = T2.PRODUCT
                )
            ),
            new_or_changed AS (
                -- New records or records with changes
                -- Only need to check against ACTIVE historical records
                SELECT 
                    T2.ACCTNO, T2.NOTENO, T2.BILL_DT, T2.BILL_PAID_DT,
                    T2.BILL_AMT, T2.BILL_AMT_PRIN, T2.BILL_AMT_INT, 
                    T2.BILL_AMT_ESCROW, T2.BILL_AMT_FEE,
                    T2.BILL_NOT_PAY_AMT, T2.BILL_NOT_PAY_AMT_PRIN, 
                    T2.BILL_NOT_PAY_AMT_INT, T2.BILL_NOT_PAY_AMT_ESCROW, 
                    T2.BILL_NOT_PAY_AMT_FEE,
                    T2.COSTCTR, T2.PRODUCT,
                    DATE '{REPTDATE.date()}' AS VALID_FROM_DT,
                    DATE '{REPTDATE.date()}' AS VALID_TO_DT
                FROM new_records T2
                WHERE NOT EXISTS (
                    SELECT 1 FROM hist_with_dates T1
                    WHERE T1.ACCTNO = T2.ACCTNO 
                    AND T1.NOTENO = T2.NOTENO
                    AND T1.BILL_DT = T2.BILL_DT
                    AND (T1.BILL_PAID_DT = T2.BILL_PAID_DT OR (T1.BILL_PAID_DT IS NULL AND T2.BILL_PAID_DT IS NULL))
                    AND T1.BILL_AMT = T2.BILL_AMT
                    AND T1.BILL_AMT_PRIN = T2.BILL_AMT_PRIN
                    AND T1.BILL_AMT_INT = T2.BILL_AMT_INT
                    AND T1.BILL_AMT_ESCROW = T2.BILL_AMT_ESCROW
                    AND T1.BILL_AMT_FEE = T2.BILL_AMT_FEE
                    AND T1.BILL_NOT_PAY_AMT = T2.BILL_NOT_PAY_AMT
                    AND T1.BILL_NOT_PAY_AMT_PRIN = T2.BILL_NOT_PAY_AMT_PRIN
                    AND T1.BILL_NOT_PAY_AMT_INT = T2.BILL_NOT_PAY_AMT_INT
                    AND T1.BILL_NOT_PAY_AMT_ESCROW = T2.BILL_NOT_PAY_AMT_ESCROW
                    AND T1.BILL_NOT_PAY_AMT_FEE = T2.BILL_NOT_PAY_AMT_FEE
                    AND T1.COSTCTR = T2.COSTCTR
                    AND T1.PRODUCT = T2.PRODUCT
                )
            )
            SELECT * FROM matched_records
            UNION ALL
            SELECT * FROM new_or_changed
        """)
        
        # Cleanup temporary historical table
        con.execute("DROP TABLE hist_with_dates")
        
    else:
        # No historical data or no active records - all records are new
        print("    No active historical records found - treating all as new...")
        con.execute(f"""
            CREATE TEMP TABLE scd_result AS
            SELECT 
                ACCTNO, NOTENO, BILL_DT, BILL_PAID_DT,
                BILL_AMT, BILL_AMT_PRIN, BILL_AMT_INT, 
                BILL_AMT_ESCROW, BILL_AMT_FEE,
                BILL_NOT_PAY_AMT, BILL_NOT_PAY_AMT_PRIN, 
                BILL_NOT_PAY_AMT_INT, BILL_NOT_PAY_AMT_ESCROW, 
                BILL_NOT_PAY_AMT_FEE,
                COSTCTR, PRODUCT,
                DATE '{REPTDATE.date()}' AS VALID_FROM_DT,
                DATE '{REPTDATE.date()}' AS VALID_TO_DT
            FROM new_records
        """)
    
    # Save chunk result
    chunk_output = temp_path / f"scd_result_{chunk_id}.parquet"
    con.execute(f"COPY scd_result TO '{chunk_output}' (FORMAT PARQUET)")
    
    # Get count for monitoring
    result_count = con.execute("SELECT COUNT(*) FROM scd_result").fetchone()[0]
    print(f"    Generated {result_count:,} records")
    
    # Cleanup
    con.execute("DROP TABLE new_records")
    con.execute("DROP TABLE scd_result")
    
    return chunk_output

# ... (rest of the functions remain the same - save_final_results, process_hp_bill_extraction)

# Main execution
if __name__ == "__main__":
    print("="*80)
    print("BILL PROCESSING PIPELINE - OPTIMIZED VERSION (TEST MODE)")
    print("="*80)
    
    # SAS date calculations for processing logic
    REPTDATE = datetime.strptime("2025-11-26 00:00:00", '%Y-%m-%d %H:%M:%S')  # Yesterday as report date
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
    input_enrh = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/input/enrichment/ENRH_LN_BILL.parquet")
    
    # Output directory
    output_dir = Path(f"/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/input/year={REPTDATE.year}/month={REPTDATE.month:02d}/day={REPTDATE.day:02d}")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Previous Output directory
    prev_dir = Path(f"/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/input/year={PREVDATE.year}/month={PREVDATE.month:02d}/day={PREVDATE.day:02d}")

    
    # STEP 1: Generate TODAY'S LOAN_BILL and ILOAN_BILL (OPTIMIZED - TEST MODE)
    loan_bill_path, iloan_bill_path = process_large_loan_bill_scd(
        input_enrh_path=input_enrh,
        output_dir=output_dir,
        prev_dir=prev_dir,
        report_date=REPTDATE,
        chunk_size=50_000,  # Smaller chunks for testing
        test_mode=True      # Enable test mode
    )
    
    if loan_bill_path is None:
        print("\n✗ STEP 1 failed. Exiting.")
        exit(1)
    
    # STEP 2: Extract HP_BILL and IHP_BILL from TODAY'S LOAN_BILL and ILOAN_BILL
    process_hp_bill_extraction(
        loan_bill_path=loan_bill_path,
        iloan_bill_path=iloan_bill_path,
        output_dir=output_dir,
        report_date=REPTDATE
    )
    
    print("\n" + "="*80)
    print("OPTIMIZED PROCESSING COMPLETED SUCCESSFULLY (TEST MODE)")
    print("="*80)
    print(f"\nGenerated Files (Date: {date_str}):")
    print(f"  1. LOAN_BILL.parquet")
    print(f"  2. ILOAN_BILL.parquet") 
    print(f"  3. HP_BILL.parquet")
    print(f"  4. IHP_BILL.parquet")
    print("="*80)
