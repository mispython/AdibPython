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
    
    print(f"Looking for historical data:")
    print(f"  LOAN_BILL: {input_ln_bill} - {'EXISTS' if input_ln_bill.exists() else 'NOT FOUND'}")
    print(f"  ILOAN_BILL: {input_iln_bill} - {'EXISTS' if input_iln_bill.exists() else 'NOT FOUND'}")
    
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
        
        # Load ALL historical records (not just active ones) - REVERTED OPTIMIZATION
        has_historical_data = False
        hist_count = 0
        
        # Check if both historical files exist
        if input_ln_bill.exists() and input_iln_bill.exists():
            print(f"  Loading ALL historical records (not just active ones)...")
            
            # Load ALL records from LOAN_BILL - NOT filtering by VALID_TO_DT
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
            
            # Load ALL records from ILOAN_BILL - NOT filtering by VALID_TO_DT
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

def process_scd_chunk_original_sas(con, chunk_file, REPTDATE, PREVDATE, temp_path, chunk_id, has_historical_data, hist_count):
    """Process ORIGINAL SAS SCD logic for a single chunk - compare with ALL historical records"""
    
    # Load chunk as new records (W5KTCTSDX in SAS)
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
            PRODUCT,
            'Y' AS NEW  -- Equivalent to SAS NEW flag
        FROM read_parquet('{chunk_file}')
    """)
    
    # Create index for faster joins
    con.execute("""
        CREATE INDEX idx_new_key ON new_records(ACCTNO, NOTENO, BILL_DT)
    """)
    
    if has_historical_data and hist_count > 0:
        print(f"    Comparing with {hist_count:,} ALL historical records...")
        
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
                END AS VALID_FROM_DT,
                -- Convert VALID_TO_DT from SAS numeric to DATE
                CASE 
                    WHEN VALID_TO_DT IS NOT NULL THEN 
                        DATE '{SAS_ORIGIN.date()}' + INTERVAL (CAST(VALID_TO_DT AS INTEGER)) DAYS
                    ELSE NULL 
                END AS VALID_TO_DT,
                -- Create EXT flag for records where VALID_TO_DT = previous date (SAS logic)
                CASE 
                    WHEN VALID_TO_DT = {(PREVDATE - SAS_ORIGIN).days} THEN 'Y'
                    ELSE NULL 
                END AS EXT
            FROM loan_bill_hist
        """)
        
        # Create index for historical data
        con.execute("""
            CREATE INDEX idx_hist_dates_key ON hist_with_dates(ACCTNO, NOTENO, BILL_DT)
        """)
        
        # ORIGINAL SAS SCD logic - MATCH (equivalent to SAS MATCH table)
        con.execute(f"""
            CREATE TEMP TABLE match_records AS
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
            INNER JOIN new_records T2 ON 
                T1.ACCTNO = T2.ACCTNO 
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
            WHERE T1.EXT = 'Y'  -- Only update records that were expiring today
        """)
        
        # ORIGINAL SAS SCD logic - CHGREC (equivalent to SAS CHGREC table)
        con.execute(f"""
            CREATE TEMP TABLE chgrec_records AS
            SELECT 
                T1.ACCTNO, T1.NOTENO, T1.BILL_DT, T1.BILL_PAID_DT,
                T1.BILL_AMT, T1.BILL_AMT_PRIN, T1.BILL_AMT_INT, 
                T1.BILL_AMT_ESCROW, T1.BILL_AMT_FEE,
                T1.BILL_NOT_PAY_AMT, T1.BILL_NOT_PAY_AMT_PRIN, 
                T1.BILL_NOT_PAY_AMT_INT, T1.BILL_NOT_PAY_AMT_ESCROW, 
                T1.BILL_NOT_PAY_AMT_FEE,
                T1.COSTCTR, T1.PRODUCT, T1.VALID_FROM_DT,
                T1.VALID_TO_DT
            FROM hist_with_dates T1
            LEFT JOIN new_records T2 ON 
                T1.ACCTNO = T2.ACCTNO 
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
            WHERE T2.NEW IS NULL  -- Records that don't match new data (keep unchanged)
        """)
        
        # ORIGINAL SAS SCD logic - UPDATEX (equivalent to SAS UPDATEX table)
        con.execute(f"""
            CREATE TEMP TABLE updatex_records AS
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
            FROM hist_with_dates T1
            RIGHT JOIN new_records T2 ON 
                T1.ACCTNO = T2.ACCTNO 
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
            WHERE T1.EXT IS NULL  -- New records that don't exist in historical
        """)
        
        # ORIGINAL SAS SCD logic - KEEP records (LOAN_BILL_KEEP in SAS)
        con.execute(f"""
            CREATE TEMP TABLE keep_records AS
            SELECT 
                ACCTNO, NOTENO, BILL_DT, BILL_PAID_DT,
                BILL_AMT, BILL_AMT_PRIN, BILL_AMT_INT, 
                BILL_AMT_ESCROW, BILL_AMT_FEE,
                BILL_NOT_PAY_AMT, BILL_NOT_PAY_AMT_PRIN, 
                BILL_NOT_PAY_AMT_INT, BILL_NOT_PAY_AMT_ESCROW, 
                BILL_NOT_PAY_AMT_FEE,
                COSTCTR, PRODUCT, VALID_FROM_DT, VALID_TO_DT
            FROM hist_with_dates
            WHERE EXT IS NULL  -- Records that are not expiring today
        """)
        
        # Combine all results (equivalent to SAS final UNION)
        con.execute(f"""
            CREATE TEMP TABLE scd_result AS
            SELECT * FROM match_records
            UNION ALL
            SELECT * FROM updatex_records
            UNION ALL
            SELECT * FROM chgrec_records
            UNION ALL
            SELECT * FROM keep_records
        """)
        
        # Cleanup temporary tables
        con.execute("DROP TABLE hist_with_dates")
        con.execute("DROP TABLE match_records")
        con.execute("DROP TABLE chgrec_records")
        con.execute("DROP TABLE updatex_records")
        con.execute("DROP TABLE keep_records")
        
    else:
        # No historical data - all records are new
        print("    No historical records found - treating all as new...")
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

# ... (save_final_results, process_hp_bill_extraction, and main execution remain the same)
