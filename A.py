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
    chunk_size: int = 3_000_000
) -> tuple:
    
    print("="*80)
    print("STEP 1: LOAD_EXDWH_LN_BILL - SCD TYPE 2 PROCESSING (OPTIMIZED)")
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
        print("\n1.2: Loading and preparing historical data...")
        
        con = duckdb.connect(':memory:')
        
        # Load historical data ONCE
        has_historical_data = False
        hist_count = 0
        
        # Check if both historical files exist
        if input_ln_bill.exists() and input_iln_bill.exists():
            print(f"  Loading historical files...")
            
            # Load parquet files into tables
            con.execute(f"""
                CREATE TABLE ln_bill_hist AS 
                SELECT * FROM read_parquet('{input_ln_bill}')
            """)
            
            con.execute(f"""
                CREATE TABLE iln_bill_hist AS 
                SELECT * FROM read_parquet('{input_iln_bill}')
            """)
            
            # Create VIEW (not materialized table) for union
            con.execute(f"""
                CREATE VIEW loan_bill_hist AS
                SELECT * FROM ln_bill_hist
                UNION ALL
                SELECT * FROM iln_bill_hist
            """)
            
            # Estimate count without full scan
            ln_count = con.execute("SELECT COUNT(*) FROM ln_bill_hist").fetchone()[0]
            iln_count = con.execute("SELECT COUNT(*) FROM iln_bill_hist").fetchone()[0]
            hist_count = ln_count + iln_count
            
            print(f"  ✓ Created view over {hist_count:,} historical records")
            
            # Create ACTIVE records table ONCE (with hash-based comparison)
            print(f"  Creating ACTIVE records table (VALID_TO_DT = {PREVDATE.date()})...")
            con.execute(f"""
                CREATE TABLE active_records AS
                SELECT 
                    ACCTNO, NOTENO,
                    CASE 
                        WHEN BILL_DT IS NOT NULL THEN 
                            DATE '1960-01-01' + CAST(BILL_DT AS INTEGER) * INTERVAL 1 DAY
                        ELSE NULL 
                    END AS BILL_DT,
                    CASE 
                        WHEN BILL_PAID_DT IS NOT NULL AND CAST(BILL_PAID_DT AS INTEGER) > 0 THEN 
                            DATE '1960-01-01' + CAST(BILL_PAID_DT AS INTEGER) * INTERVAL 1 DAY
                        ELSE NULL 
                    END AS BILL_PAID_DT,
                    BILL_AMT, BILL_AMT_PRIN, BILL_AMT_INT, BILL_AMT_ESCROW, BILL_AMT_FEE,
                    BILL_NOT_PAY_AMT, BILL_NOT_PAY_AMT_PRIN, BILL_NOT_PAY_AMT_INT,
                    BILL_NOT_PAY_AMT_ESCROW, BILL_NOT_PAY_AMT_FEE,
                    COSTCTR, PRODUCT,
                    CASE 
                        WHEN VALID_FROM_DT IS NOT NULL THEN 
                            DATE '1960-01-01' + CAST(VALID_FROM_DT AS INTEGER) * INTERVAL 1 DAY
                        ELSE NULL 
                    END AS VALID_FROM_DT,
                    CASE 
                        WHEN VALID_TO_DT IS NOT NULL THEN 
                            DATE '1960-01-01' + CAST(VALID_TO_DT AS INTEGER) * INTERVAL 1 DAY
                        ELSE NULL 
                    END AS VALID_TO_DT,
                    MD5(
                        CAST(ACCTNO AS VARCHAR) || '|' ||
                        CAST(NOTENO AS VARCHAR) || '|' ||
                        CAST(CASE 
                            WHEN BILL_DT IS NOT NULL THEN 
                                DATE '1960-01-01' + CAST(BILL_DT AS INTEGER) * INTERVAL 1 DAY
                            ELSE NULL 
                        END AS VARCHAR) || '|' ||
                        CAST(COALESCE(CASE 
                            WHEN BILL_PAID_DT IS NOT NULL AND CAST(BILL_PAID_DT AS INTEGER) > 0 THEN 
                                DATE '1960-01-01' + CAST(BILL_PAID_DT AS INTEGER) * INTERVAL 1 DAY
                            ELSE NULL 
                        END, DATE '1900-01-01') AS VARCHAR) || '|' ||
                        CAST(BILL_AMT AS VARCHAR) || '|' ||
                        CAST(BILL_AMT_PRIN AS VARCHAR) || '|' ||
                        CAST(BILL_AMT_INT AS VARCHAR) || '|' ||
                        CAST(BILL_AMT_ESCROW AS VARCHAR) || '|' ||
                        CAST(BILL_AMT_FEE AS VARCHAR) || '|' ||
                        CAST(BILL_NOT_PAY_AMT AS VARCHAR) || '|' ||
                        CAST(BILL_NOT_PAY_AMT_PRIN AS VARCHAR) || '|' ||
                        CAST(BILL_NOT_PAY_AMT_INT AS VARCHAR) || '|' ||
                        CAST(BILL_NOT_PAY_AMT_ESCROW AS VARCHAR) || '|' ||
                        CAST(BILL_NOT_PAY_AMT_FEE AS VARCHAR) || '|' ||
                        CAST(COSTCTR AS VARCHAR) || '|' ||
                        CAST(PRODUCT AS VARCHAR)
                    ) AS record_hash
                FROM loan_bill_hist
                WHERE CAST(VALID_TO_DT AS INTEGER) = {PDATE}
            """)
            
            active_count = con.execute("SELECT COUNT(*) FROM active_records").fetchone()[0]
            print(f"  ✓ Created {active_count:,} ACTIVE records with hash keys")
            
            # Create indexes on active records
            con.execute("""
                CREATE INDEX idx_active_hash ON active_records(record_hash)
            """)
            print(f"  ✓ Created index on active records")
            
            # Create INACTIVE records VIEW (not materialized - saves memory)
            print(f"  Creating INACTIVE records view...")
            con.execute(f"""
                CREATE VIEW inactive_records AS
                SELECT 
                    ACCTNO, NOTENO,
                    CASE 
                        WHEN BILL_DT IS NOT NULL THEN 
                            DATE '1960-01-01' + CAST(BILL_DT AS INTEGER) * INTERVAL 1 DAY
                        ELSE NULL 
                    END AS BILL_DT,
                    CASE 
                        WHEN BILL_PAID_DT IS NOT NULL AND CAST(BILL_PAID_DT AS INTEGER) > 0 THEN 
                            DATE '1960-01-01' + CAST(BILL_PAID_DT AS INTEGER) * INTERVAL 1 DAY
                        ELSE NULL 
                    END AS BILL_PAID_DT,
                    BILL_AMT, BILL_AMT_PRIN, BILL_AMT_INT, BILL_AMT_ESCROW, BILL_AMT_FEE,
                    BILL_NOT_PAY_AMT, BILL_NOT_PAY_AMT_PRIN, BILL_NOT_PAY_AMT_INT,
                    BILL_NOT_PAY_AMT_ESCROW, BILL_NOT_PAY_AMT_FEE,
                    COSTCTR, PRODUCT,
                    CASE 
                        WHEN VALID_FROM_DT IS NOT NULL THEN 
                            DATE '1960-01-01' + CAST(VALID_FROM_DT AS INTEGER) * INTERVAL 1 DAY
                        ELSE NULL 
                    END AS VALID_FROM_DT,
                    CASE 
                        WHEN VALID_TO_DT IS NOT NULL THEN 
                            DATE '1960-01-01' + CAST(VALID_TO_DT AS INTEGER) * INTERVAL 1 DAY
                        ELSE NULL 
                    END AS VALID_TO_DT
                FROM loan_bill_hist
                WHERE CAST(VALID_TO_DT AS INTEGER) != {PDATE}
            """)
            
            inactive_count = hist_count - active_count
            print(f"  ✓ Created view for {inactive_count:,} INACTIVE records (estimated)")
            
            has_historical_data = True
            
        else:
            # Create empty structure for first run
            con.execute("""
                CREATE TABLE active_records AS
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
                    CAST(NULL AS DATE) AS VALID_TO_DT,
                    CAST(NULL AS VARCHAR) AS record_hash
                WHERE 1=0
            """)
            
            con.execute("""
                CREATE VIEW inactive_records AS
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
            print("  ✓ No historical data found - starting fresh")
        
        # Process each chunk with HASH-BASED SCD logic
        print("\n1.3: Processing chunks with optimized SCD logic...")
        final_chunks = []
        for i, chunk_file in enumerate(chunk_files):
            print(f"  Chunk {i+1}/{len(chunk_files)}:")
            
            # Process chunk with optimized hash-based SCD logic
            chunk_result = process_scd_chunk_hash_based(
                con, chunk_file, REPTDATE, PREVDATE, temp_path, f"chunk_{i}", 
                has_historical_data
            )
            final_chunks.append(chunk_result)
        
        # Combine all chunks
        print("\n1.4: Combining all chunks...")
        if final_chunks:
            union_chunks = " UNION ALL ".join([f"SELECT * FROM read_parquet('{f}')" for f in final_chunks])
            con.execute(f"""
                CREATE TABLE loan_bill_combined AS
                {union_chunks}
            """)
            
            combined_count = con.execute("SELECT COUNT(*) FROM loan_bill_combined").fetchone()[0]
            print(f"  ✓ Combined {combined_count:,} records from all chunks")
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
        print("\n1.5: Adding ENTITY_CD with optimized lookup...")
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
            print(f"  ✓ Added ENTITY_CD")
        else:
            con.execute("""
                CREATE TABLE with_entity AS
                SELECT *, CAST(NULL AS VARCHAR) AS ENTITY_CD
                FROM loan_bill_combined
            """)
        
        # Split and save results
        print("\n1.6: Saving final results...")
        ln_bill_path, iln_bill_path = save_final_results(con, output_dir, date_str)
        
        con.close()
        
        return ln_bill_path, iln_bill_path

def process_scd_chunk_hash_based(con, chunk_file, REPTDATE, PREVDATE, temp_path, chunk_id, has_historical_data):
    """Process SCD logic with HASH-BASED comparison - ultra fast!"""
    
    # Load chunk as new records with HASH KEY
    con.execute(f"""
        CREATE TEMP TABLE new_records AS
        SELECT 
            ACCTNO, NOTENO,
            CASE 
                WHEN BILL_DT IS NOT NULL THEN 
                    DATE '1960-01-01' + CAST(BILL_DT AS INTEGER) * INTERVAL 1 DAY
                ELSE NULL 
            END AS BILL_DT,
            CASE 
                WHEN BILL_PAID_DT IS NOT NULL AND CAST(BILL_PAID_DT AS INTEGER) > 0 THEN 
                    DATE '1960-01-01' + CAST(BILL_PAID_DT AS INTEGER) * INTERVAL 1 DAY
                ELSE NULL 
            END AS BILL_PAID_DT,
            BILL_AMT, BILL_AMT_PRIN, BILL_AMT_INT, BILL_AMT_ESCROW, BILL_AMT_FEE,
            BILL_NOT_PAY_AMT, BILL_NOT_PAY_AMT_PRIN, BILL_NOT_PAY_AMT_INT,
            BILL_NOT_PAY_AMT_ESCROW, BILL_NOT_PAY_AMT_FEE,
            COSTCTR, PRODUCT,
            MD5(
                CAST(ACCTNO AS VARCHAR) || '|' ||
                CAST(NOTENO AS VARCHAR) || '|' ||
                CAST(CASE 
                    WHEN BILL_DT IS NOT NULL THEN 
                        DATE '1960-01-01' + CAST(BILL_DT AS INTEGER) * INTERVAL 1 DAY
                    ELSE NULL 
                END AS VARCHAR) || '|' ||
                CAST(COALESCE(CASE 
                    WHEN BILL_PAID_DT IS NOT NULL AND CAST(BILL_PAID_DT AS INTEGER) > 0 THEN 
                        DATE '1960-01-01' + CAST(BILL_PAID_DT AS INTEGER) * INTERVAL 1 DAY
                    ELSE NULL 
                END, DATE '1900-01-01') AS VARCHAR) || '|' ||
                CAST(BILL_AMT AS VARCHAR) || '|' ||
                CAST(BILL_AMT_PRIN AS VARCHAR) || '|' ||
                CAST(BILL_AMT_INT AS VARCHAR) || '|' ||
                CAST(BILL_AMT_ESCROW AS VARCHAR) || '|' ||
                CAST(BILL_AMT_FEE AS VARCHAR) || '|' ||
                CAST(BILL_NOT_PAY_AMT AS VARCHAR) || '|' ||
                CAST(BILL_NOT_PAY_AMT_PRIN AS VARCHAR) || '|' ||
                CAST(BILL_NOT_PAY_AMT_INT AS VARCHAR) || '|' ||
                CAST(BILL_NOT_PAY_AMT_ESCROW AS VARCHAR) || '|' ||
                CAST(BILL_NOT_PAY_AMT_FEE AS VARCHAR) || '|' ||
                CAST(COSTCTR AS VARCHAR) || '|' ||
                CAST(PRODUCT AS VARCHAR)
            ) AS record_hash
        FROM read_parquet('{chunk_file}')
    """)
    
    # Index on hash for ultra-fast lookups
    con.execute("""
        CREATE INDEX idx_new_hash ON new_records(record_hash)
    """)
    
    new_count = con.execute("SELECT COUNT(*) FROM new_records").fetchone()[0]
    print(f"    Loaded {new_count:,} new records with hash keys")
    
    if has_historical_data:
        # OPTIMIZED SCD Logic - MATCH (compare HASH only!)
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
            FROM active_records T1
            INNER JOIN new_records T2 ON T1.record_hash = T2.record_hash
        """)
        
        match_count = con.execute("SELECT COUNT(*) FROM match_records").fetchone()[0]
        print(f"    MATCH: {match_count:,} records (unchanged)")
        
        # OPTIMIZED SCD Logic - CHGREC (active records with different hash)
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
                DATE '{PREVDATE.date()}' AS VALID_TO_DT
            FROM active_records T1
            LEFT JOIN new_records T2 ON T1.record_hash = T2.record_hash
            WHERE T2.record_hash IS NULL
        """)
        
        chgrec_count = con.execute("SELECT COUNT(*) FROM chgrec_records").fetchone()[0]
        print(f"    CHGREC: {chgrec_count:,} records (closed)")
        
        # OPTIMIZED SCD Logic - UPDATEX (new records with no matching hash)
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
            FROM active_records T1
            RIGHT JOIN new_records T2 ON T1.record_hash = T2.record_hash
            WHERE T1.record_hash IS NULL
        """)
        
        updatex_count = con.execute("SELECT COUNT(*) FROM updatex_records").fetchone()[0]
        print(f"    UPDATEX: {updatex_count:,} records (new/changed)")
        
        # Combine results (inactive_records is a VIEW - only materialized here)
        con.execute(f"""
            CREATE TEMP TABLE scd_result AS
            SELECT * FROM match_records
            UNION ALL
            SELECT * FROM updatex_records
            UNION ALL
            SELECT * FROM chgrec_records
            UNION ALL
            SELECT * FROM inactive_records
        """)
        
        # Cleanup
        con.execute("DROP TABLE match_records")
        con.execute("DROP TABLE chgrec_records")
        con.execute("DROP TABLE updatex_records")
        
    else:
        # No historical data - all records are new
        print("    No historical data - treating all as new...")
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
    
    result_count = con.execute("SELECT COUNT(*) FROM scd_result").fetchone()[0]
    print(f"    ✓ Generated {result_count:,} total records for this chunk")
    
    # Cleanup
    con.execute("DROP TABLE new_records")
    con.execute("DROP TABLE scd_result")
    
    return chunk_output

def save_final_results(con, output_dir, date_str):
    """Save the final LN_BILL and ILOAN_BILL files"""
    
    con.execute("""
        CREATE TEMP TABLE ln_bill_output AS
        SELECT 
            ACCTNO, NOTENO, BILL_DT, BILL_PAID_DT,
            BILL_AMT, BILL_AMT_PRIN, BILL_AMT_INT, BILL_AMT_ESCROW, BILL_AMT_FEE,
            BILL_NOT_PAY_AMT, BILL_NOT_PAY_AMT_PRIN, BILL_NOT_PAY_AMT_INT,
            BILL_NOT_PAY_AMT_ESCROW, BILL_NOT_PAY_AMT_FEE,
            COSTCTR, PRODUCT, VALID_FROM_DT, VALID_TO_DT
        FROM with_entity
        WHERE ENTITY_CD IS NULL OR ENTITY_CD = ''
    """)

    con.execute("""
        CREATE TEMP TABLE iln_bill_output AS
        SELECT 
            ACCTNO, NOTENO, BILL_DT, BILL_PAID_DT,
            BILL_AMT, BILL_AMT_PRIN, BILL_AMT_INT, BILL_AMT_ESCROW, BILL_AMT_FEE,
            BILL_NOT_PAY_AMT, BILL_NOT_PAY_AMT_PRIN, BILL_NOT_PAY_AMT_INT,
            BILL_NOT_PAY_AMT_ESCROW, BILL_NOT_PAY_AMT_FEE,
            COSTCTR, PRODUCT, VALID_FROM_DT, VALID_TO_DT
        FROM with_entity
        WHERE ENTITY_CD = 'PIBB'
    """)

    # Save without date suffix in filename
    ln_bill_path = output_dir / f"LOAN_BILL.parquet"
    iln_bill_path = output_dir / f"ILOAN_BILL.parquet"
    
    print(f"  Saving LOAN_BILL...")
    con.execute(f"COPY ln_bill_output TO '{ln_bill_path}' (FORMAT PARQUET)")
    
    print(f"  Saving ILOAN_BILL...")  
    con.execute(f"COPY iln_bill_output TO '{iln_bill_path}' (FORMAT PARQUET)")
    
    ln_count = con.execute("SELECT COUNT(*) FROM ln_bill_output").fetchone()[0]
    iln_count = con.execute("SELECT COUNT(*) FROM iln_bill_output").fetchone()[0]
    
    print(f"  ✓ LOAN_BILL: {ln_count:,} records")
    print(f"  ✓ ILOAN_BILL: {iln_count:,} records")
    
    return ln_bill_path, iln_bill_path

def process_hp_bill_extraction(
    loan_bill_path: Path,
    iloan_bill_path: Path,
    output_dir: Path,
    report_date: datetime
    ) -> None:
    """
    STEP 2: Extract HP_BILL and IHP_BILL from TODAY'S LOAN_BILL and ILOAN_BILL
    """

    print("\n" + "="*80)
    print("STEP 2: HP/IHP BILL EXTRACTION")
    print("="*80)
    
    # yymmdd format for file naming
    date_str = report_date.strftime("%y%m%d")
    
    hp_products_str = ','.join(map(str, HP_PRODUCTS))
    
    con = duckdb.connect(':memory:')
    
    # Extract HP_BILL from TODAY'S LOAN_BILL
    print("\n2.1: Extracting HP_BILL from LOAN_BILL...")
    
    if not loan_bill_path.exists():
        print(f"  ERROR: LOAN_BILL not found: {loan_bill_path}")
        return
    
    con.execute(f"""
        CREATE TABLE loan_bill AS
        SELECT * FROM read_parquet('{loan_bill_path}')
    """)
    
    con.execute(f"""
        CREATE TABLE hp_bill AS
        SELECT * FROM loan_bill
        WHERE PRODUCT IN ({hp_products_str}) OR PRODUCT = 392
    """)
    
    hp_count = con.execute("SELECT COUNT(*) FROM hp_bill").fetchone()[0]
    print(f"  Filtered {hp_count:,} HP records")
    
    # Save HP_BILL without date suffix
    hp_bill_path = output_dir / f"HP_BILL.parquet"
    con.execute(f"COPY hp_bill TO '{hp_bill_path}' (FORMAT PARQUET)")
    hp_size = hp_bill_path.stat().st_size / (1024 * 1024)
    print(f"  ✓ HP_BILL: {hp_bill_path.name}")
    print(f"    Records: {hp_count:,}, Size: {hp_size:.2f} MB")
    
    # Extract IHP_BILL from TODAY'S ILOAN_BILL
    print("\n2.2: Extracting IHP_BILL from ILOAN_BILL...")
    
    if not iloan_bill_path.exists():
        print(f"  ERROR: ILOAN_BILL not found: {iloan_bill_path}")
        con.close()
        return
    
    con.execute(f"""
        CREATE TABLE iloan_bill AS
        SELECT * FROM read_parquet('{iloan_bill_path}')
    """)
    
    con.execute(f"""
        CREATE TABLE ihp_bill AS
        SELECT * FROM iloan_bill
        WHERE PRODUCT IN ({hp_products_str})
    """)
    
    ihp_count = con.execute("SELECT COUNT(*) FROM ihp_bill").fetchone()[0]
    print(f"  Filtered {ihp_count:,} IHP records")
    
    # Save IHP_BILL without date suffix
    ihp_bill_path = output_dir / f"IHP_BILL.parquet"
    con.execute(f"COPY ihp_bill TO '{ihp_bill_path}' (FORMAT PARQUET)")
    ihp_size = ihp_bill_path.stat().st_size / (1024 * 1024)
    print(f"  ✓ IHP_BILL: {ihp_bill_path.name}")
    print(f"    Records: {ihp_count:,}, Size: {ihp_size:.2f} MB")
    
    con.close()

# SAS date calculations for processing logic
REPTDATE = datetime(2025, 11, 28)  # Set your report date
PREVDATE = REPTDATE - timedelta(days=1)
RDATE = (REPTDATE - SAS_ORIGIN).days
PDATE = (PREVDATE - SAS_ORIGIN).days

# yymmdd format for file naming
date_str = REPTDATE.strftime("%y%m%d")
prev_date_str = PREVDATE.strftime("%y%m%d")

print(f"Report Date: {REPTDATE.strftime('%Y-%m-%d')} (SAS: {RDATE}, File: {date_str})")
print(f"Previous Date: {PREVDATE.strftime('%Y-%m-%d')} (SAS: {PDATE}, File: {prev_date_str})")
print()

# Input paths - CHANGE THESE TO YOUR PATHS
input_enrh = Path("/parquet/dwh/LOAN/enrichment/ENRH_LN_BILL.parquet")

# Output directory
output_dir = Path(f"/parquet/dwh/LOAN/year={REPTDATE.year}/month={REPTDATE.month:02d}/day={REPTDATE.day:02d}")
output_dir.mkdir(parents=True, exist_ok=True)

# Previous Output directory
prev_dir = Path(f"/parquet/dwh/LOAN/year={PREVDATE.year}/month={PREVDATE.month:02d}/day={PREVDATE.day:02d}")


# STEP 1: Generate TODAY'S LOAN_BILL and ILOAN_BILL
loan_bill_path, iloan_bill_path = process_large_loan_bill_scd(
    input_enrh_path=input_enrh,
    output_dir=output_dir,
    prev_dir=prev_dir,
    report_date=REPTDATE,
    chunk_size=3_000_000
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
    print("PROCESSING COMPLETED SUCCESSFULLY")
    print("="*80)
    print(f"\nGenerated Files (Date: {date_str}):")
    print(f"  1. LOAN_BILL.parquet")
    print(f"  2. ILOAN_BILL.parquet") 
    print(f"  3. HP_BILL.parquet")
    print(f"  4. IHP_BILL.parquet")
    print("="*80)
