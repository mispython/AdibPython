# islamic_cheques_report.py

import duckdb
import polars as pl
from pathlib import Path
import datetime

def process_islamic_cheques_report():
    """
    EIIWEPC1 - Islamic Banking Cheques Issued Report
    Uses Polars for efficient data loading and DuckDB for SQL processing
    """
    
    print("Processing Islamic Cheques Report - EIIWEPC1")
    
    # Configuration
    loan_path = Path("LOAN")
    bnm_path = Path("BNM")
    dpld_path = Path("DPLD")
    output_path = Path("output")
    output_path.mkdir(exist_ok=True)
    bnm_path.mkdir(exist_ok=True)
    
    # Initialize DuckDB
    conn = duckdb.connect()
    
    try:
        # =========================================================================
        # DATA REPTDATE Processing with Polars
        # =========================================================================
        print("Loading report date...")
        reptdate_df = pl.read_parquet(loan_path / "REPTDATE.parquet")
        
        # Extract date parameters
        first_row = reptdate_df.row(0)
        REPTDATE = first_row['REPTDATE']
        day = REPTDATE.day
        month = REPTDATE.month
        year = REPTDATE.year
        
        # Calculate PREVDATE based on day
        if day == 8:
            PREVDATE = datetime.date(year, month, 1)
        elif day == 15:
            PREVDATE = datetime.date(year, month, 9)
        elif day == 22:
            PREVDATE = datetime.date(year, month, 16)
        else:
            PREVDATE = datetime.date(year, month, 23)
        
        REPTYEAR = str(year)
        REPTMON = f"{month:02d}"
        REPTDAY = f"{day:02d}"
        RDATE = REPTDATE.strftime('%d/%m/%Y')
        
        print(f"Report Date: {RDATE}, Period: {PREVDATE} to {REPTDATE}")
        
        # =========================================================================
        # Load DPLD data with Polars
        # =========================================================================
        print("Loading DPLD data...")
        dpl_file = dpld_path / f"DPLD{REPTMON}.parquet"
        if dpl_file.exists():
            # Use Polars for efficient filtering
            dpld_df = pl.read_parquet(dpl_file)
            dpld_filtered = dpld_df.filter(
                (pl.col('REPTDATE') >= PREVDATE) & (pl.col('REPTDATE') <= REPTDATE)
            )
            dpld_filtered.write_parquet(bnm_path / "DPLD.parquet")
            
            # Register with DuckDB for SQL processing
            conn.register("dpld_filtered", dpld_filtered)
            print(f"   Loaded {dpld_filtered.height} DPLD records")
        else:
            print("   DPLD file not found")
            conn.execute("CREATE TEMP TABLE dpld_filtered AS SELECT 1 as dummy WHERE FALSE")
        
        # =========================================================================
        # Load LNLD data with Polars (more efficient than DuckDB for text parsing)
        # =========================================================================
        print("Loading LNLD data...")
        try:
            # Use Polars for fixed-width text parsing
            lnld_df = pl.read_csv("LNLD.csv", has_header=False, new_columns=["line"])
            
            # Parse fixed-width format using Polars string operations
            lnld_processed = lnld_df.with_columns([
                pl.col("line").str.slice(0, 11).str.strip_chars().alias("ACCTNO"),
                pl.col("line").str.slice(12, 5).str.strip_chars().alias("NOTENO"),
                pl.col("line").str.slice(18, 7).str.strip_chars().cast(pl.Int64).alias("COSTCTR"),
                pl.col("line").str.slice(26, 3).str.strip_chars().alias("NOTETYPE"),
                pl.col("line").str.slice(30, 8).str.strip_chars().alias("TRANDT_STR"),
                pl.col("line").str.slice(46, 3).str.strip_chars().cast(pl.Int64).alias("TRANCODE"),
                pl.col("line").str.slice(50, 3).str.strip_chars().cast(pl.Int64).alias("SEQNO"),
                pl.when(pl.col("line").str.slice(46, 3).str.strip_chars().cast(pl.Int64) == 760)
                .then(pl.col("line").str.slice(54, 2).str.strip_chars())
                .otherwise(None).alias("FEEPLAN"),
                pl.when(pl.col("line").str.slice(46, 3).str.strip_chars().cast(pl.Int64) == 760)
                .then(pl.col("line").str.slice(56, 3).str.strip_chars().cast(pl.Int64))
                .otherwise(None).alias("FEENO"),
                pl.col("line").str.slice(60, 18).str.strip_chars().cast(pl.Float64).alias("TRANAMT"),
                pl.col("line").str.slice(79, 3).str.strip_chars().cast(pl.Int64).alias("SOURCE")
            ]).with_columns([
                pl.when(pl.col("TRANDT_STR").str.len_chars() == 8)
                .then(pl.col("TRANDT_STR").str.strptime(pl.Date, format="%d%m%y"))
                .otherwise(None).alias("TRANDT")
            ]).drop(["line", "TRANDT_STR"])
            
            # Apply Islamic branch filter
            lnld_filtered = lnld_processed.filter(
                ((pl.col('COSTCTR') < 3000) | (pl.col('COSTCTR') > 3999)) & 
                (~pl.col('COSTCTR').is_in([4043, 4048]))
            )
            
            lnld_filtered.write_parquet(bnm_path / "LNLD.parquet")
            conn.register("lnld_filtered", lnld_filtered)
            print(f"   Processed {lnld_filtered.height} LNLD records")
            
        except FileNotFoundError:
            print("   LNLD.csv not found")
            conn.execute("CREATE TEMP TABLE lnld_filtered AS SELECT 1 as dummy WHERE FALSE")
        
        # =========================================================================
        # Use DuckDB for SQL-based data merging and processing
        # =========================================================================
        print("Merging and processing data with DuckDB...")
        
        # Merge LNLD and DPLD
        conn.execute("""
            CREATE TEMP TABLE tranx AS 
            SELECT l.* 
            FROM lnld_filtered l
            INNER JOIN dpld_filtered d ON l.ACCTNO = d.ACCTNO AND l.TRANDT = d.TRANDT AND l.TRANAMT = d.TRANAMT
        """)
        
        conn.execute(f"COPY tranx TO '{bnm_path}/TRANX.parquet' (FORMAT PARQUET)")
        
        # Process transactions for reporting
        conn.execute("""
            CREATE TEMP TABLE tranx_processed AS 
            SELECT 
                ACCTNO, NOTENO, COSTCTR, TRANDT, TRANCODE, FEEPLAN,
                TRANAMT / 1000 as TRANAMT1,
                1 as VALUE,
                CASE 
                    WHEN TRANCODE = 310 THEN 'LOAN DISBURSEMENT'
                    WHEN TRANCODE = 750 THEN 'PRINCIPAL INCREASE (PROGRESSIVE LOAN RELEASE)'
                    WHEN TRANCODE = 752 THEN 'DEBITING FOR INSURANCE PREMIUM'
                    WHEN TRANCODE = 753 THEN 'DEBITING FOR LEGAL FEE'
                    WHEN TRANCODE = 754 THEN 'DEBITING FOR OTHER PAYMENTS'
                    WHEN TRANCODE = 760 THEN 
                        CASE FEEPLAN
                            WHEN 'QR' THEN 'QUIT RENT'
                            WHEN 'LF' THEN 'LEGAL FEE & DISBURSEMENT'
                            WHEN 'VA' THEN 'VALUATION FEE'
                            WHEN 'IP' THEN 'INSURANCE PREMIUM'
                            WHEN 'PA' THEN 'PROFESSIONAL/OTHERS'
                            WHEN 'AC' THEN 'ADVERTISEMENT FEE'
                            WHEN 'MC' THEN 'MAINTENANCE CHARGES'
                            WHEN 'RE' THEN 'REPOSSESION CHARGES'
                            WHEN 'RI' THEN 'REPAIR CHARGES'
                            WHEN 'SC' THEN 'STORAGE CHARGES'
                            WHEN 'SF' THEN 'SEARCH FEE'
                            WHEN 'TC' THEN 'TOWING CHARGES'
                            WHEN '99' THEN 'MISCHELLANEOUS EXPENSES'
                            ELSE 'MANUAL FEE ASSESSMENT FOR PAYMENT TO 3RD PARTY'
                        END
                    ELSE 'MANUAL FEE ASSESSMENT FOR PAYMENT TO 3RD PARTY'
                END as TRNXDESC
            FROM tranx
        """)
        
        # =========================================================================
        # Generate Reports
        # =========================================================================
        
        # Report 1: Summary
        print("\n" + "="*60)
        print("REPORT ID : EIBQEPC1")
        print("PUBLIC BANK BERHAD")
        print(f"CHEQUES ISSUED BY THE BANK AS AT {RDATE}")
        print("="*60)
        
        summary = conn.execute("""
            SELECT 
                COUNT(*) as number_of_cheques,
                SUM(TRANAMT1) as value_000
            FROM tranx_processed
        """).fetchone()
        
        print(f"Number of Cheques: {summary[0]:>16,d}")
        print(f"Value of Cheques (RM'000): {summary[1]:>16.2f}")
        
        # Report 2: By number of cheques
        print(f"\nALL PAYMENTS BY NUMBER OF CHEQUES AS AT {RDATE}")
        print("-" * 60)
        
        by_count = conn.execute("""
            WITH ranked AS (
                SELECT 
                    TRNXDESC,
                    COUNT(*) as unit,
                    SUM(TRANAMT1) as value_000,
                    ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as count
                FROM tranx_processed
                GROUP BY TRNXDESC
            )
            SELECT count, TRNXDESC, unit, value_000
            FROM ranked
            ORDER BY count
        """).fetchall()
        
        print(f"{'NO':>2} {'PURPOSE':<40} {'UNIT':>10} {'VALUE (RM''000)':>15}")
        print("-" * 70)
        for row in by_count:
            print(f"{row[0]:>2} {row[1]:<40} {row[2]:>10,d} {row[3]:>15.2f}")
        
        # Report 3: By value of cheques
        print(f"\nALL PAYMENTS BY VALUE OF CHEQUES AS AT {RDATE}")
        print("-" * 60)
        
        by_value = conn.execute("""
            WITH ranked AS (
                SELECT 
                    TRNXDESC,
                    COUNT(*) as unit,
                    SUM(TRANAMT1) as value_000,
                    ROW_NUMBER() OVER (ORDER BY SUM(TRANAMT1) DESC) as count
                FROM tranx_processed
                GROUP BY TRNXDESC
            )
            SELECT count, TRNXDESC, unit, value_000
            FROM ranked
            ORDER BY count
        """).fetchall()
        
        print(f"{'NO':>2} {'PURPOSE':<40} {'UNIT':>10} {'VALUE (RM''000)':>15}")
        print("-" * 70)
        for row in by_value:
            print(f"{row[0]:>2} {row[1]:<40} {row[2]:>10,d} {row[3]:>15.2f}")
        
        # Report 4: By branch
        print(f"\nALL PAYMENTS BY BRANCH AS AT {RDATE}")
        print("-" * 60)
        
        by_branch = conn.execute("""
            SELECT 
                COSTCTR,
                TRNXDESC,
                COUNT(*) as unit,
                SUM(TRANAMT1) as value_000
            FROM tranx_processed
            GROUP BY COSTCTR, TRNXDESC
            ORDER BY COSTCTR, TRNXDESC
        """).fetchall()
        
        print(f"{'BRANCH':>6} {'PURPOSE':<40} {'UNIT':>10} {'VALUE (RM''000)':>15}")
        print("-" * 75)
        for row in by_branch:
            print(f"{row[0]:>6} {row[1]:<40} {row[2]:>10,d} {row[3]:>15.2f}")
        
        # Save results
        conn.execute(f"COPY tranx_processed TO '{output_path}/cheques_report.csv' (FORMAT CSV, HEADER true)")
        print(f"\nResults saved to: {output_path}/cheques_report.csv")
        
        print("\nProcessing completed successfully")
        
        return summary[0]  # Return count of processed transactions
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    process_islamic_cheques_report()
