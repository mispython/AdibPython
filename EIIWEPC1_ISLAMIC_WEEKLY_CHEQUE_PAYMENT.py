# islamic_cheques_report.py

import duckdb
import polars as pl
from pathlib import Path
import datetime

def process_islamic_cheques_report():
    """
    EIIWEPC1 - Islamic Banking Cheques Issued Report
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
        # DATA REPTDATE Processing using Polars
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
        
        # Load DPLD data using Polars, then process with DuckDB
        print("Loading DPLD data...")
        dpl_file = dpld_path / f"DPLD{REPTMON}.parquet"
        if dpl_file.exists():
            dpld_df = pl.read_parquet(dpl_file)
            # Filter by date range and save
            dpld_filtered = dpld_df.filter(
                (pl.col('REPTDATE') >= PREVDATE) & (pl.col('REPTDATE') <= REPTDATE)
            )
            dpld_filtered.write_parquet(bnm_path / "ISLAMIC_DPLD.parquet")
            
            # Register with DuckDB for further processing
            conn.register("dpld_filtered", dpld_filtered)
        else:
            print("DPLD file not found")
            conn.execute("CREATE TEMP TABLE dpld_filtered AS SELECT 1 as dummy WHERE FALSE")
        
        # Load and process LNLD data using DuckDB directly
        print("Processing LNLD data...")
        conn.execute("""
            CREATE TEMP TABLE lnld_raw AS 
            SELECT line FROM read_text('LNLD.csv')
        """)
        
        # Parse fixed-width LNLD data
        conn.execute("""
            CREATE TEMP TABLE lnld_processed AS 
            SELECT 
                TRIM(SUBSTRING(line, 1, 11)) as ACCTNO,
                TRIM(SUBSTRING(line, 13, 5)) as NOTENO,
                CAST(TRIM(SUBSTRING(line, 19, 7)) AS INTEGER) as COSTCTR,
                TRIM(SUBSTRING(line, 27, 3)) as NOTETYPE,
                CASE 
                    WHEN LENGTH(TRIM(SUBSTRING(line, 31, 8))) = 8 
                    THEN strptime(TRIM(SUBSTRING(line, 31, 8)), '%d%m%y')
                    ELSE NULL 
                END as TRANDT,
                CAST(TRIM(SUBSTRING(line, 47, 3)) AS INTEGER) as TRANCODE,
                CAST(TRIM(SUBSTRING(line, 51, 3)) AS INTEGER) as SEQNO,
                CASE WHEN CAST(TRIM(SUBSTRING(line, 47, 3)) AS INTEGER) = 760 
                     THEN TRIM(SUBSTRING(line, 55, 2)) ELSE NULL END as FEEPLAN,
                CASE WHEN CAST(TRIM(SUBSTRING(line, 47, 3)) AS INTEGER) = 760 
                     THEN CAST(TRIM(SUBSTRING(line, 57, 3)) AS INTEGER) ELSE NULL END as FEENO,
                CAST(TRIM(SUBSTRING(line, 61, 18)) AS DECIMAL(16,2)) as TRANAMT,
                CAST(TRIM(SUBSTRING(line, 80, 3)) AS INTEGER) as SOURCE
            FROM lnld_raw
        """)
        
        # Apply Islamic branch filter
        conn.execute("""
            CREATE TEMP TABLE lnld_filtered AS 
            SELECT * FROM lnld_processed
            WHERE (COSTCTR BETWEEN 3000 AND 3999) OR COSTCTR IN (4043, 4048)
        """)
        
        # Save filtered LNLD data
        conn.execute(f"COPY lnld_filtered TO '{bnm_path}/ISLAMIC_LNLD.parquet' (FORMAT PARQUET)")
        
        # Merge LNLD and DPLD
        print("Merging transaction data...")
        conn.execute("""
            CREATE TEMP TABLE islamic_tranx AS 
            SELECT l.* 
            FROM lnld_filtered l
            INNER JOIN dpld_filtered d ON l.ACCTNO = d.ACCTNO AND l.TRANDT = d.TRANDT AND l.TRANAMT = d.TRANAMT
        """)
        
        conn.execute(f"COPY islamic_tranx TO '{bnm_path}/ISLAMIC_TRANX.parquet' (FORMAT PARQUET)")
        
        # Process transactions for reporting
        print("Generating Islamic banking reports...")
        conn.execute("""
            CREATE TEMP TABLE tranx_processed AS 
            SELECT 
                ACCTNO, NOTENO, COSTCTR, TRANDT, TRANCODE, FEEPLAN,
                TRANAMT / 1000 as TRANAMT1,
                1 as VALUE,
                CASE 
                    WHEN TRANCODE = 310 THEN 'FINANCING DISBURSEMENT'
                    WHEN TRANCODE = 750 THEN 'PRINCIPAL INCREASE (PROGRESSIVE FINANCING RELEASE)'
                    WHEN TRANCODE = 752 THEN 'DEBITING FOR TAKAFUL PREMIUM'
                    WHEN TRANCODE = 753 THEN 'DEBITING FOR LEGAL FEE'
                    WHEN TRANCODE = 754 THEN 'DEBITING FOR OTHER PAYMENTS'
                    WHEN TRANCODE = 760 THEN 
                        CASE FEEPLAN
                            WHEN 'QR' THEN 'QUIT RENT'
                            WHEN 'LF' THEN 'LEGAL FEE & DISBURSEMENT'
                            WHEN 'VA' THEN 'VALUATION FEE'
                            WHEN 'IP' THEN 'TAKAFUL PREMIUM'
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
            FROM islamic_tranx
        """)
        
        # Report 1: Summary
        print("\n" + "="*60)
        print("REPORT ID : EIIQEPC1")
        print("PUBLIC ISLAMIC BANK BERHAD")
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
        
        print(f"{'NO':>2} {'PURPOSE':<45} {'UNIT':>10} {'VALUE (RM''000)':>15}")
        print("-" * 75)
        for row in by_count:
            print(f"{row[0]:>2} {row[1]:<45} {row[2]:>10,d} {row[3]:>15.2f}")
        
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
        
        print(f"{'NO':>2} {'PURPOSE':<45} {'UNIT':>10} {'VALUE (RM''000)':>15}")
        print("-" * 75)
        for row in by_value:
            print(f"{row[0]:>2} {row[1]:<45} {row[2]:>10,d} {row[3]:>15.2f}")
        
        # Report 4: By Islamic branch
        print(f"\nALL PAYMENTS BY ISLAMIC BRANCH AS AT {RDATE}")
        print("-" * 60)
        
        by_branch = conn.execute("""
            SELECT 
                'IB' || CAST(COSTCTR AS VARCHAR) as islamic_branch,
                TRNXDESC,
                COUNT(*) as unit,
                SUM(TRANAMT1) as value_000
            FROM tranx_processed
            GROUP BY COSTCTR, TRNXDESC
            ORDER BY COSTCTR, TRNXDESC
        """).fetchall()
        
        print(f"{'BRANCH':>8} {'PURPOSE':<40} {'UNIT':>10} {'VALUE (RM''000)':>15}")
        print("-" * 78)
        for row in by_branch:
            print(f"{row[0]:>8} {row[1]:<40} {row[2]:>10,d} {row[3]:>15.2f}")
        
        # Generate summary statistics
        stats = conn.execute("""
            SELECT 
                COUNT(*) as total_cheques,
                SUM(TRANAMT1) as total_value,
                COUNT(DISTINCT COSTCTR) as islamic_branches,
                COUNT(DISTINCT TRNXDESC) as transaction_types
            FROM tranx_processed
        """).fetchone()
        
        print(f"\nISLAMIC REPORT SUMMARY:")
        print(f"  Total Cheques: {stats[0]:,}")
        print(f"  Total Value: RM {stats[1]:,.2f} thousand")
        print(f"  Islamic Branches: {stats[2]}")
        print(f"  Transaction Types: {stats[3]}")
        
        # Save results to CSV
        conn.execute(f"COPY tranx_processed TO '{output_path}/islamic_cheques_report.csv' (FORMAT CSV, HEADER true)")
        print(f"\nResults saved to: {output_path}/islamic_cheques_report.csv")
        
        print("\nIslamic Cheques Report completed successfully")
        
        return stats[0]  # Return count of processed transactions
        
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    process_islamic_cheques_report()
