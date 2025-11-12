import duckdb
import polars as pl
from pathlib import Path
import datetime
import sys

def process_eibd_depf():
    """
    EIBDDEPF - Foreign Currency Deposit Processing
    """
    
    print("Processing EIBDDEPF - Foreign Currency Deposits")
    
    # Configuration
    mnitb_path = Path("MNITB")
    imnitb_path = Path("IMNITB")
    walk_path = Path("WALK")
    mis_path = Path("SAP/PBB/MIS")
    output_path = Path("output")
    output_path.mkdir(exist_ok=True)

    # Initialize DuckDB
    conn = duckdb.connect()
    
    try:
        # =========================================================================
        # DATA REPTDATE Processing with Polars
        # =========================================================================
        print("Loading report date...")
        reptdate_df = pl.read_parquet(mnitb_path / "REPTDATE.parquet")
        first_reptdate = reptdate_df.row(0)['REPTDATE']

        # Extract date parameters
        REPTYY = first_reptdate.strftime('%y')
        REPTYEAR = first_reptdate.strftime('%Y')
        REPTMON = f"{first_reptdate.month:02d}"
        REPTDAY = f"{first_reptdate.day:02d}"
        RDATE = first_reptdate.strftime('%d%m%y')
        XDATE = first_reptdate.strftime('%d%m%y')

        print(f"REPTYY: {REPTYY}, REPTYEAR: {REPTYEAR}, REPTMON: {REPTMON}")
        print(f"REPTDAY: {REPTDAY}, RDATE: {RDATE}")

        # DATA TDATE;
        TDATE = datetime.date.today() - datetime.timedelta(days=1)
        TDATE_STR = TDATE.strftime('%d%m%y')
        print(f"TDATE: {TDATE_STR}")

        # Create MIS directory
        mis_year_path = mis_path / f"D{REPTYEAR}"
        mis_year_path.mkdir(parents=True, exist_ok=True)

        # =========================================================================
        # Load data with Polars, process with DuckDB
        # =========================================================================
        
        # Load CURRENT datasets with Polars
        print("Loading CURRENT data...")
        current_df = pl.read_parquet(mnitb_path / "CURRENT.parquet")
        icurrent_df = pl.read_parquet(imnitb_path / "CURRENT.parquet").rename({"CURBAL": "ICURBAL"})

        # Register with DuckDB
        conn.register("current_df", current_df)
        conn.register("icurrent_df", icurrent_df)

        # Process CAFY data with DuckDB SQL
        print("Processing CAFY data...")
        conn.execute("""
            CREATE TEMP TABLE cafy_processed AS
            SELECT 
                CUSTCODE,
                CURCODE,
                PRODUCT,
                CURBAL,
                ICURBAL,
                CASE 
                    WHEN CUSTCODE IN (77,78,95,96) THEN '77'
                    WHEN CUSTCODE IN (2,3,7,10,12,81,82,83,84) THEN '02'
                    ELSE '01'
                END as CUSTCD,
                CASE 
                    WHEN (CUSTCODE IN (2,3,7,10,12,81,82,83,84) OR PRODUCT = 413) 
                    THEN CURBAL ELSE 0.0 
                END as FCAFIC,
                CASE 
                    WHEN (CUSTCODE IN (2,3,7,10,12,81,82,83,84) OR PRODUCT = 413) 
                    THEN ICURBAL ELSE 0.0 
                END as FCAFII,
                CASE 
                    WHEN (CUSTCODE NOT IN (2,3,7,10,12,81,82,83,84) AND PRODUCT != 413) 
                    THEN CURBAL ELSE 0.0 
                END as CURBAL1,
                CASE 
                    WHEN (CUSTCODE NOT IN (2,3,7,10,12,81,82,83,84) AND PRODUCT != 413) 
                    THEN ICURBAL ELSE 0.0 
                END as ICURBAL1,
                CASE 
                    WHEN (PRODUCT != 413 AND CUSTCODE IN (77,78,95,96)) 
                    THEN CURBAL ELSE 0.0 
                END as FCAIDC,
                CASE 
                    WHEN (PRODUCT != 413 AND CUSTCODE IN (77,78,95,96)) 
                    THEN ICURBAL ELSE 0.0 
                END as FCAIDI,
                CAST(? AS INTEGER) as REPTDATE
            FROM (
                SELECT * FROM current_df
                UNION ALL
                SELECT * FROM icurrent_df
            )
            WHERE CURCODE != 'MYR' 
            AND (PRODUCT BETWEEN 400 AND 411 
                OR PRODUCT = 413
                OR PRODUCT BETWEEN 420 AND 434
                OR PRODUCT BETWEEN 440 AND 444
                OR PRODUCT BETWEEN 450 AND 454)
        """, [RDATE])

        # PROC SUMMARY equivalent with DuckDB
        conn.execute("""
            CREATE TEMP TABLE cafy_summary AS
            SELECT 
                REPTDATE,
                SUM(CURBAL1) as TOTCAFY,
                SUM(ICURBAL1) as TOTCAFYI,
                SUM(FCAIDC) as TOFCAIDC,
                SUM(FCAIDI) as TOFCAIDI,
                SUM(FCAFIC) as TOFCAFIC,
                SUM(FCAFII) as TOFCAFII
            FROM cafy_processed
            GROUP BY REPTDATE
        """)

        # Print CAFY summary
        cafy_result = conn.execute("SELECT * FROM cafy_summary").fetchall()
        print("CAFY Summary:")
        for row in cafy_result:
            print(f"  REPTDATE: {row[0]}, TOTCAFY: {row[1]:.2f}, TOTCAFYI: {row[2]:.2f}")

        # =========================================================================
        # Process FD data
        # =========================================================================
        print("Processing FD data...")
        
        # Load FD data with Polars
        fdfy_df = pl.read_parquet(mnitb_path / "FD.parquet")
        conn.register("fdfy_df", fdfy_df)
        
        # Define FCY values (replace with actual values)
        FCY_VALUES = "(400,401,402,403)"  # Example values

        # Process FDFY with DuckDB
        conn.execute(f"""
            CREATE TEMP TABLE fdfy_processed AS
            SELECT 
                CUSTCODE,
                CURBAL,
                CASE 
                    WHEN CUSTCODE IN (2,3,7,10,12,81,82,83,84) THEN CURBAL 
                    ELSE 0.0 
                END as FFDFIC,
                CASE 
                    WHEN CUSTCODE NOT IN (2,3,7,10,12,81,82,83,84) THEN CURBAL 
                    ELSE 0.0 
                END as CURBAL1,
                CASE 
                    WHEN CUSTCODE IN (77,78,95,96) THEN CURBAL 
                    ELSE 0.0 
                END as FFDIDC,
                CAST(? AS INTEGER) as REPTDATE
            FROM fdfy_df
            WHERE PRODUCT IN {FCY_VALUES}
        """, [RDATE])

        # Process FCFY data if available
        try:
            fcfy_file = walk_path / f"WK{REPTYY}{REPTMON}{REPTDAY}.parquet"
            if fcfy_file.exists():
                fcfy_df = pl.read_parquet(fcfy_file)
                conn.register("fcfy_df", fcfy_df)
                
                conn.execute("""
                    CREATE TEMP TABLE fcfy_processed AS
                    SELECT 
                        (-1 * CURBAL) as FCURBAL,
                        CAST(? AS INTEGER) as REPTDATE
                    FROM fcfy_df
                    WHERE PROD = 'DCM22110'
                """, [RDATE])
            else:
                conn.execute("CREATE TEMP TABLE fcfy_processed AS SELECT 0.0 as FCURBAL, 0 as REPTDATE WHERE FALSE")
        except Exception as e:
            print(f"NOTE: FCFY data not available: {e}")
            conn.execute("CREATE TEMP TABLE fcfy_processed AS SELECT 0.0 as FCURBAL, 0 as REPTDATE WHERE FALSE")

        # Combine FDFY and FCFY summaries
        conn.execute("""
            CREATE TEMP TABLE fdfy_summary AS
            SELECT 
                REPTDATE,
                SUM(CURBAL1) as TOTFDFY,
                SUM(FCURBAL) as TOTFCFY,
                SUM(FFDFIC) as TOFFDFIC,
                SUM(FFDIDC) as TOFFDIDC
            FROM (
                SELECT REPTDATE, CURBAL1, 0.0 as FCURBAL, FFDFIC, FFDIDC FROM fdfy_processed
                UNION ALL
                SELECT REPTDATE, 0.0 as CURBAL1, FCURBAL, 0.0 as FFDFIC, 0.0 as FFDIDC FROM fcfy_processed
            )
            GROUP BY REPTDATE
        """)

        # Print FDFY summary
        fdfy_result = conn.execute("SELECT * FROM fdfy_summary").fetchall()
        print("FDFY Summary:")
        for row in fdfy_result:
            print(f"  REPTDATE: {row[0]}, TOTFDFY: {row[1]:.2f}, TOTFCFY: {row[2]:.2f}")

        # =========================================================================
        # Merge CAFY and FDFY data
        # =========================================================================
        print("Merging data...")
        conn.execute("""
            CREATE TEMP TABLE dyposn AS
            SELECT 
                COALESCE(c.REPTDATE, f.REPTDATE) as REPTDATE,
                COALESCE(c.TOTCAFY, 0) as TOTCAFY,
                COALESCE(c.TOTCAFYI, 0) as TOTCAFYI,
                COALESCE(c.TOFCAIDC, 0) as TOFCAIDC,
                COALESCE(c.TOFCAIDI, 0) as TOFCAIDI,
                COALESCE(c.TOFCAFIC, 0) as TOFCAFIC,
                COALESCE(c.TOFCAFII, 0) as TOFCAFII,
                COALESCE(f.TOTFDFY, 0) as TOTFDFY,
                COALESCE(f.TOTFCFY, 0) as TOTFCFY,
                COALESCE(f.TOFFDFIC, 0) as TOFFDFIC,
                COALESCE(f.TOFFDIDC, 0) as TOFFDIDC
            FROM cafy_summary c
            FULL OUTER JOIN fdfy_summary f ON c.REPTDATE = f.REPTDATE
        """)

        # =========================================================================
        # MACRO %PROCESS equivalent
        # =========================================================================
        if TDATE_STR != RDATE:
            print("DATA DATE IS NOT YESTERDAY- PLS RERUN LATER")
            sys.exit(77)
        else:
            output_file = mis_year_path / f"DYFCY{REPTMON}.parquet"
            
            # Apply final calculations
            conn.execute("""
                CREATE TEMP TABLE final_data AS
                SELECT 
                    REPTDATE,
                    ROUND(TOTFDFY, 0) as TOTFDFY,
                    ROUND(TOTFCFY, 0) as TOTFCFY,
                    ROUND(TOTFDFY + TOTFCFY, 0) as TOTFYFD,
                    ROUND(TOFFDFIC, 0) as TOFFDFIC,
                    ROUND(TOTCAFY, 0) as TOTCAFY,
                    ROUND(TOTCAFYI, 0) as TOTCAFYI,
                    ROUND(TOTCAFY + TOTCAFYI, 0) as TOTFYCA,
                    ROUND((TOTFDFY + TOTFCFY) + (TOTCAFY + TOTCAFYI), 0) as TOTFCY,
                    ROUND(TOFFDIDC, 0) as TOFFDIDC,
                    ROUND((TOTFDFY + TOTFCFY) - TOFFDIDC, 0) as TOFFDNDC,
                    ROUND(TOFCAFIC, 0) as TOFCAFIC,
                    ROUND(TOFCAFII, 0) as TOFCAFII,
                    ROUND(TOFCAFIC + TOFCAFII, 0) as TOTFCAFI,
                    ROUND(TOFFDFIC + (TOFCAFIC + TOFCAFII), 0) as TOTFCYFI,
                    ROUND(TOFCAIDC, 0) as TOFCAIDC,
                    ROUND(TOTCAFY - TOFCAIDC, 0) as TOFCANDC,
                    ROUND(TOFCAIDI, 0) as TOFCAIDI,
                    ROUND(TOTCAFYI - TOFCAIDI, 0) as TOFCANDI,
                    ROUND(TOFCAIDC + TOFCAIDI, 0) as TOTFCAID,
                    ROUND((TOTCAFY - TOFCAIDC) + (TOTCAFYI - TOFCAIDI), 0) as TOTFCAND
                FROM dyposn
            """)
            
            # Handle file append/create logic
            if REPTDAY == "01":
                # First day of month - create new file
                conn.execute(f"COPY final_data TO '{output_file}' (FORMAT PARQUET)")
                print(f"Created new file: {output_file}")
            else:
                # Append to existing file
                if output_file.exists():
                    # Read existing data and merge
                    existing_df = pl.read_parquet(output_file)
                    conn.register("existing_data", existing_df)
                    
                    conn.execute(f"""
                        CREATE TEMP TABLE combined_data AS
                        SELECT * FROM final_data
                        UNION ALL
                        SELECT * FROM existing_data
                    """)
                    
                    # Remove duplicates
                    conn.execute("""
                        CREATE TEMP TABLE deduplicated AS
                        SELECT DISTINCT * FROM combined_data
                    """)
                    
                    conn.execute(f"COPY deduplicated TO '{output_file}' (FORMAT PARQUET)")
                    print(f"Appended to existing file: {output_file}")
                else:
                    conn.execute(f"COPY final_data TO '{output_file}' (FORMAT PARQUET)")
                    print(f"Created new file (existing not found): {output_file}")
            
            # Save to CSV as well
            csv_file = output_path / f"dyfcy_{REPTMON}.csv"
            conn.execute(f"COPY final_data TO '{csv_file}' (FORMAT CSV, HEADER true)")
            print(f"CSV output saved to: {csv_file}")
            
            # Print final summary
            final_result = conn.execute("""
                SELECT 
                    COUNT(*) as records,
                    SUM(TOTFCY) as total_fcy
                FROM final_data
            """).fetchone()
            
            print(f"Final summary: {final_result[0]} records, Total FCY: {final_result[1]:.2f}")
            
            # Call DMMISR1F if available
            try:
                import DMMISR1F
                DMMISR1F.process()
            except ImportError:
                print("NOTE: DMMISR1F.py not found")

        print("EIBDDEPF PROCESSING COMPLETED SUCCESSFULLY")
        
    except Exception as e:
        print(f"ERROR in EIBDDEPF processing: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    process_eibd_depf()
