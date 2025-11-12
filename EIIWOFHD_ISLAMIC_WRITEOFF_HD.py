# islamic_writeoff_hd.py

import duckdb
from pathlib import Path
import datetime

def process_islamic_hd_writeoff():
    """
    EIIWOFHD - Islamic Banking HD Write-off Upload
    Simple 1:1 conversion from SAS to Python/DuckDB
    """
    
    print("Processing EIIWOFHD - Islamic HD Write-off")
    
    # Configuration
    npl_path = Path("NPL")
    npl_path.mkdir(exist_ok=True)
    
    # Initialize DuckDB
    conn = duckdb.connect()
    
    try:
        # *** UPLOAD FROM TEXT FILE PROVIDE BY CCD ***
        # DATA HD
        print("Loading WOFF file...")
        
        # Read fixed-width text file
        conn.execute(f"""
            CREATE TEMPORARY TABLE hd_raw AS 
            SELECT 
                TRIM(SUBSTRING(line, 142, 10)) as ACCTNO,
                TRIM(SUBSTRING(line, 210, 10)) as WOFFDTE_STR,
                CAST(TRIM(SUBSTRING(line, 220, 16)) AS DECIMAL(16,2)) as CAPBAL,
                CAST(TRIM(SUBSTRING(line, 236, 4)) AS INTEGER) as COSTCTR
            FROM read_text('WOFF.txt')
            WHERE LENGTH(line) >= 240
        """)
        
        # Apply transformations and filter
        conn.execute("""
            CREATE TEMPORARY TABLE hd AS 
            SELECT 
                ACCTNO,
                -- WOFFDT = UPCASE(PUT(WOFFDTE,MONNAME3.))||' '||PUT(WOFFDTE,YEAR4.)
                UPPER(
                    CASE SUBSTRING(WOFFDTE_STR, 4, 2)
                        WHEN '01' THEN 'JAN' WHEN '02' THEN 'FEB' WHEN '03' THEN 'MAR'
                        WHEN '04' THEN 'APR' WHEN '05' THEN 'MAY' WHEN '06' THEN 'JUN' 
                        WHEN '07' THEN 'JUL' WHEN '08' THEN 'AUG' WHEN '09' THEN 'SEP'
                        WHEN '10' THEN 'OCT' WHEN '11' THEN 'NOV' WHEN '12' THEN 'DEC'
                        ELSE 'JAN'
                    END
                ) || ' ' || SUBSTRING(WOFFDTE_STR, 7, 4) as WOFFDT,
                CAPBAL
            FROM hd_raw
            WHERE COSTCTR BETWEEN 3000 AND 3999 OR COSTCTR IN (4043, 4048)
        """)
        
        # PROC SORT; BY ACCTNO;
        conn.execute("CREATE TEMPORARY TABLE hd_sorted AS SELECT * FROM hd ORDER BY ACCTNO")
        
        # PROC PRINT; SUM CAPBAL;
        print("\nHD Write-off Data:")
        result = conn.execute("SELECT * FROM hd_sorted").fetchall()
        for row in result:
            print(f"  {row[0]} | {row[1]} | {row[2]:,.2f}")
        
        total = conn.execute("SELECT SUM(CAPBAL) FROM hd_sorted").fetchone()[0]
        print(f"  SUM: {total:,.2f}")
        
        # DATA NPL.HPWOFF; SET HD NPL.HPWOFF;
        # Check if existing file exists
        hpwoff_file = npl_path / "HPWOFF.parquet"
        if hpwoff_file.exists():
            conn.execute(f"CREATE TEMPORARY TABLE existing AS SELECT * FROM read_parquet('{hpwoff_file}')")
            # Combine data
            conn.execute("CREATE TEMPORARY TABLE combined AS SELECT * FROM hd_sorted UNION ALL SELECT * FROM existing")
        else:
            conn.execute("CREATE TEMPORARY TABLE combined AS SELECT * FROM hd_sorted")
        
        # PROC SORT NODUPKEYS; BY ACCTNO;
        conn.execute("""
            CREATE TEMPORARY TABLE final AS 
            SELECT ACCTNO, WOFFDT, CAPBAL
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY ACCTNO ORDER BY WOFFDT DESC) as rn
                FROM combined
            ) WHERE rn = 1
            ORDER BY ACCTNO
        """)
        
        # Save final dataset
        conn.execute(f"COPY final TO '{hpwoff_file}' (FORMAT PARQUET)")
        
        # Also save as CSV
        csv_file = Path("output/hpwoff.csv")
        csv_file.parent.mkdir(exist_ok=True)
        conn.execute(f"COPY final TO '{csv_file}' (FORMAT CSV, HEADER true)")
        
        count = conn.execute("SELECT COUNT(*) FROM final").fetchone()[0]
        print(f"\nCompleted: {count} records saved to {hpwoff_file} and {csv_file}")
        
    finally:
        conn.close()

if __name__ == "__main__":
    process_islamic_hd_writeoff()
