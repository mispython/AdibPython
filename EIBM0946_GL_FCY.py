import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime

def process_eibm0946(
    fcy_pbb_path: Path,
    fcy_pib_path: Path,
    output_path: Path,
    report_date: datetime
) -> None:
    """
    EIBM0946 - FCY (Foreign Currency) Summary to GL Interface.
    
    Steps:
    1. Read FCY data from PBB and PIBB
    2. Filter and classify by account ranges
    3. Summarize by AGLFLD, BRANCH, PRODUCT
    4. Generate GL interface file
    """
    
    print("="*80)
    print("EIBM0946 - FCY GL Interface Generation")
    print("="*80)
    print(f"Report Date: {report_date.date()}")
    
    # Calculate report date components
    day = report_date.day
    month = report_date.month
    year = report_date.year
    
    # Determine week
    if 1 <= day <= 8:
        wk = '1'
    elif 9 <= day <= 15:
        wk = '2'
    elif 16 <= day <= 22:
        wk = '3'
    else:
        wk = '4'
    
    print(f"Week: {wk}, Month: {month:02d}, Day: {day:02d}, Year: {year}")
    print("-" * 80)
    
    con = duckdb.connect(':memory:')
    
    # Step 1: Read FCY data
    print("\nStep 1: Reading FCY data...")
    
    if not fcy_pbb_path.exists() or not fcy_pib_path.exists():
        print(f"ERROR: Input files not found")
        print(f"  PBB: {fcy_pbb_path}")
        print(f"  PIB: {fcy_pib_path}")
        return
    
    con.execute(f"""
        CREATE TABLE fcy_raw AS
        SELECT * FROM read_parquet('{fcy_pbb_path}')
        UNION ALL
        SELECT * FROM read_parquet('{fcy_pib_path}')
    """)
    
    count = con.execute("SELECT COUNT(*) FROM fcy_raw").fetchone()[0]
    print(f"  Loaded {count:,} records")
    
    # Step 2: Filter and classify
    print("\nStep 2: Filtering and classifying accounts...")
    
    con.execute("""
        CREATE TABLE fcy AS
        SELECT 
            ACCTNO,
            BRANCH,
            CURBALRM,
            -- Determine AGLFLD (Entity)
            CASE 
                WHEN ACCTNO BETWEEN 3750000000 AND 3799999999 THEN 'PIBB'
                ELSE 'PBB '
            END AS AGLFLD,
            -- Determine BRANCX (3-digit branch)
            LPAD(CAST(BRANCH AS VARCHAR), 3, '0') AS BRANCX,
            -- Determine PRODUCX (Product)
            CASE 
                WHEN ACCTNO BETWEEN 1590000000 AND 1599999999 THEN 'DFDFCY'
                WHEN ACCTNO BETWEEN 3590000000 AND 3599999999 THEN 'DDMFCY'
                WHEN ACCTNO BETWEEN 3750000000 AND 3799999999 THEN 'DDMFCY'
                ELSE NULL
            END AS PRODUCX
        FROM fcy_raw
        WHERE CURBALRM > 0
          AND (
              (ACCTNO BETWEEN 1590000000 AND 1599999999) OR
              (ACCTNO BETWEEN 3590000000 AND 3599999999) OR
              (ACCTNO BETWEEN 3750000000 AND 3799999999)
          )
    """)
    
    filtered_count = con.execute("SELECT COUNT(*) FROM fcy").fetchone()[0]
    print(f"  Filtered to {filtered_count:,} FCY accounts")
    
    # Step 3: Summarize
    print("\nStep 3: Summarizing by AGLFLD, BRANCH, PRODUCT...")
    
    con.execute(f"""
        CREATE TABLE fcys AS
        SELECT 
            AGLFLD,
            BRANCX,
            PRODUCX,
            COUNT(*) AS NOACCT,
            SUM(CURBALRM) AS CURBALRM,
            'EXT' AS BGLFLD,
            'C_DEP' AS CGLFLD,
            'ACTUAL' AS DGLFLD,
            {year}{month:02d} AS YM
        FROM fcy
        GROUP BY AGLFLD, BRANCX, PRODUCX
        ORDER BY AGLFLD, BRANCX, PRODUCX
    """)
    
    summary_count = con.execute("SELECT COUNT(*) FROM fcys").fetchone()[0]
    print(f"  Summarized to {summary_count:,} groups")
    
    # Get totals
    totals = con.execute("""
        SELECT 
            COUNT(*) AS cnt,
            SUM(NOACCT) AS numac,
            SUM(CURBALRM) AS total_balance
        FROM fcys
    """).fetchone()
    
    print(f"  Total Groups: {totals[0]:,}")
    print(f"  Total Accounts: {totals[1]:,}")
    print(f"  Total Balance: {totals[2]:,.2f}")
    
    # Step 4: Generate GL interface file
    print("\nStep 4: Generating GL interface file...")
    
    result = con.execute("SELECT * FROM fcys").pl()
    
    # Write CSV in GL interface format
    with open(output_path, 'w') as f:
        for row in result.iter_rows(named=True):
            # Detail line format
            line = (
                f"D,"
                f"{row['AGLFLD']:<32},"
                f"{row['BGLFLD']:<32},"
                f"{row['CGLFLD']:<32},"
                f"{row['DGLFLD']:<32},"
                f","  # Empty field
                f"{row['BRANCX']:<32},"
                f"{'RETL':<32},"
                f"{row['PRODUCX']:<32},"
                f"{row['YM']:06d},"
                f"{row['NOACCT']:015d},"
                f"Y,"
                f","  # Empty fields
                f","
                f","
            )
            f.write(line + '\n')
        
        # Trailer line
        trailer = (
            f"T,"
            f"{totals[0]:010d},"
            f"{totals[1]:015d},"
        )
        f.write(trailer + '\n')
    
    output_size = output_path.stat().st_size / 1024
    print(f"  ✓ GL Interface file written: {output_path}")
    print(f"    Size: {output_size:.2f} KB")
    print(f"    Detail lines: {len(result):,}")
    print(f"    Trailer line: 1")
    
    # Show sample
    print("\nSample output (first 3 groups):")
    print(result.head(3))
    
    # Summary by entity
    print("\nSummary by Entity:")
    entity_summary = con.execute("""
        SELECT 
            AGLFLD,
            COUNT(*) AS groups,
            SUM(NOACCT) AS accounts,
            SUM(CURBALRM) AS balance
        FROM fcys
        GROUP BY AGLFLD
        ORDER BY AGLFLD
    """).pl()
    print(entity_summary)
    
    con.close()
    
    print("\n" + "="*80)
    print("Processing completed successfully")
    print("="*80)

if __name__ == "__main__":
    # Report date
    report_date = datetime(2024, 11, 17)
    
    # Calculate filename components
    day = report_date.day
    month = report_date.month
    year = report_date.year % 100  # 2-digit year
    
    # Input paths (using date in filename)
    input_dir = Path(".")
    fcy_pbb = input_dir / f"FCY{year:02d}{month:02d}{day:02d}.parquet"
    fcy_pib = input_dir / f"IFCY{year:02d}{month:02d}{day:02d}.parquet"
    
    # Output path
    output_file = Path("GLINT_FCY.csv")
    
    # Process
    process_eibm0946(
        fcy_pbb_path=fcy_pbb,
        fcy_pib_path=fcy_pib,
        output_path=output_file,
        report_date=report_date
    )
