# EIBDCRSF_CRS_PROCESSOR.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pathlib import Path
import os

def main():
    # Configuration using pathlib
    base_path = Path(".")
    cis_path = base_path / "CIS"
    bnm_path = base_path / "BNM"
    ibnm_path = base_path / "IBNM"
    bnmc_path = base_path / "BNMC"
    ibnmc_path = base_path / "IBNMC"
    forate_path = base_path / "FORATE"
    trx_path = base_path / "TRX"
    
    # Create output directory
    cis_path.mkdir(exist_ok=True)
    
    # Connect to DuckDB
    conn = duckdb.connect()
    
    # Step 1: Process REPTDATE and extract parameters
    reptdate_query = f"""
    SELECT * FROM read_parquet('{bnm_path / "REPTDATE.parquet"}')
    """
    reptdate_df = conn.execute(reptdate_query).arrow()
    
    if len(reptdate_df) > 0:
        reptdate = reptdate_df['REPTDATE'][0].as_py()
        rept_year = reptdate.strftime('%y')  # YEAR2 format
        rept_month = reptdate.strftime('%m')  # Z2 format
        rept_day = reptdate.strftime('%d')    # Z2 format
        tdate = reptdate.strftime('%Y-%m-%d')
        
        print(f"Processing date: {reptdate}")
        print(f"Year: {rept_year}, Month: {rept_month}, Day: {rept_day}")
    else:
        raise ValueError("No data found in REPTDATE table")
    
    # Step 2: Process FORATE data and create format
    forate_query = f"""
    WITH ranked_rates AS (
        SELECT 
            CURCODE,
            SPOTRATE,
            REPTDATE,
            ROW_NUMBER() OVER (PARTITION BY CURCODE ORDER BY REPTDATE DESC) as rn
        FROM read_parquet('{forate_path / "FORATEBKP.parquet"}')
        WHERE REPTDATE <= '{tdate}'
    )
    SELECT 
        CURCODE,
        CAST(SPOTRATE as DOUBLE) as SPOTRATE,
        REPTDATE
    FROM ranked_rates
    WHERE rn = 1
    """
    
    forate_df = conn.execute(forate_query).arrow()
    
    # Create format mapping (equivalent to PROC FORMAT)
    format_mapping = {}
    for i in range(len(forate_df)):
        curcode = forate_df['CURCODE'][i].as_py()
        spotrate = forate_df['SPOTRATE'][i].as_py()
        format_mapping[curcode] = spotrate
    
    # Step 3: Process deposit data from multiple sources
    deposit_query = f"""
    WITH saving_a AS (
        SELECT 
            ACCTNO, CURBAL, CURCODE, 'SA' as ACTYPE, 
            OPENIND, CLOSEDT, OPENDT, PRODUCT, COSTCTR,
            CASE 
                WHEN CURCODE = 'XAU' THEN COALESCE({format_mapping.get('XAU', 1)}, 1)
                ELSE NULL 
            END as FORATE,
            CASE 
                WHEN CURCODE = 'XAU' THEN CURBAL 
                ELSE NULL 
            END as FORBAL,
            CASE 
                WHEN CURCODE = 'XAU' THEN ROUND(CURBAL * COALESCE({format_mapping.get('XAU', 1)}, 1), 2)
                ELSE CURBAL 
            END as ADJ_CURBAL,
            CASE 
                WHEN CLOSEDT > 0 THEN SUBSTR(CAST(CLOSEDT as VARCHAR), 1, 8)
                ELSE NULL 
            END as CLDT,
            CASE 
                WHEN OPENDT > 0 THEN SUBSTR(CAST(OPENDT as VARCHAR), 1, 8)
                ELSE NULL 
            END as OPNDT
        FROM read_parquet('{bnm_path / "SAVING.parquet"}')
        
        UNION ALL
        
        SELECT 
            ACCTNO, CURBAL, CURCODE, 'SA' as ACTYPE,
            OPENIND, CLOSEDT, OPENDT, PRODUCT, COSTCTR,
            CASE 
                WHEN CURCODE = 'XAU' THEN COALESCE({format_mapping.get('XAU', 1)}, 1)
                ELSE NULL 
            END as FORATE,
            CASE 
                WHEN CURCODE = 'XAU' THEN CURBAL 
                ELSE NULL 
            END as FORBAL,
            CASE 
                WHEN CURCODE = 'XAU' THEN ROUND(CURBAL * COALESCE({format_mapping.get('XAU', 1)}, 1), 2)
                ELSE CURBAL 
            END as ADJ_CURBAL,
            CASE 
                WHEN CLOSEDT > 0 THEN SUBSTR(CAST(CLOSEDT as VARCHAR), 1, 8)
                ELSE NULL 
            END as CLDT,
            CASE 
                WHEN OPENDT > 0 THEN SUBSTR(CAST(OPENDT as VARCHAR), 1, 8)
                ELSE NULL 
            END as OPNDT
        FROM read_parquet('{ibnm_path / "SAVING.parquet"}')
    ),
    current_b AS (
        SELECT 
            ACCTNO, CURBAL, CURCODE, 'CA' as ACTYPE,
            OPENIND, CLOSEDT, OPENDT, PRODUCT, COSTCTR,
            CASE 
                WHEN CURCODE = 'XAU' THEN COALESCE({format_mapping.get('XAU', 1)}, 1)
                ELSE NULL 
            END as FORATE,
            CASE 
                WHEN CURCODE = 'XAU' THEN CURBAL 
                ELSE NULL 
            END as FORBAL,
            CASE 
                WHEN CURCODE = 'XAU' THEN ROUND(CURBAL * COALESCE({format_mapping.get('XAU', 1)}, 1), 2)
                ELSE CURBAL 
            END as ADJ_CURBAL,
            CASE 
                WHEN CLOSEDT > 0 THEN SUBSTR(CAST(CLOSEDT as VARCHAR), 1, 8)
                ELSE NULL 
            END as CLDT,
            CASE 
                WHEN OPENDT > 0 THEN SUBSTR(CAST(OPENDT as VARCHAR), 1, 8)
                ELSE NULL 
            END as OPNDT
        FROM read_parquet('{bnm_path / "CURRENT.parquet"}')
        
        UNION ALL
        
        SELECT 
            ACCTNO, CURBAL, CURCODE, 'CA' as ACTYPE,
            OPENIND, CLOSEDT, OPENDT, PRODUCT, COSTCTR,
            CASE 
                WHEN CURCODE = 'XAU' THEN COALESCE({format_mapping.get('XAU', 1)}, 1)
                ELSE NULL 
            END as FORATE,
            CASE 
                WHEN CURCODE = 'XAU' THEN CURBAL 
                ELSE NULL 
            END as FORBAL,
            CASE 
                WHEN CURCODE = 'XAU' THEN ROUND(CURBAL * COALESCE({format_mapping.get('XAU', 1)}, 1), 2)
                ELSE CURBAL 
            END as ADJ_CURBAL,
            CASE 
                WHEN CLOSEDT > 0 THEN SUBSTR(CAST(CLOSEDT as VARCHAR), 1, 8)
                ELSE NULL 
            END as CLDT,
            CASE 
                WHEN OPENDT > 0 THEN SUBSTR(CAST(OPENDT as VARCHAR), 1, 8)
                ELSE NULL 
            END as OPNDT
        FROM read_parquet('{ibnm_path / "CURRENT.parquet"}')
    ),
    fd_c AS (
        SELECT 
            ACCTNO, CURBAL, CURCODE, 'FD' as ACTYPE,
            OPENIND, CLOSEDT, OPENDT, PRODUCT, COSTCTR,
            CASE 
                WHEN CURCODE = 'XAU' THEN COALESCE({format_mapping.get('XAU', 1)}, 1)
                ELSE NULL 
            END as FORATE,
            CASE 
                WHEN CURCODE = 'XAU' THEN CURBAL 
                ELSE NULL 
            END as FORBAL,
            CASE 
                WHEN CURCODE = 'XAU' THEN ROUND(CURBAL * COALESCE({format_mapping.get('XAU', 1)}, 1), 2)
                ELSE CURBAL 
            END as ADJ_CURBAL,
            CASE 
                WHEN CLOSEDT > 0 THEN SUBSTR(CAST(CLOSEDT as VARCHAR), 1, 8)
                ELSE NULL 
            END as CLDT,
            CASE 
                WHEN OPENDT > 0 THEN SUBSTR(CAST(OPENDT as VARCHAR), 1, 8)
                ELSE NULL 
            END as OPNDT
        FROM read_parquet('{bnm_path / "FD.parquet"}')
        
        UNION ALL
        
        SELECT 
            ACCTNO, CURBAL, CURCODE, 'FD' as ACTYPE,
            OPENIND, CLOSEDT, OPENDT, PRODUCT, COSTCTR,
            CASE 
                WHEN CURCODE = 'XAU' THEN COALESCE({format_mapping.get('XAU', 1)}, 1)
                ELSE NULL 
            END as FORATE,
            CASE 
                WHEN CURCODE = 'XAU' THEN CURBAL 
                ELSE NULL 
            END as FORBAL,
            CASE 
                WHEN CURCODE = 'XAU' THEN ROUND(CURBAL * COALESCE({format_mapping.get('XAU', 1)}, 1), 2)
                ELSE CURBAL 
            END as ADJ_CURBAL,
            CASE 
                WHEN CLOSEDT > 0 THEN SUBSTR(CAST(CLOSEDT as VARCHAR), 1, 8)
                ELSE NULL 
            END as CLDT,
            CASE 
                WHEN OPENDT > 0 THEN SUBSTR(CAST(OPENDT as VARCHAR), 1, 8)
                ELSE NULL 
            END as OPNDT
        FROM read_parquet('{ibnm_path / "FD.parquet"}')
    ),
    combined AS (
        SELECT * FROM saving_a
        UNION ALL SELECT * FROM current_b
        UNION ALL SELECT * FROM fd_c
    )
    SELECT 
        ACCTNO,
        ADJ_CURBAL as CURBAL,
        FORBAL,
        CURCODE,
        ACTYPE,
        FORATE,
        OPENIND,
        CLDT,
        PRODUCT,
        COSTCTR,
        OPNDT
    FROM combined
    """
    
    deposit_df = conn.execute(deposit_query).arrow()
    
    # Step 4: Process PREVDP data
    prevdp_query = f"""
    SELECT ACCTNO, COALESCE(INTPDPYR, 0) as INTPDPYR
    FROM (
        SELECT ACCTNO, INTPDPYR FROM read_parquet('{bnmc_path / "SAVING.parquet"}')
        UNION ALL
        SELECT ACCTNO, INTPDPYR FROM read_parquet('{ibnmc_path / "SAVING.parquet"}')
        UNION ALL
        SELECT ACCTNO, INTPDPYR FROM read_parquet('{bnmc_path / "CURRENT.parquet"}')
        UNION ALL
        SELECT ACCTNO, INTPDPYR FROM read_parquet('{ibnmc_path / "CURRENT.parquet"}')
        UNION ALL
        SELECT ACCTNO, INTPDPYR FROM read_parquet('{trx_path / "FULLDPTRAN_CRS.parquet"}')
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCTNO ORDER BY INTPDPYR DESC) = 1
    """
    
    prevdp_df = conn.execute(prevdp_query).arrow()
    
    # Step 5: Merge DEPOSIT and PREVDP data
    final_query = """
    WITH merged_data AS (
        SELECT 
            d.*,
            COALESCE(p.INTPDPYR, 0) as INTPDPYR,
            CASE 
                WHEN d.CURCODE != 'MYR' THEN COALESCE(p.INTPDPYR, 0)
                ELSE NULL 
            END as FORPDPYR,
            CASE 
                WHEN d.CURCODE != 'MYR' THEN 
                    ROUND(COALESCE(p.INTPDPYR, 0) * COALESCE(d.FORATE, 1), 2)
                ELSE COALESCE(p.INTPDPYR, 0)
            END as ADJ_INTPDPYR,
            CASE 
                WHEN d.COSTCTR BETWEEN 3000 AND 4999 THEN 'I'
                ELSE 'C'
            END as ENTITY
        FROM deposit_df d
        LEFT JOIN prevdp_df p ON d.ACCTNO = p.ACCTNO
    )
    SELECT 
        ACCTNO,
        CURBAL,
        FORBAL,
        CURCODE,
        ACTYPE,
        FORATE,
        OPENIND,
        CLDT,
        PRODUCT,
        COSTCTR,
        OPNDT,
        ADJ_INTPDPYR as INTPDPYR,
        FORPDPYR,
        ENTITY
    FROM merged_data
    """
    
    final_df = conn.execute(final_query).arrow()
    
    # Step 6: Output to Parquet and CSV
    output_filename = f"CRS{rept_year}"
    parquet_path = cis_path / f"{output_filename}.parquet"
    csv_path = cis_path / f"{output_filename}.csv"
    
    pq.write_table(final_df, parquet_path)
    csv.write_csv(final_df, csv_path)
    
    # Step 7: Create fixed-width output (equivalent to DATA _NULL_ with FILE statement)
    fixed_width_path = cis_path / f"{output_filename}.txt"
    
    def format_fixed_width(record):
        """Format record as fixed-width string"""
        acctno = f"{record['ACCTNO']:010d}" if record['ACCTNO'] is not None else " " * 10
        curcode = f"{record['CURCODE']:3}" if record['CURCODE'] is not None else " " * 3
        forbal = f"{record['FORBAL'] or 0:20,.2f}".replace(",", "") if record['FORBAL'] is not None else " " * 20
        curbal = f"{record['CURBAL'] or 0:20,.2f}".replace(",", "") if record['CURBAL'] is not None else " " * 20
        intpdpyr = f"{record['INTPDPYR'] or 0:20,.2f}".replace(",", "") if record['INTPDPYR'] is not None else " " * 20
        fxrate = f"{record['FORATE'] or 1:13.7f}" if record['FORATE'] is not None else f"{1:13.7f}"
        openind = f"{record['OPENIND']:1}" if record['OPENIND'] is not None else " "
        cldt = f"{record['CLDT']:8}" if record['CLDT'] is not None else " " * 8
        product = f"{record['PRODUCT']:03d}" if record['PRODUCT'] is not None else " " * 3
        entity = f"{record['ENTITY']:1}" if record['ENTITY'] is not None else " "
        costctr = f"{record['COSTCTR']:04d}" if record['COSTCTR'] is not None else " " * 4
        opndt = f"{record['OPNDT']:8}" if record['OPNDT'] is not None else " " * 8
        
        return f"{acctno}{curcode}{forbal}{curbal}{intpdpyr}{fxrate}{openind}{cldt}{product}{entity}{costctr}{opndt}"
    
    # Write fixed-width file
    with open(fixed_width_path, 'w') as f:
        for record in final_df.to_pylist():
            line = format_fixed_width(record)
            f.write(line + '\n')
    
    # Print summary statistics
    print(f"\nProcessing completed!")
    print(f"Total records processed: {len(final_df)}")
    print(f"Output files created:")
    print(f"  - {parquet_path}")
    print(f"  - {csv_path}")
    print(f"  - {fixed_width_path}")
    
    # Summary by account type
    actype_summary = {}
    for record in final_df.to_pylist():
        actype = record['ACTYPE']
        actype_summary[actype] = actype_summary.get(actype, 0) + 1
    
    print(f"\nRecords by Account Type:")
    for actype, count in sorted(actype_summary.items()):
        print(f"  {actype}: {count} records")
    
    # Currency summary
    currency_summary = {}
    for record in final_df.to_pylist():
        currency = record['CURCODE']
        currency_summary[currency] = currency_summary.get(currency, 0) + 1
    
    print(f"\nRecords by Currency:")
    for currency, count in sorted(currency_summary.items()):
        print(f"  {currency}: {count} records")
    
    conn.close()

if __name__ == "__main__":
    main()
