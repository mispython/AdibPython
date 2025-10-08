"""
EIBDLNS2 - Branch Daily Outstanding Loan Summary Report
Generates branch summary on daily outstanding amounts for BAE Personal loans
"""

import duckdb
from datetime import datetime, timedelta
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv

# Configuration
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

# Input file paths
DATEFILE = INPUT_DIR / "datefile.parquet"
FEEFILE = INPUT_DIR / "feefile.parquet"
ACCTFILE = INPUT_DIR / "acctfile.parquet"
BRANCHF = INPUT_DIR / "branchfile.parquet"

# Connect to DuckDB
con = duckdb.connect(":memory:")

def get_report_dates():
    """Extract and calculate report dates from datefile"""
    result = con.execute(f"""
        SELECT 
            CAST(SUBSTR(LPAD(CAST(extdate AS VARCHAR), 11, '0'), 1, 8) AS DATE) as reptdate
        FROM read_parquet('{DATEFILE}')
        LIMIT 1
    """).fetchone()
    
    reptdate = result[0]
    prevdate = reptdate - timedelta(days=1)
    dletdate = reptdate - timedelta(days=3)
    
    # Determine previous year
    if reptdate.month == 1 and reptdate.day == 1:
        prevyear = reptdate.year - 1
    else:
        prevyear = reptdate.year
    
    dates = {
        'reptdate': reptdate,
        'prevdate': prevdate,
        'dletdate': dletdate,
        'reptday': reptdate.strftime('%d'),
        'prevday': prevdate.strftime('%d'),
        'dletday': dletdate.strftime('%d'),
        'reptmon': reptdate.strftime('%m'),
        'reptyear': str(reptdate.year),
        'prevyear': str(prevyear),
        'rdate': reptdate.strftime('%d/%m/%Y')
    }
    
    print(f"Report Date: {dates['rdate']}")
    return dates

def process_fee_plan():
    """Process fee plan data and aggregate by account and note"""
    con.execute(f"""
        CREATE OR REPLACE TABLE feepo AS
        SELECT 
            acctno,
            noteno,
            SUM(feeamta) as feeamta,
            SUM(feeamtb) as feeamtb,
            SUM(feeamtc) as feeamtc
        FROM read_parquet('{FEEFILE}')
        WHERE acctno < 3000000000
            AND loantype IN (135, 136)
            AND feepln = 'PA'
        GROUP BY acctno, noteno
        ORDER BY acctno, noteno
    """)

def process_loan_accounts(reptdate):
    """Process loan account data"""
    reptdate_int = int(reptdate.strftime('%y%m%d'))
    
    con.execute(f"""
        CREATE OR REPLACE TABLE loan_base AS
        SELECT 
            acctno,
            name,
            bankno,
            accbrch,
            noteno,
            reversed,
            loantype,
            ntbrch,
            pendbrh,
            lasttran,
            curbal,
            intamt,
            paidind,
            ntint,
            intearn,
            accrual,
            intearn2,
            intearn3,
            intearn4,
            feeamt,
            feeamt2,
            feeamt4,
            nfeeamt5,
            nfeeamt6,
            nfeeamt7,
            feeamt8,
            feeamt9,
            feeamt13,
            feeamt10,
            feeamt11,
            feeamt12,
            feeamt14,
            feeamt15,
            feeamt16,
            CASE 
                WHEN pendbrh != 0 THEN pendbrh
                WHEN ntbrch != 0 THEN ntbrch
                ELSE accbrch
            END as branch
        FROM read_parquet('{ACCTFILE}')
        WHERE loantype IN (135, 136)
            AND (
                ((reversed IS NULL OR reversed != 'Y') 
                 AND noteno IS NOT NULL 
                 AND (paidind IS NULL OR paidind != 'P'))
                OR (paidind = 'P' AND lasttran = {reptdate_int})
            )
    """)
    
    # Merge with fee data and calculate balance
    con.execute(f"""
        CREATE OR REPLACE TABLE loan AS
        SELECT 
            l.*,
            COALESCE(f.feeamta, 0) as feeamta,
            COALESCE(f.feeamtb, 0) as feeamtb,
            COALESCE(f.feeamtc, 0) as feeamtc,
            CASE 
                WHEN l.acctno > 8000000000 AND l.loantype IN (720, 725)
                THEN l.feeamt + COALESCE(f.feeamta, 0) + COALESCE(f.feeamtc, 0)
                ELSE l.feeamt + COALESCE(f.feeamta, 0)
            END as feeamt_calc,
            CASE 
                WHEN l.ntint = 'A' 
                THEN l.curbal + l.intearn - l.intamt + 
                     CASE 
                         WHEN l.acctno > 8000000000 AND l.loantype IN (720, 725)
                         THEN l.feeamt + COALESCE(f.feeamta, 0) + COALESCE(f.feeamtc, 0)
                         ELSE l.feeamt + COALESCE(f.feeamta, 0)
                     END
                ELSE l.curbal + l.accrual + 
                     CASE 
                         WHEN l.acctno > 8000000000 AND l.loantype IN (720, 725)
                         THEN l.feeamt + COALESCE(f.feeamta, 0) + COALESCE(f.feeamtc, 0)
                         ELSE l.feeamt + COALESCE(f.feeamta, 0)
                     END
            END as balance
        FROM loan_base l
        LEFT JOIN feepo f ON l.acctno = f.acctno AND l.noteno = f.noteno
    """)

def aggregate_by_branch(reptdate):
    """Aggregate loan data by branch"""
    reptdate_str = reptdate.strftime('%Y-%m-%d')
    extdate = int(reptdate.strftime('%y%m%d'))
    
    con.execute(f"""
        CREATE OR REPLACE TABLE loan_summary AS
        SELECT 
            branch,
            DATE '{reptdate_str}' as reptdate,
            {extdate} as extdate,
            COUNT(*) as noacct,
            SUM(balance) as brlnamt
        FROM loan
        GROUP BY branch
        ORDER BY branch
    """)

def load_previous_day_data(prevday):
    """Load previous day's loan summary"""
    prev_file = OUTPUT_DIR / f"loan_summary_{prevday}.parquet"
    
    if prev_file.exists():
        con.execute(f"""
            CREATE OR REPLACE TABLE prevln AS
            SELECT 
                branch,
                brlnamt as pbrlnamt
            FROM read_parquet('{prev_file}')
        """)
    else:
        print(f"Warning: Previous day file not found ({prev_file}), using zero values")
        con.execute("""
            CREATE OR REPLACE TABLE prevln AS
            SELECT 
                branch,
                0.0 as pbrlnamt
            FROM loan_summary
        """)

def merge_with_branch_data():
    """Merge loan data with branch information"""
    con.execute(f"""
        CREATE OR REPLACE TABLE mloan AS
        SELECT 
            l.branch,
            b.bank,
            b.abbrev,
            b.brchname,
            l.reptdate,
            l.extdate,
            l.noacct,
            COALESCE(p.pbrlnamt, 0) as pbrlnamt,
            l.brlnamt,
            l.brlnamt - COALESCE(p.pbrlnamt, 0) as varianln
        FROM loan_summary l
        LEFT JOIN prevln p ON l.branch = p.branch
        LEFT JOIN read_parquet('{BRANCHF}') b ON l.branch = b.branch
        ORDER BY l.branch
    """)

def generate_report(rdate):
    """Generate final report with totals"""
    report = con.execute("""
        SELECT 
            branch as code,
            abbrev,
            brchname as name,
            noacct as no_of_accounts,
            pbrlnamt as prev_amount,
            brlnamt as curr_amount,
            varianln as variance
        FROM mloan
        
        UNION ALL
        
        SELECT 
            9999 as code,
            'TOT' as abbrev,
            'TOTAL' as name,
            SUM(noacct) as no_of_accounts,
            SUM(pbrlnamt) as prev_amount,
            SUM(brlnamt) as curr_amount,
            SUM(varianln) as variance
        FROM mloan
        
        ORDER BY code
    """).arrow()
    
    return report

def main():
    """Main execution flow"""
    print("=" * 80)
    print("REPORT ID: EIBDLNSA")
    print("PUBLIC BANK BERHAD")
    print("BRANCH SUMMARY ON DAILY OUTSTANDING(RM) - BAE PERSONAL")
    print("=" * 80)
    
    # Get report dates
    dates = get_report_dates()
    
    # Process data
    print("\nProcessing fee plan data...")
    process_fee_plan()
    
    print("Processing loan accounts...")
    process_loan_accounts(dates['reptdate'])
    
    print("Aggregating by branch...")
    aggregate_by_branch(dates['reptdate'])
    
    # Save current day summary for next day's comparison
    current_summary = con.execute("SELECT * FROM loan_summary").arrow()
    pq.write_table(
        current_summary,
        OUTPUT_DIR / f"loan_summary_{dates['reptday']}.parquet"
    )
    
    # Load previous day data
    print("Loading previous day data...")
    load_previous_day_data(dates['prevday'])
    
    # Merge with branch data
    print("Merging with branch information...")
    merge_with_branch_data()
    
    # Generate final report
    print("\nGenerating report...")
    report = generate_report(dates['rdate'])
    
    # Save outputs
    output_base = f"EIBDLNS2_Branch_Loan_Summary_{dates['reptdate'].strftime('%Y%m%d')}"
    
    # Save as Parquet
    parquet_file = OUTPUT_DIR / f"{output_base}.parquet"
    pq.write_table(report, parquet_file)
    print(f"\nParquet output saved to: {parquet_file}")
    
    # Save as CSV
    csv_file = OUTPUT_DIR / f"{output_base}.csv"
    csv.write_csv(report, csv_file)
    print(f"CSV output saved to: {csv_file}")
    
    # Display summary
    print(f"\nREPORT AS AT {dates['rdate']}")
    print("=" * 80)
    print(f"{'CODE':<8} {'ABBREV':<8} {'NAME':<30} {'ACCOUNTS':>10} {'PREV AMT':>18} {'CURR AMT':>18} {'VARIANCE':>20}")
    print("-" * 120)
    
    for row in report.to_pylist():
        print(f"{row['code']:<8} {row['abbrev']:<8} {row['name']:<30} "
              f"{row['no_of_accounts']:>10,} "
              f"{row['prev_amount']:>18,.2f} "
              f"{row['curr_amount']:>18,.2f} "
              f"{row['variance']:>20,.2f}")
    
    print("=" * 80)
    print(f"\nProcessing complete!")
    
    # Cleanup
    con.close()

if __name__ == "__main__":
    main()
