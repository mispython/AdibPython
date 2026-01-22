import polars as pl
from pathlib import Path

def eiqhpcr2():
    base = Path.cwd()
    hpnpl_path = base / "HPNPL"
    
    # Read HPLOAN data from EIQHPCR1 output
    try:
        hploan_df = pl.read_parquet(hpnpl_path / "HPLOAN.parquet")
    except:
        print("HPLOAN.parquet not found. Run EIQHPCR1 first.")
        return
    
    # Sort by ACCTNO, NOTENO
    hploan_df = hploan_df.sort(["ACCTNO", "NOTENO"])
    
    # Filter for review: MTHARR >= 3 or BORSTAT in ('F','I','R')
    hpac_df = hploan_df.filter(
        (pl.col("MTHARR") >= 3) |
        (pl.col("BORSTAT").is_in(['F', 'I', 'R']))
    )
    
    # Remove duplicates
    hpac_df = hpac_df.unique(["ACCTNO", "NOTENO"])
    
    if hpac_df.is_empty():
        print("No accounts meet review criteria")
        return
    
    # Generate HPREV file
    generate_hprev_file(hpac_df, base / "HPREV.csv")
    
    print(f"EIQHPCR2 - HP Review Report")
    print(f"Accounts for review: {len(hpac_df)}")
    print(f"Total balance: {hpac_df['BALANCE'].sum():,.2f}")

def generate_hprev_file(df, output_path):
    """Generate HP review file with header and footer"""
    
    with open(output_path, 'w') as f:
        # Write header
        f.write("REPORT ID : EIQHPCR2)\n")
        f.write("PUBLIC BANK BERHAD - CCD (ATTN : JENNY YAP)\n")
        f.write("HP REVIEW FOR NPL & WRITTEN OFF ACCOUNT\n")
        f.write("MNI NO;NOTE NO;NAME;BRABBR;PRODUCT;BOR. STATUS;")
        f.write("AMT FINANCE;NET BALANCE;MTH PASS DUE;MARGIN OF FIN.;")
        f.write("LOAN TERM;MAKE OF VEC.;NEW/SECONDHAND;SP PROVISION;SP WRITTEN OFF\n")
        
        # Initialize totals
        totacc = 0
        totamt = 0
        
        # Write data rows
        for row in df.iter_rows(named=True):
            f.write(f"{row.get('ACCTNO', '')};")
            f.write(f"{row.get('NOTENO', '')};")
            f.write(f"{row.get('NAME', '')};")
            f.write(f"{row.get('BRABBR', '')};")
            f.write(f"{row.get('LOANTYPE', '')};")
            f.write(f"{row.get('BORSTAT', '')};")
            f.write(f"{row.get('NETPROC', 0):.2f};")
            f.write(f"{row.get('BALANCE', 0):.2f};")
            f.write(f"{row.get('MTHARR', 0)};")
            f.write(f"{row.get('MARGINF', 0):.1f};")
            f.write(f"{row.get('NOTETERM', 0)};")
            f.write(f"{row.get('MAKE', '')};")
            f.write(f"{row.get('NEWSEC', '')};")
            f.write(f"{row.get('SPPL', 0):.2f};")
            f.write(f"{row.get('RECOVER', 0):.2f}\n")
            
            totacc += 1
            totamt += row.get('BALANCE', 0)
        
        # Write footer
        f.write(f"TOT NO OF A/C : ;{totacc};")
        f.write(f"TOT NET BALANCE : ;{totamt:.2f};\n")
    
    print(f"Review file saved: {output_path}")

def eiqhpcr2_simple():
    """Simpler version"""
    base = Path.cwd()
    
    # Read data
    try:
        df = pl.read_parquet(base / "HPNPL/HPLOAN.parquet")
    except:
        print("Run EIQHPCR1 first to create HPLOAN.parquet")
        return
    
    # Filter and deduplicate
    df = df.filter(
        (pl.col("MTHARR") >= 3) |
        (pl.col("BORSTAT").is_in(['F', 'I', 'R']))
    ).unique(["ACCTNO", "NOTENO"])
    
    if df.is_empty():
        print("No accounts need review")
        return
    
    # Write CSV directly
    output_cols = [
        "ACCTNO", "NOTENO", "NAME", "BRABBR", "LOANTYPE",
        "BORSTAT", "NETPROC", "BALANCE", "MTHARR", "MARGINF",
        "NOTETERM", "MAKE", "NEWSEC", "SPPL", "RECOVER"
    ]
    
    # Select available columns
    available_cols = [col for col in output_cols if col in df.columns]
    output_df = df.select(available_cols)
    
    # Add header row as first row
    header_df = pl.DataFrame([{col: col for col in available_cols}])
    final_df = pl.concat([header_df, output_df])
    
    # Save
    final_df.write_csv(base / "HPREV_SIMPLE.csv", separator=";")
    
    print(f"Review report: {len(df)} accounts, total balance: {df['BALANCE'].sum():,.2f}")

if __name__ == "__main__":
    eiqhpcr2_simple()
