import polars as pl
from pathlib import Path

def eiqhpcr3():
    base = Path.cwd()
    hpnpl_path = base / "HPNPL"
    
    # Get report date
    try:
        reptdate_df = pl.read_parquet(hpnpl_path / "REPTDATE.parquet")
        rdate = reptdate_df["REPTDATE"][0].strftime("%d%m%y")
    except:
        print("REPTDATE not found")
        return
    
    print(f"EIQHPCR3 - HP Credit Review | Date: {rdate}")
    
    # Read HPLOAN data
    try:
        hploan_df = pl.read_parquet(hpnpl_path / "HPLOAN.parquet")
    except:
        print("HPLOAN.parquet not found. Run EIQHPCR1 first.")
        return
    
    # Sort by ACCTNO
    hploan_df = hploan_df.sort("ACCTNO")
    
    # Filter out LOANTYPE 983, 993 and handle missing CRRISK
    loan1_df = hploan_df.filter(
        ~pl.col("LOANTYPE").is_in([983, 993])
    ).with_columns(
        pl.when(pl.col("CRRISK") == "")
        .then("Z")
        .otherwise(pl.col("CRRISK"))
        .alias("CRRISK")
    )
    
    # Filter for NPL accounts (MTHARR >= 3 or BORSTAT in F,I,R)
    loan2_df = hploan_df.filter(
        ~pl.col("LOANTYPE").is_in([983, 993]) &
        (
            (pl.col("MTHARR") >= 3) |
            pl.col("BORSTAT").is_in(['F', 'I', 'R'])
        )
    ).with_columns(
        pl.when(pl.col("CRRISK") == "")
        .then("Z")
        .otherwise(pl.col("CRRISK"))
        .alias("CRRISK")
    )
    
    if loan1_df.is_empty():
        print("No loan data after filtering")
        return
    
    # Generate reports
    generate_credit_report(loan1_df, loan2_df, rdate, base / "HPTXT.txt")

def generate_credit_report(loan1_df, loan2_df, rdate, output_path):
    """Generate credit score report"""
    
    # Summary for all loans (LOAN1)
    ln1_summary = loan1_df.group_by(["CRRISK", "BRABBR"]).agg([
        pl.count().alias("NOACC"),
        pl.sum("BALANCE").alias("BALANCE"),
        pl.sum("SPPL").alias("SPPL"),
        pl.sum("RECOVER").alias("RECOVER"),
        pl.sum("NETSP").alias("NETSP")
    ])
    
    # Summary for NPL loans (LOAN2)
    ln2_summary = loan2_df.group_by(["CRRISK", "BRABBR"]).agg([
        pl.count().alias("NPACC"),
        pl.sum("BALANCE").alias("NPBAL")
    ])
    
    # Merge summaries
    ln3_df = ln1_summary.join(ln2_summary, on=["CRRISK", "BRABBR"], how="left")
    
    # Calculate ratios
    ln3_df = ln3_df.with_columns(
        (pl.col("NPBAL") / pl.col("BALANCE") * 100).alias("RATIO"),
        (pl.col("NETSP") / pl.col("BALANCE") * 100).alias("CREDIT")
    ).fill_null(0).sort(["CRRISK", "BRABBR"])
    
    # Generate formatted output
    with open(output_path, 'w') as f:
        page_num = 1
        line_count = 9
        
        # Page header
        def write_page_header():
            f.write(f"PROGRAM-ID : EIQHPCR3 (ATTN : JENNY YAP){' ' * 87}PAGE NO.: {page_num}\n")
            f.write("PUBLIC  BANK  BERHAD\n")
            f.write(f"REVIEW OF HPD (CONV & AITAB) CREDIT CHARGE AS AT {rdate}\n")
            f.write("CREDIT SCORE - EXCLUDE 983 & 993\n")
            f.write(" \n")
            f.write(f"{' ' * 9}TOTAL HPD{' ' * 15}TOTAL HPD{' ' * 14}NPL{' ' * 11}NPL{' ' * 21}ALLOWANCE\n")
            f.write(f"{' ' * 10}NO OF A/C{' ' * 13}BALANCE (A){' ' * 12}NO OF A/C{' ' * 11}BALANCE (B){' ' * 15}P&L (C)\n")
            f.write(f"{' ' * 9}WRITTEN BACK{' ' * 12}SP CHARGED NET{' ' * 10}NPL RATIO{' ' * 7}C CHARGE\n")
            f.write(f"{' ' * 9}   P&L (D){' ' * 13}RECOVER (C-D=E){' ' * 8}  % (B/A){' ' * 7} % (E/A)\n")
            f.write("-" * 40 + "-" * 40 + "-" * 40 + "-" * 25 + "\n")
        
        write_page_header()
        
        # Track totals
        tacc = tbals = tnplacc = tnplbals = tsppl = trecover = tnets = 0
        
        # Process each CRRISK group
        crrisk_groups = ln3_df["CRRISK"].unique().to_list()
        
        for crrisk in sorted(crrisk_groups):
            group_df = ln3_df.filter(pl.col("CRRISK") == crrisk)
            
            # Group header
            if line_count > 56:
                page_num += 1
                line_count = 9
                f.write("\f\n")  # Page break
                write_page_header()
            
            f.write(f"{'CREDIT SCORE' : <40} : {crrisk}\n")
            line_count += 1
            
            # Group totals
            gacc = gbals = gnplacc = gnplbals = gsppl = grecover = gnetsp = 0
            
            # Process each branch
            for row in group_df.iter_rows(named=True):
                if line_count > 56:
                    page_num += 1
                    line_count = 9
                    f.write("\f\n")
                    write_page_header()
                    f.write(f"{'CREDIT SCORE' : <40} : {crrisk}\n")
                    line_count += 1
                
                f.write(f"   {row['BRABBR']:<3} "
                       f"{row['NOACC']:>7,} "
                       f"{row['BALANCE']:>16,.2f} "
                       f"{row.get('NPACC', 0):>7,} "
                       f"{row.get('NPBAL', 0):>16,.2f} "
                       f"{row.get('SPPL', 0):>16,.2f} "
                       f"{row.get('RECOVER', 0):>16,.2f} "
                       f"{row.get('NETSP', 0):>16,.2f} "
                       f"{row.get('RATIO', 0):>6.2f} "
                       f"{row.get('CREDIT', 0):>6.2f}\n")
                line_count += 1
                
                # Update group totals
                gacc += row['NOACC']
                gbals += row['BALANCE']
                gnplacc += row.get('NPACC', 0)
                gnplbals += row.get('NPBAL', 0)
                gsppl += row.get('SPPL', 0)
                grecover += row.get('RECOVER', 0)
                gnetsp += row.get('NETSP', 0)
            
            # Group summary
            f.write("-" * 40 + "-" * 40 + "-" * 40 + "-" * 25 + "\n")
            f.write(f"SUB TOT{gacc:>10,}{gbals:>19,.2f}{gnplacc:>13,}{gnplbals:>19,.2f}"
                   f"{gsppl:>20,.2f}{grecover:>19,.2f}{gnetsp:>19,.2f}\n")
            f.write("-" * 40 + "-" * 40 + "-" * 40 + "-" * 25 + "\n")
            line_count += 3
            
            # Update grand totals
            tacc += gacc
            tbals += gbals
            tnplacc += gnplacc
            tnplbals += gnplbals
            tsppl += gsppl
            trecover += grecover
            tnets += gnetsp
        
        # Grand totals
        f.write(f"GRD TOT{tacc:>10,}{tbals:>19,.2f}{tnplacc:>13,}{tnplbals:>19,.2f}"
               f"{tsppl:>20,.2f}{trecover:>19,.2f}{tnets:>19,.2f}\n")
        f.write("-" * 40 + "-" * 40 + "-" * 40 + "-" * 25 + "\n")
    
    print(f"Report saved: {output_path}")
    print(f"Total accounts: {tacc:,}, Total balance: {tbals:,.2f}")

def eiqhpcr3_simple():
    """Simpler version - CSV output"""
    base = Path.cwd()
    
    # Read data
    try:
        df = pl.read_parquet(base / "HPNPL/HPLOAN.parquet")
    except:
        print("Run EIQHPCR1 first")
        return
    
    # Filter and process
    df = df.filter(~pl.col("LOANTYPE").is_in([983, 993]))
    
    # Handle missing CRRISK
    df = df.with_columns(
        pl.when(pl.col("CRRISK") == "")
        .then("Z")
        .otherwise(pl.col("CRRISK"))
        .alias("CRRISK")
    )
    
    # Identify NPL accounts
    npl_mask = (pl.col("MTHARR") >= 3) | pl.col("BORSTAT").is_in(['F', 'I', 'R'])
    
    # Group by CRRISK and BRABBR
    summary = df.group_by(["CRRISK", "BRABBR"]).agg([
        pl.count().alias("TOTAL_ACCOUNTS"),
        pl.sum("BALANCE").alias("TOTAL_BALANCE"),
        pl.sum("SPPL").alias("TOTAL_SPPL"),
        pl.sum("RECOVER").alias("TOTAL_RECOVER"),
        pl.sum("NETSP").alias("TOTAL_NETSP"),
        pl.sum(npl_mask.cast(pl.Int64)).alias("NPL_ACCOUNTS"),
        pl.sum(pl.when(npl_mask).then(pl.col("BALANCE")).otherwise(0)).alias("NPL_BALANCE")
    ])
    
    # Calculate ratios
    summary = summary.with_columns(
        (pl.col("NPL_BALANCE") / pl.col("TOTAL_BALANCE") * 100).alias("NPL_RATIO"),
        (pl.col("TOTAL_NETSP") / pl.col("TOTAL_BALANCE") * 100).alias("CREDIT_CHARGE")
    ).fill_null(0)
    
    # Save
    summary.write_csv(base / "HP_CREDIT_SUMMARY.csv")
    
    # Print summary
    print("Credit Score Summary:")
    for crrisk in summary["CRRISK"].unique().to_list():
        group = summary.filter(pl.col("CRRISK") == crrisk)
        print(f"  CRRISK {crrisk}: {len(group)} branches, "
              f"{group['TOTAL_ACCOUNTS'].sum():,} accounts, "
              f"Balance: {group['TOTAL_BALANCE'].sum():,.2f}")

if __name__ == "__main__":
    eiqhpcr3_simple()
