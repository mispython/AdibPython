import polars as pl
from pathlib import Path

def eiqhpcr4():
    base = Path.cwd()
    hpnpl_path = base / "HPNPL"
    
    # Get report date
    try:
        reptdate_df = pl.read_parquet(hpnpl_path / "REPTDATE.parquet")
        rdate = reptdate_df["REPTDATE"][0].strftime("%d%m%y")
    except:
        print("REPTDATE not found")
        return
    
    print(f"EIQHPCR4 - HP Margin Analysis | Date: {rdate}")
    
    # Read HPLOAN data
    try:
        hploan_df = pl.read_parquet(hpnpl_path / "HPLOAN.parquet")
    except:
        print("HPLOAN.parquet not found. Run EIQHPCR1 first.")
        return
    
    # Filter out LOANTYPE 983, 993
    loan1_df = hploan_df.filter(~pl.col("LOANTYPE").is_in([983, 993]))
    
    # NPL accounts filter
    loan2_df = hploan_df.filter(
        ~pl.col("LOANTYPE").is_in([983, 993]) &
        (
            (pl.col("MTHARR") >= 3) |
            pl.col("BORSTAT").is_in(['F', 'I', 'R'])
        )
    )
    
    if loan1_df.is_empty():
        print("No loan data after filtering")
        return
    
    # Generate margin report
    generate_margin_report(loan1_df, loan2_df, rdate, base / "HPTXT_MARGIN.txt")

def generate_margin_report(loan1_df, loan2_df, rdate, output_path):
    """Generate margin of finance report"""
    
    # Summary for all loans
    ln1_summary = loan1_df.group_by(["NEWSEC", "MGINGRP", "BRABBR"]).agg([
        pl.count().alias("NOACC"),
        pl.sum("BALANCE").alias("BALANCE"),
        pl.sum("SPPL").alias("SPPL"),
        pl.sum("RECOVER").alias("RECOVER"),
        pl.sum("NETSP").alias("NETSP")
    ])
    
    # Summary for NPL loans
    ln2_summary = loan2_df.group_by(["NEWSEC", "MGINGRP", "BRABBR"]).agg([
        pl.count().alias("NPACC"),
        pl.sum("BALANCE").alias("NPBAL")
    ])
    
    # Merge and calculate
    ln3_df = ln1_summary.join(ln2_summary, on=["NEWSEC", "MGINGRP", "BRABBR"], how="left")
    
    ln3_df = ln3_df.with_columns(
        (pl.col("NPBAL") / pl.col("BALANCE") * 100).alias("RATIO"),
        (pl.col("NETSP") / pl.col("BALANCE") * 100).alias("CREDIT")
    ).fill_null(0).sort(["NEWSEC", "MGINGRP", "BRABBR"])
    
    # Generate formatted output
    with open(output_path, 'w') as f:
        page_num = 1
        line_count = 8
        
        # Page header
        def write_header():
            f.write(f"PROGRAM-ID : EIQHPCR4{' ' * 97}PAGE NO.: {page_num}\n")
            f.write("PUBLIC  BANK  BERHAD - CCD (ATTN: JENNY YAP)\n")
            f.write(f"REVIEW OF HPD (CONV & AITAB) CREDIT CHARGE AS AT {rdate}\n")
            f.write("MARGIN OF FINANCING - EXCLUDE 983 & 993\n")
            f.write(" \n")
            f.write(f"{' ' * 9}TOTAL HPD{' ' * 15}TOTAL HPD{' ' * 14}NPL{' ' * 11}NPL{' ' * 21}ALLOWANCE\n")
            f.write(f"{' ' * 10}NO OF A/C{' ' * 13}BALANCE (A){' ' * 12}NO OF A/C{' ' * 11}BALANCE (B){' ' * 15}P&L (C)\n")
            f.write(f"{' ' * 9}WRITTEN BACK{' ' * 12}SP CHARGED NET{' ' * 10}NPL RATIO{' ' * 7}C CHARGE\n")
            f.write(f"{' ' * 9}   P&L (D){' ' * 13}RECOVER (C-D=E){' ' * 8}  % (B/A){' ' * 7} % (E/A)\n")
            f.write("-" * 40 + "-" * 40 + "-" * 40 + "-" * 25 + "\n")
        
        write_header()
        
        # Track totals
        tacc = tbals = tnplacc = tnplbals = tsppl = trecover = tnets = 0
        
        # Process each NEWSEC group
        newsec_groups = ln3_df["NEWSEC"].unique().to_list()
        
        for newsec in newsec_groups:
            sec_df = ln3_df.filter(pl.col("NEWSEC") == newsec)
            
            # NEWSEC header
            if line_count > 56:
                page_num += 1
                line_count = 8
                f.write("\f\n")
                write_header()
            
            f.write(f"{newsec}\n")
            line_count += 1
            
            # NEWSEC totals
            gacc = gbals = gnplacc = gnplbals = gsppl = grecover = gnetsp = 0
            
            # Process each MGINGRP
            margin_groups = sec_df["MGINGRP"].unique().to_list()
            
            for margin in margin_groups:
                margin_df = sec_df.filter(pl.col("MGINGRP") == margin)
                
                # MGINGRP header
                if line_count > 56:
                    page_num += 1
                    line_count = 8
                    f.write("\f\n")
                    write_header()
                    f.write(f"{newsec}\n")
                    f.write(f"{margin}\n")
                    line_count += 2
                else:
                    f.write(f"{margin}\n")
                    line_count += 1
                
                # Margin group totals
                sacc = sbals = snplacc = snplbals = ssppl = srecover = snetsp = 0
                
                # Process each branch
                for row in margin_df.iter_rows(named=True):
                    if line_count > 56:
                        page_num += 1
                        line_count = 8
                        f.write("\f\n")
                        write_header()
                        f.write(f"{newsec}\n")
                        f.write(f"{margin}\n")
                        line_count += 2
                    
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
                    
                    # Update totals
                    # Margin group
                    sacc += row['NOACC']
                    sbals += row['BALANCE']
                    snplacc += row.get('NPACC', 0)
                    snplbals += row.get('NPBAL', 0)
                    ssppl += row.get('SPPL', 0)
                    srecover += row.get('RECOVER', 0)
                    snetsp += row.get('NETSP', 0)
                    
                    # NEWSEC group
                    gacc += row['NOACC']
                    gbals += row['BALANCE']
                    gnplacc += row.get('NPACC', 0)
                    gnplbals += row.get('NPBAL', 0)
                    gsppl += row.get('SPPL', 0)
                    grecover += row.get('RECOVER', 0)
                    gnetsp += row.get('NETSP', 0)
                    
                    # Grand totals
                    tacc += row['NOACC']
                    tbals += row['BALANCE']
                    tnplacc += row.get('NPACC', 0)
                    tnplbals += row.get('NPBAL', 0)
                    tsppl += row.get('SPPL', 0)
                    trecover += row.get('RECOVER', 0)
                    tnets += row.get('NETSP', 0)
                
                # Margin group summary
                f.write("-" * 40 + "-" * 40 + "-" * 40 + "-" * 25 + "\n")
                f.write(f"MARGIN{sacc:>12,}{sbals:>19,.2f}{snplacc:>13,}{snplbals:>19,.2f}"
                       f"{ssppl:>20,.2f}{srecover:>19,.2f}{snetsp:>19,.2f}\n")
                f.write("-" * 40 + "-" * 40 + "-" * 40 + "-" * 25 + "\n")
                line_count += 3
            
            # NEWSEC group summary
            f.write(f"NEW/SEC{gacc:>12,}{gbals:>19,.2f}{gnplacc:>13,}{gnplbals:>19,.2f}"
                   f"{gsppl:>20,.2f}{grecover:>19,.2f}{gnetsp:>19,.2f}\n")
            f.write("-" * 40 + "-" * 40 + "-" * 40 + "-" * 25 + "\n")
            line_count += 2
        
        # Grand totals
        f.write(f"GRD TOT{tacc:>12,}{tbals:>19,.2f}{tnplacc:>13,}{tnplbals:>19,.2f}"
               f"{tsppl:>20,.2f}{trecover:>19,.2f}{tnets:>19,.2f}\n")
        f.write("-" * 40 + "-" * 40 + "-" * 40 + "-" * 25 + "\n")
    
    print(f"Margin report saved: {output_path}")
    print(f"Total: {tacc:,} accounts, {tbals:,.2f} balance")

def eiqhpcr4_simple():
    """Simpler version - CSV output"""
    base = Path.cwd()
    
    # Read data
    try:
        df = pl.read_parquet(base / "HPNPL/HPLOAN.parquet")
    except:
        print("Run EIQHPCR1 first")
        return
    
    # Filter
    df = df.filter(~pl.col("LOANTYPE").is_in([983, 993]))
    
    # NPL indicator
    npl_mask = (pl.col("MTHARR") >= 3) | pl.col("BORSTAT").is_in(['F', 'I', 'R'])
    
    # Group by NEWSEC, MGINGRP, BRABBR
    summary = df.group_by(["NEWSEC", "MGINGRP", "BRABBR"]).agg([
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
    summary.write_csv(base / "HP_MARGIN_SUMMARY.csv")
    
    # Print summary
    print("Margin of Finance Summary:")
    for newsec in summary["NEWSEC"].unique().to_list():
        sec_data = summary.filter(pl.col("NEWSEC") == newsec)
        for margin in sec_data["MGINGRP"].unique().to_list():
            margin_data = sec_data.filter(pl.col("MGINGRP") == margin)
            print(f"  {newsec}/{margin}: {margin_data['TOTAL_ACCOUNTS'].sum():,} accounts, "
                  f"{margin_data['TOTAL_BALANCE'].sum():,.2f} balance")

if __name__ == "__main__":
    eiqhpcr4_simple()
