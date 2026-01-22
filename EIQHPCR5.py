import polars as pl
from pathlib import Path

def eiqhpcr5():
    """Generate Hiring Period Margin Report - Exclude 983 & 993"""
    
    base = Path.cwd()
    hpnpl_path = base / "HPNPL"
    
    # Get report date
    try:
        reptdate_df = pl.read_parquet(hpnpl_path / "REPTDATE.parquet")
        rdate = reptdate_df["REPTDATE"][0].strftime("%d%m%y")
        print(f"Report Date: {rdate}")
    except:
        print("REPTDATE.parquet not found or error reading file")
        return
    
    # Read HPLOAN data
    try:
        hploan_df = pl.read_parquet(hpnpl_path / "HPLOAN.parquet")
        print(f"Loaded {len(hploan_df):,} loan records")
    except:
        print("HPLOAN.parquet not found. Run EIQHPCR1 first.")
        return
    
    # Create LOAN1: All loans except 983, 993
    loan1_df = hploan_df.filter(~pl.col("LOANTYPE").is_in([983, 993]))
    
    # Create LOAN2: NPL loans (MTHARR >= 3 or BORSTAT in ['F','I','R']) excluding 983, 993
    loan2_df = hploan_df.filter(
        ~pl.col("LOANTYPE").is_in([983, 993]) &
        (
            (pl.col("MTHARR") >= 3) |
            pl.col("BORSTAT").is_in(['F', 'I', 'R'])
        )
    )
    
    if loan1_df.is_empty():
        print("No loan data after filtering LOANTYPE 983, 993")
        return
    
    # Generate summary for all loans (LOAN1)
    ln1_summary = loan1_df.group_by(["NEWSEC", "TERMGRP", "BRABBR"]).agg([
        pl.count().alias("NOACC"),
        pl.sum("BALANCE").alias("BALANCE"),
        pl.sum("SPPL").alias("SPPL"),
        pl.sum("RECOVER").alias("RECOVER"),
        pl.sum("NETSP").alias("NETSP")
    ])
    
    # Generate summary for NPL loans (LOAN2)
    ln2_summary = loan2_df.group_by(["NEWSEC", "TERMGRP", "BRABBR"]).agg([
        pl.count().alias("NPACC"),
        pl.sum("BALANCE").alias("NPBAL")
    ])
    
    # Merge summaries
    ln3_df = ln1_summary.join(
        ln2_summary, 
        on=["NEWSEC", "TERMGRP", "BRABBR"], 
        how="left"
    )
    
    # Calculate ratios
    ln3_df = ln3_df.with_columns([
        (pl.col("NPBAL") / pl.col("BALANCE") * 100).alias("RATIO").round(2),
        (pl.col("NETSP") / pl.col("BALANCE") * 100).alias("CREDIT").round(2)
    ]).fill_null(0)
    
    # Sort data
    ln3_df = ln3_df.sort(["NEWSEC", "TERMGRP", "BRABBR"])
    
    # Generate detailed report
    output_path = base / f"HPTXT_HIRING_{rdate}.txt"
    generate_hiring_report(ln3_df, rdate, output_path)
    
    # Print summary statistics
    print_summary_statistics(ln3_df, loan1_df, loan2_df)
    
    # Save CSV version for analysis
    csv_path = base / f"HP_HIRING_SUMMARY_{rdate}.csv"
    ln3_df.write_csv(csv_path)
    print(f"\nCSV summary saved: {csv_path}")
    
    return ln3_df

def generate_hiring_report(ln3_df, rdate, output_path):
    """Generate detailed hiring period report"""
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            page_num = 1
            line_count = 0
            rpttitle = "HIRING PERIOD - EXCLUDE 983 & 993"
            
            def write_page_header():
                nonlocal page_num, line_count
                f.write(f"{'PROGRAM-ID : EIQHPCR5':<50}{f'PAGE NO.: {page_num}':>68}\n")
                f.write("PUBLIC  BANK  BERHAD - CCD (ATTN JENNY YAP)\n")
                f.write(f"REVIEW OF HPD (CONV & AITAB) CREDIT CHARGE AS AT {rdate}\n")
                f.write(f"{rpttitle}\n")
                f.write(" \n")
                f.write("          TOTAL HPD          TOTAL HPD      NPL        NPL          ALLOWANCE\n")
                f.write("          NO OF A/C       BALANCE (A)   NO OF A/C  BALANCE (B)      P&L (C)\n")
                f.write("         WRITTEN BACK     SP CHARGED NET          NPL RATIO      C CHARGE\n")
                f.write("            P&L (D)       RECOVER (C-D=E)          % (B/A)        % (E/A)\n")
                f.write("----------------------------------------")
                f.write("----------------------------------------")
                f.write("----------------------------------------")
                f.write("--------------------------\n")
                line_count = 9
            
            write_page_header()
            
            # Initialize totals
            tacc = tbals = tnplacc = tnplbals = tsppl = trecover = tnets = 0
            
            # Group by NEWSEC
            newsec_groups = ln3_df["NEWSEC"].unique().to_list()
            
            for newsec in newsec_groups:
                sec_df = ln3_df.filter(pl.col("NEWSEC") == newsec)
                
                # Check page break
                if line_count > 55:
                    page_num += 1
                    f.write("\f\n")
                    write_page_header()
                
                # Write NEWSEC header
                f.write(f"{newsec}\n")
                line_count += 1
                
                # NEWSEC totals
                gacc = gbals = gnplacc = gnplbals = gsppl = grecover = gnetsp = 0
                
                # Group by TERMGRP
                term_groups = sec_df["TERMGRP"].unique().to_list()
                
                for term in term_groups:
                    term_df = sec_df.filter(pl.col("TERMGRP") == term)
                    
                    # Check page break
                    if line_count > 55:
                        page_num += 1
                        f.write("\f\n")
                        write_page_header()
                        f.write(f"{newsec}\n")
                        line_count += 1
                    
                    # Write TERMGRP header
                    f.write(f"{term}\n")
                    line_count += 1
                    
                    # TERMGRP totals
                    sacc = sbals = snplacc = snplbals = ssppl = srecover = snetsp = 0
                    
                    # Process each branch
                    for row in term_df.iter_rows(named=True):
                        # Check page break
                        if line_count > 55:
                            page_num += 1
                            f.write("\f\n")
                            write_page_header()
                            f.write(f"{newsec}\n")
                            f.write(f"{term}\n")
                            line_count += 2
                        
                        # Format branch line (matching SAS output format)
                        f.write(
                            f"   {row['BRABBR']:<3}"
                            f"{row['NOACC']:>7,}"
                            f"{row['BALANCE']:>16,.2f}"
                            f"{row['NPACC']:>8,}"
                            f"{row['NPBAL']:>16,.2f}"
                            f"{row['SPPL']:>16,.2f}"
                            f"{row['RECOVER']:>16,.2f}"
                            f"{row['NETSP']:>16,.2f}"
                            f"{row['RATIO']:>8.2f}"
                            f"{row['CREDIT']:>9.2f}\n"
                        )
                        line_count += 1
                        
                        # Update totals
                        # TERMGRP totals
                        sacc += row['NOACC']
                        sbals += row['BALANCE']
                        snplacc += row['NPACC']
                        snplbals += row['NPBAL']
                        ssppl += row['SPPL']
                        srecover += row['RECOVER']
                        snetsp += row['NETSP']
                        
                        # NEWSEC totals
                        gacc += row['NOACC']
                        gbals += row['BALANCE']
                        gnplacc += row['NPACC']
                        gnplbals += row['NPBAL']
                        gsppl += row['SPPL']
                        grecover += row['RECOVER']
                        gnetsp += row['NETSP']
                        
                        # Grand totals
                        tacc += row['NOACC']
                        tbals += row['BALANCE']
                        tnplacc += row['NPACC']
                        tnplbals += row['NPBAL']
                        tsppl += row['SPPL']
                        trecover += row['RECOVER']
                        tnets += row['NETSP']
                    
                    # Write TERMGRP totals
                    f.write("-" * 40 + "-" * 40 + "-" * 40 + "-" * 25 + "\n")
                    f.write(f"TERM   "
                           f"{sacc:>7,}"
                           f"{sbals:>16,.2f}"
                           f"{snplacc:>8,}"
                           f"{snplbals:>16,.2f}"
                           f"{ssppl:>16,.2f}"
                           f"{srecover:>16,.2f}"
                           f"{snetsp:>16,.2f}\n")
                    f.write("-" * 40 + "-" * 40 + "-" * 40 + "-" * 25 + "\n")
                    line_count += 3
                
                # Write NEWSEC totals
                f.write(f"NEW/SEC"
                       f"{gacc:>7,}"
                       f"{gbals:>16,.2f}"
                       f"{gnplacc:>8,}"
                       f"{gnplbals:>16,.2f}"
                       f"{gsppl:>16,.2f}"
                       f"{grecover:>16,.2f}"
                       f"{gnetsp:>16,.2f}\n")
                f.write("-" * 40 + "-" * 40 + "-" * 40 + "-" * 25 + "\n")
                line_count += 2
            
            # Write grand totals
            f.write(f"GRD TOT"
                   f"{tacc:>7,}"
                   f"{tbals:>16,.2f}"
                   f"{tnplacc:>8,}"
                   f"{tnplbals:>16,.2f}"
                   f"{tsppl:>16,.2f}"
                   f"{trecover:>16,.2f}"
                   f"{tnets:>16,.2f}\n")
            f.write("-" * 40 + "-" * 40 + "-" * 40 + "-" * 25 + "\n")
        
        print(f"Report saved: {output_path}")
        return True
        
    except Exception as e:
        print(f"Error generating report: {e}")
        return False

def print_summary_statistics(ln3_df, loan1_df, loan2_df):
    """Print summary statistics"""
    
    # Calculate overall totals
    total_summary = ln3_df.select([
        pl.sum("NOACC").alias("total_accounts"),
        pl.sum("BALANCE").alias("total_balance"),
        pl.sum("NPACC").alias("npl_accounts"),
        pl.sum("NPBAL").alias("npl_balance"),
        pl.sum("NETSP").alias("total_netsp"),
        pl.sum("SPPL").alias("total_sppl"),
        pl.sum("RECOVER").alias("total_recover"),
    ]).row(0)
    
    total_accounts, total_balance, npl_accounts, npl_balance, total_netsp, total_sppl, total_recover = total_summary
    
    # Calculate ratios
    npl_ratio = (npl_balance / total_balance * 100) if total_balance > 0 else 0
    credit_ratio = (total_netsp / total_balance * 100) if total_balance > 0 else 0
    net_allowance = total_sppl - total_recover
    
    print(f"\n{'='*60}")
    print(f"HIRING PERIOD REPORT SUMMARY")
    print(f"{'='*60}")
    print(f"Total Loans (excl 983,993):  {total_accounts:>12,}")
    print(f"Total Balance:                {total_balance:>12,.2f}")
    print(f"NPL Loans:                    {npl_accounts:>12,} ({npl_accounts/total_accounts*100:.1f}%)")
    print(f"NPL Balance:                  {npl_balance:>12,.2f} ({npl_ratio:.2f}%)")
    print(f"Total Net SP:                 {total_netsp:>12,.2f} ({credit_ratio:.2f}%)")
    print(f"Total SPPL:                   {total_sppl:>12,.2f}")
    print(f"Total Recover:                {total_recover:>12,.2f}")
    print(f"Net Allowance:                {net_allowance:>12,.2f}")
    print(f"{'='*60}")
    
    # Print by NEWSEC
    print(f"\nBY NEWSEC:")
    newsec_summary = ln3_df.group_by("NEWSEC").agg([
        pl.sum("NOACC").alias("accounts"),
        pl.sum("BALANCE").alias("balance"),
        pl.sum("NPACC").alias("npl_accounts"),
        (pl.sum("NPBAL") / pl.sum("BALANCE") * 100).alias("npl_ratio"),
    ]).sort("balance", descending=True)
    
    for row in newsec_summary.iter_rows(named=True):
        print(f"  {row['NEWSEC']}: {row['accounts']:>6,} accounts, "
              f"{row['balance']:>15,.2f}, NPL: {row['npl_ratio']:.1f}%")

if __name__ == "__main__":
    eiqhpcr5()
