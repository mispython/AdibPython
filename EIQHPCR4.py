import polars as pl
from pathlib import Path

def eiqhpcr4():
    """Generate Margin of Finance Report - Single Version"""
    
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
    
    # Filter out LOANTYPE 983, 993
    loan_df = hploan_df.filter(~pl.col("LOANTYPE").is_in([983, 993]))
    
    if loan_df.is_empty():
        print("No loan data after filtering LOANTYPE 983, 993")
        return
    
    # Calculate NPL indicator
    npl_condition = (
        (pl.col("MTHARR") >= 3) |
        pl.col("BORSTAT").is_in(['F', 'I', 'R'])
    )
    
    # Group by NEWSEC, MGINGRP, BRABBR
    summary = loan_df.group_by(["NEWSEC", "MGINGRP", "BRABBR"]).agg([
        pl.count().alias("TOTAL_ACCOUNTS"),
        pl.sum("BALANCE").alias("TOTAL_BALANCE"),
        pl.sum("SPPL").alias("TOTAL_SPPL"),
        pl.sum("RECOVER").alias("TOTAL_RECOVER"),
        pl.sum("NETSP").alias("TOTAL_NETSP"),
        pl.sum(npl_condition.cast(pl.Int64)).alias("NPL_ACCOUNTS"),
        pl.sum(
            pl.when(npl_condition)
            .then(pl.col("BALANCE"))
            .otherwise(0)
        ).alias("NPL_BALANCE")
    ])
    
    # Calculate ratios and derived metrics
    summary = summary.with_columns([
        (pl.col("NPL_BALANCE") / pl.col("TOTAL_BALANCE") * 100)
        .alias("NPL_RATIO_PERC")
        .round(2),
        (pl.col("TOTAL_NETSP") / pl.col("TOTAL_BALANCE") * 100)
        .alias("CREDIT_CHARGE_PERC")
        .round(2),
        (pl.col("TOTAL_SPPL") - pl.col("TOTAL_RECOVER"))
        .alias("NET_ALLOWANCE")
    ]).fill_null(0)
    
    # Sort for consistent output
    summary = summary.sort(["NEWSEC", "MGINGRP", "BRABBR"])
    
    # Save CSV summary
    csv_path = base / f"HP_MARGIN_{rdate}.csv"
    summary.write_csv(csv_path)
    print(f"\nCSV summary saved: {csv_path}")
    
    # Generate detailed report
    txt_path = base / f"HPTXT_MARGIN_{rdate}.txt"
    generate_detailed_report(summary, rdate, txt_path)
    
    # Print key statistics
    print_key_statistics(summary)
    
    return summary

def generate_detailed_report(summary_df, rdate, output_path):
    """Generate detailed formatted text report"""
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            page_num = 1
            line_count = 0
            
            # Function to write header
            def write_page_header():
                nonlocal page_num, line_count
                f.write(f"{'PROGRAM-ID : EIQHPCR4':<50}{f'PAGE NO.: {page_num}':>60}\n")
                f.write("PUBLIC BANK BERHAD - CCD (ATTN: JENNY YAP)\n")
                f.write(f"REVIEW OF HPD (CONV & AITAB) CREDIT CHARGE AS AT {rdate}\n")
                f.write("MARGIN OF FINANCING - EXCLUDE 983 & 993\n")
                f.write(" \n")
                f.write("          TOTAL HPD               TOTAL HPD           NPL         NPL                  ALLOWANCE\n")
                f.write("          NO OF A/C            BALANCE (A)        NO OF A/C     BALANCE (B)          P&L (C)\n")
                f.write("         WRITTEN BACK          SP CHARGED NET     NPL RATIO     C CHARGE\n")
                f.write("            P&L (D)          RECOVER (C-D=E)       % (B/A)       % (E/A)\n")
                f.write("-" * 140 + "\n")
                line_count = 8
            
            write_page_header()
            
            # Process data by NEWSEC groups
            newsec_groups = summary_df["NEWSEC"].unique().to_list()
            grand_totals = {
                'accounts': 0, 'balance': 0.0, 'npl_accounts': 0, 'npl_balance': 0.0,
                'sppl': 0.0, 'recover': 0.0, 'netsp': 0.0
            }
            
            for newsec in newsec_groups:
                sec_data = summary_df.filter(pl.col("NEWSEC") == newsec)
                
                # Check for page break
                if line_count > 56:
                    page_num += 1
                    f.write("\f\n")
                    write_page_header()
                
                # Write NEWSEC header
                f.write(f"{newsec}\n")
                line_count += 1
                
                # NEWSEC totals
                sec_totals = {'accounts': 0, 'balance': 0.0, 'npl_accounts': 0, 'npl_balance': 0.0,
                             'sppl': 0.0, 'recover': 0.0, 'netsp': 0.0}
                
                # Process MGINGRP groups
                margin_groups = sec_data["MGINGRP"].unique().to_list()
                
                for margin in margin_groups:
                    margin_data = sec_data.filter(pl.col("MGINGRP") == margin)
                    
                    # Check for page break
                    if line_count > 56:
                        page_num += 1
                        f.write("\f\n")
                        write_page_header()
                        f.write(f"{newsec}\n")
                        line_count += 1
                    
                    # Write MGINGRP header
                    f.write(f"{margin}\n")
                    line_count += 1
                    
                    # MGINGRP totals
                    margin_totals = {'accounts': 0, 'balance': 0.0, 'npl_accounts': 0, 'npl_balance': 0.0,
                                    'sppl': 0.0, 'recover': 0.0, 'netsp': 0.0}
                    
                    # Write branch details
                    for row in margin_data.iter_rows(named=True):
                        if line_count > 56:
                            page_num += 1
                            f.write("\f\n")
                            write_page_header()
                            f.write(f"{newsec}\n")
                            f.write(f"{margin}\n")
                            line_count += 2
                        
                        # Format and write branch line
                        f.write(
                            f"   {row['BRABBR']:<3} "
                            f"{row['TOTAL_ACCOUNTS']:>7,} "
                            f"{row['TOTAL_BALANCE']:>16,.2f} "
                            f"{row['NPL_ACCOUNTS']:>7,} "
                            f"{row['NPL_BALANCE']:>16,.2f} "
                            f"{row['TOTAL_SPPL']:>16,.2f} "
                            f"{row['TOTAL_RECOVER']:>16,.2f} "
                            f"{row['TOTAL_NETSP']:>16,.2f} "
                            f"{row['NPL_RATIO_PERC']:>6.2f} "
                            f"{row['CREDIT_CHARGE_PERC']:>6.2f}\n"
                        )
                        line_count += 1
                        
                        # Update totals
                        for total_dict, row_key, dict_key in [
                            (margin_totals, 'TOTAL_ACCOUNTS', 'accounts'),
                            (margin_totals, 'TOTAL_BALANCE', 'balance'),
                            (margin_totals, 'NPL_ACCOUNTS', 'npl_accounts'),
                            (margin_totals, 'NPL_BALANCE', 'npl_balance'),
                            (margin_totals, 'TOTAL_SPPL', 'sppl'),
                            (margin_totals, 'TOTAL_RECOVER', 'recover'),
                            (margin_totals, 'TOTAL_NETSP', 'netsp'),
                        ]:
                            total_dict[dict_key] += row[row_key]
                    
                    # Write MGINGRP totals
                    f.write("-" * 140 + "\n")
                    f.write(
                        f"MARGIN "
                        f"{margin_totals['accounts']:>12,}"
                        f"{margin_totals['balance']:>19,.2f}"
                        f"{margin_totals['npl_accounts']:>13,}"
                        f"{margin_totals['npl_balance']:>19,.2f}"
                        f"{margin_totals['sppl']:>20,.2f}"
                        f"{margin_totals['recover']:>19,.2f}"
                        f"{margin_totals['netsp']:>19,.2f}\n"
                    )
                    f.write("-" * 140 + "\n")
                    line_count += 3
                    
                    # Update NEWSEC totals
                    for key in sec_totals:
                        sec_totals[key] += margin_totals[key]
                
                # Write NEWSEC totals
                f.write(
                    f"NEW/SEC "
                    f"{sec_totals['accounts']:>12,}"
                    f"{sec_totals['balance']:>19,.2f}"
                    f"{sec_totals['npl_accounts']:>13,}"
                    f"{sec_totals['npl_balance']:>19,.2f}"
                    f"{sec_totals['sppl']:>20,.2f}"
                    f"{sec_totals['recover']:>19,.2f}"
                    f"{sec_totals['netsp']:>19,.2f}\n"
                )
                f.write("-" * 140 + "\n")
                line_count += 2
                
                # Update grand totals
                for key in grand_totals:
                    grand_totals[key] += sec_totals[key]
            
            # Write grand totals
            f.write(
                f"GRD TOT "
                f"{grand_totals['accounts']:>12,}"
                f"{grand_totals['balance']:>19,.2f}"
                f"{grand_totals['npl_accounts']:>13,}"
                f"{grand_totals['npl_balance']:>19,.2f}"
                f"{grand_totals['sppl']:>20,.2f}"
                f"{grand_totals['recover']:>19,.2f}"
                f"{grand_totals['netsp']:>19,.2f}\n"
            )
            f.write("-" * 140 + "\n")
        
        print(f"Detailed report saved: {output_path}")
        return True
        
    except Exception as e:
        print(f"Error generating detailed report: {e}")
        return False

def print_key_statistics(summary_df):
    """Print key statistics from the report"""
    
    # Calculate overall totals
    totals = summary_df.select([
        pl.sum("TOTAL_ACCOUNTS").alias("total_accounts"),
        pl.sum("TOTAL_BALANCE").alias("total_balance"),
        pl.sum("NPL_ACCOUNTS").alias("total_npl_accounts"),
        pl.sum("NPL_BALANCE").alias("total_npl_balance"),
        pl.sum("TOTAL_NETSP").alias("total_netsp"),
    ]).row(0)
    
    total_accounts, total_balance, npl_accounts, npl_balance, total_netsp = totals
    
    # Calculate ratios
    npl_ratio = (npl_balance / total_balance * 100) if total_balance > 0 else 0
    credit_charge_ratio = (total_netsp / total_balance * 100) if total_balance > 0 else 0
    
    print(f"\n{'='*60}")
    print(f"KEY STATISTICS")
    print(f"{'='*60}")
    print(f"Total Accounts:        {total_accounts:>12,}")
    print(f"Total Balance:         {total_balance:>12,.2f}")
    print(f"NPL Accounts:          {npl_accounts:>12,} ({npl_accounts/total_accounts*100:.1f}%)")
    print(f"NPL Balance:           {npl_balance:>12,.2f} ({npl_ratio:.2f}%)")
    print(f"Total Net SP:          {total_netsp:>12,.2f} ({credit_charge_ratio:.2f}%)")
    print(f"{'='*60}")
    
    # Print by NEWSEC
    print(f"\nBY NEWSEC:")
    newsec_summary = summary_df.group_by("NEWSEC").agg([
        pl.sum("TOTAL_ACCOUNTS").alias("accounts"),
        pl.sum("TOTAL_BALANCE").alias("balance"),
    ]).sort("balance", descending=True)
    
    for row in newsec_summary.iter_rows(named=True):
        print(f"  {row['NEWSEC']}: {row['accounts']:>6,} accounts, {row['balance']:>15,.2f}")

if __name__ == "__main__":
    eiqhpcr4()
