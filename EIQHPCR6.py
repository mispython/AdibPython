import polars as pl
from pathlib import Path

def eiqhpcr6():
    """Generate Make of Vehicle Report - Exclude 983 & 993"""
    
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
    
    # Create CLSTIT column (NEWSEC||GOODS||CATGY||CARS||MAKE)
    hploan_df = hploan_df.with_columns(
        (pl.col("NEWSEC").cast(pl.Utf8) + 
         pl.col("GOODS").cast(pl.Utf8) + 
         pl.col("CATGY").cast(pl.Utf8) + 
         pl.col("CARS").cast(pl.Utf8) + 
         pl.col("MAKE").cast(pl.Utf8)).alias("CLSTIT")
    )
    
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
    ln1_summary = loan1_df.group_by(["CLSTIT", "BRABBR"]).agg([
        pl.count().alias("NOACC"),
        pl.sum("BALANCE").alias("BALANCE"),
        pl.sum("SPPL").alias("SPPL"),
        pl.sum("RECOVER").alias("RECOVER"),
        pl.sum("NETSP").alias("NETSP")
    ])
    
    # Generate summary for NPL loans (LOAN2)
    ln2_summary = loan2_df.group_by(["CLSTIT", "BRABBR"]).agg([
        pl.count().alias("NPACC"),
        pl.sum("BALANCE").alias("NPBAL")
    ])
    
    # Merge summaries
    ln3_df = ln1_summary.join(
        ln2_summary, 
        on=["CLSTIT", "BRABBR"], 
        how="left"
    )
    
    # Calculate ratios
    ln3_df = ln3_df.with_columns([
        (pl.col("NPBAL") / pl.col("BALANCE") * 100).alias("RATIO").round(2),
        (pl.col("NETSP") / pl.col("BALANCE") * 100).alias("CREDIT").round(2)
    ]).fill_null(0)
    
    # Sort data
    ln3_df = ln3_df.sort(["CLSTIT", "BRABBR"])
    
    # Generate detailed report
    output_path = base / f"HPTXT_VEHICLE_{rdate}.txt"
    generate_vehicle_report(ln3_df, rdate, output_path)
    
    # Print summary statistics
    print_vehicle_statistics(ln3_df, loan1_df, loan2_df)
    
    # Save CSV version for analysis
    csv_path = base / f"HP_VEHICLE_SUMMARY_{rdate}.csv"
    ln3_df.write_csv(csv_path)
    print(f"\nCSV summary saved: {csv_path}")
    
    return ln3_df

def generate_vehicle_report(ln3_df, rdate, output_path):
    """Generate detailed vehicle make report"""
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            page_num = 1
            line_count = 0
            rpttitle = "MAKE OF VEHICLE - EXCLUDE 983 & 993"
            
            def write_page_header():
                nonlocal page_num, line_count
                f.write(f"{'PROGRAM-ID : EIARPT05':<50}{f'PAGE NO.: {page_num}':>68}\n")
                f.write("PUBLIC  BANK  BERHAD - CCD (ATTN:JENNY YAP)\n")
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
            
            # Initialize grand totals
            tacc = tbals = tnplacc = tnplbals = tsppl = trecover = tnets = 0
            
            # Group by CLSTIT (Vehicle classification)
            clstit_groups = ln3_df["CLSTIT"].unique().to_list()
            
            for clstit in clstit_groups:
                clstit_df = ln3_df.filter(pl.col("CLSTIT") == clstit)
                
                # Check page break (56 lines per page as per SAS code)
                if line_count > 56:
                    page_num += 1
                    f.write("\f\n")
                    write_page_header()
                
                # Write CLSTIT header
                f.write(f"{clstit}\n")
                line_count += 1
                
                # CLSTIT totals
                cacc = cbals = cnplacc = cnplbals = csppl = crecover = cnetsp = 0
                
                # Process each branch within CLSTIT
                for row in clstit_df.iter_rows(named=True):
                    # Check page break
                    if line_count > 56:
                        page_num += 1
                        f.write("\f\n")
                        write_page_header()
                        f.write(f"{clstit}\n")
                        line_count += 1
                    
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
                    # CLSTIT totals
                    cacc += row['NOACC']
                    cbals += row['BALANCE']
                    cnplacc += row['NPACC']
                    cnplbals += row['NPBAL']
                    csppl += row['SPPL']
                    crecover += row['RECOVER']
                    cnetsp += row['NETSP']
                    
                    # Grand totals
                    tacc += row['NOACC']
                    tbals += row['BALANCE']
                    tnplacc += row['NPACC']
                    tnplbals += row['NPBAL']
                    tsppl += row['SPPL']
                    trecover += row['RECOVER']
                    tnets += row['NETSP']
                
                # Write CLSTIT subtotal (using 'SUB' label as per SAS code)
                f.write(f"SUB    "
                       f"{cacc:>7,}"
                       f"{cbals:>16,.2f}"
                       f"{cnplacc:>8,}"
                       f"{cnplbals:>16,.2f}"
                       f"{csppl:>16,.2f}"
                       f"{crecover:>16,.2f}"
                       f"{cnetsp:>16,.2f}\n")
                f.write("-" * 40 + "-" * 40 + "-" * 40 + "-" * 25 + "\n")
                line_count = 2  # Reset line count after each CLSTIT as per SAS code
            
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

def print_vehicle_statistics(ln3_df, loan1_df, loan2_df):
    """Print vehicle make statistics"""
    
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
    print(f"MAKE OF VEHICLE REPORT SUMMARY")
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
    
    # Get top vehicle makes by balance
    print(f"\nTOP 10 VEHICLE MAKES BY BALANCE:")
    
    # Group by CLSTIT for vehicle analysis
    vehicle_summary = ln3_df.group_by("CLSTIT").agg([
        pl.sum("NOACC").alias("accounts"),
        pl.sum("BALANCE").alias("balance"),
        pl.sum("NPACC").alias("npl_accounts"),
        (pl.sum("NPBAL") / pl.sum("BALANCE") * 100).alias("npl_ratio"),
    ]).sort("balance", descending=True).head(10)
    
    for i, row in enumerate(vehicle_summary.iter_rows(named=True), 1):
        clstit_display = row['CLSTIT'] if len(row['CLSTIT']) <= 15 else row['CLSTIT'][:12] + "..."
        print(f"  {i:2d}. {clstit_display:<15} {row['accounts']:>6,} accounts, "
              f"{row['balance']:>15,.2f}, NPL: {row['npl_ratio']:.1f}%")

def analyze_vehicle_components(hploan_df):
    """Analyze components of CLSTIT for better understanding"""
    print(f"\n{'='*60}")
    print(f"VEHICLE COMPOSITION ANALYSIS")
    print(f"{'='*60}")
    
    # Count unique values in each component
    components = ["NEWSEC", "GOODS", "CATGY", "CARS", "MAKE"]
    for comp in components:
        unique_vals = hploan_df[comp].unique().len()
        sample_vals = hploan_df[comp].unique().head(5).to_list()
        print(f"{comp:>10}: {unique_vals:>3} unique values")
        if unique_vals <= 10:
            print(f"           Values: {sample_vals}")
    
    # Show CLSTIT construction example
    sample = hploan_df.head(1)
    if len(sample) > 0:
        print(f"\nCLSTIT construction example:")
        print(f"  NEWSEC: {sample['NEWSEC'][0]}")
        print(f"  GOODS:  {sample['GOODS'][0]}")
        print(f"  CATGY:  {sample['CATGY'][0]}")
        print(f"  CARS:   {sample['CARS'][0]}")
        print(f"  MAKE:   {sample['MAKE'][0]}")
        print(f"  CLSTIT: {sample['NEWSEC'][0]}{sample['GOODS'][0]}{sample['CATGY'][0]}{sample['CARS'][0]}{sample['MAKE'][0]}")

if __name__ == "__main__":
    result = eiqhpcr6()
    if result is not None:
        print("\nAnalysis completed successfully!")
