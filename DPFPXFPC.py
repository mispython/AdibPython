"""
EIMIR101 SAS to Python Conversion
Processes loan arrears reports with branch-level summaries
"""

from pathlib import Path
from datetime import date, timedelta
import pandas as pd
import pyreadstat
import numpy as np
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

# Setup paths
BASE_PATH = Path(".")
INPUT_PATH = BASE_PATH / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMIR101"
OUTPUT_PATH = BASE_PATH / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMIR101"
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# ============================================================================
# 1. REPTDATE Processing (using yesterday's date)
# ============================================================================

def process_repdate() -> Dict[str, str]:
    """Process REPTDATE using yesterday's date"""
    repdate = date.today() - timedelta(days=1)
    
    return {
        'RDATE': repdate.strftime("%d%m%y"),  # DDMMYY8.
        'REPTYEAR': str(repdate.year),        # YEAR4.
        'REPTMON': f"{repdate.month:02d}",    # Z2.
        'REPTDAY': f"{repdate.day:02d}",      # Z2.
        'REPTDATE': repdate
    }

# ============================================================================
# 2. Load and Process Loan Data
# ============================================================================

def load_loan_data() -> pd.DataFrame:
    """Load loan data from SAS file using pyreadstat"""
    loan_path = INPUT_PATH / "loantemp.sas7bdat"
    
    try:
        df, meta = pyreadstat.read_sas7bdat(loan_path)
        print(f"Loaded {len(df)} records from loantemp.sas7bdat")
        print(f"Columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        print(f"Error loading SAS file: {e}")
        return pd.DataFrame()

def load_branch_data() -> pd.DataFrame:
    """Load branch header data from fixed-width flat file"""
    branch_path = INPUT_PATH / "LKP_BRANCH"
    
    if branch_path.exists():
        try:
            # Parse fixed-width file (LRECL=80)
            with open(branch_path, 'r') as f:
                lines = f.readlines()
            
            # Parse each line based on fixed-width format
            data = []
            for line in lines:
                line = line.strip('\n').ljust(80)
                
                # Extract fields - adjust positions based on actual format
                branch = line[0:3].strip()
                brhcode = line[3:10].strip()
                
                if branch and brhcode:
                    try:
                        data.append({
                            'BRANCH': int(branch),
                            'BRHCODE': brhcode
                        })
                    except ValueError:
                        continue
            
            df = pd.DataFrame(data)
            print(f"Loaded {len(df)} records from LKP_BRANCH")
            return df
        except Exception as e:
            print(f"Error loading branch file: {e}")
            return pd.DataFrame()
    else:
        print("Branch file not found, creating default branch mapping")
        return pd.DataFrame()

def create_default_branches(loan_df: pd.DataFrame) -> pd.DataFrame:
    """Create default branch data from loan data if branch file doesn't exist"""
    if 'BRANCH' not in loan_df.columns:
        return pd.DataFrame()
    
    unique_branches = loan_df['BRANCH'].unique()
    branch_data = []
    for branch in unique_branches:
        if pd.notna(branch):
            branch_data.append({
                'BRANCH': int(branch),
                'BRHCODE': f"BR{int(branch):03d}"
            })
    
    df = pd.DataFrame(branch_data)
    print(f"Created {len(df)} default branch records")
    return df

# ============================================================================
# 3. Categorize Loans
# ============================================================================

def categorize_loans(loan_df: pd.DataFrame, hpd_list: List[str]) -> pd.DataFrame:
    """Categorize loans into different types (A,B,C,D)"""
    
    if loan_df.empty:
        return pd.DataFrame()
    
    # Filter: BALANCE > 0 AND BORSTAT != 'Z'
    filtered_df = loan_df[
        (loan_df.get('BALANCE', 0) > 0) & 
        (loan_df.get('BORSTAT', '') != 'Z')
    ].copy()
    
    print(f"Filtered to {len(filtered_df)} records (BALANCE>0 and BORSTAT!='Z')")
    
    if filtered_df.empty:
        return pd.DataFrame()
    
    # Prepare HPD numbers
    hpd_numbers = [int(x.strip("'")) for x in hpd_list]
    
    # Create categorized rows
    categorized_rows = []
    
    # Category A: (HPD-C) - Products 380, 381, 700, 705, 720, 725
    cat_a = filtered_df[filtered_df['PRODUCT'].isin([380, 381, 700, 705, 720, 725])].copy()
    if not cat_a.empty:
        cat_a['CAT'] = 'A'
        cat_a['TYPE'] = '(HPD-C)'
        categorized_rows.append(cat_a)
        print(f"  Category A: {len(cat_a)} records")
    
    # Category B: (HP 380/381)
    cat_b = filtered_df[filtered_df['PRODUCT'].isin([380, 381])].copy()
    if not cat_b.empty:
        cat_b['CAT'] = 'B'
        cat_b['TYPE'] = '(HP 380/381)'
        categorized_rows.append(cat_b)
        print(f"  Category B: {len(cat_b)} records")
    
    # Category C: (AITAB)
    cat_c = filtered_df[filtered_df['PRODUCT'].isin([128, 130, 131, 132])].copy()
    if not cat_c.empty:
        cat_c['CAT'] = 'C'
        cat_c['TYPE'] = '(AITAB)'
        categorized_rows.append(cat_c)
        print(f"  Category C: {len(cat_c)} records")
    
    # Category D: (-HPD-)
    cat_d = filtered_df[filtered_df['PRODUCT'].isin(hpd_numbers)].copy()
    if not cat_d.empty:
        cat_d['CAT'] = 'D'
        cat_d['TYPE'] = '(-HPD-)'
        categorized_rows.append(cat_d)
        print(f"  Category D: {len(cat_d)} records")
    
    # Combine all categories
    if categorized_rows:
        combined = pd.concat(categorized_rows, ignore_index=True)
        print(f"Total categorized: {len(combined)} records")
        return combined
    else:
        return pd.DataFrame()

# ============================================================================
# 4. Merge with Branch Data
# ============================================================================

def merge_branch_data(loan_df: pd.DataFrame, branch_df: pd.DataFrame) -> pd.DataFrame:
    """Merge loan data with branch header data"""
    if loan_df.empty:
        return loan_df
    
    # If no branch data, create default
    if branch_df.empty:
        branch_df = create_default_branches(loan_df)
    
    if branch_df.empty:
        print("No branch data available, proceeding without branch merge")
        loan_df['BRHCODE'] = loan_df['BRANCH'].apply(lambda x: f"BR{int(x):03d}" if pd.notna(x) else '')
        return loan_df
    
    # Inner join on BRANCH
    merged = loan_df.merge(branch_df, on='BRANCH', how='left')
    
    # Fill missing BRHCODE with default
    if 'BRHCODE' in merged.columns:
        merged['BRHCODE'] = merged['BRHCODE'].fillna(
            merged['BRANCH'].apply(lambda x: f"BR{int(x):03d}" if pd.notna(x) else '')
        )
    else:
        merged['BRHCODE'] = merged['BRANCH'].apply(lambda x: f"BR{int(x):03d}" if pd.notna(x) else '')
    
    print(f"Merged records: {len(merged)}")
    return merged

# ============================================================================
# 5. Create Branch-Level Summaries
# ============================================================================

def calculate_branch_summaries(loan_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate branch-level summaries for arrears buckets"""
    
    if loan_df.empty:
        return pd.DataFrame()
    
    # Check if ARREAR2 exists
    if 'ARREAR2' not in loan_df.columns:
        print("ARREAR2 column not found, using ARREAR instead")
        if 'ARREAR' in loan_df.columns:
            loan_df['ARREAR2'] = loan_df['ARREAR']
        else:
            print("No arrears column found, using default value 0")
            loan_df['ARREAR2'] = 0
    
    # Filter arrears buckets 1-14
    filtered = loan_df[
        (loan_df['ARREAR2'] >= 1) & (loan_df['ARREAR2'] <= 14)
    ].copy()
    
    if filtered.empty:
        print("No records in arrears buckets 1-14")
        return pd.DataFrame()
    
    print(f"Records in arrears buckets 1-14: {len(filtered)}")
    
    # Use ACCTNO as account identifier, or NOTENO, or create index
    account_col = 'ACCTNO' if 'ACCTNO' in filtered.columns else ('NOTENO' if 'NOTENO' in filtered.columns else None)
    
    # Group by CAT, BRANCH, ARREAR2
    agg_dict = {
        'BALANCE': 'sum'
    }
    
    # Add BRHCODE if exists
    if 'BRHCODE' in filtered.columns:
        agg_dict['BRHCODE'] = 'first'
    
    # Add TYPE if exists
    if 'TYPE' in filtered.columns:
        agg_dict['TYPE'] = 'first'
    
    # Add account count if account column exists
    if account_col:
        agg_dict[account_col] = 'count'
    
    grouped = filtered.groupby(['CAT', 'BRANCH', 'ARREAR2']).agg(agg_dict).reset_index()
    
    # Rename columns
    rename_map = {
        'BALANCE': 'BRHAMT'
    }
    if account_col:
        rename_map[account_col] = 'NOACC'
    else:
        # If no account column, use count of records
        grouped['NOACC'] = grouped.groupby(['CAT', 'BRANCH', 'ARREAR2']).cumcount() + 1
    
    grouped = grouped.rename(columns=rename_map)
    
    # Pivot to get arrears buckets as columns
    # Amount pivot
    amount_pivot = grouped.pivot_table(
        index=['CAT', 'BRANCH'] + (['BRHCODE'] if 'BRHCODE' in grouped.columns else []) + (['TYPE'] if 'TYPE' in grouped.columns else []),
        columns='ARREAR2',
        values='BRHAMT',
        fill_value=0
    ).reset_index()
    
    # Count pivot
    count_pivot = grouped.pivot_table(
        index=['CAT', 'BRANCH'] + (['BRHCODE'] if 'BRHCODE' in grouped.columns else []) + (['TYPE'] if 'TYPE' in grouped.columns else []),
        columns='ARREAR2',
        values='NOACC',
        fill_value=0
    ).reset_index()
    
    # Rename columns
    amount_pivot.columns = ['CAT', 'BRANCH'] + \
        (['BRHCODE'] if 'BRHCODE' in grouped.columns else []) + \
        (['TYPE'] if 'TYPE' in grouped.columns else []) + \
        [f'BRHAMT{i}' for i in range(1, 15) if i in amount_pivot.columns]
    
    count_pivot.columns = ['CAT', 'BRANCH'] + \
        (['BRHCODE'] if 'BRHCODE' in grouped.columns else []) + \
        (['TYPE'] if 'TYPE' in grouped.columns else []) + \
        [f'NOACC{i}' for i in range(1, 15) if i in count_pivot.columns]
    
    # Determine merge keys
    merge_keys = ['CAT', 'BRANCH']
    if 'BRHCODE' in grouped.columns:
        merge_keys.append('BRHCODE')
    if 'TYPE' in grouped.columns:
        merge_keys.append('TYPE')
    
    # Merge amount and count
    result = amount_pivot.merge(
        count_pivot,
        on=merge_keys,
        how='outer'
    )
    
    # Ensure all columns exist (1-14)
    for i in range(1, 15):
        if f'BRHAMT{i}' not in result.columns:
            result[f'BRHAMT{i}'] = 0
        if f'NOACC{i}' not in result.columns:
            result[f'NOACC{i}'] = 0
    
    # Fill NaN with 0
    result = result.fillna(0)
    
    # Sort
    sort_cols = ['CAT', 'BRANCH']
    result = result.sort_values(sort_cols)
    
    print(f"Created summary with {len(result)} branch records")
    return result

# ============================================================================
# 6. Calculate Subtotals and Totals
# ============================================================================

def calculate_totals(branch_summary: pd.DataFrame) -> Dict:
    """Calculate various subtotals and totals"""
    
    if branch_summary.empty:
        return {}
    
    results = {}
    
    for cat in branch_summary['CAT'].unique():
        cat_data = branch_summary[branch_summary['CAT'] == cat]
        
        # Initialize arrays for totals
        totamt = [0.0] * 15  # 1-14 (index 0 unused)
        totacc = [0] * 15
        
        # Sum across all branches in category
        for i in range(1, 15):
            col_amt = f'BRHAMT{i}'
            col_acc = f'NOACC{i}'
            if col_amt in cat_data.columns:
                totamt[i] = cat_data[col_amt].sum()
            if col_acc in cat_data.columns:
                totacc[i] = cat_data[col_acc].sum()
        
        # Calculate subtotals (matching SAS logic)
        subbrh = sum(totamt[4:15])  # 4-14 inclusive
        subbr2 = sum(totamt[7:15])  # 7-14 inclusive
        subacc = sum(totacc[4:15])
        subac2 = sum(totacc[7:15])
        totbrh = subbrh + sum(totamt[1:4])
        sotacc = subacc + sum(totacc[1:4])
        
        # Category totals
        sgtotbrh = sum(totamt[4:15])
        sgtotbr2 = sum(totamt[7:15])
        sgtotacc = sum(totacc[4:15])
        sgtotac2 = sum(totacc[7:15])
        gtotbrh = sgtotbrh + sum(totamt[1:4])
        gtotacc = sgtotacc + sum(totacc[1:4])
        
        results[cat] = {
            'branch_summary': cat_data,
            'totamt': totamt,
            'totacc': totacc,
            'subbrh': subbrh,
            'subbr2': subbr2,
            'subacc': subacc,
            'subac2': subac2,
            'totbrh': totbrh,
            'sotacc': sotacc,
            'sgtotbrh': sgtotbrh,
            'sgtotbr2': sgtotbr2,
            'sgtotacc': sgtotacc,
            'sgtotac2': sgtotac2,
            'gtotbrh': gtotbrh,
            'gtotacc': gtotacc
        }
    
    return results

# ============================================================================
# 7. Generate Reports
# ============================================================================

def generate_report_a(loan_data: pd.DataFrame, variables: Dict) -> pd.DataFrame:
    """Generate first report (EIMAR101-A)"""
    report_df = loan_data.copy()
    report_df['PROGID'] = 'EIMAR101-A'
    return report_df

def generate_report_b(loan_data: pd.DataFrame, variables: Dict, hpd_list: List[str]) -> pd.DataFrame:
    """Generate second report (EIMAR101-B) with exclusions"""
    
    if loan_data.empty:
        return pd.DataFrame()
    
    hpd_numbers = [int(x.strip("'")) for x in hpd_list]
    
    filtered = loan_data[
        (~loan_data['TYPE'].isin(['(AITAB)', '(-HPD-)'])) &
        (~loan_data['BORSTAT'].isin(['F', 'I', 'R'])) &
        (loan_data['PRODUCT'].isin(hpd_numbers))
    ].copy()
    
    filtered['PROGID'] = 'EIMAR101-B'
    print(f"Report B filtered records: {len(filtered)}")
    
    return filtered

# ============================================================================
# 8. Write Text Outputs
# ============================================================================

def write_report_txt(branch_summary: pd.DataFrame, report_name: str, variables: Dict):
    """Write main report as text file"""
    txt_path = OUTPUT_PATH / f"{report_name}.txt"
    
    if branch_summary.empty:
        print(f"No data for {report_name}")
        return
    
    with open(txt_path, 'w') as f:
        # Write header
        f.write("=" * 120 + "\n")
        f.write(f"EIMIR101 {report_name} - Loan Arrears Report\n")
        f.write(f"Report Date: {variables['RDATE']} ({variables['REPTDATE']})\n")
        f.write("=" * 120 + "\n\n")
        
        # Write column headers
        header = "CAT\tBRANCH\tBRHCODE\tTYPE"
        for i in range(1, 15):
            header += f"\tNOACC{i}\tBRHAMT{i}"
        f.write(header + "\n")
        f.write("-" * 120 + "\n")
        
        # Write data rows
        for _, row in branch_summary.iterrows():
            line = f"{row.get('CAT', '')}\t{row.get('BRANCH', '')}\t{row.get('BRHCODE', '')}\t{row.get('TYPE', '')}"
            for i in range(1, 15):
                noacc = row.get(f'NOACC{i}', 0)
                brhamt = row.get(f'BRHAMT{i}', 0)
                line += f"\t{int(noacc)}\t{brhamt:,.2f}"
            f.write(line + "\n")
        
        f.write("-" * 120 + "\n")
        f.write(f"Total branches: {len(branch_summary)}\n")
        f.write("=" * 120 + "\n")
    
    print(f"✓ Report saved to: {txt_path}")

def write_detailed_txt(branch_summary: pd.DataFrame, variables: Dict):
    """Write detailed summary text file (LOAN7A equivalent)"""
    txt_path = OUTPUT_PATH / "EIMIR101_DETAILED.txt"
    
    if branch_summary.empty:
        print("No data for detailed summary")
        return
    
    with open(txt_path, 'w') as f:
        # Write header
        f.write("=" * 120 + "\n")
        f.write("EIMIR101 DETAILED - Loan Arrears Detailed Report\n")
        f.write(f"Report Date: {variables['RDATE']} ({variables['REPTDATE']})\n")
        f.write("=" * 120 + "\n\n")
        
        # Write column headers
        header = "BRHCODE\tTYPE"
        for i in range(1, 15):
            header += f"\tNOACC{i}\tBRHAMT{i}"
        f.write(header + "\n")
        f.write("-" * 120 + "\n")
        
        # Write data rows (no CAT column, detailed by branch)
        for _, row in branch_summary.iterrows():
            line = f"{row.get('BRHCODE', '')}\t{row.get('TYPE', '')}"
            for i in range(1, 15):
                noacc = row.get(f'NOACC{i}', 0)
                brhamt = row.get(f'BRHAMT{i}', 0)
                line += f"\t{int(noacc)}\t{brhamt:,.2f}"
            f.write(line + "\n")
        
        f.write("-" * 120 + "\n")
        f.write(f"Total branches: {len(branch_summary)}\n")
        f.write("=" * 120 + "\n")
    
    print(f"✓ Detailed report saved to: {txt_path}")

def write_summary_txt(branch_summary: pd.DataFrame, totals: Dict, report_name: str, variables: Dict):
    """Write summary with totals as text file"""
    txt_path = OUTPUT_PATH / f"{report_name}_SUMMARY.txt"
    
    if branch_summary.empty:
        print(f"No data for {report_name}")
        return
    
    with open(txt_path, 'w') as f:
        # Write header
        f.write("=" * 120 + "\n")
        f.write(f"EIMIR101 {report_name} - Summary Report with Totals\n")
        f.write(f"Report Date: {variables['RDATE']} ({variables['REPTDATE']})\n")
        f.write("=" * 120 + "\n\n")
        
        # Write summary by category
        for cat, cat_totals in totals.items():
            f.write(f"\nCategory {cat}: {cat_totals.get('branch_summary', pd.DataFrame())['TYPE'].iloc[0] if not cat_totals.get('branch_summary', pd.DataFrame()).empty else ''}\n")
            f.write("-" * 80 + "\n")
            
            # Write arrears buckets
            f.write("Arrears Bucket\t")
            for i in range(1, 15):
                f.write(f"{i}\t")
            f.write("\n")
            
            f.write("Amount\t\t")
            for i in range(1, 15):
                f.write(f"{cat_totals['totamt'][i]:,.2f}\t")
            f.write("\n")
            
            f.write("Count\t\t")
            for i in range(1, 15):
                f.write(f"{cat_totals['totacc'][i]}\t")
            f.write("\n")
            
            # Write subtotals
            f.write("\nSubtotals:\n")
            f.write(f"  SUBBRH (Buckets 4-14): {cat_totals['subbrh']:,.2f}\n")
            f.write(f"  SUBBR2 (Buckets 7-14): {cat_totals['subbr2']:,.2f}\n")
            f.write(f"  SUBACC (Count 4-14): {cat_totals['subacc']}\n")
            f.write(f"  SUBAC2 (Count 7-14): {cat_totals['subac2']}\n")
            f.write(f"  TOTBRH (Buckets 1-14): {cat_totals['totbrh']:,.2f}\n")
            f.write(f"  SOTACC (Count 1-14): {cat_totals['sotacc']}\n")
            f.write(f"  GTOTBRH (Grand Total Amount): {cat_totals['gtotbrh']:,.2f}\n")
            f.write(f"  GTOTACC (Grand Total Count): {cat_totals['gtotacc']}\n")
            f.write("-" * 80 + "\n")
        
        f.write("\n" + "=" * 120 + "\n")
        f.write(f"Total categories: {len(totals)}\n")
        f.write(f"Total branches: {len(branch_summary)}\n")
        f.write("=" * 120 + "\n")
    
    print(f"✓ Summary report saved to: {txt_path}")

# ============================================================================
# 9. Main Execution
# ============================================================================

def main():
    """Main execution function"""
    print("=" * 60)
    print("EIMIR101 SAS to Python Conversion")
    print("=" * 60)
    
    # HPD list (would come from macro variable &HPD)
    HPD_LIST = ["110", "115", "700", "705", "720", "725"]
    
    # 1. Process REPTDATE (using yesterday)
    print("\n1. Processing REPTDATE (yesterday)...")
    variables = process_repdate()
    print(f"   Report Date: {variables['RDATE']} ({variables['REPTDATE']})")
    
    # 2. Load data
    print("\n2. Loading data...")
    loan_df = load_loan_data()
    branch_df = load_branch_data()
    print(f"   Loans: {len(loan_df)}, Branches: {len(branch_df)}")
    
    if loan_df.empty:
        print("ERROR: No loan data loaded")
        return
    
    # 3. Categorize loans
    print("\n3. Categorizing loans...")
    categorized = categorize_loans(loan_df, HPD_LIST)
    
    if categorized.empty:
        print("ERROR: No categorized records")
        return
    
    # 4. Merge with branch data
    print("\n4. Merging with branch data...")
    merged_data = merge_branch_data(categorized, branch_df)
    
    # 5. Generate Report A
    print("\n5. Generating Report A (EIMAR101-A)...")
    report_a = generate_report_a(merged_data, variables)
    summary_a = calculate_branch_summaries(report_a)
    totals_a = calculate_totals(summary_a)
    print(f"   Report A: {len(report_a)} records, {len(summary_a)} branch summaries")
    
    # 6. Generate Report B
    print("\n6. Generating Report B (EIMAR101-B)...")
    report_b = generate_report_b(merged_data, variables, HPD_LIST)
    summary_b = calculate_branch_summaries(report_b)
    totals_b = calculate_totals(summary_b)
    print(f"   Report B: {len(report_b)} records, {len(summary_b)} branch summaries")
    
    # 7. Write text outputs
    print("\n7. Writing text reports...")
    
    # Write Report A
    if not summary_a.empty:
        write_report_txt(summary_a, "EIMIR101-A", variables)
        write_summary_txt(summary_a, totals_a, "EIMIR101-A", variables)
    
    # Write Report B
    if not summary_b.empty:
        write_report_txt(summary_b, "EIMIR101-B", variables)
        write_summary_txt(summary_b, totals_b, "EIMIR101-B", variables)
        
        # Write detailed report (from Report B)
        write_detailed_txt(summary_b, variables)
    
    # 8. Save parquet outputs for reference (optional)
    print("\n8. Saving parquet files for reference...")
    if not merged_data.empty:
        merged_data.to_parquet(OUTPUT_PATH / "LOANTEMP_CATEGORIZED.parquet")
    
    if not summary_a.empty:
        summary_a.to_parquet(OUTPUT_PATH / "REPORT_A_SUMMARY.parquet")
    if not summary_b.empty:
        summary_b.to_parquet(OUTPUT_PATH / "REPORT_B_SUMMARY.parquet")
    
    # Save variables
    variables_df = pd.DataFrame([variables])
    variables_df.to_parquet(OUTPUT_PATH / "EIMIR101_VARIABLES.parquet")
    
    # Save totals calculations
    totals_data = []
    for cat, cat_totals in totals_a.items():
        totals_data.append({
            "CAT": cat,
            "REPORT": "A",
            **{k: str(v) for k, v in cat_totals.items() if k != 'branch_summary'}
        })
    
    for cat, cat_totals in totals_b.items():
        totals_data.append({
            "CAT": cat,
            "REPORT": "B",
            **{k: str(v) for k, v in cat_totals.items() if k != 'branch_summary'}
        })
    
    if totals_data:
        totals_df = pd.DataFrame(totals_data)
        totals_df.to_parquet(OUTPUT_PATH / "TOTALS_CALCULATIONS.parquet")
    
    print("\n" + "=" * 60)
    print("CONVERSION COMPLETE")
    print("=" * 60)
    print(f"Report A records: {len(report_a)}")
    print(f"Report B records: {len(report_b)}")
    print(f"Categories processed: {len(totals_a) + len(totals_b)}")
    print(f"Output saved to: {OUTPUT_PATH}")
    print("\nText Report Files:")
    for file in sorted(OUTPUT_PATH.glob("*.txt")):
        print(f"  - {file.name}")
    print("\nParquet Files (for reference):")
    for file in sorted(OUTPUT_PATH.glob("*.parquet")):
        print(f"  - {file.name}")

# ============================================================================
# 10. Run the conversion
# ============================================================================

if __name__ == "__main__":
    main()
