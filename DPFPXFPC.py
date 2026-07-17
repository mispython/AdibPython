import pandas as pd
import numpy as np
from functools import partial
import warnings
import os
from datetime import datetime
warnings.filterwarnings('ignore')

# Configuration - these would be defined elsewhere in your Python environment
REPTMON = '202401'  # Example value
NOWK = '4'          # Example value (Week number)
RDATE = '2024-01-31' # Report date

# Create output directory if it doesn't exist
OUTPUT_DIR = 'EL_Reports'
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Define the main processing function
def process_el_data(day, is_alternative=False):
    """
    Process EL data for a specific day
    is_alternative: boolean indicating if this is the alternative version (DAYI)
    """
    
    # Load datasets - these would be read from your data sources
    # For demonstration, we'll create sample dataframes
    def load_elg_gold(reptmon, nowk):
        # Mock data - replace with actual data loading
        return pd.DataFrame({
            'ELDAY': ['DAYA', 'DAYA', 'DAYB', 'DAYC', 'DAYD', 'DAYE', 'DAYF', 'DAYG', 'DAYH', 'DAYI'],
            'BNMCODE': ['4017100000000Y', '4019000000000Y', '4019100000000Y', '4013000000000Y',
                       '4314017000000Y', '3219910000000Y', '4410000000000Y', '4017100000000Y',
                       '4019000000000Y', '4019100000000Y'],
            'AMOUNT': [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000],
            'IDX': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'],
            'SIGN': ['+', '-', '+', '+', '-', '+', '-', '+', '+', '-'],
            'FMTNAME': ['RMEL', 'RMEL', 'RMEL', 'ELSRR', 'RMEL', 'RMEL', 'RMEL', 'RMEL', 'RMEL', 'RMEL'],
            'DESC': ['Description1', 'Description2', 'Description3', 'Description4',
                    'Description5', 'Description6', 'Description7', 'Description8',
                    'Description9', 'Description10']
        })
    
    def load_bnmk_tbl1(reptmon, nowk):
        return pd.DataFrame({
            'ELDAY': ['DAYA', 'DAYA', 'DAYB', 'DAYC', 'DAYD', 'DAYE', 'DAYF', 'DAYG', 'DAYH', 'DAYI'],
            'BNMCODE': ['3250000000000Y', '4017100000000Y', '4411100000000Y', '4414000000000Y',
                       '3219902000000Y', '3219903000000Y', '3219912000000Y', '4929980000000Y',
                       '4019000000000Y', '4413000000000Y'],
            'AMOUNT': [5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000, 14000],
            'NETAMT': [4500, 5500, 6500, 7500, 8500, 9500, 10500, 11500, 12500, 13500]
        })
    
    def load_bnmk_dci(reptmon, nowk):
        return pd.DataFrame({
            'ELDAY': ['DAYA', 'DAYB', 'DAYC', 'DAYD', 'DAYE', 'DAYF', 'DAYG', 'DAYH', 'DAYI'],
            'BNMCODE': ['3219902000000Y', '3219903000000Y', '3219912000000Y', '3219910000000Y',
                       '4411100000000Y', '4414000000000Y', '4413000000000Y', '4019000000000Y',
                       '4019100000000Y'],
            'AMOUNT': [9000, 10000, 11000, 12000, 13000, 14000, 15000, 16000, 17000]
        })
    
    def load_bnm_elw(reptmon, nowk):
        return pd.DataFrame({
            'ELDAY': ['DAYA', 'DAYA', 'DAYB', 'DAYC', 'DAYD', 'DAYE', 'DAYF', 'DAYG', 'DAYH', 'DAYI'],
            'BNMCODE': ['4929980000000Y', '3219902000000Y', '4411100000000Y', '4019000000000Y',
                       '3219903000000Y', '3219912000000Y', '4414000000000Y', '4413000000000Y',
                       '4017100000000Y', '4314017000000Y'],
            'AMOUNT': [-1000, -2000, -3000, -4000, -5000, -6000, -7000, -8000, -9000, -10000],
            'BRANCH': [1000, 2000, 3500, 4000, 1500, 2500, 4500, 5500, 1000, 2000]
        })
    
    def load_el_item():
        return pd.DataFrame({
            'BNMCODE': ['4017100000000Y', '4019000000000Y', '4019100000000Y', '4013000000000Y',
                       '4314017000000Y', '3219910000000Y', '4410000000000Y', '3250000000000Y',
                       '3219902000000Y', '3219903000000Y'],
            'FMTNAME': ['RMEL', 'RMEL', 'RMEA', 'ELSRR', 'RMEL', 'RMEL', 'RMEL', 'RMEL', 
                       'RMEL', 'RMEL'],
            'IDX': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'],
            'SIGN': ['+', '-', '+', '+', '-', '+', '-', '+', '+', '-'],
            'DESC': ['RM Marketable Securities', 'EL Item 2', 'EL Item 3', 'ELSRR Item',
                    'O/W RM IBB', 'EL Item 6', 'EL Item 7', 'EL Item 8',
                    'EL Item 9', 'EL Item 10']
        })
    
    # Load all datasets
    elg = load_elg_gold(REPTMON, NOWK)
    pmm_tbl1 = load_bnmk_tbl1(REPTMON, NOWK)
    pmm_dci = load_bnmk_dci(REPTMON, NOWK)
    elw1 = load_bnm_elw(REPTMON, NOWK)
    el_item = load_el_item()
    
    # Filter by day
    elg_day = elg[elg['ELDAY'] == day].copy()
    
    if is_alternative:
        # Alternative version (DAYI) - REP7 dataset
        pmm = pmm_tbl1[pmm_tbl1['ELDAY'] == day].copy()
        dci_day = pmm_dci[pmm_dci['ELDAY'] == day].copy()
        
        # Process REP7
        rep7 = pmm_tbl1[pmm_tbl1['ELDAY'] == day].copy()
        rep7.loc[rep7['BNMCODE'] == '3250000000000Y', 'AMOUNT'] = rep7['NETAMT']
        rep7['BNMCODE'] = '4017100000000Y'
        
        # Summarize REP7
        rep7_summary = rep7.groupby('BNMCODE', as_index=False)['AMOUNT'].sum()
        
        # Process ELW1
        elw_day = elw1[elw1['ELDAY'] == day][['BNMCODE', 'AMOUNT']].copy()
        elw_day['AMOUNT'] = elw_day['AMOUNT'].abs()
        
        # Remove specific branch
        elw_day = elw_day[~((elw_day['BNMCODE'] == '4929980000000Y') & 
                           (elw1['BRANCH'] > 3000))]
        
        # Process specific BNMCODE mappings
        def process_bnmcode_mapping(df):
            rows_to_append = []
            rows_to_remove = []
            
            for idx, row in df.iterrows():
                bnmcode = row['BNMCODE']
                if bnmcode in ['3219902000000Y', '3219903000000Y', '3219912000000Y']:
                    row_new = row.copy()
                    row_new['BNMCODE'] = '3219910000000Y'
                    rows_to_append.append(row_new)
                    rows_to_remove.append(idx)
                elif bnmcode in ['4411100000000Y', '4414000000000Y', '4413000000000Y']:
                    bnxcodes = {
                        '4411100000000Y': '4411100000000Y',
                        '4414000000000Y': '4414000000000Y',
                        '4413000000000Y': None
                    }
                    for code in ['4411100000000Y', '4414000000000Y']:
                        if code in bnxcodes and code != bnmcode:
                            row_new = row.copy()
                            row_new['BNMCODE'] = code
                            rows_to_append.append(row_new)
                    row_new = row.copy()
                    row_new['BNMCODE'] = '4410000000000Y'
                    rows_to_append.append(row_new)
                    rows_to_remove.append(idx)
                elif bnmcode == '4019000000000Y':
                    row_new1 = row.copy()
                    row_new1['BNMCODE'] = '4019100000000Y'
                    rows_to_append.append(row_new1)
                    row_new2 = row.copy()
                    row_new2['BNMCODE'] = '4019000000000Y'
                    row_new2['AMOUNT'] = 0.00
                    rows_to_append.append(row_new2)
                    rows_to_remove.append(idx)
            
            # Remove processed rows and append new ones
            df_new = df.drop(rows_to_remove).copy()
            for row in rows_to_append:
                df_new = pd.concat([df_new, pd.DataFrame([row])], ignore_index=True)
            
            return df_new
        
        elw_day = process_bnmcode_mapping(elw_day)
        
        # Combine all datasets
        elw_combined = pd.concat([
            elw_day,
            rep7_summary,
            dci_day[['BNMCODE', 'AMOUNT']],
            elg_day[['BNMCODE', 'AMOUNT']]
        ], ignore_index=True)
        
    else:
        # Original version
        pmm = pd.concat([
            pmm_tbl1[pmm_tbl1['ELDAY'] == day],
            pmm_dci[pmm_dci['ELDAY'] == day]
        ], ignore_index=True)
        pmm = pmm[['BNMCODE', 'AMOUNT']]
        
        # Process REP6
        rep6 = pmm_tbl1[pmm_tbl1['ELDAY'] == day].copy()
        rep6.loc[rep6['BNMCODE'] == '3250000000000Y', 'AMOUNT'] = rep6['NETAMT']
        rep6['BNMCODE'] = '4017100000000Y'
        rep6 = rep6[['BNMCODE', 'AMOUNT']]
        
        # Process ELW1
        elw_day = elw1[elw1['ELDAY'] == day][['BNMCODE', 'AMOUNT']].copy()
        elw_day['AMOUNT'] = elw_day['AMOUNT'].abs()
        
        # Remove specific branch
        elw_day = elw_day[~((elw_day['BNMCODE'] == '4929980000000Y') & 
                           (elw1['BRANCH'] > 3000))]
        
        # Process specific BNMCODE mappings
        def process_bnmcode_mapping(df):
            rows_to_append = []
            rows_to_remove = []
            
            for idx, row in df.iterrows():
                bnmcode = row['BNMCODE']
                if bnmcode in ['3219902000000Y', '3219903000000Y', '3219912000000Y']:
                    row_new = row.copy()
                    row_new['BNMCODE'] = '3219910000000Y'
                    rows_to_append.append(row_new)
                    rows_to_remove.append(idx)
                elif bnmcode in ['4411100000000Y', '4414000000000Y', '4413000000000Y']:
                    for code in ['4411100000000Y', '4414000000000Y']:
                        if code != bnmcode:
                            row_new = row.copy()
                            row_new['BNMCODE'] = code
                            rows_to_append.append(row_new)
                    row_new = row.copy()
                    row_new['BNMCODE'] = '4410000000000Y'
                    rows_to_append.append(row_new)
                    rows_to_remove.append(idx)
                elif bnmcode == '4019000000000Y':
                    row_new1 = row.copy()
                    row_new1['BNMCODE'] = '4019100000000Y'
                    rows_to_append.append(row_new1)
                    row_new2 = row.copy()
                    row_new2['BNMCODE'] = '4019000000000Y'
                    row_new2['AMOUNT'] = 0.00
                    rows_to_append.append(row_new2)
                    rows_to_remove.append(idx)
            
            df_new = df.drop(rows_to_remove).copy()
            for row in rows_to_append:
                df_new = pd.concat([df_new, pd.DataFrame([row])], ignore_index=True)
            
            return df_new
        
        elw_day = process_bnmcode_mapping(elw_day)
        
        # Combine all datasets
        elw_combined = pd.concat([
            elw_day,
            rep6,
            pmm,
            elg_day[['BNMCODE', 'AMOUNT']]
        ], ignore_index=True)
    
    # Summarize by BNMCODE
    elw_summary = elw_combined.groupby('BNMCODE', as_index=False)['AMOUNT'].sum()
    
    # Merge with ELITEM
    el_item_sorted = el_item.sort_values('BNMCODE').reset_index(drop=True)
    
    # Merge and process
    elw_final = el_item_sorted.merge(elw_summary, on='BNMCODE', how='inner')
    elw_final['AMOUNT'] = elw_final['AMOUNT'].fillna(0.00)
    elw_final['TOTAL'] = elw_final['AMOUNT']
    
    # Create FMTNAME
    elw_final['FMTNAME'] = elw_final['IDX'] + '-' + elw_final['FMTNAME']
    
    # Special handling for specific BNMCODE
    mask = elw_final['BNMCODE'] == '4314017000000Y'
    if mask.any():
        elw_final.loc[mask, 'DESC'] = 'O/W RM IBB FROM CAGAMAS ' + elw_final.loc[mask, 'AMOUNT'].apply(lambda x: f'{x:,.2f}')
        elw_final.loc[mask, 'AMOUNT'] = 0.00
        elw_final.loc[mask, 'TOTAL'] = 0.00
    
    # Calculate AMOUNX and TOTALX with sign handling
    elw_final['AMOUNX'] = elw_final['AMOUNT']
    elw_final['TOTALX'] = elw_final['TOTAL']
    mask_neg = elw_final['SIGN'] == '-'
    elw_final.loc[mask_neg, 'AMOUNX'] = -elw_final.loc[mask_neg, 'AMOUNT']
    elw_final.loc[mask_neg, 'TOTALX'] = elw_final.loc[mask_neg, 'AMOUNX']
    
    # Set description for specific BNMCODE
    mask_desc = elw_final['BNMCODE'] == '4017100000000Y'
    if mask_desc.any():
        elw_final.loc[mask_desc, 'DESC'] = 'TOTAL RM MARKETABLE SECURITIES'
    
    # Delete specific BNMCODEs based on week
    if NOWK != '4':
        elw_final = elw_final[~elw_final['BNMCODE'].isin(['4019000000000Y', '4019100000000Y'])]
    
    # Create ELWT dataset
    elwt = elw_final.copy()
    elwt['BNMCODE'] = '4013000000000Y'
    elwt['FMTNAME'] = 'E-ELSRR'
    elwt['SIGN'] = '+'
    elwt['DESC'] = 'ELIGIBLE LIABILITIES FOR SRR NEXT MONTH'
    
    # Handle sign conversion for specific IDX values
    mask_idx = elwt['IDX'].isin(['B', 'D'])
    elwt.loc[mask_idx, 'AMOUNX'] = -elwt.loc[mask_idx, 'AMOUNT']
    elwt.loc[mask_idx, 'TOTALX'] = elwt.loc[mask_idx, 'AMOUNX']
    
    # Summarize ELWT
    elwt_summary = elwt.groupby(['BNMCODE', 'FMTNAME', 'DESC', 'SIGN'], as_index=False).agg({
        'AMOUNT': 'sum',
        'TOTAL': 'sum',
        'AMOUNX': 'sum',
        'TOTALX': 'sum'
    })
    
    # Finalize ELWT
    elwt_final = elwt_summary.copy()
    elwt_final['AMOUNT'] = elwt_final['AMOUNX']
    elwt_final['TOTAL'] = elwt_final['TOTALX']
    
    # Combine ELWT and ELW
    elw_final_combined = pd.concat([
        elwt_final,
        elw_final
    ], ignore_index=True)
    
    # Sort
    elw_final_combined = elw_final_combined.sort_values(['FMTNAME', 'SIGN', 'BNMCODE']).reset_index(drop=True)
    
    # Generate formatted text file reports
    generate_formatted_text_report(elw_final_combined, day, is_alternative)
    
    return elw_final_combined

def generate_formatted_text_report(df, day, is_alternative=False):
    """Generate formatted text file reports similar to SAS PROC REPORT output"""
    
    # Create filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{OUTPUT_DIR}/EL_{day}_report_{timestamp}.txt"
    
    with open(filename, 'w') as f:
        # Write header
        f.write("="*100 + "\n")
        if is_alternative:
            f.write(f"DETAIL TOTAL ELIGIBLE LIABILITIES ITEMS FOR : {day} (ALTERNATIVE VERSION)\n")
        else:
            f.write(f"DETAIL TOTAL ELIGIBLE LIABILITIES ITEMS FOR : {day}\n")
        f.write(f"REPORT DATE : {RDATE}\n")
        f.write(f"REPORT GENERATED: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*100 + "\n\n")
        
        # Report 1: A-RMEL and B-RMEA
        mask = df['FMTNAME'].isin(['A-RMEL', 'B-RMEA'])
        report1 = df[mask].copy()
        
        if not report1.empty:
            f.write("="*100 + "\n")
            f.write("ELIGIBLE LIABILITIES DETAIL (A-RMEL, B-RMEA)\n")
            f.write("="*100 + "\n\n")
            
            # Header
            f.write(f"{'FMTNAME':<10} {'BNMCODE':<15} {'DESC':<45} {'SIGN':<5} {'AMOUNT':>15} {'TOTAL':>15} {'AMOUNX':>15} {'TOTALX':>15}\n")
            f.write("-"*135 + "\n")
            
            # Data rows
            for _, row in report1.iterrows():
                f.write(f"{row['FMTNAME']:<10} {row['BNMCODE']:<15} {row['DESC']:<45} "
                        f"{row['SIGN']:<5} {row['AMOUNT']:>15,.2f} {row['TOTAL']:>15,.2f} "
                        f"{row['AMOUNX']:>15,.2f} {row['TOTALX']:>15,.2f}\n")
            
            # Totals by FMTNAME
            f.write("-"*135 + "\n")
            for fmtname in report1['FMTNAME'].unique():
                subset = report1[report1['FMTNAME'] == fmtname]
                if not subset.empty:
                    total_amounx = subset['AMOUNX'].sum()
                    total_totalx = subset['TOTALX'].sum()
                    f.write(f"{'TOTAL FOR ' + fmtname:<50} {total_amounx:>30,.2f} {total_totalx:>30,.2f}\n")
                    f.write("-"*135 + "\n")
            
            f.write("\n")
        
        # Report 2: All other FMTNAMEs
        mask_other = ~df['FMTNAME'].isin(['A-RMEL', 'B-RMEA'])
        report2 = df[mask_other].copy()
        
        if not report2.empty:
            f.write("="*100 + "\n")
            f.write("ELIGIBLE LIABILITIES DETAIL (Other FMTNAMEs)\n")
            f.write("="*100 + "\n\n")
            
            # Header
            f.write(f"{'FMTNAME':<10} {'BNMCODE':<15} {'DESC':<45} {'SIGN':<5} {'AMOUNT':>15} {'TOTAL':>15}\n")
            f.write("-"*110 + "\n")
            
            # Data rows
            for _, row in report2.iterrows():
                f.write(f"{row['FMTNAME']:<10} {row['BNMCODE']:<15} {row['DESC']:<45} "
                        f"{row['SIGN']:<5} {row['AMOUNT']:>15,.2f} {row['TOTAL']:>15,.2f}\n")
            
            f.write("-"*110 + "\n\n")
        
        # Summary statistics
        f.write("="*100 + "\n")
        f.write("SUMMARY STATISTICS\n")
        f.write("="*100 + "\n\n")
        f.write(f"Total Records: {len(df)}\n")
        f.write(f"Unique FMTNAMEs: {df['FMTNAME'].nunique()}\n")
        f.write(f"Unique BNMCODEs: {df['BNMCODE'].nunique()}\n")
        
        # Summary by FMTNAME
        f.write("\nSummary by FMTNAME:\n")
        f.write("-"*50 + "\n")
        summary_by_fmt = df.groupby('FMTNAME').agg({
            'AMOUNT': ['sum', 'count', 'mean']
        }).round(2)
        f.write(summary_by_fmt.to_string())
        f.write("\n\n")
        
        f.write("="*100 + "\n")
        f.write("END OF REPORT\n")
        f.write("="*100 + "\n")
    
    print(f"Report generated: {filename}")
    return filename

# Main execution
if __name__ == "__main__":
    print("="*100)
    print("ELIGIBLE LIABILITIES REPORT GENERATOR")
    print("="*100)
    print(f"Report Date: {RDATE}")
    print(f"Report Month: {REPTMON}")
    print(f"Week: {NOWK}")
    print(f"Output Directory: {OUTPUT_DIR}")
    print("="*100)
    print()
    
    # Process all days
    days = ['DAYA', 'DAYB', 'DAYC', 'DAYD', 'DAYE', 'DAYF', 'DAYG', 'DAYH']
    
    generated_files = []
    
    for day in days:
        print(f"\nProcessing {day}...")
        result = process_el_data(day, is_alternative=False)
        if result is not None and not result.empty:
            # Save CSV as well for reference
            csv_file = f"{OUTPUT_DIR}/EL_{day}_data.csv"
            result.to_csv(csv_file, index=False)
            generated_files.append(csv_file)
    
    # Process DAYI with alternative version
    print(f"\nProcessing DAYI (Alternative Version)...")
    result_alt = process_el_data('DAYI', is_alternative=True)
    if result_alt is not None and not result_alt.empty:
        csv_file = f"{OUTPUT_DIR}/EL_DAYI_data.csv"
        result_alt.to_csv(csv_file, index=False)
        generated_files.append(csv_file)
    
    print("\n" + "="*100)
    print("REPORT GENERATION COMPLETE!")
    print("="*100)
    print(f"\nGenerated {len(generated_files)} files in '{OUTPUT_DIR}' directory:")
    for file in generated_files:
        print(f"  - {file}")
    print("\nCheck the text files for formatted reports and CSV files for data.")
    print("="*100)
