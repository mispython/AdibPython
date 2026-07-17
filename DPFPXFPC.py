import pandas as pd
import numpy as np
from functools import partial
import warnings
warnings.filterwarnings('ignore')

# Configuration - these would be defined elsewhere in your Python environment
REPTMON = '202401'  # Example value
NOWK = '4'          # Example value (Week number)
RDATE = '2024-01-31' # Report date

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
            'ELDAY': ['DAYA', 'DAYA', 'DAYB', 'DAYC'],
            'BNMCODE': ['4017100000000Y', '4019000000000Y', '4019100000000Y', '4013000000000Y'],
            'AMOUNT': [1000, 2000, 3000, 4000],
            'IDX': ['A', 'B', 'C', 'D'],
            'SIGN': ['+', '-', '+', '+'],
            'FMTNAME': ['RMEL', 'RMEL', 'RMEL', 'ELSRR'],
            'DESC': ['Description1', 'Description2', 'Description3', 'Description4']
        })
    
    def load_bnmk_tbl1(reptmon, nowk):
        return pd.DataFrame({
            'ELDAY': ['DAYA', 'DAYA', 'DAYB', 'DAYC'],
            'BNMCODE': ['3250000000000Y', '4017100000000Y', '4411100000000Y', '4414000000000Y'],
            'AMOUNT': [5000, 6000, 7000, 8000],
            'NETAMT': [4500, 5500, 6500, 7500]
        })
    
    def load_bnmk_dci(reptmon, nowk):
        return pd.DataFrame({
            'ELDAY': ['DAYA', 'DAYB', 'DAYC', 'DAYD'],
            'BNMCODE': ['3219902000000Y', '3219903000000Y', '3219912000000Y', '3219910000000Y'],
            'AMOUNT': [9000, 10000, 11000, 12000]
        })
    
    def load_bnm_elw(reptmon, nowk):
        return pd.DataFrame({
            'ELDAY': ['DAYA', 'DAYA', 'DAYB', 'DAYC'],
            'BNMCODE': ['4929980000000Y', '3219902000000Y', '4411100000000Y', '4019000000000Y'],
            'AMOUNT': [-1000, -2000, -3000, -4000],
            'BRANCH': [1000, 2000, 3500, 4000]
        })
    
    def load_el_item():
        return pd.DataFrame({
            'BNMCODE': ['4017100000000Y', '4019000000000Y', '4019100000000Y', '4013000000000Y'],
            'FMTNAME': ['A-RMEL', 'A-RMEL', 'B-RMEA', 'E-ELSRR'],
            'IDX': ['A', 'B', 'C', 'D'],
            'SIGN': ['+', '-', '+', '+'],
            'DESC': ['RM Marketable Securities', 'EL Item 2', 'EL Item 3', 'ELSRR Item']
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
        rep7 = pd.DataFrame()
        rep2 = pd.DataFrame()  # Would be loaded from actual source
        rep4 = pd.DataFrame()  # Would be loaded from actual source
        
        # For demonstration, create rep7 from pmm_tbl1
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
        rep6 = pd.DataFrame()  # Would be loaded from actual source
        rep2 = pd.DataFrame()  # Would be loaded from actual source
        rep4 = pd.DataFrame()  # Would be loaded from actual source
        
        # For demonstration, create rep6 from pmm_tbl1
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
    if not is_alternative:
        if NOWK != '4':
            elw_final = elw_final[~elw_final['BNMCODE'].isin(['4019000000000Y', '4019100000000Y'])]
    else:
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
    
    # Generate reports
    generate_reports(elw_final_combined, day, is_alternative)
    
    return elw_final_combined

def generate_reports(df, day, is_alternative=False):
    """Generate reports similar to SAS PROC REPORT"""
    
    print(f"\n{'='*80}")
    if is_alternative:
        print(f"DETAIL TOTAL ELIGIBLE LIABILITIES ITEMS FOR : {day}")
    else:
        print(f"DETAIL TOTAL ELIGIBLE LIABILITIES ITEMS FOR : {day}")
    print(f"REPORT DATE : {RDATE}")
    print(f"{'='*80}\n")
    
    # Report 1: A-RMEL and B-RMEA
    mask = df['FMTNAME'].isin(['A-RMEL', 'B-RMEA'])
    report1 = df[mask].copy()
    
    if not report1.empty:
        print("\n=== ELIGIBLE LIABILITIES DETAIL (A-RMEL, B-RMEA) ===\n")
        print(f"{'FMTNAME':<10} {'BNMCODE':<15} {'DESC':<45} {'SIGN':<5} {'AMOUNT':>15} {'TOTAL':>15} {'AMOUNX':>15} {'TOTALX':>15}")
        print("-" * 130)
        
        for _, row in report1.iterrows():
            print(f"{row['FMTNAME']:<10} {row['BNMCODE']:<15} {row['DESC']:<45} "
                  f"{row['SIGN']:<5} {row['AMOUNT']:>15,.2f} {row['TOTAL']:>15,.2f} "
                  f"{row['AMOUNX']:>15,.2f} {row['TOTALX']:>15,.2f}")
        
        # Totals by FMTNAME
        for fmtname in report1['FMTNAME'].unique():
            subset = report1[report1['FMTNAME'] == fmtname]
            if not subset.empty:
                total_amounx = subset['AMOUNX'].sum()
                total_totalx = subset['TOTALX'].sum()
                print("-" * 130)
                print(f"{'TOTAL FOR ' + fmtname:<50} {total_amounx:>30,.2f} {total_totalx:>30,.2f}")
                print("-" * 130)
    
    # Report 2: All other FMTNAMEs
    mask_other = ~df['FMTNAME'].isin(['A-RMEL', 'B-RMEA'])
    report2 = df[mask_other].copy()
    
    if not report2.empty:
        print("\n=== ELIGIBLE LIABILITIES DETAIL (Other) ===\n")
        print(f"{'FMTNAME':<10} {'BNMCODE':<15} {'DESC':<45} {'SIGN':<5} {'AMOUNT':>15} {'TOTAL':>15}")
        print("-" * 110)
        
        for _, row in report2.iterrows():
            print(f"{row['FMTNAME']:<10} {row['BNMCODE']:<15} {row['DESC']:<45} "
                  f"{row['SIGN']:<5} {row['AMOUNT']:>15,.2f} {row['TOTAL']:>15,.2f}")

# Process all days
days = ['DAYA', 'DAYB', 'DAYC', 'DAYD', 'DAYE', 'DAYF', 'DAYG', 'DAYH']

for day in days:
    print(f"\n\n{'#'*80}")
    print(f"Processing {day}")
    print(f"{'#'*80}")
    result = process_el_data(day, is_alternative=False)

# Process DAYI with alternative version
print(f"\n\n{'#'*80}")
print("Processing DAYI (Alternative Version)")
print(f"{'#'*80}")
result_alt = process_el_data('DAYI', is_alternative=True)

print("\nProcessing complete!")
