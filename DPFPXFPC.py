# EIBDMSFX_NLF_PROCESSOR_DEBUG.py
# Full debug version to compare SAS production vs Python output

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import os
from datetime import datetime, timedelta
import pyreadstat
import pandas as pd
import sys

# Try to import saspy for SAS dataset writing
try:
    import saspy
    SASPY_AVAILABLE = True
except ImportError:
    SASPY_AVAILABLE = False
    print("Warning: saspy not available. SAS dataset output will be skipped.")

def main():
    # Configuration
    output_base_path = "MISFX"
    final_base_path = "FINAL"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_base_path, exist_ok=True)
    
    # ============================================================
    # STEP 1: DATE SETUP - Compare Production vs Current
    # ============================================================
    
    # Production date from SAS output (REPTDATE = 24258)
    PRODUCTION_SAS_DATE = 24258
    production_date = datetime(1960, 1, 1) + timedelta(days=PRODUCTION_SAS_DATE)
    
    # Current date (for comparison)
    current_date = datetime.now()
    current_sas_date = (current_date - datetime(1960, 1, 1)).days
    
    print(f"\n{'='*70}")
    print(f"DEBUG VERSION - Comparing Production vs Python Output")
    print(f"{'='*70}\n")
    
    print(f"Production SAS Date: {PRODUCTION_SAS_DATE} -> {production_date.strftime('%Y-%m-%d')}")
    print(f"Current SAS Date:    {current_sas_date} -> {current_date.strftime('%Y-%m-%d')}")
    
    # Choose which date to process
    # Option 1: Use production date to compare
    reptdate = production_date
    sas_date_num = PRODUCTION_SAS_DATE
    
    # Option 2: Use current date
    # reptdate = current_date
    # sas_date_num = current_sas_date
    
    print(f"\n{'='*70}")
    print(f"PROCESSING DATE: {reptdate.strftime('%Y-%m-%d')} (SAS: {sas_date_num})")
    print(f"{'='*70}\n")
    
    # Calculate SAS date parameters
    rept_year = reptdate.year
    rept_month = str(reptdate.month).zfill(2)
    rept_day = str(reptdate.day).zfill(2)
    rdate = str(reptdate.year)[2:] + str(reptdate.strftime('%j')).zfill(3)
    
    print(f"REPTYEAR: {rept_year}")
    print(f"REPTMON: {rept_month}")
    print(f"REPTDAY: {rept_day}")
    print(f"RDATE: {rdate} (YYDDD format)")
    
    # ============================================================
    # STEP 2: DEBUG - Check available dates in all files
    # ============================================================
    
    print(f"\n{'='*70}")
    print("DEBUG: Checking Available Dates in SAS Files")
    print(f"{'='*70}\n")
    
    sas_files = {
        'BEHAVEINDFXFD': f'{final_base_path}/behaveindfxfd.sas7bdat',
        'BEHAVENONFXFD': f'{final_base_path}/behavenonfxfd.sas7bdat',
        'BEHAVEINDFXCA': f'{final_base_path}/behaveindfxca.sas7bdat',
        'BEHAVENONFXCA': f'{final_base_path}/behavenonfxca.sas7bdat'
    }
    
    all_dates = {}
    for name, filepath in sas_files.items():
        if not os.path.exists(filepath):
            print(f"⚠️  {name}: File not found: {filepath}")
            continue
            
        try:
            df, meta = pyreadstat.read_sas7bdat(filepath)
            if 'DATE' in df.columns:
                unique_dates = sorted(df['DATE'].unique())
                all_dates[name] = unique_dates
                print(f"{name}:")
                print(f"  Total records: {len(df)}")
                print(f"  Date range: {unique_dates[0]} to {unique_dates[-1]}")
                print(f"  Number of unique dates: {len(unique_dates)}")
                
                # Check if our target date exists
                if sas_date_num in unique_dates:
                    print(f"  ✅ Target date {sas_date_num} FOUND in this file")
                else:
                    print(f"  ❌ Target date {sas_date_num} NOT FOUND in this file")
                    # Show closest dates
                    closest_before = max([d for d in unique_dates if d < sas_date_num], default=None)
                    closest_after = min([d for d in unique_dates if d > sas_date_num], default=None)
                    if closest_before:
                        print(f"     Closest date before: {closest_before}")
                    if closest_after:
                        print(f"     Closest date after: {closest_after}")
                print()
            else:
                print(f"{name}: No DATE column found")
        except Exception as e:
            print(f"{name}: Error reading - {e}")
    
    # ============================================================
    # STEP 3: DEBUG - Load raw data for the target date
    # ============================================================
    
    print(f"{'='*70}")
    print(f"DEBUG: Raw Data for SAS Date {sas_date_num}")
    print(f"{'='*70}\n")
    
    conn = duckdb.connect()
    nlf_data = []
    raw_data_details = {}
    
    for name, filepath in sas_files.items():
        if not os.path.exists(filepath):
            continue
            
        try:
            df, meta = pyreadstat.read_sas7bdat(filepath)
            
            if 'DATE' not in df.columns:
                continue
            
            df_filtered = df[df['DATE'] == sas_date_num].copy()
            
            if len(df_filtered) == 0:
                print(f"{name}: No data for date {sas_date_num}")
                continue
            
            # Store raw data for debugging
            raw_data_details[name] = {
                'records': len(df_filtered),
                'balance_values': df_filtered['BALANCE'].tolist() if 'BALANCE' in df_filtered.columns else [],
                'sum': df_filtered['BALANCE'].sum() if 'BALANCE' in df_filtered.columns else 0
            }
            
            print(f"{name}:")
            print(f"  Records: {len(df_filtered)}")
            if 'BALANCE' in df_filtered.columns:
                print(f"  BALANCE values: {df_filtered['BALANCE'].tolist()}")
                print(f"  Sum of BALANCE: {df_filtered['BALANCE'].sum():.2f}")
            print(f"  All columns: {df_filtered.columns.tolist()}")
            
            # Show first few rows
            print(f"  First row data:")
            for col in df_filtered.columns:
                print(f"    {col}: {df_filtered[col].iloc[0]}")
            print()
            
            # Rename BALANCE to appropriate column name
            if name == 'BEHAVEINDFXFD':
                df_filtered = df_filtered.rename(columns={
                    'BALANCE': 'INDFXFDBAL',
                    'DATE': 'REPTDATE'
                })
                df_filtered['NONFXFDBAL'] = None
                df_filtered['INDFXCABAL'] = None
                df_filtered['NONFXCABAL'] = None
                
            elif name == 'BEHAVENONFXFD':
                df_filtered = df_filtered.rename(columns={
                    'BALANCE': 'NONFXFDBAL',
                    'DATE': 'REPTDATE'
                })
                df_filtered['INDFXFDBAL'] = None
                df_filtered['INDFXCABAL'] = None
                df_filtered['NONFXCABAL'] = None
                
            elif name == 'BEHAVEINDFXCA':
                df_filtered = df_filtered.rename(columns={
                    'BALANCE': 'INDFXCABAL',
                    'DATE': 'REPTDATE'
                })
                df_filtered['INDFXFDBAL'] = None
                df_filtered['NONFXFDBAL'] = None
                df_filtered['NONFXCABAL'] = None
                
            elif name == 'BEHAVENONFXCA':
                df_filtered = df_filtered.rename(columns={
                    'BALANCE': 'NONFXCABAL',
                    'DATE': 'REPTDATE'
                })
                df_filtered['INDFXFDBAL'] = None
                df_filtered['NONFXFDBAL'] = None
                df_filtered['INDFXCABAL'] = None
            
            # Keep only required columns
            keep_cols = ['REPTDATE', 'INDFXFDBAL', 'NONFXFDBAL', 'INDFXCABAL', 'NONFXCABAL']
            df_filtered = df_filtered[keep_cols]
            
            nlf_data.append(df_filtered)
            
        except Exception as e:
            print(f"{name}: Error - {e}")
            continue
    
    if not nlf_data:
        print(f"\n❌ No data found for SAS date {sas_date_num}")
        print("Cannot continue with summarization.")
        conn.close()
        sys.exit(1)
    
    # ============================================================
    # STEP 4: Combine and Summarize
    # ============================================================
    
    print(f"{'='*70}")
    print("DEBUG: Combined Data and Summary")
    print(f"{'='*70}\n")
    
    nlf_combined = pd.concat(nlf_data, ignore_index=True)
    print(f"Total records combined: {len(nlf_combined)}")
    print(f"Combined data preview:")
    print(nlf_combined.head())
    print()
    
    # Show per-source contributions before summary
    print("Per-source contributions (before summary):")
    for name, details in raw_data_details.items():
        if name == 'BEHAVEINDFXFD':
            print(f"  INDFXFDBAL from {name}: {details['sum']:.2f}")
        elif name == 'BEHAVENONFXFD':
            print(f"  NONFXFDBAL from {name}: {details['sum']:.2f}")
        elif name == 'BEHAVEINDFXCA':
            print(f"  INDFXCABAL from {name}: {details['sum']:.2f}")
        elif name == 'BEHAVENONFXCA':
            print(f"  NONFXCABAL from {name}: {details['sum']:.2f}")
    print()
    
    # Step 3: PROC SUMMARY
    summary_cols = ['INDFXFDBAL', 'NONFXFDBAL', 'INDFXCABAL', 'NONFXCABAL']
    nlf_summary = nlf_combined.groupby('REPTDATE', as_index=False)[summary_cols].sum()
    nlf_summary = nlf_summary.fillna(0)
    
    print("SUMMARY RESULTS:")
    print(f"Records in summary: {len(nlf_summary)}")
    print(nlf_summary.to_string(index=False))
    print()
    
    # ============================================================
    # STEP 5: Compare with Production Values
    # ============================================================
    
    print(f"{'='*70}")
    print("COMPARISON: Production SAS vs Python Output")
    print(f"{'='*70}\n")
    
    # Production values (from your SAS output)
    prod_values = {
        'REPTDATE': 24258,
        'INDFXFDBAL': 4860344,
        'NONFXFDBAL': 23539949,
        'INDFXCABAL': 282381,
        'NONFXCABAL': 2564513
    }
    
    # Python values
    python_values = {
        'REPTDATE': nlf_summary['REPTDATE'].iloc[0] if len(nlf_summary) > 0 else None,
        'INDFXFDBAL': nlf_summary['INDFXFDBAL'].iloc[0] if len(nlf_summary) > 0 else 0,
        'NONFXFDBAL': nlf_summary['NONFXFDBAL'].iloc[0] if len(nlf_summary) > 0 else 0,
        'INDFXCABAL': nlf_summary['INDFXCABAL'].iloc[0] if len(nlf_summary) > 0 else 0,
        'NONFXCABAL': nlf_summary['NONFXCABAL'].iloc[0] if len(nlf_summary) > 0 else 0
    }
    
    print("┌─────────────────┬─────────────┬─────────────┬─────────────┐")
    print("│ Metric          │ Production  │ Python      │ Difference  │")
    print("├─────────────────┼─────────────┼─────────────┼─────────────┤")
    
    for key in ['REPTDATE', 'INDFXFDBAL', 'NONFXFDBAL', 'INDFXCABAL', 'NONFXCABAL']:
        prod_val = prod_values.get(key, 0)
        py_val = python_values.get(key, 0)
        diff = py_val - prod_val if prod_val != 0 else 0
        
        if key == 'REPTDATE':
            print(f"│ {key:<15} │ {prod_val:>11} │ {py_val:>11} │ {diff:>11} │")
        else:
            print(f"│ {key:<15} │ {prod_val:>11,} │ {py_val:>11,} │ {diff:>11,} │")
    
    print("└─────────────────┴─────────────┴─────────────┴─────────────┘")
    
    # Check if dates match
    if prod_values['REPTDATE'] == python_values['REPTDATE']:
        print(f"\n✅ Same date being compared: {prod_values['REPTDATE']}")
        
        # Calculate percentage differences
        print("\nPercentage differences (Python vs Production):")
        for key in ['INDFXFDBAL', 'NONFXFDBAL', 'INDFXCABAL', 'NONFXCABAL']:
            prod_val = prod_values.get(key, 1)
            py_val = python_values.get(key, 0)
            pct_diff = ((py_val - prod_val) / prod_val) * 100 if prod_val != 0 else 0
            print(f"  {key}: {pct_diff:>8.2f}%")
    else:
        print(f"\n❌ DIFFERENT DATES being compared!")
        print(f"   Production REPTDATE: {prod_values['REPTDATE']} ({datetime(1960,1,1) + timedelta(days=prod_values['REPTDATE'])})")
        print(f"   Python REPTDATE:     {python_values['REPTDATE']} ({datetime(1960,1,1) + timedelta(days=python_values['REPTDATE'])})")
        print(f"\n   Please run with the same date for comparison!")
    
    # ============================================================
    # STEP 6: Write Output Files
    # ============================================================
    
    print(f"\n{'='*70}")
    print("Writing Output Files")
    print(f"{'='*70}\n")
    
    output_filename = f"NLF{rept_month}"
    output_parquet_path = f"{output_base_path}/{output_filename}.parquet"
    output_sas_path = f"{output_base_path}/{output_filename}.sas7bdat"
    output_csv_path = f"{output_base_path}/{output_filename}.csv"
    
    nlf_summary_arrow = pa.Table.from_pandas(nlf_summary)
    
    if rept_day == "01":
        pq.write_table(nlf_summary_arrow, output_parquet_path)
        nlf_summary.to_csv(output_csv_path, index=False)
        write_sas_dataset(nlf_summary, output_sas_path)
        print(f"✅ Created new files:")
        print(f"   {output_parquet_path}")
        print(f"   {output_sas_path}")
        print(f"   {output_csv_path}")
    else:
        if os.path.exists(output_parquet_path):
            try:
                existing_df = pd.read_parquet(output_parquet_path)
                existing_df = existing_df[existing_df['REPTDATE'] != sas_date_num]
                combined_df = pd.concat([existing_df, nlf_summary], ignore_index=True)
                
                combined_arrow = pa.Table.from_pandas(combined_df)
                pq.write_table(combined_arrow, output_parquet_path)
                combined_df.to_csv(output_csv_path, index=False)
                write_sas_dataset(combined_df, output_sas_path)
                
                print(f"✅ Updated files:")
                print(f"   {output_parquet_path}")
                print(f"   {output_sas_path}")
                print(f"   {output_csv_path}")
            except Exception as e:
                print(f"Error updating files: {e}")
                pq.write_table(nlf_summary_arrow, output_parquet_path)
                nlf_summary.to_csv(output_csv_path, index=False)
                write_sas_dataset(nlf_summary, output_sas_path)
                print(f"✅ Created new files (fallback)")
        else:
            pq.write_table(nlf_summary_arrow, output_parquet_path)
            nlf_summary.to_csv(output_csv_path, index=False)
            write_sas_dataset(nlf_summary, output_sas_path)
            print(f"✅ Created new files")
    
    conn.close()
    print(f"\n{'='*70}")
    print("✅ DEBUG COMPLETE")
    print(f"{'='*70}\n")

def write_sas_dataset(df, output_path):
    """Write pandas DataFrame to SAS dataset using saspy or pyreadstat"""
    sas = None
    try:
        if SASPY_AVAILABLE:
            try:
                sas = saspy.SASsession()
                sas.dataframe2sasdata(df, table='NLF_TEMP')
                
                sas_code = f'''
                    libname out "{os.path.dirname(output_path)}";
                    data out.{os.path.basename(output_path).replace('.sas7bdat', '')};
                        set work.NLF_TEMP;
                    run;
                    proc datasets lib=work;
                        delete NLF_TEMP;
                    run;
                '''
                sas.submit(sas_code)
                sas.endsas()
                print("   ✅ SAS dataset written successfully")
                return True
            except Exception as e:
                print(f"   saspy method failed: {e}")
                if sas:
                    try:
                        sas.endsas()
                    except:
                        pass
        return False
    except Exception as e:
        print(f"⚠️  Warning: Could not write SAS file: {e}")
        return False

if __name__ == "__main__":
    main()
