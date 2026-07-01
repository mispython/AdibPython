# EIBDMSFX_NLF_PROCESSOR.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import os
from datetime import datetime, timedelta
import pyreadstat
import pandas as pd
import sys

def main():
    # Configuration
    output_base_path = "MISFX"
    final_base_path = "FINAL"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_base_path, exist_ok=True)
    
    # Step 1: Get REPTDATE parameters
    # Use specific date for testing - change as needed
    reptdate = datetime(2026, 6, 29)  # SAS date 24286
    
    # Alternative: Use current date with fallback to latest available
    # reptdate = datetime.now()
    # latest_date, latest_sas_num = get_latest_available_date(final_base_path)
    # if latest_date and (reptdate - datetime(1960,1,1)).days > latest_sas_num:
    #     print(f"⚠️  Using latest available date: {latest_date.strftime('%Y-%m-%d')}")
    #     reptdate = latest_date
    
    # Calculate SAS date parameters
    sas_base_date = datetime(1960, 1, 1)
    sas_date_num = (reptdate - sas_base_date).days
    
    rept_year = reptdate.year
    rept_month = str(reptdate.month).zfill(2)
    rept_day = str(reptdate.day).zfill(2)
    # RDATE in SAS: 5-digit date (YYDDD)
    rdate = str(reptdate.year)[2:] + str(reptdate.strftime('%j')).zfill(3)
    
    print(f"\n{'='*60}")
    print(f"Processing date: {reptdate.strftime('%Y-%m-%d')}")
    print(f"SAS date number: {sas_date_num}")
    print(f"REPTYEAR: {rept_year}")
    print(f"REPTMON: {rept_month}")
    print(f"REPTDAY: {rept_day}")
    print(f"RDATE: {rdate} (YYDDD format)")
    print(f"{'='*60}\n")
    
    # Step 2: Create NLF dataset (equivalent to SAS DATA NLF step)
    conn = duckdb.connect()
    
    # Define SAS file paths (lowercase)
    sas_files = {
        'BEHAVEINDFXFD': f'{final_base_path}/behaveindfxfd.sas7bdat',
        'BEHAVENONFXFD': f'{final_base_path}/behavenonfxfd.sas7bdat',
        'BEHAVEINDFXCA': f'{final_base_path}/behaveindfxca.sas7bdat',
        'BEHAVENONFXCA': f'{final_base_path}/behavenonfxca.sas7bdat'
    }
    
    # Read each SAS file and filter by date
    nlf_data = []
    file_loaded = False
    
    for name, filepath in sas_files.items():
        if not os.path.exists(filepath):
            print(f"Warning: {filepath} not found, skipping...")
            continue
        
        try:
            # Read SAS file
            df, meta = pyreadstat.read_sas7bdat(filepath)
            
            # Check if DATE column exists
            if 'DATE' not in df.columns:
                print(f"Warning: DATE column not found in {name}")
                continue
            
            # Filter by SAS date number (equivalent to WHERE DATE=&RDATE)
            df_filtered = df[df['DATE'] == sas_date_num].copy()
            
            if len(df_filtered) == 0:
                print(f"No data found for SAS date {sas_date_num} in {name}")
                continue
            
            # Rename BALANCE to appropriate column name (equivalent to RENAME)
            # Also rename DATE to REPTDATE
            if name == 'BEHAVEINDFXFD':
                df_filtered = df_filtered.rename(columns={
                    'BALANCE': 'INDFXFDBAL',
                    'DATE': 'REPTDATE'
                })
                # Add NULL columns for other balances
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
            file_loaded = True
            print(f"✅ Loaded {len(df_filtered)} records from {name}")
            
        except Exception as e:
            print(f"Error reading {filepath}: {e}")
            continue
    
    if not file_loaded:
        print(f"\n❌ No data found for date {reptdate.strftime('%Y-%m-%d')} (SAS date: {sas_date_num})")
        print("Available SAS dates in your files:")
        show_available_dates(final_base_path)
        conn.close()
        sys.exit(1)
    
    # Combine all data (equivalent to SET statement with multiple datasets)
    # Filter out any empty DataFrames before concatenation to avoid warning
    nlf_data_filtered = [df for df in nlf_data if len(df) > 0]
    if nlf_data_filtered:
        nlf_combined = pd.concat(nlf_data_filtered, ignore_index=True)
        print(f"\n📊 Total records combined: {len(nlf_combined)}")
    else:
        print("\n❌ No data to combine")
        conn.close()
        sys.exit(1)
    
    # Step 3: PROC SUMMARY (equivalent to PROC SUMMARY with NWAY)
    # Group by REPTDATE and sum all balance columns
    summary_cols = ['INDFXFDBAL', 'NONFXFDBAL', 'INDFXCABAL', 'NONFXCABAL']
    
    # Perform summary (equivalent to PROC SUMMARY)
    nlf_summary = nlf_combined.groupby('REPTDATE', as_index=False)[summary_cols].sum()
    
    # Handle NULL values - replace with 0
    nlf_summary = nlf_summary.fillna(0)
    
    print(f"\n📊 Summary statistics:")
    print(f"  Records: {len(nlf_summary)}")
    if len(nlf_summary) > 0:
        print(f"  INDFXFDBAL Total: {nlf_summary['INDFXFDBAL'].iloc[0]:.2f}")
        print(f"  NONFXFDBAL Total: {nlf_summary['NONFXFDBAL'].iloc[0]:.2f}")
        print(f"  INDFXCABAL Total: {nlf_summary['INDFXCABAL'].iloc[0]:.2f}")
        print(f"  NONFXCABAL Total: {nlf_summary['NONFXCABAL'].iloc[0]:.2f}")
    
    # Step 4: APPEND macro (equivalent to %MACRO APPEND)
    output_filename = f"NLF{rept_month}"
    output_parquet_path = f"{output_base_path}/{output_filename}.parquet"
    output_sas_path = f"{output_base_path}/{output_filename}.sas7bdat"
    output_csv_path = f"{output_base_path}/{output_filename}.csv"
    
    # Convert to Arrow for writing
    nlf_summary_arrow = pa.Table.from_pandas(nlf_summary)
    
    if rept_day == "01":
        # First day of month - create new files (equivalent to %IF &REPTDAY EQ "01")
        pq.write_table(nlf_summary_arrow, output_parquet_path)
        nlf_summary.to_csv(output_csv_path, index=False)
        
        # Write SAS file
        try:
            pyreadstat.write_sas7bdat(output_sas_path, nlf_summary)
            print(f"\n✅ Created new files:")
            print(f"   {output_parquet_path}")
            print(f"   {output_sas_path}")
            print(f"   {output_csv_path}")
        except Exception as e:
            print(f"\n⚠️  Warning: Could not write SAS file: {e}")
            print(f"   Created Parquet and CSV files only:")
            print(f"   {output_parquet_path}")
            print(f"   {output_csv_path}")
    else:
        # Other days - append to existing file after removing existing record (equivalent to %ELSE)
        if os.path.exists(output_parquet_path):
            try:
                # Read existing data
                existing_df = pd.read_parquet(output_parquet_path)
                print(f"\n📁 Existing records before update: {len(existing_df)}")
                
                # Remove existing record for the current date (equivalent to DELETE step)
                existing_df = existing_df[existing_df['REPTDATE'] != sas_date_num]
                print(f"   Records after removing date {sas_date_num}: {len(existing_df)}")
                
                # Append new data (equivalent to PROC APPEND)
                combined_df = pd.concat([existing_df, nlf_summary], ignore_index=True)
                print(f"   Total records after append: {len(combined_df)}")
                
                # Write combined data
                combined_arrow = pa.Table.from_pandas(combined_df)
                pq.write_table(combined_arrow, output_parquet_path)
                combined_df.to_csv(output_csv_path, index=False)
                
                # Write SAS file
                try:
                    pyreadstat.write_sas7bdat(output_sas_path, combined_df)
                    print(f"\n✅ Updated files:")
                    print(f"   {output_parquet_path}")
                    print(f"   {output_sas_path}")
                    print(f"   {output_csv_path}")
                except Exception as e:
                    print(f"\n⚠️  Warning: Could not write SAS file: {e}")
                    print(f"   Updated Parquet and CSV files only:")
                    print(f"   {output_parquet_path}")
                    print(f"   {output_csv_path}")
                    
            except Exception as e:
                print(f"Error updating existing files: {e}")
                # Fallback: create new file
                pq.write_table(nlf_summary_arrow, output_parquet_path)
                nlf_summary.to_csv(output_csv_path, index=False)
                try:
                    pyreadstat.write_sas7bdat(output_sas_path, nlf_summary)
                    print(f"\n✅ Created new files (fallback):")
                    print(f"   {output_parquet_path}")
                    print(f"   {output_sas_path}")
                    print(f"   {output_csv_path}")
                except Exception as e2:
                    print(f"\n⚠️  Warning: Could not write SAS file: {e2}")
                    print(f"   Created Parquet and CSV files only:")
                    print(f"   {output_parquet_path}")
                    print(f"   {output_csv_path}")
        else:
            # File doesn't exist, create new one
            pq.write_table(nlf_summary_arrow, output_parquet_path)
            nlf_summary.to_csv(output_csv_path, index=False)
            try:
                pyreadstat.write_sas7bdat(output_sas_path, nlf_summary)
                print(f"\n✅ Created new files:")
                print(f"   {output_parquet_path}")
                print(f"   {output_sas_path}")
                print(f"   {output_csv_path}")
            except Exception as e:
                print(f"\n⚠️  Warning: Could not write SAS file: {e}")
                print(f"   Created Parquet and CSV files only:")
                print(f"   {output_parquet_path}")
                print(f"   {output_csv_path}")
    
    conn.close()
    print(f"\n{'='*60}")
    print(f"✅ Processing complete! Output saved to: {output_base_path}/")
    print(f"{'='*60}")

def get_latest_available_date(final_base_path):
    """Get the latest date available in the SAS files"""
    sas_files = [
        f'{final_base_path}/behaveindfxfd.sas7bdat',
        f'{final_base_path}/behavenonfxfd.sas7bdat',
        f'{final_base_path}/behaveindfxca.sas7bdat',
        f'{final_base_path}/behavenonfxca.sas7bdat'
    ]
    
    all_dates = []
    for filepath in sas_files:
        if os.path.exists(filepath):
            try:
                df, meta = pyreadstat.read_sas7bdat(filepath)
                if 'DATE' in df.columns:
                    all_dates.extend(df['DATE'].unique())
            except:
                pass
    
    if all_dates:
        latest_sas = max(all_dates)
        latest_date = datetime(1960, 1, 1) + timedelta(days=latest_sas)
        return latest_date, latest_sas
    else:
        return None, None

def show_available_dates(final_base_path):
    """Show what dates are available in the data files"""
    sas_files = [
        f'{final_base_path}/behaveindfxfd.sas7bdat',
        f'{final_base_path}/behavenonfxfd.sas7bdat',
        f'{final_base_path}/behaveindfxca.sas7bdat',
        f'{final_base_path}/behavenonfxca.sas7bdat'
    ]
    
    all_dates = set()
    for filepath in sas_files:
        if os.path.exists(filepath):
            try:
                df, meta = pyreadstat.read_sas7bdat(filepath)
                if 'DATE' in df.columns:
                    all_dates.update(df['DATE'].unique())
            except:
                pass
    
    if all_dates:
        sorted_dates = sorted(all_dates)
        readable_dates = [datetime(1960, 1, 1) + timedelta(days=d) for d in sorted_dates]
        print(f"  Date range: {readable_dates[0].strftime('%Y-%m-%d')} to {readable_dates[-1].strftime('%Y-%m-%d')}")
        print(f"  Number of dates available: {len(readable_dates)}")
        print(f"  First 10 dates: {', '.join([d.strftime('%Y-%m-%d') for d in readable_dates[:10]])}")
        if len(readable_dates) > 10:
            print("  ...")
    else:
        print("  No SAS files found or no DATE column present")

if __name__ == "__main__":
    main()
