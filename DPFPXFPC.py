# EIBDMSFX_NLF_PROCESSOR.py

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
    
    # Step 1: Get REPTDATE parameters
    # Use current date or specific date for testing
    reptdate = datetime.now()  # Use today's date
    
    # For testing specific dates:
    # reptdate = datetime(2026, 6, 29)  # SAS date 24286
    
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
        conn.close()
        sys.exit(1)
    
    # Combine all data
    nlf_data_filtered = [df for df in nlf_data if len(df) > 0]
    if nlf_data_filtered:
        nlf_combined = pd.concat(nlf_data_filtered, ignore_index=True)
        print(f"\n📊 Total records combined: {len(nlf_combined)}")
    else:
        print("\n❌ No data to combine")
        conn.close()
        sys.exit(1)
    
    # Step 3: PROC SUMMARY (equivalent to PROC SUMMARY with NWAY)
    summary_cols = ['INDFXFDBAL', 'NONFXFDBAL', 'INDFXCABAL', 'NONFXCABAL']
    nlf_summary = nlf_combined.groupby('REPTDATE', as_index=False)[summary_cols].sum()
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
    
    nlf_summary_arrow = pa.Table.from_pandas(nlf_summary)
    
    if rept_day == "01":
        # First day of month - create new files
        pq.write_table(nlf_summary_arrow, output_parquet_path)
        nlf_summary.to_csv(output_csv_path, index=False)
        write_sas_dataset(nlf_summary, output_sas_path)
        print(f"\n✅ Created new files:")
        print(f"   {output_parquet_path}")
        print(f"   {output_sas_path}")
        print(f"   {output_csv_path}")
    else:
        # Other days - append to existing file
        if os.path.exists(output_parquet_path):
            try:
                existing_df = pd.read_parquet(output_parquet_path)
                print(f"\n📁 Existing records before update: {len(existing_df)}")
                
                # Remove existing record for the current date
                existing_df = existing_df[existing_df['REPTDATE'] != sas_date_num]
                print(f"   Records after removing date {sas_date_num}: {len(existing_df)}")
                
                # Append new data
                combined_df = pd.concat([existing_df, nlf_summary], ignore_index=True)
                print(f"   Total records after append: {len(combined_df)}")
                
                # Write combined data
                combined_arrow = pa.Table.from_pandas(combined_df)
                pq.write_table(combined_arrow, output_parquet_path)
                combined_df.to_csv(output_csv_path, index=False)
                write_sas_dataset(combined_df, output_sas_path)
                
                print(f"\n✅ Updated files:")
                print(f"   {output_parquet_path}")
                print(f"   {output_sas_path}")
                print(f"   {output_csv_path}")
                    
            except Exception as e:
                print(f"Error updating existing files: {e}")
                # Fallback: create new file
                pq.write_table(nlf_summary_arrow, output_parquet_path)
                nlf_summary.to_csv(output_csv_path, index=False)
                write_sas_dataset(nlf_summary, output_sas_path)
                print(f"\n✅ Created new files (fallback):")
                print(f"   {output_parquet_path}")
                print(f"   {output_sas_path}")
                print(f"   {output_csv_path}")
        else:
            # File doesn't exist, create new one
            pq.write_table(nlf_summary_arrow, output_parquet_path)
            nlf_summary.to_csv(output_csv_path, index=False)
            write_sas_dataset(nlf_summary, output_sas_path)
            print(f"\n✅ Created new files:")
            print(f"   {output_parquet_path}")
            print(f"   {output_sas_path}")
            print(f"   {output_csv_path}")
    
    conn.close()
    print(f"\n{'='*60}")
    print(f"✅ Processing complete! Output saved to: {output_base_path}/")
    print(f"{'='*60}")

def write_sas_dataset(df, output_path):
    """Write pandas DataFrame to SAS dataset using saspy or pyreadstat"""
    sas = None
    try:
        if SASPY_AVAILABLE:
            try:
                sas = saspy.SASsession()
                sas.dataframe2sasdata(df, table='NLF_TEMP')
                
                # Use SAS code to export
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
