# EIBDMSFX_NLF_PROCESSOR.py - Daily Version
# This version processes ONE date per run (like production SAS)

import os
import sys
import warnings
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyreadstat

# Suppress pandas warnings
warnings.filterwarnings('ignore', category=FutureWarning)

# Try to import saspy for SAS dataset writing
try:
    import saspy
    SASPY_AVAILABLE = True
except ImportError:
    SASPY_AVAILABLE = False

# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Configuration settings for the NLF processor"""
    FINAL_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDMSFX"
    OUTPUT_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDMSFX"
    SAS_BASE_DATE = datetime(1960, 1, 1)
    
    SAS_FILES = {
        'INDFXFD': 'behaveindfxfd.sas7bdat',
        'NONFXFD': 'behavenonfxfd.sas7bdat',
        'INDFXCA': 'behaveindfxca.sas7bdat',
        'NONFXCA': 'behavenonfxca.sas7bdat'
    }
    
    COLUMN_MAPPINGS = {
        'INDFXFD': {'BALANCE': 'INDFXFDBAL'},
        'NONFXFD': {'BALANCE': 'NONFXFDBAL'},
        'INDFXCA': {'BALANCE': 'INDFXCABAL'},
        'NONFXCA': {'BALANCE': 'NONFXCABAL'}
    }
    
    BALANCE_COLS = ['INDFXFDBAL', 'NONFXFDBAL', 'INDFXCABAL', 'NONFXCABAL']

# ============================================================================
# DATE UTILITIES
# ============================================================================

class DateUtils:
    @staticmethod
    def sas_to_date(sas_date_num):
        return Config.SAS_BASE_DATE + timedelta(days=sas_date_num)
    
    @staticmethod
    def date_to_sas(date_obj):
        return (date_obj - Config.SAS_BASE_DATE).days
    
    @staticmethod
    def get_date_parameters(date_obj):
        """Get all date parameters needed for processing"""
        return {
            'date': date_obj,
            'sas_date': (date_obj - Config.SAS_BASE_DATE).days,
            'year': date_obj.year,
            'month': str(date_obj.month).zfill(2),
            'day': str(date_obj.day).zfill(2),
            'rdate': str(date_obj.year)[2:] + str(date_obj.strftime('%j')).zfill(3),
            'is_first_day': date_obj.day == 1
        }

# ============================================================================
# DATA LOADER
# ============================================================================

class DataLoader:
    def __init__(self, final_path):
        self.final_path = final_path
        self.available_dates = None
    
    def get_available_dates(self):
        if self.available_dates is not None:
            return self.available_dates
        
        all_dates = set()
        for name, filename in Config.SAS_FILES.items():
            filepath = os.path.join(self.final_path, filename)
            if os.path.exists(filepath):
                try:
                    df, _ = pyreadstat.read_sas7bdat(filepath)
                    if 'DATE' in df.columns:
                        all_dates.update(df['DATE'].unique())
                except Exception as e:
                    print(f"⚠️  Warning: Could not read {filepath}: {e}")
        
        self.available_dates = sorted(all_dates)
        return self.available_dates
    
    def get_processing_date(self, target_date=None):
        """
        Determine the correct processing date.
        For daily runs, use the target date if available,
        otherwise skip (don't create empty records).
        """
        available_dates = self.get_available_dates()
        
        if not available_dates:
            return None, None
        
        # If no target date provided, use today
        if target_date is None:
            target_date = datetime.now()
        
        target_sas = DateUtils.date_to_sas(target_date)
        
        # Check if target date exists
        if target_sas in available_dates:
            return target_date, target_sas
        else:
            # For daily runs, return None if date doesn't exist
            # This prevents creating empty records
            print(f"\n⚠️  Date {target_date.strftime('%Y-%m-%d')} (SAS: {target_sas}) not available")
            print(f"   Skipping - no data for this date")
            return None, None
    
    def load_date_data(self, sas_date_num):
        """Load data for a specific SAS date from all source files"""
        all_data = []
        
        for source_name, filename in Config.SAS_FILES.items():
            filepath = os.path.join(self.final_path, filename)
            
            if not os.path.exists(filepath):
                continue
            
            try:
                df, _ = pyreadstat.read_sas7bdat(filepath)
                
                if 'DATE' not in df.columns:
                    continue
                
                df_filtered = df[df['DATE'] == sas_date_num].copy()
                
                if len(df_filtered) == 0:
                    continue
                
                balance_col = Config.COLUMN_MAPPINGS[source_name]['BALANCE']
                df_filtered = df_filtered.rename(columns={
                    'BALANCE': balance_col,
                    'DATE': 'REPTDATE'
                })
                
                for col in Config.BALANCE_COLS:
                    if col not in df_filtered.columns:
                        df_filtered[col] = None
                
                df_filtered = df_filtered[['REPTDATE'] + Config.BALANCE_COLS]
                all_data.append(df_filtered)
                
            except Exception as e:
                continue
        
        if not all_data:
            return None
        
        combined = pd.concat(all_data, ignore_index=True)
        return combined

# ============================================================================
# NLF PROCESSOR
# ============================================================================

class NLFProcessor:
    def __init__(self, output_path):
        self.output_path = output_path
        os.makedirs(output_path, exist_ok=True)
    
    def process_date(self, date_obj, sas_date):
        """Process a SINGLE date (like production daily run)"""
        
        loader = DataLoader(Config.FINAL_PATH)
        
        print(f"\n📅 Processing date: {date_obj.strftime('%Y-%m-%d')} (SAS: {sas_date})")
        
        # Load data for this date
        raw_data = loader.load_date_data(sas_date)
        if raw_data is None or len(raw_data) == 0:
            print(f"  ⚠️  No data for this date")
            return False
        
        # Summarize
        summary = self.summarize_data(raw_data)
        if summary is None or len(summary) == 0:
            print(f"  ⚠️  No summary for this date")
            return False
        
        print(f"  ✅ Processed: {len(summary)} record(s)")
        
        # Append to monthly file
        return self.append_to_monthly(summary, date_obj)
    
    def append_to_monthly(self, summary_df, date_obj):
        """Append a single record to the monthly file (like PROC APPEND)"""
        
        month = str(date_obj.month).zfill(2)
        output_filename = f"NLF{month}"
        output_parquet = os.path.join(self.output_path, f"{output_filename}.parquet")
        output_sas = os.path.join(self.output_path, f"{output_filename}.sas7bdat")
        output_csv = os.path.join(self.output_path, f"{output_filename}.csv")
        
        # Check if this is the first day of the month
        is_first_day = date_obj.day == 1
        
        if is_first_day or not os.path.exists(output_parquet):
            # First day of month or file doesn't exist - create new
            combined_df = summary_df
            print(f"  📁 Creating new monthly file: {output_filename}")
        else:
            # Append to existing file
            try:
                existing_df = pd.read_parquet(output_parquet)
                
                # Check if record already exists for this date
                sas_date = summary_df['REPTDATE'].iloc[0]
                existing_df = existing_df[existing_df['REPTDATE'] != sas_date]
                
                # Append new data
                combined_df = pd.concat([existing_df, summary_df], ignore_index=True)
                combined_df = combined_df.sort_values('REPTDATE').reset_index(drop=True)
                
                print(f"  📁 Appending to existing file: {output_filename}")
                print(f"     Existing: {len(existing_df)} records")
                print(f"     After append: {len(combined_df)} records")
                
            except Exception as e:
                print(f"  ⚠️  Error reading existing file: {e}")
                combined_df = summary_df
        
        # Write Parquet
        combined_arrow = pa.Table.from_pandas(combined_df)
        pq.write_table(combined_arrow, output_parquet)
        print(f"  ✅ Updated: {output_parquet}")
        
        # Write CSV
        combined_df.to_csv(output_csv, index=False)
        print(f"  ✅ Updated: {output_csv}")
        
        # Write SAS
        if self._write_sas_dataset(combined_df, output_sas):
            print(f"  ✅ Updated: {output_sas}")
        
        return True
    
    def summarize_data(self, df):
        if df is None or len(df) == 0:
            return None
        
        summary = df.groupby('REPTDATE', as_index=False)[Config.BALANCE_COLS].sum()
        summary = summary.fillna(0)
        return summary
    
    def _write_sas_dataset(self, df, output_path):
        if not SASPY_AVAILABLE:
            print("  ℹ️  saspy not available - skipping SAS dataset")
            return False
        
        sas = None
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
            return True
        except Exception as e:
            print(f"  ⚠️  Could not write SAS dataset: {e}")
            if sas:
                try:
                    sas.endsas()
                except:
                    pass
            return False

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def print_header(title, char='='):
    """Print a formatted header"""
    print(f"\n{char*70}")
    print(f"{title:^70}")
    print(f"{char*70}\n")

def main():
    """Main execution function - Daily run (like production)"""
    
    print_header("NLF PROCESSOR - Daily Run")
    
    # Initialize loader
    loader = DataLoader(Config.FINAL_PATH)
    
    # Get available dates
    all_dates = loader.get_available_dates()
    if not all_dates:
        print("❌ No data found in FINAL directory!")
        print("   Please check that SAS files exist in:", Config.FINAL_PATH)
        sys.exit(1)
    
    print(f"📁 Data available from {DateUtils.sas_to_date(min(all_dates)).strftime('%Y-%m-%d')} to {DateUtils.sas_to_date(max(all_dates)).strftime('%Y-%m-%d')}")
    print(f"   Total dates: {len(all_dates)}")
    
    # For daily runs, use today's date (like production)
    # Or you can specify a date for testing
    target_date = datetime.now()
    
    # For testing specific dates, uncomment:
    # target_date = datetime(2026, 6, 8)  # Process June 8
    
    year = target_date.year
    month = target_date.month
    day = target_date.day
    
    print(f"📅 Target date: {target_date.strftime('%Y-%m-%d')}")
    
    # Get the processing date (only if it exists in data)
    process_date, sas_date = loader.get_processing_date(target_date)
    
    if process_date is None or sas_date is None:
        print(f"\n⚠️  No data for {target_date.strftime('%Y-%m-%d')}")
        print("   This is normal - the daily SAS job would also skip this date.")
        print("   The monthly file will retain its existing records.")
        sys.exit(0)  # Exit gracefully
    
    # Process the date
    processor = NLFProcessor(Config.OUTPUT_PATH)
    success = processor.process_date(process_date, sas_date)
    
    if success:
        print_header("✅ PROCESSING COMPLETE")
        print(f"Output saved to: {Config.OUTPUT_PATH}/")
        print("="*70 + "\n")
    else:
        print_header("❌ PROCESSING FAILED")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
