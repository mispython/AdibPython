24265	4958530	23814718	280019	2536339
24272	4900929	23395057	278544	2410841
24279	4965001	23198194	280390	2507282
24287	4811085	23217526	279525	2908128

this is the output after i ran below code:

# EIBDMSFX_NLF_PROCESSOR.py
# Process ALL dates in a month

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
    def get_month_dates(sas_dates, target_month, target_year):
        """Get all SAS dates in a specific month"""
        month_dates = []
        for sas_date in sas_dates:
            date_obj = DateUtils.sas_to_date(sas_date)
            if date_obj.year == target_year and date_obj.month == target_month:
                month_dates.append(sas_date)
        return sorted(month_dates)

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
    
    def process_month(self, year, month):
        """Process ALL available dates in a month"""
        loader = DataLoader(Config.FINAL_PATH)
        
        # Get all available dates
        all_dates = loader.get_available_dates()
        if not all_dates:
            print("❌ No data found!")
            return False
        
        # Get dates for this month
        month_dates = DateUtils.get_month_dates(all_dates, month, year)
        
        if not month_dates:
            print(f"❌ No data found for {year}-{month:02d}")
            return False
        
        print(f"\n📅 Processing {len(month_dates)} dates for {year}-{month:02d}")
        
        # Process each date
        all_summaries = []
        
        for sas_date in month_dates:
            date_obj = DateUtils.sas_to_date(sas_date)
            print(f"  Processing: {date_obj.strftime('%Y-%m-%d')} (SAS: {sas_date})")
            
            # Load data
            raw_data = loader.load_date_data(sas_date)
            if raw_data is None or len(raw_data) == 0:
                print(f"    ⚠️  No data for this date")
                continue
            
            # Summarize
            summary = self.summarize_data(raw_data)
            if summary is not None and len(summary) > 0:
                all_summaries.append(summary)
                print(f"    ✅ Added: {len(summary)} record(s)")
        
        if not all_summaries:
            print(f"❌ No data processed for {year}-{month:02d}")
            return False
        
        # Combine all summaries
        combined_df = pd.concat(all_summaries, ignore_index=True)
        
        # Sort by REPTDATE
        combined_df = combined_df.sort_values('REPTDATE').reset_index(drop=True)
        
        print(f"\n📊 Total records: {len(combined_df)}")
        
        # Write output
        output_filename = f"NLF{str(month).zfill(2)}"
        output_parquet = os.path.join(self.output_path, f"{output_filename}.parquet")
        output_sas = os.path.join(self.output_path, f"{output_filename}.sas7bdat")
        output_csv = os.path.join(self.output_path, f"{output_filename}.csv")
        
        # Write Parquet
        combined_arrow = pa.Table.from_pandas(combined_df)
        pq.write_table(combined_arrow, output_parquet)
        print(f"  ✅ Created: {output_parquet}")
        
        # Write CSV
        combined_df.to_csv(output_csv, index=False)
        print(f"  ✅ Created: {output_csv}")
        
        # Write SAS
        if self._write_sas_dataset(combined_df, output_sas):
            print(f"  ✅ Created: {output_sas}")
        
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

def main():
    print("\n" + "="*70)
    print("NLF PROCESSOR - Monthly Batch Processing".center(70))
    print("="*70 + "\n")
    
    # Get current date (or specify a specific date)
    # For production, use today's date
    #target_date = datetime.now()
    
    # For testing specific months:
    target_date = datetime(2026, 6, 30)  # Process June 2026
    
    year = target_date.year
    month = target_date.month
    
    print(f"📅 Processing month: {year}-{month:02d}")
    
    # Process the month
    processor = NLFProcessor(Config.OUTPUT_PATH)
    success = processor.process_month(year, month)
    
    if success:
        print("\n" + "="*70)
        print("✅ PROCESSING COMPLETE".center(70))
        print(f"Output saved to: {Config.OUTPUT_PATH}/".center(70))
        print("="*70 + "\n")
    else:
        print("\n" + "="*70)
        print("❌ PROCESSING FAILED".center(70))
        print("="*70 + "\n")
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
