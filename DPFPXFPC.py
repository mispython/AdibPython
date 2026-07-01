import pyreadstat
import os
from datetime import datetime, timedelta

def verify_all_dates():
    """Verify all dates in the SAS files"""
    
    sas_files = [
        'FINAL/behaveindfxfd.sas7bdat',
        'FINAL/behavenonfxfd.sas7bdat',
        'FINAL/behaveindfxca.sas7bdat',
        'FINAL/behavenonfxca.sas7bdat'
    ]
    
    all_dates = set()
    
    for filepath in sas_files:
        if os.path.exists(filepath):
            df, meta = pyreadstat.read_sas7bdat(filepath)
            if 'DATE' in df.columns:
                dates = set(df['DATE'].unique())
                all_dates.update(dates)
                print(f"{filepath}: {len(dates)} unique dates")
    
    sorted_dates = sorted(all_dates)
    print(f"\nTotal unique dates: {len(sorted_dates)}")
    print(f"Date range: {sorted_dates[0]} to {sorted_dates[-1]}")
    
    # Check specific dates
    check_dates = [24258, 24259, 24260, 24286, 24287]
    for date in check_dates:
        if date in all_dates:
            print(f"✅ {date} = {datetime(1960,1,1) + timedelta(days=date)} - EXISTS")
        else:
            print(f"❌ {date} - NOT FOUND")
    
    return sorted_dates

verify_all_dates()
