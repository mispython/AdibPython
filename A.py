import polars as pl

def npgsrpt(df, rdate):
    """Minimal NPGS report - just show data"""
    if df.is_empty():
        print("No data")
        return
    
    print(f"NPGS Report - {rdate}")
    print("=" * 60)
    
    # Just show the data with original column names
    print(df)
    print(f"\nTotal: {len(df)} records")
    
    return df

def generate_report(df, output_file=None, rdate=""):
    """For other programs to call"""
    result = npgsrpt(df, rdate)
    
    if output_file:
        # Save as CSV if output file specified
        df.write_csv(output_file)
    
    return result

# For SAS compatibility naming
pgm = generate_report  # alias for %INC PGM(NPGSRPT)
