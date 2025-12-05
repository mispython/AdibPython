import polars as pl
from datetime import date, timedelta
from pathlib import Path
import sys

# ============================================
# SAS FILE READER
# ============================================

def read_sas(filepath):
    """Read SAS7BDAT file"""
    try:
        import pyreadstat
        df, _ = pyreadstat.read_sas7bdat(str(filepath))
        return pl.from_pandas(df)
    except:
        import pandas as pd
        df = pd.read_sas(str(filepath), format='sas7bdat')
        return pl.from_pandas(df)

def fix_cols(df):
    """Standardize column names"""
    col_map = {}
    for col in df.columns:
        c = col.lower()
        if 'tranche' in c: col_map[col] = 'trancheno'
        elif 'curbal' in c: col_map[col] = 'curbal'
        elif 'nidstat' in c: col_map[col] = 'nidstat'
        elif 'cdstat' in c: col_map[col] = 'cdstat'
        elif 'matdt' in c: col_map[col] = 'matdt'
        elif 'startdt' in c: col_map[col] = 'startdt'
        elif 'early' in c: col_map[col] = 'early_wddt'
        elif 'bid' in c: col_map[col] = 'intplrate_bid'
        elif 'offer' in c: col_map[col] = 'intplrate_offer'
    return df.rename(col_map) if col_map else df

# ============================================
# FORMATS (REMFMTA & REMFMTB)
# ============================================

FMTA = {(-1e9,6):'1. LE  6      ',(6,12):'2. GT  6 TO 12',
        (12,24):'3. GT 12 TO 24',(24,36):'4. GT 24 TO 36',
        (36,60):'5. GT 36 TO 60','other':'              '}

FMTB = {(-1e9,1):'1. LE  1      ',(1,3):'2. GT  1 TO  3',
        (3,6):'3. GT  3 TO  6',(6,9):'4. GT  6 TO  9',
        (9,12):'5. GT  9 TO 12',(12,24):'6. GT 12 TO 24',
        (24,36):'7. GT 24 TO 36',(36,60):'8. GT 36 TO 60',
        'other':'             '}

def apply_fmt(val, fmt):
    """Apply format like PUT function"""
    if not val or val != val: return fmt['other']
    for (l,h),lab in fmt.items():
        if l != 'other' and l <= val < h: return lab
    return fmt['other']

# ============================================
# CORE CALCULATIONS
# ============================================

def calc_remmth(matdt, reptdate):
    """Calculate remaining months (SAS logic)"""
    if not matdt or matdt <= reptdate: return None
    if (matdt - reptdate).days < 8: return 0.1
    
    y1,m1,d1 = matdt.year, matdt.month, matdt.day
    y2,m2,d2 = reptdate.year, reptdate.month, reptdate.day
    
    # Days in month
    month_days = [31,29 if y1%4==0 else 28,31,30,31,30,31,31,30,31,30,31]
    d1 = min(d1, month_days[m1-1])
    
    remy = y1 - y2
    remm = m1 - m2
    remd = d1 - d2
    
    if remd < 0:
        remm -= 1
        if remm < 0: remy -= 1; remm += 12
        remd += month_days[(m1-2)%12]
    
    return remy*12 + remm + remd/month_days[m1-1]

# ============================================
# MAIN PROCESSING WITH PARQUET OUTPUT
# ============================================

def eibmrnid_parquet(nid_path, trnch_path=None, output_dir='.', rep_date=None):
    """EIBMRNID report with Parquet output"""
    
    # 1. Set dates
    reptdate = rep_date or (date.today().replace(day=1) - timedelta(days=1))
    startdte = date(reptdate.year, reptdate.month, 1)
    
    print(f"EIBMRNID Report for {reptdate.strftime('%d/%m/%Y')}")
    
    # 2. Load data
    rnid = fix_cols(read_sas(nid_path)).filter(pl.col('curbal') > 0)
    
    if trnch_path and Path(trnch_path).exists():
        trnch = fix_cols(read_sas(trnch_path))
        data = rnid.join(trnch, on='trancheno', how='left')
    else:
        data = rnid
    
    # Convert dates
    for col in ['matdt','startdt','early_wddt']:
        if col in data.columns and data[col].dtype != pl.Date:
            try:
                data = data.with_columns(pl.col(col).str.strptime(pl.Date))
            except:
                pass
    
    # Add report dates
    data = data.with_columns([
        pl.lit(reptdate).alias('reptdate'),
        pl.lit(startdte).alias('startdte')
    ])
    
    # 3. Filter for active records
    active = data.filter(
        (pl.col('matdt') > pl.col('reptdate')) &
        (pl.col('startdt') <= pl.col('reptdate'))
    )
    
    # Add remmth calculation
    active = active.with_columns(
        pl.col('matdt').map_elements(lambda x: calc_remmth(x, reptdate)).alias('remmth')
    )
    
    # 4. Create Table 1 (Outstanding)
    tbl1 = active.filter(
        (pl.col('nidstat') == 'N') & (pl.col('cdstat') == 'A')
    ).with_columns([
        pl.lit(0).alias('heldmkt'),
        (pl.col('curbal') - pl.col('heldmkt')).alias('outstanding'),
        pl.col('remmth').map_elements(lambda x: apply_fmt(x, FMTA)).alias('remmfmt')
    ])
    
    # 5. Create Table 2 (Trading)
    tbl2 = data.filter(
        (pl.col('nidstat') == 'E') &
        (pl.col('early_wddt') >= startdte) &
        (pl.col('early_wddt') <= reptdate)
    ).with_columns(
        pl.lit(reptdate).alias('report_date')
    )
    
    # 6. Create Table 3 (Yield)
    tbl3 = active.filter(
        (pl.col('nidstat') == 'N') & (pl.col('cdstat') == 'A')
    ).with_columns([
        ((pl.col('intplrate_bid') + pl.col('intplrate_offer')) / 2).alias('yield'),
        pl.col('remmth').map_elements(lambda x: apply_fmt(x, FMTB)).alias('remmfmt')
    ]).unique(subset=['remmfmt', 'trancheno'])
    
    # 7. Create summary tables (also as Parquet)
    
    # Table 1 Summary
    tbl1_sum = tbl1.group_by('remmfmt').agg([
        pl.sum('curbal').alias('curbal'),
        pl.sum('heldmkt').alias('heldmkt'),
        pl.sum('outstanding').alias('outstanding'),
        pl.len().alias('record_count')
    ]).sort('remmfmt')
    
    # Table 3 Summary
    tbl3_pos = tbl3.filter(pl.col('yield') > 0)
    tbl3_sum = tbl3_pos.group_by('remmfmt').agg([
        pl.mean('yield').alias('midyield'),
        pl.std('yield').alias('yield_std'),
        pl.len().alias('record_count')
    ]).sort('remmfmt')
    
    # 8. Save to Parquet files
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)
    
    # Base filename from input or date
    base_name = Path(nid_path).stem.replace('rnidm', 'EIBMRNID_')
    if not base_name.startswith('EIBMRNID'):
        base_name = f"EIBMRNID_{reptdate.strftime('%Y%m')}"
    
    # Save all tables
    files_saved = {}
    
    # Detailed tables
    files_saved['raw_data'] = output_dir / f"{base_name}_RAW.parquet"
    data.write_parquet(files_saved['raw_data'])
    
    files_saved['table1'] = output_dir / f"{base_name}_TABLE1.parquet"
    tbl1.write_parquet(files_saved['table1'])
    
    files_saved['table2'] = output_dir / f"{base_name}_TABLE2.parquet"
    tbl2.write_parquet(files_saved['table2'])
    
    files_saved['table3'] = output_dir / f"{base_name}_TABLE3.parquet"
    tbl3.write_parquet(files_saved['table3'])
    
    # Summary tables
    files_saved['table1_summary'] = output_dir / f"{base_name}_TABLE1_SUMMARY.parquet"
    tbl1_sum.write_parquet(files_saved['table1_summary'])
    
    files_saved['table3_summary'] = output_dir / f"{base_name}_TABLE3_SUMMARY.parquet"
    tbl3_sum.write_parquet(files_saved['table3_summary'])
    
    # Metadata
    metadata = pl.DataFrame({
        'report_date': [reptdate],
        'start_date': [startdte],
        'source_file': [Path(nid_path).name],
        'trnch_file': [Path(trnch_path).name if trnch_path else None],
        'generated_at': [datetime.now()],
        'table1_records': [len(tbl1)],
        'table2_records': [len(tbl2)],
        'table3_records': [len(tbl3)],
        'total_records': [len(data)]
    })
    
    files_saved['metadata'] = output_dir / f"{base_name}_METADATA.parquet"
    metadata.write_parquet(files_saved['metadata'])
    
    # 9. Also save as single combined file (optional)
    combined = pl.DataFrame({
        'dataset': ['raw_data', 'table1', 'table2', 'table3', 
                   'table1_summary', 'table3_summary', 'metadata'],
        'file_path': [str(f) for f in files_saved.values()],
        'record_count': [len(data), len(tbl1), len(tbl2), len(tbl3),
                        len(tbl1_sum), len(tbl3_sum), len(metadata)]
    })
    
    files_saved['manifest'] = output_dir / f"{base_name}_MANIFEST.parquet"
    combined.write_parquet(files_saved['manifest'])
    
    # 10. Print summary
    print(f"\nParquet files saved to: {output_dir}")
    for name, path in files_saved.items():
        size_mb = path.stat().st_size / (1024 * 1024)
        print(f"  {name:20} {path.name:30} {size_mb:.2f} MB")
    
    # Return results for further processing
    return {
        'success': True,
        'output_dir': str(output_dir),
        'files': files_saved,
        'summary': {
            'report_date': reptdate,
            'table1_records': len(tbl1),
            'table2_records': len(tbl2),
            'table3_records': len(tbl3),
            'table1_summary': tbl1_sum,
            'table3_summary': tbl3_sum
        }
    }

# ============================================
# PARQUET UTILITIES
# ============================================

def read_parquet_report(output_dir, base_name=None):
    """Read all Parquet files from a report"""
    output_dir = Path(output_dir)
    
    if base_name:
        pattern = f"{base_name}*.parquet"
    else:
        pattern = "EIBMRNID_*.parquet"
    
    files = list(output_dir.glob(pattern))
    
    if not files:
        print(f"No Parquet files found in {output_dir}")
        return None
    
    # Read all files into dictionary
    data = {}
    for file in files:
        try:
            df = pl.read_parquet(file)
            data[file.stem] = df
            print(f"Loaded: {file.name} ({len(df):,} records)")
        except Exception as e:
            print(f"Error reading {file}: {e}")
    
    return data

def create_parquet_summary(output_dir, months=None):
    """Create monthly summary from Parquet files"""
    output_dir = Path(output_dir)
    summary_data = []
    
    # Find all metadata files
    metadata_files = list(output_dir.glob("*_METADATA.parquet"))
    
    for meta_file in metadata_files:
        try:
            meta = pl.read_parquet(meta_file)
            base_name = meta_file.stem.replace('_METADATA', '')
            
            # Read corresponding summary files
            t1_file = output_dir / f"{base_name}_TABLE1_SUMMARY.parquet"
            t3_file = output_dir / f"{base_name}_TABLE3_SUMMARY.parquet"
            
            if t1_file.exists() and t3_file.exists():
                t1_sum = pl.read_parquet(t1_file)
                t3_sum = pl.read_parquet(t3_file)
                
                # Calculate totals
                total_curbal = t1_sum['curbal'].sum()
                total_outstanding = t1_sum['outstanding'].sum()
                avg_yield = t3_sum.filter(pl.col('midyield').is_not_null())['midyield'].mean()
                
                summary_data.append({
                    'report_period': base_name,
                    'report_date': meta['report_date'][0],
                    'total_curbal': total_curbal,
                    'total_outstanding': total_outstanding,
                    'avg_yield': avg_yield,
                    'source_file': meta['source_file'][0],
                    'record_count': meta['total_records'][0]
                })
                
        except Exception as e:
            print(f"Error processing {meta_file}: {e}")
    
    if summary_data:
        summary_df = pl.DataFrame(summary_data)
        summary_file = output_dir / "EIBMRNID_MONTHLY_SUMMARY.parquet"
        summary_df.write_parquet(summary_file)
        print(f"Monthly summary saved: {summary_file}")
        return summary_df
    
    return None

# ============================================
# COMMAND LINE INTERFACE
# ============================================

if __name__ == "__main__":
    import argparse
    from datetime import datetime
    
    parser = argparse.ArgumentParser(description='EIBMRNID Report with Parquet Output')
    parser.add_argument('--nid', required=True, help='NID SAS file')
    parser.add_argument('--trnch', help='TRNCH SAS file (optional)')
    parser.add_argument('--output-dir', default='./parquet_output', help='Output directory')
    parser.add_argument('--date', help='Report date YYYY-MM-DD')
    parser.add_argument('--read-only', action='store_true', help='Read existing Parquet files')
    parser.add_argument('--summary', action='store_true', help='Create monthly summary')
    
    args = parser.parse_args()
    
    # Check if reading only
    if args.read_only:
        data = read_parquet_report(args.output_dir)
        if data:
            print(f"\nLoaded {len(data)} Parquet files")
            for name, df in data.items():
                print(f"\n{name}:")
                print(f"  Records: {len(df):,}")
                print(f"  Columns: {df.columns}")
                if len(df) > 0:
                    print(f"  Sample: {df.head(3).to_dicts()}")
        sys.exit(0)
    
    # Check if creating summary only
    if args.summary:
        summary = create_parquet_summary(args.output_dir)
        if summary is not None:
            print(f"\nMonthly Summary:")
            print(summary)
        sys.exit(0)
    
    # Generate report
    if not Path(args.nid).exists():
        print(f"Error: NID file not found: {args.nid}")
        sys.exit(1)
    
    # Parse date
    rep_date = None
    if args.date:
        try:
            rep_date = date.fromisoformat(args.date)
        except:
            print(f"Invalid date: {args.date}. Use YYYY-MM-DD")
            sys.exit(1)
    
    # Generate report
    try:
        result = eibmrnid_parquet(
            nid_path=args.nid,
            trnch_path=args.trnch,
            output_dir=args.output_dir,
            rep_date=rep_date
        )
        
        if result['success']:
            print(f"\n✓ Report generated successfully")
            print(f"  Output directory: {result['output_dir']}")
            print(f"  Files created: {len(result['files'])}")
            
            # Show summary statistics
            print(f"\nSummary Statistics:")
            print(f"  Table 1 (Outstanding): {result['summary']['table1_records']:,} records")
            print(f"  Table 2 (Trading): {result['summary']['table2_records']:,} records")
            print(f"  Table 3 (Yield): {result['summary']['table3_records']:,} records")
            
            sys.exit(0)
        else:
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

# ============================================
# BATCH PROCESSING WITH PARQUET
# ============================================

def batch_process_parquet(data_dir, output_dir, months=None):
    """Process multiple months to Parquet"""
    from pathlib import Path
    
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)
    
    months = months or range(1, 13)
    results = []
    
    for month in months:
        m = f"{month:02d}"
        nid_file = next(Path(data_dir).glob(f"*rnidm{m}*.sas7bdat"), None)
        
        if not nid_file:
            print(f"No NID file for month {m}")
            continue
        
        trnch_file = next(Path(data_dir).glob(f"*trnchm{m}*.sas7bdat"), None)
        
        # Calculate report date (last day of month)
        year = date.today().year
        if month == 12:
            rep_date = date(year, 12, 31)
        else:
            rep_date = date(year, month + 1, 1) - timedelta(days=1)
        
        print(f"\nProcessing month {m}/{year}...")
        
        try:
            result = eibmrnid_parquet(
                nid_path=str(nid_file),
                trnch_path=str(trnch_file) if trnch_file else None,
                output_dir=output_dir,
                rep_date=rep_date
            )
            results.append((m, result))
            print(f"  ✓ Success")
        except Exception as e:
            print(f"  ✗ Failed: {e}")
    
    # Create combined summary
    if results:
        create_parquet_summary(output_dir)
    
    return results
