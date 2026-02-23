"""
EIBQDCIS - Deposit CIS (Customer Information System) Processing
Processes multiple deposit relationship tables (DPTRBL1-14, DPTRBORG, DPTRB999)
Creates consolidated customer ICNO mappings by account with join counts
Includes PBBDPFMT formats for product descriptions
"""

import polars as pl
from pathlib import Path
import re

# =============================================================================
# CONFIG
# =============================================================================
PATHS = {
    'DEPOSIT': 'data/deposit/',
    'OUTPUT': 'output/'
}
for p in PATHS.values(): Path(p).mkdir(exist_ok=True)

# Formats from PBBDPFMT
JOIN_FMT = {i: f'JOIN {i}' for i in range(12)}
JOIN_FMT[0] = 'ORGANISATION'

PROD_FMD = {
    '42110': 'CA (A)', '42310': 'CA (A)', '34180': 'CA (A)',
    '42610': 'FX CA (A)', '42120': 'SA (B)', '42320': 'SA (B)',
    '42130': 'FD (C)', '42630': 'FX FD (C)', '42132': 'GID',
    '42133': 'GID', '42180': 'HOUSING DEV (D)', '42XXX': 'ATM/SI (E)',
    '46795': 'DEBIT CARD (E)'
}

PROD_BRH = {
    '42110': 'DDMAND', '42310': 'DDMAND', '34180': 'DDMAND',
    '42120': 'DSVING', '42320': 'DSVING', '42130': 'DFIXED',
    '42132': 'DFIXED', '42133': 'DFIXED', '42180': 'DDMAND',
    '42610': 'FDMAND', '42630': 'FFIXED', '42XXX': 'ATM/SI (E)',
    '46795': 'DEBIT CARD (E)'
}

PROD_FMI = {
    '42110': 'CA (A)', '42310': 'CA (A)', '34180': 'CA (A)',
    '42120': 'SA (D)', '42320': 'SA (D)', '42130': 'FD',
    '42610': 'FXCA', '42630': 'FXFD', '42132': 'GID (B)',
    '42133': 'GID (B)', '42180': 'HOUSING DEV (C)',
    '42XXX': 'ATM/SI (E)'
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def clean_icno(icno_str):
    """Clean ICNO string - remove special chars, handle empty/DUPLI/99999"""
    if not icno_str or icno_str.strip() == '':
        return None
    if icno_str[3:8] in ('99999', 'DUPLI') or icno_str[:5] in ('99999', 'DUPLI'):
        return None
    # Remove parentheses, slashes, hyphens
    cleaned = re.sub(r'[()\/\-]', '', icno_str)
    return cleaned.strip()

def load_cisbnmid():
    """Load CISBNMID reference file"""
    try:
        df = pl.read_csv(
            f"{PATHS['DEPOSIT']}CISBNMID.txt",
            has_header=False,
            skip_rows=0
        )
        # Parse fixed positions
        data = []
        for row in df.rows():
            line = row[0]
            if len(line) >= 343:
                custno = line[4:15].strip()
                icnox = line[88:128].strip()
                last_change = line[342:352].strip()
                if icnox:
                    icno_clean = clean_icno(icnox)
                    if icno_clean:
                        data.append({
                            'CUSTNO': custno,
                            'ICNO_BNM': icno_clean,
                            'ICNOX_BNM': icnox,
                            'LAST_CHANGE': last_change
                        })
        
        df = pl.DataFrame(data)
        # Sort by CUSTNO and LAST_CHANGE descending, then take first
        if not df.is_empty():
            df = df.sort(['CUSTNO', 'LAST_CHANGE'], descending=[False, True])
            df = df.unique(subset=['CUSTNO'], keep='first')
        return df
    except:
        return pl.DataFrame()

def parse_dep_file(filename, join_val):
    """Parse a single DPTRBL file"""
    try:
        df = pl.read_csv(
            filename,
            has_header=False,
            skip_rows=0
        )
        
        data = []
        for row in df.rows():
            line = row[0]
            if len(line) < 104:  # Need at least up to position 104
                continue
            
            bankno = int(line[0:3]) if line[0:3].strip().isdigit() else 0
            applcode = line[3:8].strip()
            acctno = line[9:19].strip()
            prisec = line[28:29].strip()
            relcode = int(line[29:32].strip()) if line[29:32].strip().isdigit() else 0
            custno = line[32:43].strip()
            indorg = line[52:53].strip()
            acctbrch = line[53:60].strip()
            icnox = line[60:100].strip()
            ecpcode = line[100:103].strip() if len(line) > 100 else ''
            
            # Determine ICNO
            icno = clean_icno(icnox)
            if icno is None:
                icno = custno
            else:
                icno = icno
            
            data.append({
                'BANKNO': bankno,
                'APPLCODE': applcode,
                'ACCTNO': acctno,
                'PRISEC': prisec,
                'RELCODE': relcode,
                'CUSTNO': custno,
                'INDORG': indorg,
                'ACCTBRCH': acctbrch,
                'ICNOX': icnox,
                'ECPCODE': ecpcode,
                'ICNO': icno,
                'JOIN': join_val
            })
        
        return pl.DataFrame(data)
    except:
        return pl.DataFrame()

def parse_org_file(filename, join_val):
    """Parse DPTRBORG/DPTRB999 file (different field name)"""
    try:
        df = pl.read_csv(
            filename,
            has_header=False,
            skip_rows=0
        )
        
        data = []
        for row in df.rows():
            line = row[0]
            if len(line) < 104:
                continue
            
            bankno = int(line[0:3]) if line[0:3].strip().isdigit() else 0
            applcode = line[3:8].strip()
            acctno = line[9:19].strip()
            prisec = line[28:29].strip()
            relcode = int(line[29:32].strip()) if line[29:32].strip().isdigit() else 0
            custno = line[32:43].strip()
            indorg = line[52:53].strip()
            branch = line[53:60].strip()  # BRANCH instead of ACCTBRCH
            icnox = line[60:100].strip()
            ecpcode = line[100:103].strip() if len(line) > 100 else ''
            
            icno = clean_icno(icnox)
            if icno is None:
                icno = custno
            else:
                icno = icno
            
            data.append({
                'BANKNO': bankno,
                'APPLCODE': applcode,
                'ACCTNO': acctno,
                'PRISEC': prisec,
                'RELCODE': relcode,
                'CUSTNO': custno,
                'INDORG': indorg,
                'BRANCH': branch,  # Different field name
                'ICNOX': icnox,
                'ECPCODE': ecpcode,
                'ICNO': icno,
                'JOIN': join_val
            })
        
        return pl.DataFrame(data)
    except:
        return pl.DataFrame()

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("="*60)
    print("EIBQDCIS - Deposit CIS Processing")
    print("="*60)
    
    # Load CISBNMID reference
    print("\nLoading CISBNMID...")
    cisref = load_cisbnmid()
    print(f"  Loaded {len(cisref)} records")
    
    # Process all DEP files (1-11,14)
    print("\nProcessing DEP files...")
    all_deps = []
    
    # Files 1-11
    for i in range(1, 12):
        if i == 1:  # Special case - TRANS01, not DEP1
            continue
        f = f"{PATHS['DEPOSIT']}DPTRBL{i}.txt"
        if Path(f).exists():
            df = parse_dep_file(f, i)
            if not df.is_empty():
                all_deps.append(df)
                print(f"  DEP{i}: {len(df)} records")
    
    # File 14
    f14 = f"{PATHS['DEPOSIT']}DPTRBL14.txt"
    if Path(f14).exists():
        df14 = parse_dep_file(f14, 14)
        if not df14.is_empty():
            all_deps.append(df14)
            print(f"  DEP14: {len(df14)} records")
    
    # Process ORG and 999 files
    print("\nProcessing ORG and 999 files...")
    
    forg = f"{PATHS['DEPOSIT']}DPTRBORG.txt"
    if Path(forg).exists():
        org_df = parse_org_file(forg, 0)
        if not org_df.is_empty():
            print(f"  DPTRBORG: {len(org_df)} records")
    
    f999 = f"{PATHS['DEPOSIT']}DPTRB999.txt"
    if Path(f999).exists():
        df999 = parse_org_file(f999, 0)
        if not df999.is_empty():
            print(f"  DPTRB999: {len(df999)} records")
    
    # Process DEP1 separately (TRANS01)
    print("\nProcessing DEP1 (TRANS01)...")
    f1 = f"{PATHS['DEPOSIT']}DPTRBL1.txt"
    if Path(f1).exists():
        dep1 = parse_dep_file(f1, 1)
        if not dep1.is_empty():
            print(f"  DEP1: {len(dep1)} records")
    
    # Merge with CISBNMID for all datasets
    print("\nMerging with CISBNMID...")
    
    def merge_with_cisref(df, name):
        if df.is_empty():
            return df
        if 'CUSTNO' not in df.columns:
            return df
        
        merged = df.join(cisref, on='CUSTNO', how='left')
        # Apply ICNO replacement logic
        merged = merged.with_columns([
            pl.when(
                (~pl.col('ICNO').str.slice(0,2).eq('IC')) & 
                pl.col('ICNO_BNM').is_not_null()
            ).then(pl.col('ICNO_BNM')).otherwise(pl.col('ICNO')).alias('ICNO'),
            pl.when(
                (~pl.col('ICNO').str.slice(0,2).eq('IC')) & 
                pl.col('ICNO_BNM').is_not_null()
            ).then(pl.col('ICNOX_BNM')).otherwise(pl.col('ICNOX')).alias('ICNOX')
        ])
        return merged
    
    # Apply to all datasets
    if dep1:
        dep1 = merge_with_cisref(dep1, 'DEP1')
    
    merged_deps = []
    for df in all_deps:
        merged_deps.append(merge_with_cisref(df, 'DEP'))
    
    if org_df:
        org_df = merge_with_cisref(org_df, 'ORG')
    
    if df999:
        df999 = merge_with_cisref(df999, '999')
    
    # ========== CREATE TRANS POSED DATASETS ==========
    print("\nCreating transposed datasets...")
    
    def create_transposed(df, join_val):
        """Create transposed dataset for a given join count"""
        if df.is_empty():
            return pl.DataFrame()
        
        # Group by ACCTNO and collect all ICNOs
        grouped = df.group_by('ACCTNO').agg([
            pl.col('ICNO').alias('ICNOS'),
            pl.first('BANKNO').alias('BANKNO'),
            pl.first('APPLCODE').alias('APPLCODE'),
            pl.first('PRISEC').alias('PRISEC'),
            pl.first('RELCODE').alias('RELCODE'),
            pl.first('CUSTNO').alias('CUSTNO'),
            pl.first('INDORG').alias('INDORG'),
            pl.first('ACCTBRCH' if 'ACCTBRCH' in df.columns else 'BRANCH').alias('BRANCH'),
            pl.first('ECPCODE').alias('ECPCODE')
        ])
        
        # Create KEY string
        grouped = grouped.with_columns([
            pl.col('ICNOS').map_elements(
                lambda x: ','.join([str(i) for i in x if i]), 
                return_dtype=pl.Utf8
            ).alias('KEY')
        ])
        
        grouped = grouped.with_columns(pl.lit(join_val).alias('JOIN'))
        
        return grouped
    
    # Create transposed for each join count
    transposed = []
    
    # JOIN 0 (ORG and 999)
    if org_df:
        t0 = create_transposed(org_df, 0)
        if not t0.is_empty():
            transposed.append(t0)
            print(f"  TRANS0: {len(t0)} records")
    
    if df999:
        t999 = create_transposed(df999, 0)
        if not t999.is_empty():
            transposed.append(t999)
            print(f"  TRANS999: {len(t999)} records")
    
    # JOIN 1 (DEP1)
    if dep1:
        t1 = create_transposed(dep1, 1)
        if not t1.is_empty():
            transposed.append(t1)
            print(f"  TRANS1: {len(t1)} records")
    
    # JOIN 2-11,14
    join_map = {2: [], 3: [], 4: [], 5: [], 6: [], 7: [], 8: [], 9: [], 10: [], 11: [], 14: []}
    for df in merged_deps:
        if df.is_empty():
            continue
        join_val = df['JOIN'][0] if not df.is_empty() else None
        if join_val in join_map:
            t = create_transposed(df, join_val)
            if not t.is_empty():
                transposed.append(t)
                print(f"  TRANS{join_val}: {len(t)} records")
    
    # ========== COMBINE ALL ==========
    print("\nCombining all transposed datasets...")
    
    if not transposed:
        print("  No data found!")
        return
    
    combined = pl.concat(transposed)
    
    # Deduplicate by ACCTNO (keep highest JOIN count? SAS keeps all but we'll keep one per source)
    # SAS keeps separate files per JOIN value, we'll keep combined with JOIN indicator
    
    print(f"  Total combined: {len(combined)} records")
    
    # Save combined output
    combined.write_parquet(f"{PATHS['OUTPUT']}DEPOSIT_TRANS.parquet")
    
    # Also save separate files per JOIN (like SAS)
    for join_val in sorted(combined['JOIN'].unique().to_list()):
        subset = combined.filter(pl.col('JOIN') == join_val)
        if not subset.is_empty():
            subset.write_parquet(f"{PATHS['OUTPUT']}TRANS{join_val:02d}.parquet")
            print(f"  TRANS{join_val:02d}: {len(subset)} records")
    
    # ========== SAMPLE PRINT ==========
    print("\nSample records (first 100):")
    sample = combined.head(100)
    for row in sample.rows(named=True):
        print(f"  ACCTNO: {row['ACCTNO']}, JOIN: {row['JOIN']}, KEY: {row['KEY'][:50]}...")
    
    # ========== SUMMARY ==========
    print("\n"+"="*60)
    print("SUMMARY")
    print("="*60)
    
    print(f"\nTotal accounts processed: {len(combined)}")
    print(f"JOIN count distribution:")
    join_dist = combined.group_by('JOIN').agg(pl.len().alias('COUNT')).sort('JOIN')
    for row in join_dist.rows(named=True):
        join_desc = JOIN_FMT.get(row['JOIN'], f'JOIN {row["JOIN"]}')
        print(f"  {join_desc}: {row['COUNT']:,} accounts")
    
    # Average ICNOs per account
    icno_counts = combined.with_columns([
        pl.col('KEY').str.split(',').map_elements(len, return_dtype=pl.Int32).alias('ICNO_COUNT')
    ])
    avg_icnos = icno_counts['ICNO_COUNT'].mean()
    max_icnos = icno_counts['ICNO_COUNT'].max()
    print(f"\nAverage ICNOs per account: {avg_icnos:.2f}")
    print(f"Maximum ICNOs per account: {max_icnos}")
    
    print("\n"+"="*60)
    print("✓ EIBQDCIS Complete")

if __name__ == "__main__":
    main()
