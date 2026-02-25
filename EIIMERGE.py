"""
EIIMERGE - Islamic FM File Merger (PIBB) - Production Ready

Purpose:
- Merge FM files from Islamic NPL (EIIM1503) and Islamic IL (EIIMSPCL)
- Islamic version of EIBMERGE
- Combine count, amount, and average files for PIBB entity
- Produce consolidated FM reports for Islamic banking

Input Files (from EIIM1503 and EIIMSPCL):
- NPLA: PIBB NPL amount file (B_NPL measure)
- SPLA: PIBB IL amount file (B_IL measure)
- NPLC: PIBB NPL count file (C_NPL measure)
- SPLC: PIBB IL count file (C_IL measure)
- NPLV: PIBB NPL average file (B_NPL measure)
- SPLV: PIBB IL average file (B_IL measure)

Output Files:
- CNT: Merged count file (C_NPL + C_IL) - PIBB entity
- AMT: Merged amount file (B_NPL + B_IL) - PIBB entity
- AMT2: Pipe-delimited version - PIBB entity
- AVG: Merged average file (B_NPL + B_IL) - PIBB entity

Key Difference from EIBMERGE:
- Entity: 'PIBB' (Islamic) instead of 'PBB' (Conventional)
- Same processing logic and file structure

Processing:
1. Read and combine Islamic NPL + IL files
2. Calculate totals
3. Write merged FM format files with PIBB entity
"""

import polars as pl
import os

# Directories
INPUT_DIR = 'data/output/'
OUTPUT_DIR = 'data/output/merged/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIIMERGE - Islamic FM File Merger (PIBB)")
print("=" * 60)

# Helper function to parse FM file
def parse_fm_line(line, value_type='amount'):
    """Parse FM format line and extract fields"""
    if line[0:1] != 'D':
        return None
    
    ym = line[266:273].strip()
    lob = line[200:210].strip()
    producx = line[233:243].strip()
    brancx = line[167:172].strip()
    type_code = line[68:73].strip()
    y_flag = line[315:316].strip()
    
    if value_type == 'amount':
        value = float(line[299:314]) if line[299:314].strip() else 0.0
    elif value_type == 'count':
        value = int(line[299:314]) if line[299:314].strip() else 0
    else:  # average
        avg_type = line[101:111].strip()
        value = float(line[299:314]) if line[299:314].strip() else 0.0
        return {
            'YM': ym,
            'LOB': lob,
            'PRODUCX': producx,
            'BRANCX': brancx,
            'TYPE': type_code,
            'Y': y_flag,
            'AVGAMR': value,
            'AVG': avg_type
        }
    
    return {
        'YM': ym,
        'LOB': lob,
        'PRODUCX': producx,
        'BRANCX': brancx,
        'TYPE': type_code,
        'Y': y_flag,
        'AMOUNT' if value_type == 'amount' else 'NOACC': value
    }

# Process Amount Files (SAMT + FAMT)
print("\nProcessing Islamic Amount Files...")
try:
    # Read SPLA (Islamic IL amount)
    samt_data = []
    with open(f'{INPUT_DIR}NPLA', 'r') as f:
        for line in f:
            parsed = parse_fm_line(line, 'amount')
            if parsed:
                samt_data.append(parsed)
    
    # Read NPLA (Islamic NPL amount)
    famt_data = []
    with open(f'{INPUT_DIR}NPLA', 'r') as f:
        for line in f:
            parsed = parse_fm_line(line, 'amount')
            if parsed:
                famt_data.append(parsed)
    
    # Combine
    amt_data = samt_data + famt_data
    df_amt = pl.DataFrame(amt_data) if amt_data else pl.DataFrame([])
    
    print(f"  ✓ SPLA (IL): {len(samt_data):,} records")
    print(f"  ✓ NPLA (NPL): {len(famt_data):,} records")
    print(f"  ✓ Combined Amount: {len(df_amt):,} records")

except Exception as e:
    print(f"  ⚠ Amount files: {e}")
    df_amt = pl.DataFrame([])

# Process Count Files (SCNT + FCNT)
print("\nProcessing Islamic Count Files...")
try:
    # Read SPLC (Islamic IL count)
    scnt_data = []
    with open(f'{INPUT_DIR}NPLC', 'r') as f:
        for line in f:
            parsed = parse_fm_line(line, 'count')
            if parsed:
                scnt_data.append(parsed)
    
    # Read NPLC (Islamic NPL count)
    fcnt_data = []
    with open(f'{INPUT_DIR}NPLC', 'r') as f:
        for line in f:
            parsed = parse_fm_line(line, 'count')
            if parsed:
                fcnt_data.append(parsed)
    
    # Combine
    cnt_data = scnt_data + fcnt_data
    df_cnt = pl.DataFrame(cnt_data) if cnt_data else pl.DataFrame([])
    
    print(f"  ✓ SPLC (IL): {len(scnt_data):,} records")
    print(f"  ✓ NPLC (NPL): {len(fcnt_data):,} records")
    print(f"  ✓ Combined Count: {len(df_cnt):,} records")

except Exception as e:
    print(f"  ⚠ Count files: {e}")
    df_cnt = pl.DataFrame([])

# Process Average Files (SAVG + FAVG)
print("\nProcessing Islamic Average Files...")
try:
    # Read SPLV (Islamic IL average)
    savg_data = []
    with open(f'{INPUT_DIR}NPLV', 'r') as f:
        for line in f:
            parsed = parse_fm_line(line, 'average')
            if parsed:
                savg_data.append(parsed)
    
    # Read NPLV (Islamic NPL average)
    favg_data = []
    with open(f'{INPUT_DIR}NPLV', 'r') as f:
        for line in f:
            parsed = parse_fm_line(line, 'average')
            if parsed:
                favg_data.append(parsed)
    
    # Combine
    avg_data = savg_data + favg_data
    df_avg = pl.DataFrame(avg_data) if avg_data else pl.DataFrame([])
    
    print(f"  ✓ SPLV (IL): {len(savg_data):,} records")
    print(f"  ✓ NPLV (NPL): {len(favg_data):,} records")
    print(f"  ✓ Combined Average: {len(df_avg):,} records")

except Exception as e:
    print(f"  ⚠ Average files: {e}")
    df_avg = pl.DataFrame([])

# Generate merged CNT file (PIBB)
print("\nGenerating merged CNT file (PIBB)...")
if len(df_cnt) > 0:
    totcnt = df_cnt['NOACC'].sum()
    lcnt = len(df_cnt)
    
    with open(f'{OUTPUT_DIR}CNT', 'w') as f:
        for row in df_cnt.iter_rows(named=True):
            line = (
                f"D,PIBB{'':64}{row['TYPE']}"
                f"{'':196}{row['YM']}{row['NOACC']:>15}"
                f"{row['LOB']:>10}{row['PRODUCX']:>10}EXT{'':65}ACTUAL{'':65}"
                f"{row['BRANCX']:>5},{'':66},{'':32},MYR{'':32},"
                f"{'':32},{'':32},{'':32},{row['Y']},{'':32},{'':32},{'':32},;"
            )
            f.write(line + '\n')
        
        # Trailer
        f.write(f"T,{lcnt:>10},{totcnt:>15},;\n")
    
    print(f"  ✓ CNT (PIBB): {lcnt:,} records, Total: {totcnt:,}")
else:
    print(f"  ⚠ No count data to merge")

# Generate merged AMT file (PIBB)
print("\nGenerating merged AMT file (PIBB)...")
if len(df_amt) > 0:
    totamt = df_amt['AMOUNT'].sum()
    lcnt = len(df_amt)
    
    with open(f'{OUTPUT_DIR}AMT', 'w') as f:
        for row in df_amt.iter_rows(named=True):
            line = (
                f"D,PIBB{'':64}{row['TYPE']}"
                f"{'':196}{row['YM']}{row['AMOUNT']:>15.2f}"
                f"{row['LOB']:>10}{row['PRODUCX']:>10}EXT{'':65}ACTUAL{'':65}"
                f"{row['BRANCX']:>5},{'':66},{'':32},MYR{'':32},"
                f"{'':32},{'':32},{'':32},{row['Y']},{'':32},{'':32},{'':32},;"
            )
            f.write(line + '\n')
        
        # Trailer
        f.write(f"T,{lcnt:>10},{totamt:>15.2f},;\n")
    
    print(f"  ✓ AMT (PIBB): {lcnt:,} records, Total: RM {totamt:,.2f}")
else:
    print(f"  ⚠ No amount data to merge")

# Generate pipe-delimited AMT2 file (PIBB)
print("\nGenerating pipe-delimited AMT2 file (PIBB)...")
if len(df_amt) > 0:
    with open(f'{OUTPUT_DIR}AMT2', 'w') as f:
        for row in df_amt.iter_rows(named=True):
            line = (
                f"D|PIBB{'':30}|EXT{'':31}|{row['TYPE']}"
                f"{'':31}|ACTUAL{'':31}|MYR{'':31}|{row['BRANCX']:>5}"
                f"{'':31}|{row['LOB']:>10}{'':21}|{row['PRODUCX']:>10}"
                f"{'':21}|{row['YM']}{'':31}|{row['AMOUNT']:>15.2f}|"
                f"{row['Y']}|{'':32}|{'':32}|{'':32}|"
            )
            f.write(line + '\n')
    
    print(f"  ✓ AMT2 (PIBB pipe-delimited): {lcnt:,} records")
else:
    print(f"  ⚠ No amount data for AMT2")

# Generate merged AVG file (PIBB)
print("\nGenerating merged AVG file (PIBB)...")
if len(df_avg) > 0:
    tavg = df_avg['AVGAMR'].sum()
    lcnt = len(df_avg)
    
    with open(f'{OUTPUT_DIR}AVG', 'w') as f:
        for row in df_avg.iter_rows(named=True):
            line = (
                f"D,PIBB{'':64}{row['TYPE']}"
                f"{'':196}{row['YM']}{row['AVGAMR']:>15.2f}"
                f"{row['LOB']:>10}{row['PRODUCX']:>10}EXT{'':65}{row['AVG']}"
                f"{'':65}{row['BRANCX']:>5},{'':66},{'':32},MYR{'':32},"
                f"{'':32},{'':32},{'':32},{row['Y']},{'':32},{'':32},{'':32},;"
            )
            f.write(line + '\n')
        
        # Trailer
        f.write(f"T,{lcnt:>10},{tavg:>15.2f},;\n")
    
    print(f"  ✓ AVG (PIBB): {lcnt:,} records, Total: RM {tavg:,.2f}")
else:
    print(f"  ⚠ No average data to merge")

print(f"\n{'='*60}")
print(f"✓ EIIMERGE Complete!")
print(f"{'='*60}")
print(f"\nIslamic FM Files Summary:")
print(f"\nInput Files (PIBB):")
print(f"  NPL Files (EIIM1503):")
print(f"    - NPLA (B_NPL amount)")
print(f"    - NPLC (C_NPL count)")
print(f"    - NPLV (B_NPL average)")
print(f"\n  IL Files (EIIMSPCL):")
print(f"    - SPLA (B_IL amount)")
print(f"    - SPLC (C_IL count)")
print(f"    - SPLV (B_IL average)")
print(f"\nOutput Files (PIBB):")
print(f"  - CNT: Merged count (NPL + IL)")
print(f"  - AMT: Merged amount (NPL + IL)")
print(f"  - AMT2: Pipe-delimited amount")
print(f"  - AVG: Merged average (NPL + IL)")
print(f"\nEntity Comparison:")
print(f"  EIBMERGE:  PBB (Conventional)")
print(f"  EIIMERGE: PIBB (Islamic)")
print(f"  Logic: Same")
print(f"  Format: FM standard")
