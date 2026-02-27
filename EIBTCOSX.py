"""
EIBTCOSX - BTR System Cost Analysis

Purpose: Count new GL accounts created in BTR system within period

Logic:
- Read current and previous month REPTDATEs
- Filter GL accounts created between dates
- Exclude specific GL codes
- Count by category

Transaction Code Lists:
- TRX: Transaction codes
- XRN, TRN, BTR, SRC, SRE, SRX: Various code categories
- LIAB format: Deposit/Investment classification
"""

import polars as pl
from datetime import datetime
import os

# Directories
MNILN_DIR = 'data/mniln/'
MNILNP_DIR = 'data/mnilnp/'
BTRSA_DIR = 'data/btrsa/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIBTCOSX - BTR System Cost Analysis")

# Transaction code lists (for reference)
TRX = [150,151,202,205,208,209,293,340,343,600,601,602,603,604,605,606,607,608,609,
       610,612,613,616,620,621,622,623,626,627,629,630,631,637,641,648,649,651,658,
       659,660,661,662,663]

XRN = [150,151,202,205,293,340,343,230,241,242,253,261,263,270,280,281,282,285,287,
       290,291,292,313,315,325,531,532,533,561,630,640,650,651,660,670,680]

TRN = [150,151,202,205,293,340,343,230,241,242,253,261,263,270,280,281,282,285,287,
       290,291,292,313,315,325,531,532,533,561,630,640,650,651,660,670,680]

BTR = [361]

SRC = [511,512,513,521,522,523,526,527]

SRE = [1,3,100,211,213,215,228,230,241,242,253,261,263,270,280,281,282,285,287,290,
       291,292,313,315,325,531,532,533,561,630,640,650,651,660,670,680]

SRX = [0,511,512,513,521,522,523,526,527,5,251,321,323,399,325,331,333,225,226,341,
       342,343,220,361]

# LIAB format classification
LIAB_D = ['TFL','TML','TFC','TMC','TFO','TMO','TLL','TNL','TLC','TNC','TLO','TNO','PBU',
          'PBR','TLZ','TLQ','TLS','TLX','TLY','BAP','BAI','BAS','BAE','PBA','FAS','FAU',
          'PFU','FDS','FDU','PFD','FCL','DAS','DAU','PAU','DDS','DDU','DDT','PDU','PDT',
          'FTB','ITB','PTB','POS','PRO','BRM','BRN','PBO','PBD','PBQ','PBZ','VAL','DIL',
          'FIL','PRE','PCR','BRF','BRL','PUM']

LIAB_I = ['IFS','IFD','IFU','IFO','ILS','ILB','ILU','ILL','SFC','SLC','TFR','TLR','BFC',
          'BLC','DLC','RFC','RLC','PLC','ALC','SGL','SGC','APG','BGF','BGT','BGP','190',
          '200','BUF','BUL','BRA']

# Excluded GL codes
EXCLUDED_GL = ['0062','0056','0053','0064','0066','0012','0014','0016',
               '0059','0047','0049','0044','0042','0034']

# Read current REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{MNILN_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    edate = int(reptdate.strftime('%y%m%d'))
    reptday = f"{reptdate.day:02d}"
    reptmon = f"{reptdate.month:02d}"
    
    print(f"Current REPTDATE: {reptdate.strftime('%d/%m/%Y')} (EDATE: {edate})")
except Exception as e:
    print(f"✗ ERROR reading REPTDATE: {e}")
    import sys
    sys.exit(1)

# Read previous month REPTDATE
try:
    df_sdate = pl.read_parquet(f'{MNILNP_DIR}REPTDATE.parquet')
    sdate_dt = df_sdate['REPTDATE'][0]
    sdate = int(sdate_dt.strftime('%y%m%d'))
    
    print(f"Previous REPTDATE: {sdate_dt.strftime('%d/%m/%Y')} (SDATE: {sdate})")
except Exception as e:
    print(f"✗ ERROR reading SDATE: {e}")
    import sys
    sys.exit(1)

# Read BTRSA COMM file
try:
    comm_file = f'{BTRSA_DIR}COMM{reptday}{reptmon}.parquet'
    
    if not os.path.exists(comm_file):
        print(f"⚠ COMM file not found: {comm_file}")
        # Create empty result
        df_prngl = pl.DataFrame([])
    else:
        df_comm = pl.read_parquet(comm_file)
        
        print(f"✓ Read COMM{reptday}{reptmon}: {len(df_comm):,} records")
        
        # Process GL accounts
        df_prngl = df_comm.with_columns([
            # Convert CREATDS to date
            pl.when(pl.col('CREATDS').is_not_null() & (pl.col('CREATDS') != 0))
              .then(
                  pl.col('CREATDS').cast(pl.Utf8).str.zfill(6)
              )
              .otherwise(None)
              .alias('ISSDTE_STR')
        ])
        
        # Convert ISSDTE_STR to integer for comparison (YYMMDD format)
        df_prngl = df_prngl.with_columns([
            pl.when(pl.col('ISSDTE_STR').is_not_null())
              .then(pl.col('ISSDTE_STR').cast(pl.Int64))
              .otherwise(None)
              .alias('ISSDTE')
        ])
        
        # Filter conditions
        df_prngl = df_prngl.filter(
            # GL mnemonic starts with '00'
            (pl.col('GLMNEMO').str.slice(0, 2) == '00') &
            # Created between SDATE and EDATE
            (pl.col('ISSDTE') > sdate) &
            (pl.col('ISSDTE') <= edate) &
            # Not in excluded list
            (~pl.col('GLMNEMO').is_in(EXCLUDED_GL))
        )
        
        # Add fields
        df_prngl = df_prngl.with_columns([
            pl.lit(1).alias('NOACC'),
            pl.lit('BTR-SYS').alias('CAT')
        ])
        
        print(f"✓ Filtered: {len(df_prngl):,} new GL accounts")

except Exception as e:
    print(f"✗ ERROR processing COMM: {e}")
    df_prngl = pl.DataFrame([])

# Summarize
if len(df_prngl) > 0:
    df_summary = df_prngl.group_by('CAT').agg([
        pl.col('NOACC').sum().alias('NOACC')
    ])
    
    # Generate output
    with open(f'{OUTPUT_DIR}COSTX', 'w') as f:
        for row in df_summary.iter_rows(named=True):
            line = f"{row['CAT']:<15}{row['NOACC']:>12,}\n"
            f.write(line)
    
    print(f"\n✓ Generated COSTX report")
    print(f"\nSummary:")
    for row in df_summary.iter_rows(named=True):
        print(f"  {row['CAT']}: {row['NOACC']:,} accounts")
else:
    print("\n⚠ No new GL accounts in period")
    # Write empty report
    with open(f'{OUTPUT_DIR}COSTX', 'w') as f:
        f.write(f"{'BTR-SYS':<15}{0:>12,}\n")

print("\n✓ EIBTCOSX Complete")
print(f"\nPeriod: {sdate_dt.strftime('%d/%m/%Y')} to {reptdate.strftime('%d/%m/%Y')}")
print(f"GL Filter: Starts with '00', excludes {len(EXCLUDED_GL)} specific codes")
print(f"\nTransaction Code Categories:")
print(f"  TRX: {len(TRX)} codes")
print(f"  XRN: {len(XRN)} codes")
print(f"  TRN: {len(TRN)} codes")
print(f"  BTR: {len(BTR)} codes")
print(f"  SRC: {len(SRC)} codes")
print(f"  SRE: {len(SRE)} codes")
print(f"  SRX: {len(SRX)} codes")
print(f"\nLIAB Classification:")
print(f"  Deposit ('D'): {len(LIAB_D)} codes")
print(f"  Investment ('I'): {len(LIAB_I)} codes")
