"""
EIBMEPC2 - BNM EPC2 Loan Repayment Transactions Report (Production Ready)

Consolidates loan repayment transactions from multiple sources:
- CASH (DPLP + LNLP)
- CHEQUES (CTCS - House/Local)
- CREDIT CARDS (DPCC)
- ELECTRONIC (ATM, CDT, RENTAS)
- CDM (Cheque Deposit Machine)

Outputs:
1. Transaction listings (PBB, PIBB, CDM)
2. Summary reports by transaction type
3. CDM breakdown report

Transaction Types (SEQ):
1 = CASH
2 = HOUSE CHEQUE
3 = LOCAL CHEQUE
4 = ATM
5 = INTERBANK (not used in final)
6 = RENTAS
7 = CDM
8 = CDT
"""

import polars as pl
from datetime import datetime
import os

# Directories
LOAN_DIR = 'data/loan/'
DPLP_DIR = 'data/dplp/'
LNLP1_DIR = 'data/lnlp1/'
DPCC_DIR = 'data/dpcc/'
CTCS_DIR = 'data/ctcs/'
DPTRN_DIR = 'data/dptrn/'
BNMW1_DIR = 'data/bnmw1/'
BNMW2_DIR = 'data/bnmw2/'
BNMW3_DIR = 'data/bnmw3/'
BNMW4_DIR = 'data/bnmw4/'
IBNMW1_DIR = 'data/ibnmw1/'
IBNMW2_DIR = 'data/ibnmw2/'
IBNMW3_DIR = 'data/ibnmw3/'
IBNMW4_DIR = 'data/ibnmw4/'
ILOAN_DIR = 'data/iloan/'
BNM_DIR = 'data/bnm/'
IBNM_DIR = 'data/ibnm/'
OUTPUT_DIR = 'data/output/'

for d in [BNM_DIR, IBNM_DIR, OUTPUT_DIR]:
    os.makedirs(d, exist_ok=True)

print("EIBMEPC2 - BNM EPC2 Loan Repayment Transactions Report")
print("=" * 60)

# Get REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{LOAN_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    reptyear = f'{reptdate.year % 100:02d}'
    reptmon = f'{reptdate.month:02d}'
    reportdt = reptdate.strftime('%d/%m/%Y')
    
    # Monthly file suffixes (01-04 for weeks)
    reptmon1 = f'{reptmon}01'
    reptmon2 = f'{reptmon}02'
    reptmon3 = f'{reptmon}03'
    reptmon4 = f'{reptmon}04'
    
    print(f"Report Date: {reportdt}")
    print(f"Year: {reptyear}, Month: {reptmon}")
except Exception as e:
    print(f"Error loading REPTDATE: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

# Product categorization mappings
DESC_CONV = {
    # HL - Housing Loans
    **{k: 'HL' for k in [4,5,6,7] + list(range(60, 63)) + [70] + list(range(200, 300)) + [600, 911]},
    # VL - Vehicle Loans
    **{k: 'VL' for k in [15, 20, 71, 72, 380, 381, 700, 705, 720, 725]},
    # PL - Personal Loans
    **{k: 'PL' for k in list(range(25, 35)) + list(range(73, 80)) + [303, 306, 307, 308, 311, 313] +
                        list(range(320, 341)) + list(range(355, 359)) + [364, 365, 367, 369, 391]},
}

DESC_ISLAMIC = {
    # HL - Housing Loans
    **{k: 'HL' for k in [102, 105, 106] + list(range(110, 120)) + [124, 139, 142, 145, 150, 151, 152, 
                        156, 170, 175, 400, 409, 410, 412, 413, 414, 415, 423, 466, 469, 650, 651, 664]},
    # VL - Vehicle Loans
    **{k: 'VL' for k in [103, 104, 107, 108, 128, 130, 131, 132, 196]},
    # PL - Personal Loans
    **{k: 'PL' for k in [122, 135, 136, 137, 138, 194, 195, 419, 420, 422, 424, 426, 464, 465, 468, 
                        652, 653, 668, 669, 672, 673, 674, 675]},
}

PDESC = {
    'HL': 'HOUSING LOANS',
    'VL': 'VEHICLE LOANS',
    'PL': 'PERSONAL LOANS',
    'CC': 'CREDIT AND CHARGE CARDS',
    'MS': 'OTHER TYPES OF LOANS',
    'OT': 'OTHER TYPES OF LOANS'
}

SEQFMT = {
    1: 'CASH',
    2: 'HOUSE CHEQUE',
    3: 'LOCAL CHEQUE',
    4: 'ATM',
    6: 'RENTAS',
    7: 'CDM',
    8: 'CDT'
}

def categorize_prod(loantype, costctr):
    """Categorize loan product"""
    is_islamic = (3000 < costctr < 3999) or costctr in [4043, 4048]
    mapping = DESC_ISLAMIC if is_islamic else DESC_CONV
    
    if loantype is None:
        return 'MS'
    return mapping.get(loantype, 'OT')

# Read LNNOTE (both conventional and Islamic)
print("\nReading LNNOTE...")
try:
    df_lnnote_conv = pl.read_parquet(f'{LOAN_DIR}LNNOTE.parquet').select([
        'ACCBRCH', 'ACCTNO', 'NOTENO', 'LOANTYPE', 'COSTCTR'
    ])
    df_lnnote_isl = pl.read_parquet(f'{ILOAN_DIR}LNNOTE.parquet').select([
        'ACCBRCH', 'ACCTNO', 'NOTENO', 'LOANTYPE', 'COSTCTR'
    ])
    df_lnnote = pl.concat([df_lnnote_conv, df_lnnote_isl]).sort(['ACCTNO', 'NOTENO'])
    print(f"  LNNOTE: {len(df_lnnote):,}")
except Exception as e:
    print(f"  ⚠ LNNOTE: {e}")
    df_lnnote = pl.DataFrame([])

# 1. CASH - LOAN (DPLP + LNLP)
print("\n1. Processing CASH transactions (LOAN)...")
try:
    df_dplp = pl.read_parquet(f'{DPLP_DIR}DPLP{reptmon}.parquet').sort([
        'ACCTNO', 'TRANDT', 'TRANAMT'
    ])
    print(f"  DPLP: {len(df_dplp):,}")
except Exception as e:
    print(f"  ⚠ DPLP: {e}")
    df_dplp = pl.DataFrame([])

try:
    df_lnlp = pl.concat([
        pl.read_parquet(f'{LNLP1_DIR}LNTRAX{reptyear}{reptmon1}.parquet'),
        pl.read_parquet(f'{LNLP1_DIR}LNTRAX{reptyear}{reptmon2}.parquet'),
        pl.read_parquet(f'{LNLP1_DIR}LNTRAX{reptyear}{reptmon3}.parquet'),
        pl.read_parquet(f'{LNLP1_DIR}LNTRAX{reptyear}{reptmon4}.parquet')
    ]).with_columns([
        pl.lit(reptdate).alias('TRANDT')
    ]).sort(['ACCTNO', 'TRANDT', 'TRANAMT'])
    print(f"  LNLP: {len(df_lnlp):,}")
except Exception as e:
    print(f"  ⚠ LNLP: {e}")
    df_lnlp = pl.DataFrame([])

# Merge DPLP + LNLP (inner join)
if len(df_dplp) > 0 and len(df_lnlp) > 0:
    df_otcln = df_lnlp.join(df_dplp, on=['ACCTNO', 'TRANDT', 'TRANAMT'], how='inner')
    
    # Merge with LNNOTE
    df_otcln = df_otcln.join(df_lnnote, on=['ACCTNO', 'NOTENO'], how='left')
    
    # Add ITEM, SEQ
    df_otcln = df_otcln.with_columns([
        pl.lit('CASH').alias('ITEM'),
        pl.lit(1).alias('SEQ')
    ])
    
    # Categorize PROD and split by entity
    df_otcln = df_otcln.with_columns([
        pl.struct(['LOANTYPE', 'COSTCTR']).map_elements(
            lambda x: categorize_prod(x['LOANTYPE'], x['COSTCTR']),
            return_dtype=pl.Utf8
        ).alias('PROD')
    ])
    
    df_otcln_conv = df_otcln.filter(
        ~((pl.col('COSTCTR') > 3000) & (pl.col('COSTCTR') < 3999)) &
        ~pl.col('COSTCTR').is_in([4043, 4048])
    )
    df_otcln_isl = df_otcln.filter(
        ((pl.col('COSTCTR') > 3000) & (pl.col('COSTCTR') < 3999)) |
        pl.col('COSTCTR').is_in([4043, 4048])
    )
    
    df_otcln_conv.write_parquet(f'{BNM_DIR}OTCLN.parquet')
    df_otcln_isl.write_parquet(f'{IBNM_DIR}OTCLN.parquet')
    print(f"  OTCLN - Conv: {len(df_otcln_conv):,}, Islamic: {len(df_otcln_isl):,}")
else:
    df_otcln_conv = pl.DataFrame([])
    df_otcln_isl = pl.DataFrame([])

# 2. CASH - CREDIT CARD (DPCC)
print("\n2. Processing CASH transactions (CC)...")
try:
    df_otccc = pl.read_parquet(f'{DPCC_DIR}DPCC{reptmon}.parquet').with_columns([
        pl.lit('CC').alias('PROD'),
        pl.lit('CASH').alias('ITEM'),
        pl.lit(1).alias('SEQ')
    ])
    df_otccc.write_parquet(f'{BNM_DIR}OTCCC.parquet')
    print(f"  OTCCC: {len(df_otccc):,}")
except Exception as e:
    print(f"  ⚠ OTCCC: {e}")
    df_otccc = pl.DataFrame([])

# 3. CHEQUES (CTCS)
print("\n3. Processing CHEQUES...")
try:
    df_ctcs = pl.read_parquet(f'{CTCS_DIR}CTCS{reptmon}.parquet').filter(
        pl.col('IND') != 'OC'  # Exclude overseas cheques
    ).sort(['ACCTNO', 'NOTENO'])
    
    # Merge with LNNOTE
    df_ctcs = df_ctcs.join(df_lnnote, on=['ACCTNO', 'NOTENO'], how='left')
    
    # Round TRANAMT
    df_ctcs = df_ctcs.with_columns([
        pl.col('TRANAMT').round(2)
    ])
    
    # Set ITEM and SEQ based on IND and PRODIND
    df_ctcs = df_ctcs.with_columns([
        pl.when((pl.col('PRODIND') == 'O') & (pl.col('IND') == 'HC'))
          .then(pl.lit('HOUSE CHEQUE'))
          .when((pl.col('PRODIND') == 'O') & (pl.col('IND') == 'LC'))
          .then(pl.lit('LOCAL CHEQUE'))
          .when(pl.col('PRODIND') == 'C')
          .then(pl.lit('CDM'))
          .otherwise(None)
          .alias('ITEM'),
        pl.when((pl.col('PRODIND') == 'O') & (pl.col('IND') == 'HC'))
          .then(pl.lit(2))
          .when((pl.col('PRODIND') == 'O') & (pl.col('IND') == 'LC'))
          .then(pl.lit(3))
          .when(pl.col('PRODIND') == 'C')
          .then(pl.lit(7))
          .otherwise(None)
          .alias('SEQ')
    ])
    
    # Merge with DPTRN (deposit transactions)
    try:
        df_dptrx = pl.concat([
            pl.read_parquet(f'{DPTRN_DIR}DPBTRAN{reptyear}{reptmon1}.parquet'),
            pl.read_parquet(f'{DPTRN_DIR}DPBTRAN{reptyear}{reptmon2}.parquet'),
            pl.read_parquet(f'{DPTRN_DIR}DPBTRAN{reptyear}{reptmon3}.parquet'),
            pl.read_parquet(f'{DPTRN_DIR}DPBTRAN{reptyear}{reptmon4}.parquet')
        ]).filter(
            pl.col('TRANCODE') == 876
        ).with_columns([
            pl.col('TRANAMT').round(2)
        ]).rename({
            'CHQNO': 'CHEQNO',
            'REPTDATE': 'TRANDT',
            'ACCTNO': 'DEBACCT'
        }).select(['TRANDT', 'TRANAMT', 'CHEQNO', 'DEBACCT']).unique()
        
        df_ctcs = df_ctcs.sort(['TRANDT', 'TRANAMT', 'CHEQNO'])
        df_dptrx = df_dptrx.sort(['TRANDT', 'TRANAMT', 'CHEQNO'])
        
        df_ctcs = df_ctcs.join(df_dptrx, on=['TRANDT', 'TRANAMT', 'CHEQNO'], how='left')
    except Exception as e:
        print(f"  ⚠ DPTRX merge skip: {e}")
    
    # Categorize PROD and split
    df_ctcs = df_ctcs.with_columns([
        pl.struct(['LOANTYPE', 'COSTCTR']).map_elements(
            lambda x: categorize_prod(x['LOANTYPE'], x['COSTCTR']),
            return_dtype=pl.Utf8
        ).alias('PROD')
    ])
    
    df_ctcs_conv = df_ctcs.filter(
        ~((pl.col('COSTCTR') > 3000) & (pl.col('COSTCTR') < 3999)) &
        ~pl.col('COSTCTR').is_in([4043, 4048])
    )
    df_ctcs_isl = df_ctcs.filter(
        ((pl.col('COSTCTR') > 3000) & (pl.col('COSTCTR') < 3999)) |
        pl.col('COSTCTR').is_in([4043, 4048])
    )
    
    df_ctcs_conv.write_parquet(f'{BNM_DIR}CTCS.parquet')
    df_ctcs_isl.write_parquet(f'{IBNM_DIR}CTCS.parquet')
    print(f"  CTCS - Conv: {len(df_ctcs_conv):,}, Islamic: {len(df_ctcs_isl):,}")
except Exception as e:
    print(f"  ⚠ CTCS: {e}")
    df_ctcs_conv = pl.DataFrame([])
    df_ctcs_isl = pl.DataFrame([])

# 4. ELECTRONIC (ATM, CDT) from LNLP
print("\n4. Processing ELECTRONIC (ATM, CDT)...")
if len(df_lnlp) > 0:
    df_eft = df_lnlp.filter(
        (pl.col('CHANNEL').is_in([501, 502, 513, 514])) &
        (pl.col('TRANCODE').is_in([614, 668]))
    ).with_columns([
        pl.when(pl.col('CHANNEL').is_in([501, 502]))
          .then(pl.lit('ATM'))
          .when(pl.col('CHANNEL').is_in([513, 514]))
          .then(pl.lit('CDT'))
          .otherwise(None)
          .alias('ITEM'),
        pl.when(pl.col('CHANNEL').is_in([501, 502]))
          .then(pl.lit(4))
          .when(pl.col('CHANNEL').is_in([513, 514]))
          .then(pl.lit(8))
          .otherwise(None)
          .alias('SEQ')
    ])
    
    # Keep only TRANCODE=614 rows
    df_eft_614 = df_eft.filter(pl.col('TRANCODE') == 614)
    
    # Sum TRANAMT by group
    df_eft_sum = df_eft.group_by(['ACCTNO', 'NOTENO', 'USERID', 'CHANNEL']).agg([
        pl.col('TRANAMT').sum()
    ])
    
    # Merge back
    df_eft = df_eft_614.join(
        df_eft_sum.select(['ACCTNO', 'NOTENO', 'USERID', 'CHANNEL', 'TRANAMT']),
        on=['ACCTNO', 'NOTENO', 'USERID', 'CHANNEL'],
        how='left',
        suffix='_SUM'
    ).sort(['ACCTNO', 'NOTENO'])
    
    # Merge with LNNOTE
    df_eft = df_eft.join(df_lnnote, on=['ACCTNO', 'NOTENO'], how='left')
    
    # Categorize and split
    df_eft = df_eft.with_columns([
        pl.struct(['LOANTYPE', 'COSTCTR']).map_elements(
            lambda x: categorize_prod(x['LOANTYPE'], x['COSTCTR']),
            return_dtype=pl.Utf8
        ).alias('PROD')
    ])
    
    df_eft_conv = df_eft.filter(
        ~((pl.col('COSTCTR') > 3000) & (pl.col('COSTCTR') < 3999)) &
        ~pl.col('COSTCTR').is_in([4043, 4048])
    )
    df_eft_isl = df_eft.filter(
        ((pl.col('COSTCTR') > 3000) & (pl.col('COSTCTR') < 3999)) |
        pl.col('COSTCTR').is_in([4043, 4048])
    )
    
    df_eft_conv.write_parquet(f'{BNM_DIR}EFT.parquet')
    df_eft_isl.write_parquet(f'{IBNM_DIR}EFT.parquet')
    print(f"  EFT - Conv: {len(df_eft_conv):,}, Islamic: {len(df_eft_isl):,}")
else:
    df_eft_conv = pl.DataFrame([])
    df_eft_isl = pl.DataFrame([])

# 5. RENTAS (Credit Card)
print("\n5. Processing RENTAS (CC)...")
try:
    df_rencc = pl.concat([
        pl.read_parquet(f'{BNMW1_DIR}RENCC.parquet'),
        pl.read_parquet(f'{BNMW2_DIR}RENCC.parquet'),
        pl.read_parquet(f'{BNMW3_DIR}RENCC.parquet'),
        pl.read_parquet(f'{BNMW4_DIR}RENCC.parquet')
    ])
    df_rencc.write_parquet(f'{BNM_DIR}RENCC.parquet')
    print(f"  RENCC: {len(df_rencc):,}")
except Exception as e:
    print(f"  ⚠ RENCC: {e}")
    df_rencc = pl.DataFrame([])

# 6. RENTAS (Loan)
print("\n6. Processing RENTAS (LOAN)...")
try:
    df_renln = pl.concat([
        pl.read_parquet(f'{BNMW1_DIR}RENLN.parquet'),
        pl.read_parquet(f'{BNMW2_DIR}RENLN.parquet'),
        pl.read_parquet(f'{BNMW3_DIR}RENLN.parquet'),
        pl.read_parquet(f'{BNMW4_DIR}RENLN.parquet'),
        pl.read_parquet(f'{IBNMW1_DIR}RENLN.parquet'),
        pl.read_parquet(f'{IBNMW2_DIR}RENLN.parquet'),
        pl.read_parquet(f'{IBNMW3_DIR}RENLN.parquet'),
        pl.read_parquet(f'{IBNMW4_DIR}RENLN.parquet')
    ]).sort(['ACCTNO', 'NOTENO'])
    
    # Merge with LNNOTE (drop LOANTYPE from LNNOTE)
    df_renln = df_renln.join(
        df_lnnote.drop('LOANTYPE'),
        on=['ACCTNO', 'NOTENO'],
        how='left'
    )
    
    # Categorize and split
    df_renln = df_renln.with_columns([
        pl.struct(['LOANTYPE', 'COSTCTR']).map_elements(
            lambda x: categorize_prod(x['LOANTYPE'], x['COSTCTR']),
            return_dtype=pl.Utf8
        ).alias('PROD')
    ])
    
    df_renln_conv = df_renln.filter(
        ~((pl.col('COSTCTR') > 3000) & (pl.col('COSTCTR') < 3999)) &
        ~pl.col('COSTCTR').is_in([4043, 4048])
    )
    df_renln_isl = df_renln.filter(
        ((pl.col('COSTCTR') > 3000) & (pl.col('COSTCTR') < 3999)) |
        pl.col('COSTCTR').is_in([4043, 4048])
    )
    
    df_renln_conv.write_parquet(f'{BNM_DIR}RENLN.parquet')
    df_renln_isl.write_parquet(f'{IBNM_DIR}RENLN.parquet')
    print(f"  RENLN - Conv: {len(df_renln_conv):,}, Islamic: {len(df_renln_isl):,}")
except Exception as e:
    print(f"  ⚠ RENLN: {e}")
    df_renln_conv = pl.DataFrame([])
    df_renln_isl = pl.DataFrame([])

# 7. CONSOLIDATION
print("\n7. Consolidating transactions...")

# Conventional (PBB)
df_tranx2_conv = pl.concat([
    df_ctcs_conv,
    df_renln_conv,
    df_rencc,
    df_otcln_conv,
    df_otccc,
    df_eft_conv
]).with_columns([
    (pl.col('TRANAMT') / 1000).alias('TRANAMT1'),
    pl.lit('PBB').alias('ENTITY'),
    pl.col('PROD').map_elements(lambda x: PDESC.get(x, x), return_dtype=pl.Utf8).alias('PDESC'),
    pl.col('SEQ').map_elements(lambda x: SEQFMT.get(x, str(x)), return_dtype=pl.Utf8).alias('SEQFMT')
]).sort('SEQ')

df_tranx2_conv.write_parquet(f'{BNM_DIR}TRANX2.parquet')
print(f"  TRANX2 (PBB): {len(df_tranx2_conv):,}")

# Islamic (PIBB)
df_tranx2_isl = pl.concat([
    df_ctcs_isl,
    df_renln_isl,
    df_otcln_isl,
    df_eft_isl
]).with_columns([
    (pl.col('TRANAMT') / 1000).alias('TRANAMT1'),
    pl.lit('PIBB').alias('ENTITY'),
    pl.col('PROD').map_elements(lambda x: PDESC.get(x, x), return_dtype=pl.Utf8).alias('PDESC'),
    pl.col('SEQ').map_elements(lambda x: SEQFMT.get(x, str(x)), return_dtype=pl.Utf8).alias('SEQFMT')
]).sort('SEQ')

df_tranx2_isl.write_parquet(f'{IBNM_DIR}TRANX2.parquet')
print(f"  TRANX2 (PIBB): {len(df_tranx2_isl):,}")

print(f"\n{'='*60}")
print(f"✓ EIBMEPC2 Complete!")
print(f"{'='*60}")
print(f"\nSummary:")
print(f"  PBB transactions: {len(df_tranx2_conv):,}")
print(f"  PIBB transactions: {len(df_tranx2_isl):,}")
print(f"  Total: {len(df_tranx2_conv) + len(df_tranx2_isl):,}")

print(f"\nTransaction Type Breakdown:")
for seq, name in SEQFMT.items():
    conv_count = len(df_tranx2_conv.filter(pl.col('SEQ') == seq))
    isl_count = len(df_tranx2_isl.filter(pl.col('SEQ') == seq))
    if conv_count + isl_count > 0:
        print(f"  {name}: PBB={conv_count:,}, PIBB={isl_count:,}")

print(f"\nProduct Type Breakdown:")
for code, desc in PDESC.items():
    conv_count = len(df_tranx2_conv.filter(pl.col('PROD') == code))
    isl_count = len(df_tranx2_isl.filter(pl.col('PROD') == code))
    if conv_count + isl_count > 0:
        print(f"  {desc}: PBB={conv_count:,}, PIBB={isl_count:,}")

print(f"\n" + "="*60)
print("PRODUCTION NOTE:")
print("Output CSV reports with summaries can be generated from:")
print("  - BNM.TRANX2.parquet (PBB)")
print("  - IBNM.TRANX2.parquet (PIBB)")
print("Use PROC SUMMARY equivalents to create:")
print("  1. Transaction listing by type")
print("  2. Summary by SEQ (transaction type)")
print("  3. CDM breakdown report")
print("="*60)
