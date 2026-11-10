"""
EIIDMELW - Islamic BNM EL Daily Deposit Extract (Production Ready)
SAS reference: daily_deposit_extraction (EL computation)

Input : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDMELW/DPTRBLGS.parquet
Output: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDMELW/MELW{MM}{W}.sas7bdat

Includes:
- PBBLNFMT     : Loan formats (ACE products, ODPROD, ODCUSTCD, etc.)
- PBBDPFMT_AII : Deposit formats (SAPROD, CAPROD, FDPRODD, SACUSTCD, DDCUSTCD, FDCUSTCD)

Key behaviour notes (all align with SAS source):
  * Islamic only (COSTCTR 3000-3999)
  * Product 318 (deptype C) is dropped entirely
  * ACE (>5000) split only fires for products in PBBDPFMT_AII.ACE
    = {40, 42, 43, 150, 151, 152, 181}
  * Overdraft balances grouped by ODCUSTCD 02/03/11/12/13/17
  * BNM codes:
      MISC : 4929980000000D  (Islamic)
      SPTF : 4929980000000Y  (Islamic, branch + 3000)
  * Interest bucket routing:
      INTP : prodcd in {42110,42120,42130,42310,42320,42133}
      SPTF : prodcd == 42132
      MISC : everything else
"""

import os
import sys
from datetime import datetime, timedelta

import polars as pl
import pandas as pd
import pyreadstat

# ------------------------------------------------------------------
# Format libraries
# ------------------------------------------------------------------
import PBBDPFMT_AII as DPF       # Islamic deposit format library
import PBBLNFMT    as LNF        # Loan format library (ODPROD, ODCUSTCD)


# ------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------
BNM_DIR        = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDMELW/'
DPTRBL_PARQUET = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDMELW/DPTRBLGS.parquet'
EMIT_ZERO_ROWS = True

os.makedirs(BNM_DIR, exist_ok=True)


# ------------------------------------------------------------------
# ACE products — real macro from PBBDPFMT_AII
# ------------------------------------------------------------------
ACE_PRODUCTS = set(DPF.ACE)   # {40, 42, 43, 150, 151, 152, 181}

INTEREST_CUSTCODES = {
    '80', '81', '82', '83', '84', '85', '86', '87', '88', '89',
    '90', '91', '92', '93', '94', '95', '96', '97', '98', '99',
}


# ------------------------------------------------------------------
# SAS format wrappers
# ------------------------------------------------------------------
# SAPROD: SAS OTHERWISE leaves PRODCD blank (discarded by downstream SELECT).
# Public saprod_format() defaults unknown to '42120'. We need blank for parity.
# Try to find the mapping dict regardless of which name PBBDPFMT_AII exposes.
def _find_saprod_map(module):
    for name in ('_SAPROD_MAPPINGS', 'SAPROD_MAPPINGS'):
        if hasattr(module, name):
            return getattr(module, name)
    return None

_SAPROD_MAP = _find_saprod_map(DPF)

def _saprod(product: int) -> str:
    """PUT(PRODUCT, SAPROD.) — OTHERWISE leaves PRODCD blank."""
    if _SAPROD_MAP is not None:
        return _SAPROD_MAP.get(product, '')
    # Fall back to the public formatter (defaults to '42120' for unknowns).
    return DPF.saprod_format(product)

def _caprod(product: int) -> str:   return DPF.caprod_format(product)
def _fdprodd(product: int) -> str:  return DPF.fdprodd_format(product)
def _odprod(product: int) -> str:   return LNF.ODPROD(product)
def _odcustcd(cc: int) -> str:      return LNF.ODCUSTCD(cc)
def _sacustcd(cc: int) -> str:      return DPF.sacustcd_format(cc)
def _ddcustcd(cc: int) -> str:      return DPF.ddcustcd_format(cc)
def _fdcustcd(cc: int) -> str:      return DPF.fdcustcd_format(cc)


# ------------------------------------------------------------------
# TBDATE parser — handles float/int/str/datetime, SAS serial, YYMMDD, YYYYMMDD
# ------------------------------------------------------------------
def _parse_tbdate(raw):
    """Return a Python date from a TBDATE value."""
    if raw is None:
        raise ValueError("TBDATE is null")
    if isinstance(raw, float) and raw != raw:        # NaN
        raise ValueError("TBDATE is NaN")

    if isinstance(raw, datetime):
        return raw.date()
    if hasattr(raw, 'year') and hasattr(raw, 'month') and hasattr(raw, 'day') \
       and not isinstance(raw, (int, float)):
        return raw

    if isinstance(raw, float):
        if raw == 0.0:
            raise ValueError("TBDATE is 0 (SAS missing)")
        raw = int(raw)

    s = str(raw).strip()
    if not s:
        raise ValueError("TBDATE is blank")

    if len(s) == 6 and s.isdigit():
        return datetime.strptime(s, '%y%m%d').date()
    if len(s) == 8 and s.isdigit():
        return datetime.strptime(s, '%Y%m%d').date()
    for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d-%b-%Y'):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass

    n = raw if isinstance(raw, int) else int(raw)

    # SAS date serial (days since 1960-01-01)
    if 0 < n < 60000:
        return (datetime(1960, 1, 1) + timedelta(days=n)).date()

    # YYMMDD int (e.g. 250111)
    if 100000 <= n <= 991231:
        return datetime.strptime(f'{n:06d}', '%y%m%d').date()

    # YYYYMMDD int (e.g. 20250111)
    if 19000101 <= n <= 29991231:
        return datetime.strptime(f'{n:08d}', '%Y%m%d').date()

    raise ValueError(f"Unrecognised TBDATE value: {raw!r}")


# ------------------------------------------------------------------
# Read input parquet
# ------------------------------------------------------------------
print("EIIDMELW - Islamic BNM EL Daily Deposit Extract")
print("=" * 60)

print(f"\nReading {DPTRBL_PARQUET} ...")
try:
    df_in = pl.read_parquet(DPTRBL_PARQUET)
except Exception as e:
    print(f"Error reading parquet: {e}")
    sys.exit(1)

REQUIRED_COLS = {
    'TBDATE', 'BANKNO', 'REPTNO', 'FMTCODE', 'BRANCH', 'DEBIT', 'CREDIT',
    'CUSTCODE', 'PRODUCT', 'DEPTYPE', 'INT1', 'INT2', 'CURBAL',
    'OPENIND', 'COSTCTR',
}
missing = REQUIRED_COLS - set(df_in.columns)
if missing:
    print(f"ERROR: DPTRBL parquet missing columns: {sorted(missing)}")
    print(f"       Present columns: {sorted(df_in.columns)}")
    sys.exit(1)

tbdate_raw = None
try:
    tbdate_raw = df_in.select(pl.col('TBDATE').first()).item()
    reptdate   = _parse_tbdate(tbdate_raw)
except Exception as e:
    print(f"Error parsing TBDATE: {e}")
    print(f"  Raw value: {tbdate_raw!r}  (type: {type(tbdate_raw).__name__})")
    sys.exit(1)

day = reptdate.day
if   1 <= day <=  8: nowk = '1'
elif 9 <= day <= 15: nowk = '2'
elif 16<= day <= 22: nowk = '3'
else:                nowk = '4'

reptmon = f'{reptdate.month:02d}'
reptday = f'{reptdate.day:02d}'

print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"Week: {nowk}, Month: {reptmon}, Day: {reptday}")
print("=" * 60)


# ------------------------------------------------------------------
# ELDAY mapping
# ------------------------------------------------------------------
def get_elday(day: int, nowk: str, reptdate) -> str:
    m = {
        '1': {1:'DAYA', 2:'DAYB', 3:'DAYC', 4:'DAYD', 5:'DAYE', 6:'DAYF', 7:'DAYG', 8:'DAYI'},
        '2': {9:'DAYA',10:'DAYB',11:'DAYC',12:'DAYD',13:'DAYE',14:'DAYF',15:'DAYI'},
        '3': {16:'DAYA',17:'DAYB',18:'DAYC',19:'DAYD',20:'DAYE',21:'DAYF',22:'DAYI'},
    }
    if nowk == '4':
        if 23 <= day <= 27:
            return {23:'DAYA',24:'DAYB',25:'DAYC',26:'DAYD',27:'DAYE'}[day]
        if day == 28:
            return 'DAYI' if (reptdate + timedelta(days=1)).day == 1 else 'DAYF'
        if day == 29:
            return 'DAYI' if (reptdate + timedelta(days=1)).day == 1 else 'DAYG'
        if day == 30:
            return 'DAYI' if (reptdate + timedelta(days=1)).day == 1 else 'DAYH'
        if day == 31:
            return 'DAYI'
    return m.get(nowk, {}).get(day, '')

elday = get_elday(day, nowk, reptdate)
print(f"ELDAY: {elday}")


# ------------------------------------------------------------------
# Branch accumulators
# ------------------------------------------------------------------
_FIELDS = ('INTP','MISC','SPTF','ODFIN',
           'SA','SACAI','CA','HDA',
           'ODCOM','ODIS','ODMB','ODDH','ODCAG')
branch_data = {i: {k: 0.0 for k in _FIELDS} for i in range(1, 1000)}


# ------------------------------------------------------------------
# Null-safe scalar extraction
# ------------------------------------------------------------------
def _int(v, default=0):
    return default if v is None else int(v)

def _float(v, default=0.0):
    return default if v is None else float(v)

def _str(v, default=''):
    return default if v is None else str(v).strip()


# ------------------------------------------------------------------
# Main scan
# ------------------------------------------------------------------
print("\nProcessing DPTRBL parquet (Islamic ONLY, COSTCTR 3000-3999)...")

n_read = 0
n_kept = 0

for row in df_in.iter_rows(named=True):
    n_read += 1

    bankno  = _int(row['BANKNO'])
    reptno  = _int(row['REPTNO'])
    fmtcode = _int(row['FMTCODE'])
    if not (bankno == 33 and reptno == 1001 and fmtcode == 1):
        continue

    branch   = _int(row['BRANCH'])
    custcode = _int(row['CUSTCODE'])
    product  = _int(row['PRODUCT'])
    deptype  = _str(row['DEPTYPE'])
    openind  = _str(row['OPENIND'])
    costctr  = _int(row['COSTCTR'])

    debit    = _float(row['DEBIT'])
    credit   = _float(row['CREDIT'])
    int1     = _float(row['INT1'])
    int2     = _float(row['INT2'])
    curbal   = _float(row['CURBAL'])

    # --- Islamic only ---
    if not (3000 <= costctr <= 3999):
        continue
    if openind in ('B', 'C', 'P') or product in (297, 298):
        continue

    n_kept += 1
    intpaybl = int1 + int2

    # ---------- PRODCD ----------
    if deptype == 'S':
        prodcd = _saprod(product)
    elif deptype in ('D', 'N'):
        prodcd = _caprod(product)
    elif deptype == 'C':
        if product == 318:
            continue
        prodcd = _fdprodd(product)
    else:
        prodcd = ''

    # ---------- CUSTCD ----------
    if product == 104:
        custcd = '02'
    elif product == 105:
        custcd = '81'
    elif deptype == 'S':
        custcd = _sacustcd(custcode)
    elif deptype in ('D', 'N'):
        custcd = _ddcustcd(custcode)
    elif deptype == 'C':
        custcd = _fdcustcd(custcode)
    else:
        custcd = '79'

    # ---------- Accumulation ----------
    if deptype == 'S' and curbal >= 0:
        if   prodcd == '42120': branch_data[branch]['SA']    += curbal
        elif prodcd == '42320': branch_data[branch]['SACAI'] += curbal

    elif deptype in ('D', 'N'):
        if curbal >= 0:
            if prodcd == '42110':
                if product in ACE_PRODUCTS and curbal > 5000:
                    branch_data[branch]['SA'] += curbal - 5000
                    branch_data[branch]['CA'] += 5000
                else:
                    branch_data[branch]['CA'] += curbal
            elif prodcd == '42180':
                branch_data[branch]['HDA']   += curbal
            elif prodcd == '42310':
                branch_data[branch]['SACAI'] += curbal
        else:
            od_bal    = -curbal
            od_prodcd = _odprod(product)
            if od_prodcd == '34240':
                od_prodcd = '34180'
            if od_prodcd in ('34180', '34380'):
                odc = _odcustcd(custcode)
                if   odc == '02': branch_data[branch]['ODCOM'] += od_bal
                elif odc == '03': branch_data[branch]['ODIS']  += od_bal
                elif odc == '11': branch_data[branch]['ODFIN'] += od_bal
                elif odc == '12': branch_data[branch]['ODMB']  += od_bal
                elif odc == '13': branch_data[branch]['ODDH']  += od_bal
                elif odc == '17': branch_data[branch]['ODCAG'] += od_bal

    # ---------- Interest buckets ----------
    if custcd in INTEREST_CUSTCODES and deptype in ('S', 'D', 'N', 'C'):
        if prodcd in ('42110','42120','42130','42310','42320','42133'):
            branch_data[branch]['INTP'] += intpaybl
        elif prodcd == '42132':
            branch_data[branch]['SPTF'] += intpaybl
        else:
            branch_data[branch]['MISC'] += intpaybl

print(f"  Rows read: {n_read:,}   kept (Islamic): {n_kept:,}")


# ------------------------------------------------------------------
# Emit MELW records
# ------------------------------------------------------------------
print("\nGenerating BNM code records...")

def _emit(recs, branch, code, amount):
    if EMIT_ZERO_ROWS or amount != 0:
        recs.append({'REPTDATE': reptdate, 'BRANCH': branch,
                     'ELDAY': elday, 'BNMCODE': code,
                     'AMOUNT': float(amount)})

melw_records = []
for branch in range(1, 1000):
    d = branch_data[branch]
    _emit(melw_records, branch,        '3410011000000Y', d['ODFIN'])
    _emit(melw_records, branch,        '4911080000000Y', d['INTP'])
    _emit(melw_records, branch,        '4929980000000D', d['MISC'])
    _emit(melw_records, branch + 3000, '4929980000000Y', d['SPTF'])
    _emit(melw_records, branch, '4212000000000Y', d['SA'])
    _emit(melw_records, branch, '4230000000000Y', d['SACAI'])
    _emit(melw_records, branch, '4211000000000Y', d['CA'])
    _emit(melw_records, branch, '4218000000000Y', d['HDA'])
    _emit(melw_records, branch, '3410002000000Y', d['ODCOM'])
    _emit(melw_records, branch, '3410003000000Y', d['ODIS'])
    _emit(melw_records, branch, '3410012000000Y', d['ODMB'])
    _emit(melw_records, branch, '3410013000000Y', d['ODDH'])
    _emit(melw_records, branch, '3410017000000Y', d['ODCAG'])

if not melw_records:
    print("  WARNING: no MELW records produced")
    df_melw = pl.DataFrame(schema={
        'REPTDATE': pl.Date, 'BRANCH': pl.Int32,
        'ELDAY': pl.Utf8, 'BNMCODE': pl.Utf8, 'AMOUNT': pl.Float64,
    })
else:
    df_melw = pl.DataFrame(melw_records, schema={
        'REPTDATE': pl.Date, 'BRANCH': pl.Int32,
        'ELDAY': pl.Utf8, 'BNMCODE': pl.Utf8, 'AMOUNT': pl.Float64,
    })

print(f"  Generated {len(df_melw):,} BNM code records")


# ------------------------------------------------------------------
# SAS7BDAT writer helpers
# ------------------------------------------------------------------
SAS_COLUMN_LABELS = {
    'REPTDATE': 'Report date',
    'BRANCH'  : 'Branch code',
    'ELDAY'   : 'EL day label',
    'BNMCODE' : 'BNM item code',
    'AMOUNT'  : 'Amount',
}

def _write_sas7bdat(df_pl: pl.DataFrame, filepath: str) -> None:
    """Write a Polars DataFrame to SAS7BDAT via pandas + pyreadstat."""
    df_pd = df_pl.to_pandas()
    if 'REPTDATE' in df_pd.columns:
        df_pd['REPTDATE'] = pd.to_datetime(df_pd['REPTDATE'])
    pyreadstat.write_sas7bdat(
        df_pd,
        filepath,
        column_labels=SAS_COLUMN_LABELS,
        file_label='EIIDMELW - Islamic BNM EL Daily Deposit',
    )

def _read_sas7bdat(filepath: str) -> pl.DataFrame:
    """Read a SAS7BDAT back into Polars, restoring the expected schema."""
    df_pd, _meta = pyreadstat.read_sas7bdat(filepath)
    if 'REPTDATE' in df_pd.columns:
        df_pd['REPTDATE'] = pd.to_datetime(df_pd['REPTDATE']).dt.date
    return pl.from_pandas(df_pd)


# ------------------------------------------------------------------
# Append / replace logic (SAS %MACRO APPEND parity)
# ------------------------------------------------------------------
filename = f'MELW{reptmon}{nowk}.sas7bdat'
filepath = f'{BNM_DIR}{filename}'

print(f"\nWriting to {filename} ...")

if reptday in ('01', '09', '16', '23'):
    print(f"  Week start (Day {reptday}): full replace")
    _write_sas7bdat(df_melw, filepath)
else:
    print(f"  Mid-week (Day {reptday}): append (dedupe on REPTDATE)")
    if not os.path.exists(filepath):
        print(f"  ERROR: {filepath} missing but reptday={reptday} (mid-week).")
        print(f"         Run week-start day first (01/09/16/23).")
        sys.exit(1)
    try:
        df_existing = _read_sas7bdat(filepath)
        df_existing = df_existing.filter(pl.col('REPTDATE') != reptdate)
        df_combined = pl.concat([df_existing, df_melw], how='diagonal_relaxed')
        _write_sas7bdat(df_combined, filepath)
        print(f"    Combined: {len(df_combined):,} records")
    except Exception as e:
        print(f"  ERROR appending to {filepath}: {e}")
        sys.exit(1)

print(f"\n{'='*60}")
print("EIIDMELW Complete")
print(f"{'='*60}")
print(f"Output  : {filepath}")
print(f"Records : {len(df_melw):,}")
print(f"ELDAY   : {elday}, Week: {nowk}")
