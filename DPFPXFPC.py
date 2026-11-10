"""
EIIDMELW - Islamic BNM EL Daily Deposit Extract (Production Ready, SASPy Output)
SAS reference: daily_deposit_extraction (EL computation)

Input : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDMELW/DPTRBLGS.parquet
Output: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDMELW/MELW{MM}{W}.sas7bdat
        (written via SASPy df2sd)

Requires:
- A running SAS session accessible via SASPy (configure sascfg_personal.py)
- Python packages: polars, pandas, saspy
"""

import os
import sys
import traceback
from datetime import datetime, timedelta

import polars as pl
import pandas as pd
import saspy

# ------------------------------------------------------------------
# Format libraries
# ------------------------------------------------------------------
import PBBDPFMT_AII as DPF
import PBBLNFMT    as LNF


# ------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------
BNM_DIR        = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDMELW/'
DPTRBL_PARQUET = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDMELW/DPTRBLGS.parquet'

# SASPy connection configuration name (must match sascfg_personal.py)
SASPY_CFGNAME = 'your_config_name'   # <-- CHANGE THIS
SAS_LIBREF    = 'BNMLIB'             # SAS library reference for output

EMIT_ZERO_ROWS   = True
PROGRESS_EVERY   = 500_000
WEEK_START_DAYS  = ('01', '09', '16', '23')

os.makedirs(BNM_DIR, exist_ok=True)


# ------------------------------------------------------------------
# Startup format-library checks
# ------------------------------------------------------------------
_DPF_REQUIRED = (
    'ACE', 'saprod_format', 'caprod_format', 'fdprodd_format',
    'sacustcd_format', 'ddcustcd_format', 'fdcustcd_format',
)
_LNF_REQUIRED = ('ODPROD', 'ODCUSTCD')

_missing_dpf = [n for n in _DPF_REQUIRED if not hasattr(DPF, n)]
_missing_lnf = [n for n in _LNF_REQUIRED if not hasattr(LNF, n)]
if _missing_dpf or _missing_lnf:
    print("FATAL: format library missing expected symbols.")
    if _missing_dpf:
        print(f"  PBBDPFMT_AII missing: {_missing_dpf}")
    if _missing_lnf:
        print(f"  PBBLNFMT    missing: {_missing_lnf}")
    sys.exit(1)


ACE_PRODUCTS = set(DPF.ACE)
INTEREST_CUSTCODES = {
    '80', '81', '82', '83', '84', '85', '86', '87', '88', '89',
    '90', '91', '92', '93', '94', '95', '96', '97', '98', '99',
}


# ------------------------------------------------------------------
# SAS format wrappers
# ------------------------------------------------------------------
def _find_saprod_map(module):
    for name in ('_SAPROD_MAPPINGS', 'SAPROD_MAPPINGS'):
        if hasattr(module, name):
            return getattr(module, name)
    return None

_SAPROD_MAP = _find_saprod_map(DPF)

def _saprod(product: int) -> str:
    if _SAPROD_MAP is not None:
        return _SAPROD_MAP.get(product, '')
    return DPF.saprod_format(product)

def _caprod(product: int) -> str:   return DPF.caprod_format(product)
def _fdprodd(product: int) -> str:  return DPF.fdprodd_format(product)
def _odprod(product: int) -> str:   return LNF.ODPROD(product)
def _odcustcd(cc: int) -> str:      return LNF.ODCUSTCD(cc)
def _sacustcd(cc: int) -> str:      return DPF.sacustcd_format(cc)
def _ddcustcd(cc: int) -> str:      return DPF.ddcustcd_format(cc)
def _fdcustcd(cc: int) -> str:      return DPF.fdcustcd_format(cc)


# ------------------------------------------------------------------
# TBDATE parser
# ------------------------------------------------------------------
def _parse_tbdate(raw):
    if raw is None:
        raise ValueError("TBDATE is null")
    if isinstance(raw, float) and raw != raw:
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
        try: return datetime.strptime(s, '%y%m%d').date()
        except ValueError: pass
    if len(s) == 8 and s.isdigit():
        try: return datetime.strptime(s, '%Y%m%d').date()
        except ValueError: pass
    for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d-%b-%Y'):
        try: return datetime.strptime(s, fmt).date()
        except ValueError: pass
    if s.isdigit():
        for width, fmt in ((8, '%Y%m%d'), (8, '%d%m%Y'), (8, '%m%d%Y'), (6, '%y%m%d')):
            if len(s) >= width:
                try: return datetime.strptime(s[:width], fmt).date()
                except ValueError: continue
    n = raw if isinstance(raw, int) else int(raw)
    if 0 < n < 60000:
        return (datetime(1960, 1, 1) + timedelta(days=n)).date()
    raise ValueError(f"Unrecognised TBDATE value: {raw!r}")


# ------------------------------------------------------------------
# Null-safe scalar helpers
# ------------------------------------------------------------------
def _int(v, default=0):
    return default if v is None else int(v)

def _float(v, default=0.0):
    return default if v is None else float(v)

def _str(v, default=''):
    return default if v is None else str(v).strip()


# ------------------------------------------------------------------
# SASPy writer helpers
# ------------------------------------------------------------------
def _sas_table_exists(sas, libref: str, table: str) -> bool:
    """Check whether a SAS dataset exists in the given library."""
    try:
        ds = sas.sasdata(table, libref)
        # If sasdata returns an object and the table is accessible, it exists.
        # SASPy sasdata does not always raise; check via dictionary.
        tables = sas.saslib(libref, 'LIST') if False else None
        # Simpler: try to read the table; if it fails, treat as missing.
        sas.sd2df(table, libref)
        return True
    except Exception:
        return False

def _read_sas_table_to_df(sas, libref: str, table: str) -> pd.DataFrame:
    """Read an existing SAS table into pandas."""
    return sas.sd2df(table, libref)

def _write_df_to_sas(sas, df_pd: pd.DataFrame, libref: str, table: str) -> None:
    """Write a pandas DataFrame to a SAS dataset via df2sd."""
    # Ensure REPTDATE is datetime for SAS date type
    if 'REPTDATE' in df_pd.columns:
        df_pd = df_pd.copy()
        df_pd['REPTDATE'] = pd.to_datetime(df_pd['REPTDATE'])
    sas.df2sd(df_pd, table=table, libref=libref)


# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------
def main() -> int:
    print("EIIDMELW - Islamic BNM EL Daily Deposit Extract")
    print("=" * 60)

    # ---------- connect to SAS ----------
    print(f"\nConnecting to SAS via SASPy (cfgname={SASPY_CFGNAME}) ...")
    try:
        sas = saspy.SASsession(cfgname=SASPY_CFGNAME)
    except Exception as e:
        print(f"FATAL: cannot establish SAS session: {e}")
        return 1

    # Assign library pointing to BNM_DIR
    try:
        sas.saslib(SAS_LIBREF, path=BNM_DIR)
        print(f"SAS library {SAS_LIBREF} assigned to {BNM_DIR}")
    except Exception as e:
        print(f"FATAL: cannot assign SAS library {SAS_LIBREF}: {e}")
        return 1

    # ---------- read parquet ----------
    print(f"\nReading {DPTRBL_PARQUET} ...")
    try:
        df_in = pl.read_parquet(DPTRBL_PARQUET)
    except Exception as e:
        print(f"Error reading parquet: {e}")
        return 1

    REQUIRED_COLS = {
        'TBDATE', 'BANKNO', 'REPTNO', 'FMTCODE', 'BRANCH', 'DEBIT', 'CREDIT',
        'CUSTCODE', 'PRODUCT', 'DEPTYPE', 'INT1', 'INT2', 'CURBAL',
        'OPENIND', 'COSTCTR',
    }
    missing = REQUIRED_COLS - set(df_in.columns)
    if missing:
        print(f"ERROR: DPTRBL parquet missing columns: {sorted(missing)}")
        return 1

    # ---------- parse TBDATE ----------
    tbdate_raw = None
    try:
        tbdate_raw = df_in.select(pl.col('TBDATE').first()).item()
        reptdate   = _parse_tbdate(tbdate_raw)
    except Exception as e:
        print(f"Error parsing TBDATE: {e}")
        print(f"  Raw value: {tbdate_raw!r}")
        return 1

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

    # ---------- ELDAY ----------
    def _elday_for(day_, nowk_, date_):
        m = {
            '1': {1:'DAYA', 2:'DAYB', 3:'DAYC', 4:'DAYD', 5:'DAYE', 6:'DAYF', 7:'DAYG', 8:'DAYI'},
            '2': {9:'DAYA',10:'DAYB',11:'DAYC',12:'DAYD',13:'DAYE',14:'DAYF',15:'DAYI'},
            '3': {16:'DAYA',17:'DAYB',18:'DAYC',19:'DAYD',20:'DAYE',21:'DAYF',22:'DAYI'},
        }
        if nowk_ == '4':
            if 23 <= day_ <= 27:
                return {23:'DAYA',24:'DAYB',25:'DAYC',26:'DAYD',27:'DAYE'}[day_]
            if day_ == 28:
                return 'DAYI' if (date_ + timedelta(days=1)).day == 1 else 'DAYF'
            if day_ == 29:
                return 'DAYI' if (date_ + timedelta(days=1)).day == 1 else 'DAYG'
            if day_ == 30:
                return 'DAYI' if (date_ + timedelta(days=1)).day == 1 else 'DAYH'
            if day_ == 31:
                return 'DAYI'
        return m.get(nowk_, {}).get(day_, '')

    elday = _elday_for(day, nowk, reptdate)
    print(f"ELDAY: {elday}")

    # ---------- accumulators ----------
    _FIELDS = ('INTP','MISC','SPTF','ODFIN',
               'SA','SACAI','CA','HDA',
               'ODCOM','ODIS','ODMB','ODDH','ODCAG')
    branch_data = {i: {k: 0.0 for k in _FIELDS} for i in range(1, 1000)}

    # ---------- main scan ----------
    print("\nProcessing DPTRBL parquet (Islamic ONLY, COSTCTR 3000-3999)...")
    n_read = 0
    n_kept = 0

    try:
        for row in df_in.iter_rows(named=True):
            n_read += 1
            if n_read % PROGRESS_EVERY == 0:
                print(f"  ... {n_read:,} rows read, {n_kept:,} kept")

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

            int1     = _float(row['INT1'])
            int2     = _float(row['INT2'])
            curbal   = _float(row['CURBAL'])

            if not (3000 <= costctr <= 3999):
                continue
            if openind in ('B', 'C', 'P') or product in (297, 298):
                continue

            n_kept += 1
            intpaybl = int1 + int2

            # PRODCD
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

            # CUSTCD
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

            # Accumulation
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

            # Interest buckets
            if custcd in INTEREST_CUSTCODES and deptype in ('S', 'D', 'N', 'C'):
                if prodcd in ('42110','42120','42130','42310','42320','42133'):
                    branch_data[branch]['INTP'] += intpaybl
                elif prodcd == '42132':
                    branch_data[branch]['SPTF'] += intpaybl
                else:
                    branch_data[branch]['MISC'] += intpaybl
    except Exception:
        print("ERROR while scanning rows:")
        traceback.print_exc()
        return 1

    print(f"  Rows read: {n_read:,}   kept (Islamic): {n_kept:,}")

    # ---------- emit MELW ----------
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
        df_melw = pd.DataFrame(columns=['REPTDATE','BRANCH','ELDAY','BNMCODE','AMOUNT'])
    else:
        df_melw = pd.DataFrame(melw_records)

    print(f"  Generated {len(df_melw):,} BNM code records")

    # ---------- write via SASPy ----------
    table_name = f'MELW{reptmon}{nowk}'
    print(f"\nWriting to SAS table {SAS_LIBREF}.{table_name} ...")

    try:
        if reptday in WEEK_START_DAYS:
            print(f"  Week start (Day {reptday}): full replace")
            _write_df_to_sas(sas, df_melw, SAS_LIBREF, table_name)
        else:
            print(f"  Mid-week (Day {reptday}): append (dedupe on REPTDATE)")
            if not _sas_table_exists(sas, SAS_LIBREF, table_name):
                print(f"  NOTE: {table_name} not found — bootstrapping fresh table.")
                _write_df_to_sas(sas, df_melw, SAS_LIBREF, table_name)
            else:
                try:
                    df_existing = _read_sas_table_to_df(sas, SAS_LIBREF, table_name)
                except Exception as e:
                    print(f"  ERROR reading existing {table_name}: {e}")
                    return 1
                # Deduplicate on REPTDATE
                if 'REPTDATE' in df_existing.columns:
                    df_existing = df_existing[df_existing['REPTDATE'].dt.date != reptdate]
                df_combined = pd.concat([df_existing, df_melw], ignore_index=True)
                _write_df_to_sas(sas, df_combined, SAS_LIBREF, table_name)
                print(f"    Combined: {len(df_combined):,} records")
    except Exception:
        print("ERROR while writing output via SASPy:")
        traceback.print_exc()
        return 1

    print(f"\n{'='*60}")
    print("EIIDMELW Complete")
    print(f"{'='*60}")
    print(f"Output  : {SAS_LIBREF}.{table_name} ({BNM_DIR})")
    print(f"Records : {len(df_melw):,}")
    print(f"ELDAY   : {elday}, Week: {nowk}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
