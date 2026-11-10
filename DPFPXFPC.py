"""
EIIDMELW - Islamic BNM EL Daily Deposit Extract (Production Ready)
SAS reference: daily_deposit_extraction (EL computation)

Includes:
- PBBLNFMT: Loan formats (ACE products, ODPROD, ODCUSTCD, etc.)
- PBBDPFMT: Deposit formats (SAPROD, CAPROD, FDPRODD, SACUSTCD, DDCUSTCD, FDCUSTCD)

Key behaviour notes (all align with SAS source):
  * Islamic only (COSTCTR 3000-3999)
  * Product 318 (deptype C) is dropped entirely
  * ACE (>5000) split only fires for products in PBBDPFMT.ACE
    = {40, 42, 43, 150, 151, 152, 181}
  * Overdraft balances grouped by ODCUSTCD 02/03/11/12/13/17
  * BNM codes:
      MISC : 4929980000000D  (Islamic)
      SPTF : 4929980000000Y  (Islamic, branch + 3000)
  * Interest bucket routing:
      INTP : prodcd in {42110,42120,42130,42310,42320,42133}
      SPTF : prodcd == 42132
      MISC : everything else

Parity notes:
  * SAPROD unknown → ''  (SAS OTHERWISE in savings SELECT discards)
  * CAPROD unknown → '42110'   (matches SAS SELECT WHEN('42110'))
  * FDPRODD unknown → '42130'  (matches SAS SELECT WHEN('42130'))
  * ODPROD unknown → '34180'   (matches SAS SELECT WHEN('34180','34380'))
  * ODCUSTCD unknown → '79'    (matches SAS OTHERWISE)
  * Zero-amount rows are emitted (EMIT_ZERO_ROWS); SAS emits 0 but
    drops missing cells. Toggle to skip zeros.
"""

import os
import sys
from datetime import datetime, timedelta

import polars as pl

# ------------------------------------------------------------------
# Format libraries
# ------------------------------------------------------------------
import PBBDPFMT as DPF
import PBBLNFMT as LNF


# ------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------
BNM_DIR        = 'data/bnm_pibb/'
DPTRBL_DIR     = 'data/dptrbl/'
EMIT_ZERO_ROWS = True   # Match SAS "IF AMOUNT NE . THEN OUTPUT" (0 kept)

os.makedirs(BNM_DIR, exist_ok=True)


# ------------------------------------------------------------------
# ACE products — real macro from PBBDPFMT
# ------------------------------------------------------------------
ACE_PRODUCTS = set(DPF.ACE)   # {40, 42, 43, 150, 151, 152, 181}

# Interest customer codes (post-format)
INTEREST_CUSTCODES = {
    '80', '81', '82', '83', '84', '85', '86', '87', '88', '89',
    '90', '91', '92', '93', '94', '95', '96', '97', '98', '99',
}


# ------------------------------------------------------------------
# SAS format wrappers
# ------------------------------------------------------------------
_SAPROD_MAP   = DPF._SAPROD_MAPPINGS       # need blank default, not '42120'
_SAPROD_MAP_GET = _SAPROD_MAP.get

def _saprod(product: int) -> str:
    """PUT(PRODUCT, SAPROD.) — OTHERWISE leaves PRODCD blank."""
    return _SAPROD_MAP_GET(product, '')

def _caprod(product: int) -> str:
    """PUT(PRODUCT, CAPROD.) — defaults '42110' in real format."""
    return DPF.caprod_format(product)

def _fdprodd(product: int) -> str:
    """PUT(PRODUCT, FDPRODD.) — defaults '42130'."""
    return DPF.fdprodd_format(product)

def _odprod(product: int) -> str:
    """PUT(PRODUCT, ODPROD.) — defaults '34180'."""
    return LNF.ODPROD(product)

def _odcustcd(cc: int) -> str:
    """PUT(CUSTCODE, ODCUSTCD.) — defaults '79'."""
    return LNF.ODCUSTCD(cc)

def _sacustcd(cc: int) -> str:
    return DPF.sacustcd_format(cc)         # default '78'

def _ddcustcd(cc: int) -> str:
    return DPF.ddcustcd_format(cc)         # default '79'

def _fdcustcd(cc: int) -> str:
    return DPF.fdcustcd_format(cc)         # default '78'


# ------------------------------------------------------------------
# Header / report date
# ------------------------------------------------------------------
print("EIIDMELW - Islamic BNM EL Daily Deposit Extract")
print("=" * 60)

print("\nReading DPTRBL header...")
try:
    with open(f'{DPTRBL_DIR}DPTRBL.txt', 'r') as f:
        header      = f.readline()
        tbdate_str  = header[105:111]
    reptdate = datetime.strptime(tbdate_str, '%y%m%d').date()
except Exception as e:
    print(f"Error reading DPTRBL: {e}")
    sys.exit(1)

day = reptdate.day
if   1 <= day <=  8: nowk = '1'
elif 9 <= day <= 15: nowk = '2'
elif 16<= day <= 22: nowk = '3'
else:                nowk = '4'

reptyear = f'{reptdate.year}'
reptmon  = f'{reptdate.month:02d}'
reptday  = f'{reptdate.day:02d}'
rdate    = reptdate.strftime('%d%m%y')

print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"Week: {nowk}, Month: {reptmon}, Day: {reptday}")
print("=" * 60)


# ------------------------------------------------------------------
# ELDAY mapping (mirrors SAS SELECT on &NOWK + DAY(REPTDATE))
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
# Branch accumulators (999 branches, mirroring SAS arrays)
# ------------------------------------------------------------------
_FIELDS = ('INTP','MISC','SPTF','ODFIN',
           'SA','SACAI','CA','HDA',
           'ODCOM','ODIS','ODMB','ODDH','ODCAG')
branch_data = {i: {k: 0.0 for k in _FIELDS} for i in range(1, 1000)}


# ------------------------------------------------------------------
# Field helpers (SAS 1-based @n → python [n-1:...])
# ------------------------------------------------------------------
def _get(s, a, b, default=''):
    return s[a:b].strip() if len(s) >= b else default

def _to_int(s):
    s = s.strip()
    return int(s) if s else 0

def _to_float(s):
    s = s.strip()
    return float(s) if s else 0.0


# ------------------------------------------------------------------
# Main DPTRBL scan
# ------------------------------------------------------------------
print("\nProcessing DPTRBL (Islamic ONLY, COSTCTR 3000-3999)...")

try:
    with open(f'{DPTRBL_DIR}DPTRBL.txt', 'r') as f:
        for ln, line in enumerate(f):
            if ln == 0:            # header
                continue
            if len(line) < 900:
                continue

            # --- pre-screen on bankno / reptno / fmtcode ---
            try:
                bankno  = int(line[2:5])
                reptno  = int(line[23:27])
                fmtcode = int(line[26:29])
            except ValueError:
                continue

            if not (bankno == 33 and reptno == 1001 and fmtcode == 1):
                continue

            # --- full field extraction ---
            try:
                branch   = _to_int(line[105:109])   # @106 PD4
                debit    = _to_float(line[142:150]) # @143 PD8.2
                credit   = _to_float(line[150:157]) # @151 PD7.2
                custcode = _to_int(line[175:178])   # @176 PD3
                product  = _to_int(line[396:398])   # @397 PD2
                deptype  = _get(line, 430, 431)     # @431 $1
                int1     = _to_float(line[431:437]) # @432 PD6.2
                curbal   = _to_float(line[465:472]) # @466 PD7.2
                int2     = _to_float(line[515:524]) # @516 PD10.9
                openind  = _get(line, 337, 338)     # @338 $1
                costctr  = _to_int(line[829:833])   # @830 PD4
            except (ValueError, IndexError):
                continue

            # --- Islamic only ---
            if not (3000 <= costctr <= 3999):
                continue
            # --- Exclude blocked open indicators & special products ---
            if openind in ('B', 'C', 'P') or product in (297, 298):
                continue

            intpaybl = int1 + int2

            # ---------------- PRODCD resolution ----------------
            if deptype == 'S':
                prodcd = _saprod(product)          # '' or '42120'/'42320'/'N'
            elif deptype in ('D', 'N'):
                prodcd = _caprod(product)          # '42110'/'42180'/'42310'/'43110'/'42199'/'34180'
            elif deptype == 'C':
                if product == 318:                 # SAS: DELETE
                    continue
                prodcd = _fdprodd(product)         # '42130' or '42132'
            else:
                prodcd = ''

            # ---------------- CUSTCD resolution ----------------
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

            # ---------------- Accumulation ----------------
            if deptype == 'S' and curbal >= 0:
                if   prodcd == '42120': branch_data[branch]['SA']    += curbal
                elif prodcd == '42320': branch_data[branch]['SACAI'] += curbal
                # '' or 'N' → no bucket (matches SAS OTHERWISE)

            elif deptype in ('D', 'N'):
                if curbal >= 0:
                    if prodcd == '42110':
                        if product in ACE_PRODUCTS and curbal > 5000:
                            sabal = curbal - 5000
                            branch_data[branch]['SA'] += sabal
                            branch_data[branch]['CA'] += 5000
                        else:
                            branch_data[branch]['CA'] += curbal
                    elif prodcd == '42180':
                        branch_data[branch]['HDA']   += curbal
                    elif prodcd == '42310':
                        branch_data[branch]['SACAI'] += curbal
                    # 43110 / 42199 / 34180 / '' → no bucket
                else:
                    # Overdraft: SAS reassigns CURBAL = -CURBAL
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

            # ---------------- Interest payable buckets ----------------
            if custcd in INTEREST_CUSTCODES and deptype in ('S', 'D', 'N', 'C'):
                if prodcd in ('42110','42120','42130','42310','42320','42133'):
                    branch_data[branch]['INTP'] += intpaybl
                elif prodcd == '42132':
                    branch_data[branch]['SPTF'] += intpaybl
                else:
                    branch_data[branch]['MISC'] += intpaybl

except Exception as e:
    print(f"Error scanning DPTRBL: {e}")
    sys.exit(1)

print("  Processed deposit records")


# ------------------------------------------------------------------
# Emit MELW rows
# (mirrors MELW2 = MELW1 ∪ MELW22 ∪ MELW23)
# ------------------------------------------------------------------
print("\nGenerating BNM code records...")

def _emit(recs, branch, code, amount, reptdate, elday):
    """Mirror SAS: IF AMOUNT NE . THEN OUTPUT."""
    if EMIT_ZERO_ROWS:
        recs.append({'REPTDATE': reptdate, 'BRANCH': branch, 'ELDAY': elday,
                     'BNMCODE': code, 'AMOUNT': float(amount)})
    else:
        if amount != 0:
            recs.append({'REPTDATE': reptdate, 'BRANCH': branch, 'ELDAY': elday,
                         'BNMCODE': code, 'AMOUNT': float(amount)})

melw_records = []
for branch in range(1, 1000):
    d = branch_data[branch]

    # --- MELW2 branch-set (ODFIN, INTP, MISC, SPTF) ---
    _emit(melw_records, branch,        '3410011000000Y', d['ODFIN'], reptdate, elday)
    _emit(melw_records, branch,        '4911080000000Y', d['INTP'],  reptdate, elday)
    _emit(melw_records, branch,        '4929980000000D', d['MISC'],  reptdate, elday)  # Islamic MISC
    _emit(melw_records, branch + 3000, '4929980000000Y', d['SPTF'],  reptdate, elday)  # Islamic SPTF

    # --- MELW22 (Savings / current / HDA) ---
    _emit(melw_records, branch, '4212000000000Y', d['SA'],    reptdate, elday)
    _emit(melw_records, branch, '4230000000000Y', d['SACAI'], reptdate, elday)
    _emit(melw_records, branch, '4211000000000Y', d['CA'],    reptdate, elday)
    _emit(melw_records, branch, '4218000000000Y', d['HDA'],   reptdate, elday)

    # --- MELW23 (Overdraft buckets) ---
    _emit(melw_records, branch, '3410002000000Y', d['ODCOM'], reptdate, elday)
    _emit(melw_records, branch, '3410003000000Y', d['ODIS'],  reptdate, elday)
    _emit(melw_records, branch, '3410012000000Y', d['ODMB'],  reptdate, elday)
    _emit(melw_records, branch, '3410013000000Y', d['ODDH'],  reptdate, elday)
    _emit(melw_records, branch, '3410017000000Y', d['ODCAG'], reptdate, elday)

if not melw_records:
    print("  WARNING: no MELW records produced")
    df_melw = pl.DataFrame(schema={
        'REPTDATE': pl.Date, 'BRANCH': pl.Int32,
        'ELDAY': pl.Utf8, 'BNMCODE': pl.Utf8, 'AMOUNT': pl.Float64,
    })
else:
    df_melw = pl.DataFrame(melw_records)

print(f"  Generated {len(df_melw):,} BNM code records")


# ------------------------------------------------------------------
# Append logic (SAS %MACRO APPEND)
# ------------------------------------------------------------------
filename = f'MELW{reptmon}{nowk}.parquet'
filepath = f'{BNM_DIR}{filename}'

print(f"\nSaving to {filename}...")

if reptday in ('01', '09', '16', '23'):
    # Week start: SAS deletes member then appends → full replace
    print(f"  Week start (Day {reptday}): full replace")
    df_melw.write_parquet(filepath)
else:
    print(f"  Mid-week (Day {reptday}): append (dedupe on REPTDATE)")
    if os.path.exists(filepath):
        try:
            df_existing = pl.read_parquet(filepath)
            df_existing = df_existing.filter(pl.col('REPTDATE') != reptdate)
            df_combined = pl.concat([df_existing, df_melw])
            df_combined.write_parquet(filepath)
            print(f"    Combined: {len(df_combined):,} records")
        except Exception as e:
            print(f"  ERROR appending to existing file {filepath}: {e}")
            sys.exit(1)
    else:
        # SAS DATA step against missing member would fail too
        print(f"  ERROR: {filepath} missing but reptday={reptday} (mid-week).")
        print(f"         Run week-start day first (01/09/16/23).")
        sys.exit(1)

print(f"\n{'='*60}")
print("EIIDMELW Complete")
print(f"{'='*60}")
print(f"Output : {filename}")
print(f"Records: {len(df_melw):,}")
print(f"ELDAY  : {elday}, Week: {nowk}")
print("Key behaviour:")
print("  - Islamic only (COSTCTR 3000-3999)")
print("  - ACE split (>5000) for ACE products {40,42,43,150,151,152,181}")
print("  - Product 318 (deptype C) excluded")
print("  - MISC BNM code: 4929980000000D (Islamic)")
print("  - SPTF BNM code: 4929980000000Y at branch+3000")
