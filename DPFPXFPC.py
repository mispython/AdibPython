#!/usr/bin/env python3
"""
Program Name: EIBMHPFS.py
Purpose: HP FISS - Disbursement, Repayment, Approval reporting for BNM
         - Derives REPTDATE as yesterday (run on 1st of month -> prior month-end)
         - Identifies newly issued / settled HP accounts from LOAN.LNNOTE
         - Merges with BNM.LOAN<MM><WK> for current balances
         - Builds RDAL fixed-width output file (LRECL=80) with BNM codes
           covering disbursement by: purpose code, custcd, sectorial code,
           count by custcd; with new sector mapping rollup
         - Produces EXCEPT and HPSNR exception/same-month release reports
           (ASA carriage-control, LRECL=133)

ESMR: 06-1485, 06-1762

DATE RESOLUTION (Option B2 — yesterday):
    REPTDATE = date.today() - 1 day.
    Program runs monthly on the 1st. On 2026-11-01 it uses 2026-10-31:
    REPTMON='10', REPTYEAR='2026', WK='4' -> reads loan104.sas7bdat
    and produces header HPRDAL31102026.

usecols NOTE:
    pyreadstat matches usecols case-sensitively against the SAS column
    names. We probe metadata once and translate logical lowercase names
    to actual stored names before reading.

ENTITY_CD FILTER:
    WHERE ENTITY_CD != 'PIBB' is applied to LNNOTE only.
    The BNM.LOAN<MM><WK> file is read without the entity filter.
"""

from PBBLNFMT import put

try:
    from PBBLNFMT import HP_ALL
except ImportError:
    HP_ALL = [128, 130, 131, 132, 380, 381, 700, 705, 720, 725,
              983, 993, 996, 678, 679, 698, 699]

import os
from datetime import date, timedelta
from typing import Optional

import pyreadstat
import polars as pl

# =============================================================================
# TESTING / CHUNKING CONFIGURATION
# =============================================================================

CHUNK_SIZE    = 250_000
MAX_ROWS_TEST = None                 # None = read full file (production)
EXCLUDE_ENTITY_CD = 'PIBB'
ENTITY_COL_CANDIDATES = ['entity_cd', 'entitycd', 'ent_cd', 'entity']

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

LNNOTE   = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/a_lnnote/enrh_ln_note_m{REPTMON}.sas7bdat"
BNM_LOAN = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP01/loan{REPTMON}{NOWK}.sas7bdat"

OUTPUT_DIR = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMHPFS"
RDAL_TXT   = os.path.join(OUTPUT_DIR, "hp_rdal.txt")
EXCEPT_TXT = os.path.join(OUTPUT_DIR, "hp_except.txt")
HPSNR_TXT  = os.path.join(OUTPUT_DIR, "hp_snr.txt")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =============================================================================
# &HPD MACRO — HP product codes
# =============================================================================
HPD_SET: set[int] = set(HP_ALL) | {15, 20, 63, 71, 72}

# =============================================================================
# PAGE / ASA CONSTANTS
# =============================================================================

PAGE_LENGTH = 60

# =============================================================================
# DATE / UTILITY HELPERS
# =============================================================================

def sas_date_to_pydate(val) -> Optional[date]:
    if val is None:
        return None
    if isinstance(val, float) and val != val:
        return None
    if isinstance(val, (int, float)):
        return date(1960, 1, 1) + timedelta(days=int(val))
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        s = val.strip()
        if len(s) == 8 and s.isdigit():
            try:
                dd = int(s[0:2]); mm = int(s[2:4]); yy = int(s[4:8])
                yyyy = (2000 + yy) if yy < 50 else (1900 + yy)
                return date(yyyy, mm, dd)
            except (ValueError, TypeError):
                pass
    return None


def coalesce_s(val, default: str = '') -> str:
    if val is None:
        return default
    if isinstance(val, float) and val != val:
        return default
    return str(val).strip()


def coalesce_i(val, default: int = 0) -> int:
    if val is None or (isinstance(val, float) and val != val):
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def coalesce_f(val, default: float = 0.0) -> float:
    if val is None or (isinstance(val, float) and val != val):
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def parse_z11_to_date(z11_val) -> Optional[date]:
    """INPUT(SUBSTR(PUT(val, Z11.), 1, 8), MMDDYY8.)"""
    if z11_val is None or (isinstance(z11_val, float) and z11_val != z11_val):
        return None
    try:
        s = str(int(z11_val)).zfill(11)[:8]
        mm = int(s[0:2]); dd = int(s[2:4]); yy = int(s[4:8])
        if mm < 1 or mm > 12 or dd < 1 or dd > 31:
            return None
        return date(yy, mm, dd)
    except (ValueError, TypeError):
        return None


def fmt_date9(d: Optional[date]) -> str:
    if d is None:
        return '         '
    months = ['JAN','FEB','MAR','APR','MAY','JUN',
              'JUL','AUG','SEP','OCT','NOV','DEC']
    return f"{d.day:02d}{months[d.month-1]}{d.year:04d}"


def fmt_comma15_2(val) -> str:
    if val is None or (isinstance(val, float) and val != val):
        return ' ' * 15
    v = float(val)
    s = f"{abs(v):,.2f}"
    if v < 0:
        s = '-' + s
    return s.rjust(15)

# =============================================================================
# CHUNKED SAS7BDAT READER WITH CASE-AWARE USECOLS
# =============================================================================

def iter_sas7bdat_chunks(path: str,
                          columns: Optional[list] = None,
                          chunk_size: int = CHUNK_SIZE,
                          max_rows: Optional[int] = MAX_ROWS_TEST,
                          exclude_entity_cd: Optional[str] = EXCLUDE_ENTITY_CD):
    """Yield DataFrames (lowercased columns) from a SAS7BDAT file in chunks.

    - `columns` is a list of *lowercase* logical names; we translate them to
      the actual stored SAS names (case-sensitive) before reading.
    - `exclude_entity_cd`: if not None, apply WHERE <entity_col> NE <value>.
      If None, no entity filter is applied.
    """
    if not os.path.exists(path):
        print(f"  Warning: File not found: {path}")
        return

    # -------- Probe metadata once (cheap) --------
    try:
        _, meta = pyreadstat.read_sas7bdat(path, metadataonly=True)
    except Exception as e:
        print(f"  Error reading metadata from {path}: {e}")
        return

    actual_by_lower = {c.lower(): c for c in meta.column_names}
    print(f"  [{os.path.basename(path)}] {meta.number_rows:,} rows, "
          f"{meta.number_columns} cols")

    # -------- Entity column (only if we're filtering) --------
    entity_col_lower  = None
    entity_col_actual = None
    if exclude_entity_cd is not None:
        for cand in ENTITY_COL_CANDIDATES:
            if cand in actual_by_lower:
                entity_col_lower  = cand
                entity_col_actual = actual_by_lower[cand]
                print(f"  [{os.path.basename(path)}] entity column = "
                      f"'{entity_col_actual}'")
                break
        if entity_col_lower is None:
            print(f"  Warning: no entity column in {path} "
                  f"(tried {ENTITY_COL_CANDIDATES}) — filter NOT applied")

    # -------- Build actual-case usecols --------
    requested_lower = list(columns) if columns else list(actual_by_lower.keys())
    if entity_col_lower and entity_col_lower not in requested_lower:
        requested_lower.append(entity_col_lower)

    actual_usecols = [actual_by_lower[c] for c in requested_lower
                      if c in actual_by_lower]
    missing = [c for c in requested_lower if c not in actual_by_lower]
    if missing:
        print(f"  [{os.path.basename(path)}] missing columns: {missing}")

    # -------- Chunked read loop --------
    offset, total_read, n_chunks = 0, 0, 0

    while True:
        if max_rows is not None and total_read >= max_rows:
            break
        rows_this_chunk = chunk_size
        if max_rows is not None:
            rows_this_chunk = min(chunk_size, max_rows - total_read)
        if rows_this_chunk <= 0:
            break

        try:
            df, _ = pyreadstat.read_sas7bdat(
                path,
                usecols=actual_usecols,     # <-- actual SAS case
                row_limit=rows_this_chunk,
                row_offset=offset,
            )
        except Exception as e:
            print(f"  Error reading {path} at offset {offset}: {e}")
            break

        raw_len = 0 if df is None else len(df)
        if raw_len == 0:
            break

        df.columns = [c.lower() for c in df.columns]
        n_chunks  += 1

        # WHERE entity_col NE '<exclude>'  — only if requested
        if exclude_entity_cd is not None and \
           entity_col_lower and entity_col_lower in df.columns:
            before = len(df)
            df = df[df[entity_col_lower].astype(str).str.strip()
                    != exclude_entity_cd]
            dropped = before - len(df)
            if dropped:
                print(f"    filter {entity_col_lower} != "
                      f"'{exclude_entity_cd}': dropped {dropped:,} rows")

        # Drop entity helper column if caller didn't ask for it
        if columns is not None and entity_col_lower and \
           entity_col_lower in df.columns and \
           entity_col_lower not in [c.lower() for c in columns]:
            df = df.drop(columns=[entity_col_lower])

        total_read += raw_len
        if len(df) > 0:
            yield df
        if raw_len < rows_this_chunk:
            break
        offset += rows_this_chunk

    print(f"  [{os.path.basename(path)}] read {n_chunks} chunk(s), "
          f"{total_read:,} raw rows")

# =============================================================================
# REPORT DATE VARIABLES — Option B2 (yesterday)
# =============================================================================

def get_report_vars() -> dict:
    """
    Option B2: REPTDATE = date.today() - 1 day.

    Verified: program runs monthly on the 1st, so 'yesterday' lands on
    the last day of the prior month (e.g. 2026-11-01 -> REPTDATE =
    2026-10-31 -> loan104 -> HPRDAL31102026).
    """
    reptdate = date.today() - timedelta(days=1)
    day      = reptdate.day

    if day == 8:
        wk, wk1 = '1', '4'
    elif day == 15:
        wk, wk1 = '2', '1'
    elif day == 22:
        wk, wk1 = '3', '2'
    else:
        wk, wk1 = '4', '3'

    mm  = reptdate.month
    mm1 = mm - 1
    yy1 = reptdate.year
    if mm1 == 0:
        mm1 = 12
        yy1 -= 1

    sdate  = date(reptdate.year, mm,  1)
    psdate = date(yy1,           mm1, 1)

    return {
        'reptdate':  reptdate,
        'sdate':     sdate,
        'psdate':    psdate,
        'wk':        wk,
        'wk1':       wk1,
        'reptmon':   str(mm).zfill(2),
        'reptmon1':  str(mm1).zfill(2),
        'reptyear':  str(reptdate.year),
        'reptday':   str(day).zfill(2),
        'rdate':     reptdate.strftime('%d/%m/%y'),
        'sdate_s':   sdate.strftime('%d/%m/%y'),
        'psdate_s':  psdate.strftime('%d/%m/%y'),
        'nowk':      wk,
        'nowk1':     wk1,
    }

# =============================================================================
# RESOLVE FILE PATHS
# =============================================================================

def resolve_bnm_loan_path(rv: dict) -> str:
    return (BNM_LOAN
            .replace('{REPTMON}', rv['reptmon'])
            .replace('{NOWK}',    rv['nowk'])
            .replace('{REPTYEAR}', rv['reptyear']))


def resolve_lnnote_path(rv: dict) -> str:
    return LNNOTE.replace('{REPTMON}', rv['reptmon'])

# =============================================================================
# BUILD HPSETTLE — ENTITY_CD filter applied here
# =============================================================================

def build_hpsettle(rv: dict) -> pl.DataFrame:
    """
    DATA HPSETTLE(KEEP=ACCTNO NOTENO CUSTCODE SECTOR LOANTYPE
       NETPROC ISSDTE CRISPURP CLOSEDTE);
       SET LOAN.LNNOTE;
       WHERE ENTITY_CD NE 'PIBB';
       IF (LOANTYPE IN &HPD OR LOANTYPE IN (15,20,63,71,72)) AND PAIDIND='P';
       CLOSEDTE = INPUT(SUBSTR(PUT(LASTTRAN,Z11.),1,8), MMDDYY8.);
       ISSDTE   = INPUT(SUBSTR(PUT(ISSUEDT, Z11.),1,8), MMDDYY8.);
    """
    path = resolve_lnnote_path(rv)
    keep_cols = ['acctno','noteno','custcode','sector','loantype',
                 'netproc','crispurp','lasttran','issuedt','paidind']

    out_rows = []
    for df_chunk in iter_sas7bdat_chunks(
            path,
            columns=keep_cols,
            exclude_entity_cd=EXCLUDE_ENTITY_CD,     # <-- filter LNNOTE
    ):
        chunk_pl = pl.from_pandas(df_chunk)
        avail = [c for c in keep_cols if c in chunk_pl.columns]
        chunk_pl = chunk_pl.select(avail)

        for row in chunk_pl.to_dicts():
            loantype = coalesce_i(row.get('loantype'))
            paidind  = coalesce_s(row.get('paidind'))
            if loantype not in HPD_SET:
                continue
            if paidind != 'P':
                continue

            out_rows.append({
                'acctno':   row.get('acctno'),
                'noteno':   row.get('noteno'),
                'custcode': coalesce_s(row.get('custcode')),
                'sector':   coalesce_s(row.get('sector')),
                'loantype': loantype,
                'netproc':  coalesce_f(row.get('netproc')),
                'crispurp': coalesce_s(row.get('crispurp')),
                'issdte':   parse_z11_to_date(row.get('issuedt')),
                'closedte': parse_z11_to_date(row.get('lasttran')),
            })

        print(f"    HPSETTLE cumulative kept = {len(out_rows):,}")

    return pl.from_dicts(out_rows) if out_rows else pl.DataFrame()

# =============================================================================
# LOAD BNM LOAN — NO entity filter on this file
# =============================================================================

def load_bnm_loan(rv: dict) -> pl.DataFrame:
    """Read BNM.LOAN<MM><WK> — no ENTITY_CD filter per business rule."""
    path = resolve_bnm_loan_path(rv)
    keep_cols = ['acctno','noteno','fisspurp','product','noteterm',
                 'earnterm','netproc','apprdate','apprlim2','prodcd',
                 'custcode',            # actual name in loan file is CUSTCODE
                 'amtind','sectorcd','balance','curbal',
                 'issdte','acctype']

    frames = []
    for df_chunk in iter_sas7bdat_chunks(
            path,
            columns=keep_cols,
            exclude_entity_cd=None,             # <-- no filter on loan file
    ):
        chunk_pl = pl.from_pandas(df_chunk)
        avail = [c for c in keep_cols if c in chunk_pl.columns]
        chunk_pl = chunk_pl.select(avail)
        frames.append(chunk_pl)

    if not frames:
        return pl.DataFrame()

    result = pl.concat(frames, how='diagonal')

    # Rename CUSTCODE -> CUSTCD for downstream compatibility with SAS logic
    if 'custcode' in result.columns and 'custcd' not in result.columns:
        result = result.rename({'custcode': 'custcd'})

    return result

# =============================================================================
# BUILD HPSNR
# =============================================================================

def build_hpsnr(hpsettle: pl.DataFrame, sdate: date,
                bnm_loan: pl.DataFrame) -> pl.DataFrame:
    if hpsettle.is_empty():
        return pl.DataFrame()

    out_rows = []
    for row in hpsettle.to_dicts():
        issdte   = row.get('issdte')
        closedte = row.get('closedte')
        if issdte is None or closedte is None:
            continue
        if issdte >= sdate and closedte >= sdate:
            out_rows.append(row)

    if not out_rows:
        return pl.DataFrame()

    hpsnr = pl.from_dicts(out_rows).sort(['acctno','noteno'])

    if bnm_loan.is_empty():
        return hpsnr

    loan_keep = [c for c in ['acctno','noteno','fisspurp','product','noteterm',
                              'earnterm','netproc','apprdate','apprlim2','prodcd',
                              'custcd','amtind','sectorcd','balance','issdte',
                              'acctype'] if c in bnm_loan.columns]
    loan_sel  = bnm_loan.select(loan_keep)

    merged = hpsnr.join(loan_sel, on=['acctno','noteno'], how='left', suffix='_ln')
    for c in [x for x in loan_keep if x not in ('acctno','noteno')]:
        lc = f"{c}_ln"
        if lc in merged.columns:
            merged = merged.with_columns(
                pl.when(pl.col(lc).is_not_null())
                  .then(pl.col(lc))
                  .otherwise(pl.col(c) if c in merged.columns else pl.lit(None))
                  .alias(c)
            ).drop(lc)

    return merged.select([c for c in ['acctno','noteno','fisspurp','product',
                                       'noteterm','earnterm','netproc','apprdate',
                                       'apprlim2','prodcd','custcd','amtind',
                                       'sectorcd','balance','issdte','acctype']
                          if c in merged.columns])

# =============================================================================
# BUILD HP — new HP accounts issued this month
# =============================================================================

def build_hp_new(bnm_loan: pl.DataFrame, sdate: date) -> pl.DataFrame:
    if bnm_loan.is_empty():
        return pl.DataFrame()

    keep = [c for c in ['acctno','noteno','fisspurp','product','noteterm',
                         'earnterm','netproc','apprdate','apprlim2','prodcd',
                         'custcd','amtind','sectorcd','balance','curbal',
                         'issdte','acctype'] if c in bnm_loan.columns]
    df   = bnm_loan.select(keep)

    out_rows = []
    for row in df.to_dicts():
        curbal = coalesce_f(row.get('curbal'))
        prodcd = coalesce_s(row.get('prodcd'))
        issdte = row.get('issdte')

        if isinstance(issdte, (int, float, str)):
            issdte = sas_date_to_pydate(issdte)

        if issdte is None:
            continue
        if curbal > 0 and prodcd == '34111' and issdte >= sdate:
            r2 = dict(row)
            r2['issdte'] = issdte
            out_rows.append(r2)

    if not out_rows:
        return pl.DataFrame()

    result = pl.from_dicts(out_rows)
    drop_cols = [c for c in ['curbal'] if c in result.columns]
    if drop_cols:
        result = result.drop(drop_cols)
    return result

# =============================================================================
# MERGE HPSETTLE + HP
# =============================================================================

def merge_hp_settle(hpsettle_df: pl.DataFrame, hp_new: pl.DataFrame,
                    hpsnr_df: pl.DataFrame) -> tuple:
    rename_map = {
        'noteno':   'onote',
        'custcode': 'ocustcd',
        'sector':   'osector',
        'loantype': 'oprod',
        'netproc':  'onet',
        'issdte':   'oissdte',
        'crispurp': 'ofiss',
        'closedte': 'oclose',
    }
    if not hpsettle_df.is_empty():
        cols_to_rename = {k: v for k, v in rename_map.items()
                          if k in hpsettle_df.columns}
        settle = hpsettle_df.rename(cols_to_rename)
        settle = settle.unique(subset=['acctno'], keep='first').sort('acctno')
    else:
        settle = pl.DataFrame()

    if not hpsnr_df.is_empty():
        hpsnr_dedup = hpsnr_df.unique(subset=['acctno'], keep='first').sort('acctno')
    else:
        hpsnr_dedup = pl.DataFrame()

    if not hp_new.is_empty():
        hp_sorted = hp_new.sort('acctno')
    else:
        hp_sorted = pl.DataFrame()

    hp_rows, except_rows = [], []

    if not hp_sorted.is_empty():
        if not settle.is_empty():
            merged = hp_sorted.join(settle, on='acctno', how='left', suffix='_st')
            for row in merged.to_dicts():
                in_b    = row.get('onote') is not None
                issdte  = row.get('issdte')
                oissdte = row.get('oissdte')

                if in_b:
                    except_rows.append(dict(row))
                    if (issdte is not None and oissdte is not None and
                            issdte.month == oissdte.month and
                            issdte.year  == oissdte.year):
                        hp_rows.append(dict(row))
                else:
                    hp_rows.append(dict(row))
        else:
            hp_rows = hp_sorted.to_dicts()

    hp_df     = pl.from_dicts(hp_rows)     if hp_rows     else pl.DataFrame()
    except_df = pl.from_dicts(except_rows) if except_rows else pl.DataFrame()

    final_hpsnr = pl.DataFrame()
    if not hpsnr_dedup.is_empty() and not hp_df.is_empty():
        hp_keys = hp_df.select('acctno').with_columns(pl.lit(True).alias('_in_a'))
        snr_m   = hpsnr_dedup.join(hp_keys, on='acctno', how='left')
        snr_m   = snr_m.filter(pl.col('_in_a').is_null()).drop('_in_a')
        final_hpsnr = snr_m
    elif not hpsnr_dedup.is_empty():
        final_hpsnr = hpsnr_dedup

    if not final_hpsnr.is_empty():
        hp_df = pl.concat([hp_df, final_hpsnr], how='diagonal') \
                if not hp_df.is_empty() else final_hpsnr

    return hp_df, except_df, final_hpsnr

# =============================================================================
# SECTOR MAPPING
# =============================================================================

def apply_sectcd_mapping(rows: list) -> list:
    for row in rows:
        scd = coalesce_s(row.get('sectorcd'))

        secta = put(scd, '$NEWSECT')
        secta = scd if secta is None else str(secta)

        secvalid = put(scd, '$VALIDSE')
        secvalid = '' if secvalid is None else str(secvalid)

        row['secvalid'] = secvalid

        if secta.strip() and secta.strip() != '    ':
            sectcd = secta.strip()
        else:
            sectcd = scd

        if secvalid == 'INVALID':
            sectcd = remap_invalid_sectcd(sectcd)

        row['sectcd'] = sectcd

    return rows


def remap_invalid_sectcd(sectcd: str) -> str:
    p1 = sectcd[0:1] if sectcd else ''
    p2 = sectcd[0:2] if len(sectcd) >= 2 else ''

    if p1 == '1': return '1400'
    if p1 == '2': return '2900'
    if p1 == '3': return '3919'
    if p1 == '4': return '4010'
    if p1 == '5': return '5999'
    if p2 == '61': return '6120'
    if p2 == '62': return '6130'
    if p2 == '63': return '6310'
    if p2 in ('64','65','66','67','68','69'): return '6130'
    if p1 == '7': return '7199'
    if p2 in ('81','82'): return '8110'
    if p2 in ('83','84','85','86','87','88','89'): return '8999'
    if p2 == '91': return '9101'
    if p2 == '92': return '9410'
    if p2 in ('93','94','95'): return '9499'
    if p2 in ('96','97','98','99'): return '9999'
    return sectcd


def expand_alm2_rows(row: dict) -> list:
    sectcd  = coalesce_s(row.get('sectcd'))
    outputs = []

    def emit(scd: str):
        r = dict(row); r['sectorcd'] = scd; outputs.append(r)

    if sectcd in ('1111','1112','1113','1114','1115','1116','1117','1119','1120','1130','1140','1150'):
        if sectcd in ('1111','1113','1115','1117','1119'): emit('1110')
        emit('1100')
    if sectcd in ('2210','2220'): emit('2200')
    if sectcd in ('2301','2302','2303'):
        if sectcd in ('2301','2302'): emit('2300')
        emit('2300')
        if sectcd == '2303': emit('2302')
    if sectcd in ('3110','3115','3111','3112','3113','3114'):
        if sectcd in ('3110','3113','3114'): emit('3100')
        if sectcd in ('3115','3111','3112'): emit('3110')
    if sectcd in ('3211','3212','3219'): emit('3210')
    if sectcd in ('3221','3222'):        emit('3220')
    if sectcd in ('3231','3232'):        emit('3230')
    if sectcd in ('3241','3242'):        emit('3240')
    if sectcd in ('3270','3280','3290','3271','3272','3273','3311','3312','3313'):
        if sectcd in ('3270','3280','3290','3271','3272','3273'): emit('3260')
        if sectcd in ('3271','3272','3273'): emit('3270')
        if sectcd in ('3311','3312','3313'): emit('3310')
    if sectcd in ('3431','3432','3433'): emit('3430')
    if sectcd in ('3551','3552'):        emit('3550')
    if sectcd in ('3611','3619'):        emit('3610')
    if sectcd in ('3710','3720','3730','3721','3731','3732'):
        emit('3700')
        if sectcd == '3721': emit('3720')
        if sectcd in ('3731','3732'): emit('3730')
    if sectcd in ('3811','3812'): emit('3800')
    if sectcd in ('3813','3814','3819'): emit('3812')
    if sectcd in ('3832','3834','3835','3833'):
        emit('3831')
        if sectcd == '3833': emit('3832')
    if sectcd in ('3842','3843','3844'): emit('3841')
    if sectcd in ('3851','3852','3853'): emit('3850')
    if sectcd in ('3861','3862','3863','3864','3865','3866'): emit('3860')
    if sectcd in ('3871','3872'): emit('3870')
    if sectcd in ('3891','3892','3893','3894'): emit('3890')
    if sectcd in ('3911','3919'): emit('3910')
    if sectcd in ('3951','3952','3953','3954','3955','3956','3957'):
        emit('3950')
        if sectcd in ('3952','3953'): emit('3951')
        if sectcd in ('3955','3956','3957'): emit('3954')
    if sectcd in ('5001','5002','5003','5004','5005','5006','5008'): emit('5010')
    if sectcd in ('6110','6120','6130'): emit('6100')
    if sectcd in ('6310','6320'):        emit('6300')
    if sectcd in ('7111','7112','7117','7113','7114','7115','7116'): emit('7110')
    if sectcd in ('7113','7114','7115','7116'): emit('7112')
    if sectcd in ('7112','7114'): emit('7113')
    if sectcd == '7116': emit('7115')
    if sectcd in ('7121','7123','7124'):
        emit('7120')
        if sectcd == '7124': emit('7123')
    if sectcd in ('7131','7132','7133','7134'): emit('7130')
    if sectcd in ('7191','7192','7193','7199'): emit('7190')
    if sectcd in ('7210','7220'): emit('7200')
    if sectcd in ('8110','8120','8130'): emit('8100')
    if sectcd in ('8310','8330','8340','8320','8331','8332'):
        emit('8300')
        if sectcd in ('8320','8331','8332'): emit('8330')
    if sectcd == '8321': emit('8320')
    if sectcd == '8333': emit('8332')
    if sectcd in ('8420','8411','8412','8413','8414','8415','8416'):
        emit('8400')
        if sectcd in ('8411','8412','8413','8414','8415','8416'): emit('8410')
    if sectcd[:2] == '89':
        emit('8900')
        if sectcd in ('8910','8911','8912','8913','8914'):
            if sectcd in ('8911','8912','8913','8914'): emit('8910')
            if sectcd == '8910': emit('8914')
        if sectcd in ('8921','8922','8920'):
            if sectcd in ('8921','8922'): emit('8920')
            if sectcd == '8920': emit('8922')
        if sectcd in ('8931','8932'): emit('8930')
        if sectcd in ('8991','8999'): emit('8990')
    if sectcd in ('9101','9102','9103'): emit('9100')
    if sectcd in ('9201','9202','9203'): emit('9200')
    if sectcd in ('9311','9312','9313','9314'): emit('9300')
    if sectcd[:2] == '94':
        emit('9400')
        if sectcd in ('9433','9434','9435','9432','9431'):
            if sectcd in ('9433','9434','9435'): emit('9432')
            emit('9430')
        if sectcd in ('9410','9420','9440','9450'): emit('9499')

    return outputs


def build_alma_rollup(rows: list) -> list:
    rollup = {
        '1000': {'1100','1200','1300','1400'},
        '2000': {'2100','2200','2300','2400','2900'},
        '3000': {'3100','3120','3210','3220','3230','3240','3250','3260','3310',
                 '3430','3550','3610','3700','3800','3825','3831','3841','3850',
                 '3860','3870','3890','3910','3950','3960'},
        '4000': {'4010','4020','4030'},
        '5000': {'5010','5020','5030','5040','5050','5999'},
        '6000': {'6100','6300'},
        '7000': {'7110','7120','7130','7190','7200'},
        '8000': {'8100','8300','8400','8900'},
        '9000': {'9100','9200','9300','9400','9500','9600'},
    }
    out = []
    for row in rows:
        scd = coalesce_s(row.get('sectorcd'))
        for major, sub_set in rollup.items():
            if scd in sub_set:
                r2 = dict(row); r2['sectorcd'] = major
                out.append(r2)
                break
    return out


def build_alm_with_sector(hp_df: pl.DataFrame) -> pl.DataFrame:
    if hp_df.is_empty():
        return pl.DataFrame()

    filt = hp_df
    if 'sectorcd' in filt.columns:
        filt = filt.filter(pl.col('sectorcd') != '0410')

    agg_v = [c for c in ['netproc'] if c in filt.columns]
    grp_v = [c for c in ['sectorcd','amtind'] if c in filt.columns]
    if not agg_v or not grp_v:
        return pl.DataFrame()

    alm_sum = filt.group_by(grp_v).agg([pl.col(c).sum() for c in agg_v])
    rows    = alm_sum.to_dicts()

    rows = apply_sectcd_mapping(rows)

    for row in rows:
        row['sectorcd'] = row['sectcd']

    alm2_rows = []
    for row in rows:
        alm2_rows.extend(expand_alm2_rows(row))

    alm_all   = list(rows) + alm2_rows
    alma_rows = build_alma_rollup(alm_all)

    alm_final = alm_all + alma_rows
    for row in alm_final:
        if not coalesce_s(row.get('sectorcd')).strip():
            row['sectorcd'] = '9999'

    return pl.from_dicts(alm_final) if alm_final else pl.DataFrame()


def build_alm_smi_with_sector(hp_df: pl.DataFrame) -> pl.DataFrame:
    if hp_df.is_empty():
        return pl.DataFrame()

    filt = hp_df
    if 'sectorcd' in filt.columns:
        filt = filt.filter(pl.col('sectorcd') != '0410')

    grp_v = [c for c in ['sectorcd','custcd','amtind'] if c in filt.columns]
    if not grp_v or 'netproc' not in filt.columns:
        return pl.DataFrame()

    alm_sum = filt.group_by(grp_v).agg(pl.col('netproc').sum())
    rows    = alm_sum.to_dicts()

    rows = apply_sectcd_mapping(rows)

    for row in rows:
        row['sectorcd'] = row['sectcd']

    alm2_rows = []
    for row in rows:
        alm2_rows.extend(expand_alm2_rows(row))

    alm_all   = list(rows) + alm2_rows
    alma_rows = build_alma_rollup(alm_all)
    alm_final = alm_all + alma_rows

    for row in alm_final:
        if not coalesce_s(row.get('sectorcd')).strip():
            row['sectorcd'] = '9999'

    return pl.from_dicts(alm_final) if alm_final else pl.DataFrame()

# =============================================================================
# BUILD LALM RECORDS
# =============================================================================

def build_lalm(hp_df: pl.DataFrame) -> list:
    lalm = []
    if hp_df.is_empty():
        return lalm

    def add(bnmcode: str, amtind: str, amount: float):
        lalm.append((bnmcode[:14], amtind, amount))

    # 1. DISBURSEMENT BY PURPOSE CODE
    grp1 = [c for c in ['fisspurp','amtind'] if c in hp_df.columns]
    if grp1 and 'netproc' in hp_df.columns:
        sum1 = hp_df.group_by(grp1).agg(pl.col('netproc').sum())
        rows1 = sum1.to_dicts()
        extra = []
        for row in rows1:
            fp = coalesce_s(row.get('fisspurp'))
            if fp in ('0220','0230','0210','0211','0212'):
                extra.append({**row, 'fisspurp': '0200'})
        rows1 = rows1 + extra
        for row in rows1:
            fp     = coalesce_s(row.get('fisspurp'))
            amtind = coalesce_s(row.get('amtind'))
            amt    = coalesce_f(row.get('netproc'))
            add('821510000' + fp + 'Y', amtind, amt)

    # 2. DISBURSEMENT SMI BY FISSPURP + CUSTCD
    grp2 = [c for c in ['fisspurp','custcd','amtind'] if c in hp_df.columns]
    if grp2 and 'netproc' in hp_df.columns:
        sum2 = hp_df.group_by(grp2).agg(pl.col('netproc').sum())
        rows2 = sum2.to_dicts()
        extra2 = []
        for row in rows2:
            fp = coalesce_s(row.get('fisspurp'))
            if fp in ('0220','0230','0210','0211','0212'):
                extra2.append({**row, 'fisspurp': '0200'})
        rows2 = rows2 + extra2

        smi_grp1  = {'41','42','43','44','46','47','48','49','51','52','53','54','77','78','79'}
        smi_grp11 = {'02','03','11','12'}
        smi_grp12 = {'20','13','17','30','32','33','34','35','36','37','38','39','40','04','05','06'}
        smi_grp17 = {'71','72','73','74'}
        smi_grp18 = {'81','82','83','84','85','86','90','91','92','95','96','98','99'}
        smi_g161  = {'41','42','43','61'}
        smi_g162  = {'62','44','46','47'}
        smi_g163  = {'63','48','49','51'}
        smi_g164  = {'64','52','53','54','59','75','57'}
        smi_g165  = {'41','42','43','44','46','47','48','49','51','52','53','54','61','62','63','64'}

        for row in rows2:
            fp     = coalesce_s(row.get('fisspurp'))
            cd     = coalesce_s(row.get('custcd'))
            amtind = coalesce_s(row.get('amtind'))
            amt    = coalesce_f(row.get('netproc'))

            if cd in smi_grp1:  add(f'82151{cd}00{fp}Y', amtind, amt)
            if cd in smi_grp11: add(f'821511000{fp}Y', amtind, amt)
            if cd in smi_grp12: add(f'821512000{fp}Y', amtind, amt)
            if cd in smi_grp17: add(f'821517000{fp}Y', amtind, amt)
            if cd in smi_grp18: add(f'821518000{fp}Y', amtind, amt)
            if cd in smi_g161:  add(f'821516100{fp}Y', amtind, amt)
            if cd in smi_g162:  add(f'821516200{fp}Y', amtind, amt)
            if cd in smi_g163:  add(f'821516300{fp}Y', amtind, amt)
            if cd in smi_g164:  add(f'821516400{fp}Y', amtind, amt)
            if cd in smi_g165:  add(f'821516500{fp}Y', amtind, amt)

    # 3. DISBURSEMENT BY CUSTCD
    grp3 = [c for c in ['custcd','amtind'] if c in hp_df.columns]
    if grp3 and 'netproc' in hp_df.columns:
        sum3 = hp_df.group_by(grp3).agg(pl.col('netproc').sum())
        for row in sum3.to_dicts():
            cd     = coalesce_s(row.get('custcd'))
            amtind = coalesce_s(row.get('amtind'))
            amt    = coalesce_f(row.get('netproc'))

            if cd in ('77','78'): add('8215176009700Y', amtind, amt)
            if cd == '77':        add('8215177009700Y', amtind, amt)
            if cd == '78':        add('8215178009700Y', amtind, amt)
            if cd in ('95','96'):
                add('8215185009700Y', amtind, amt)
                add('8215195009700Y', amtind, amt)
            if cd in ('80','81','82','83','84','85','86','87','88','89',
                      '90','91','92','93','94','97','98','99'):
                add('8215185009999Y', amtind, amt)
            if cd in ('77','78','95','96'):
                add('8215100009700Y', amtind, amt)

    # 4. DISBURSEMENT BY SECTORIAL CODE
    alm_sec = build_alm_with_sector(hp_df)
    if not alm_sec.is_empty():
        for row in alm_sec.to_dicts():
            add(f"821510000{coalesce_s(row.get('sectorcd'))}Y",
                coalesce_s(row.get('amtind')),
                coalesce_f(row.get('netproc')))

    # 5. DISBURSEMENT SMI BY SECTORIAL CODE + CUSTCD
    alm_smi_sec = build_alm_smi_with_sector(hp_df)
    if not alm_smi_sec.is_empty():
        smi_grp1  = {'41','42','43','44','46','47','48','49','51','52','53','54','77','78','79'}
        smi_grp11 = {'02','03','11','12'}
        smi_grp12 = {'20','13','17','30','32','33','34','35','36','37','38','39','40','04','05','06'}
        smi_grp17 = {'71','72','73','74'}
        smi_grp18 = {'81','82','83','84','85','86','90','91','92','95','96','98','99'}
        smi_g161  = {'41','42','43','61'}
        smi_g162  = {'62','44','46','47'}
        smi_g163  = {'63','48','49','51'}
        smi_g164  = {'64','52','53','54','59','75','57'}
        smi_g165  = {'41','42','43','44','46','47','48','49','51','52','53','54','61','62','63','64'}

        for row in alm_smi_sec.to_dicts():
            scd    = coalesce_s(row.get('sectorcd'))
            cd     = coalesce_s(row.get('custcd'))
            amtind = coalesce_s(row.get('amtind'))
            amt    = coalesce_f(row.get('netproc'))

            if cd in smi_grp1:  add(f'82151{cd}00{scd}Y', amtind, amt)
            if cd in smi_grp11: add(f'821511000{scd}Y', amtind, amt)
            if cd in smi_grp12: add(f'821512000{scd}Y', amtind, amt)
            if cd in smi_grp17: add(f'821517000{scd}Y', amtind, amt)
            if cd in smi_grp18 and scd != '9999':
                add(f'821518000{scd}Y', amtind, amt)
            if cd in smi_g161:  add(f'821516100{scd}Y', amtind, amt)
            if cd in smi_g162:  add(f'821516200{scd}Y', amtind, amt)
            if cd in smi_g163:  add(f'821516300{scd}Y', amtind, amt)
            if cd in smi_g164:  add(f'821516400{scd}Y', amtind, amt)
            if cd in smi_g165:  add(f'821516500{scd}Y', amtind, amt)

    # 6. COUNT BY CUSTCD / FISSPURP
    if not hp_df.is_empty() and 'custcd' in hp_df.columns:
        if 'amtind' in hp_df.columns:
            type1 = hp_df.group_by('amtind').agg(pl.len().alias('_freq_'))
            for row in type1.to_dicts():
                add('8015000000000Y', coalesce_s(row.get('amtind')),
                    float(coalesce_i(row.get('_freq_')) * 1000))

        grp5 = [c for c in ['custcd','amtind'] if c in hp_df.columns]
        if grp5:
            type5 = hp_df.group_by(grp5).agg(pl.len().alias('_freq_'))
            for row in type5.to_dicts():
                cd     = coalesce_s(row.get('custcd'))
                amtind = coalesce_s(row.get('amtind'))
                amt    = float(coalesce_i(row.get('_freq_')) * 1000)

                if cd in ('41','42','43','61'):
                    add('8015061000000Y', amtind, amt)
                if cd != '61':
                    add(f'80150{cd}000000Y', amtind, amt)

        grp3b = [c for c in ['fisspurp','amtind'] if c in hp_df.columns]
        if grp3b:
            type3 = hp_df.group_by(grp3b).agg(pl.len().alias('_freq_'))
            for row in type3.to_dicts():
                fp     = coalesce_s(row.get('fisspurp'))
                amtind = coalesce_s(row.get('amtind'))
                amt    = float(coalesce_i(row.get('_freq_')) * 1000)
                if fp == '0211': add('8030000000211Y', amtind, amt)
                if fp == '0212': add('8030000000212Y', amtind, amt)

    return lalm

# =============================================================================
# FINAL CONSOLIDATION AND WRITE RDAL
# =============================================================================

def write_rdal(lalm: list, rv: dict):
    if not lalm:
        with open(RDAL_TXT, 'w', encoding='utf-8'):
            pass
        return

    from collections import defaultdict

    agg: dict[tuple, float] = defaultdict(float)
    for bnmcode, amtind, amount in lalm:
        agg[(bnmcode, amtind)] += amount

    phead = f"HPRDAL{rv['reptday']}{rv['reptmon']}{rv['reptyear']}"
    lines = [phead, 'AL']

    by_bnm: dict[str, dict] = {}
    for (bnmcode, amtind), total in agg.items():
        if bnmcode not in by_bnm:
            by_bnm[bnmcode] = {'D': 0.0, 'I': 0.0}
        key = 'D' if amtind == 'D' else 'I'
        by_bnm[bnmcode][key] += total

    for bnmcode in sorted(by_bnm.keys()):
        vals  = by_bnm[bnmcode]
        amt_d = round(vals['D'] / 1000)
        amt_i = round(vals['I'] / 1000)
        amt_d += amt_i
        lines.append(f"{bnmcode};{int(amt_d)};{int(amt_i)}")

    with open(RDAL_TXT, 'w', encoding='utf-8', newline='\n') as f:
        for ln in lines:
            f.write(ln + '\n')

# =============================================================================
# EXCEPTION / HPSNR REPORT WRITERS
# =============================================================================

def _report_header(title1: str, title2: str) -> list:
    lines = []
    lines.append('1' + title1)
    lines.append(' ' + title2)
    lines.append(' ')
    hdr = (f"{'ACCOUNT':>12} {'NOTE':>7} {'CUSTCODE':>8} {'SECTOR':>7}"
           f" {'PRODUCT':>8} {'NETPROC':>15} {'ISSUE DATE':>10} {'PURPOSE CODE':>12}")
    lines.append(' ' + hdr)
    lines.append(' ' + '-' * len(hdr))
    return lines


def write_exception_report(df: pl.DataFrame, filepath: str,
                            title1: str, title2: str):
    lines       = []
    line_cnt    = 0
    tot_netproc = 0.0

    def new_page():
        nonlocal line_cnt
        line_cnt = 0
        for ln in _report_header(title1, title2):
            lines.append(ln)
            line_cnt += 1

    new_page()

    if not df.is_empty():
        for row in df.sort(['acctno','noteno']).iter_rows(named=True):
            if line_cnt >= PAGE_LENGTH:
                new_page()

            acctno   = coalesce_i(row.get('acctno'))
            noteno   = coalesce_i(row.get('noteno'))
            custcd   = coalesce_s(row.get('custcd') or row.get('ocustcd'))
            sectorcd = coalesce_s(row.get('sectorcd') or row.get('osector'))
            product  = coalesce_i(row.get('product') or row.get('oprod'))
            netproc  = coalesce_f(row.get('netproc') or row.get('onet'))
            issdte   = row.get('issdte') or row.get('oissdte')
            fisspurp = coalesce_s(row.get('fisspurp') or row.get('ofiss'))

            if isinstance(issdte, (int, float, str)):
                issdte = sas_date_to_pydate(issdte)

            line = (f" {acctno:>12} {noteno:>7} {custcd:>8} {sectorcd:>7}"
                    f" {product:>8} {fmt_comma15_2(netproc):>15}"
                    f" {fmt_date9(issdte):>10} {fisspurp:>12}")
            lines.append(' ' + line)
            line_cnt    += 1
            tot_netproc += netproc

    lines.append(' ' + '=' * 90)
    lines.append(' ' + (f" {'SUM':>12} {'':>7} {'':>8} {'':>7} {'':>8}"
                        f" {fmt_comma15_2(tot_netproc):>15}"))

    with open(filepath, 'w', encoding='utf-8') as f:
        for ln in lines:
            f.write(ln + '\n')

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("EIBMHPFS: Starting HP FISS disbursement processing...")
    print(f"  CHUNK_SIZE={CHUNK_SIZE:,}  MAX_ROWS_TEST={MAX_ROWS_TEST}")
    print(f"  EXCLUDE_ENTITY_CD='{EXCLUDE_ENTITY_CD}' (applied to LNNOTE only)")

    rv = get_report_vars()
    print(f"  [REPTDATE] {rv['reptdate']}  MM={rv['reptmon']} "
          f"YY={rv['reptyear']} DAY={rv['reptday']} "
          f"WK={rv['nowk']} WK1={rv['nowk1']}")
    print(f"  [SDATE]    {rv['sdate']}  (ISSDTE >= this)")

    lnnote_path = resolve_lnnote_path(rv)
    bnm_path    = resolve_bnm_loan_path(rv)
    print(f"  [LNNOTE]   {lnnote_path}")
    print(f"  [BNM_LOAN] {bnm_path}")

    hpsettle_df = build_hpsettle(rv)
    print(f"  HPSETTLE rows: {len(hpsettle_df)}")

    bnm_loan = load_bnm_loan(rv)
    print(f"  BNM_LOAN rows: {len(bnm_loan)}")

    hpsnr_raw = build_hpsnr(hpsettle_df, rv['sdate'], bnm_loan)
    print(f"  HPSNR rows: {len(hpsnr_raw)}")

    hp_new = build_hp_new(bnm_loan, rv['sdate'])
    print(f"  HP_NEW rows: {len(hp_new)}")

    hp_df, except_df, hpsnr_df = merge_hp_settle(hpsettle_df, hp_new, hpsnr_raw)
    print(f"  HP (final): {len(hp_df)}, EXCEPT: {len(except_df)}, "
          f"HPSNR: {len(hpsnr_df)}")

    lalm = build_lalm(hp_df)
    print(f"  LALM records: {len(lalm)}")

    write_rdal(lalm, rv)
    print(f"  Written: {RDAL_TXT}")

    except_t1 = (f"EXCEPTION REPORT FOR HP DISBURSE FOR MONTH "
                 f"{rv['reptmon']}/{rv['reptyear']}")
    write_exception_report(
        except_df, EXCEPT_TXT,
        title1=except_t1,
        title2='REPORT ID : EIBMHPFS',
    )
    print(f"  Written: {EXCEPT_TXT}")

    hpsnr_t1 = (f"HP A/C RELEASED AND SETTLED ON SAME MONTH "
                f"{rv['reptmon']}/{rv['reptyear']}")
    write_exception_report(
        hpsnr_df, HPSNR_TXT,
        title1=hpsnr_t1,
        title2='REPORT ID : EIBMHPFS',
    )
    print(f"  Written: {HPSNR_TXT}")

    print("EIBMHPFS: Processing complete.")


if __name__ == '__main__':
    main()
