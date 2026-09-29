#!/usr/bin/env python3
"""
Program Name: EIMBNM01.py
Purpose: Public Bank Berhad - Monthly Loan Summary Reports (M&I)
"""

import os
import time
from datetime import date, timedelta
from typing import Optional, Sequence

import numpy as np
import pandas as pd
import pyreadstat
import saspy

# -----------------------------------------------------------------------------
# %INC PGM(PBBLNFMT); %INC PGM(RDL2PBIF);
# -----------------------------------------------------------------------------
import PBBLNFMT          # noqa: F401
from RDL2PBIF import (
    build_pbif,
    format_fisstype,
    format_fissgroup,
)

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

BASE_DIR = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01"

BNM_LOAN_PREFIX    = f"{BASE_DIR}/loan{{reptmon}}{{nowk}}.sas7bdat"
BNM_LNWOF_PREFIX   = f"{BASE_DIR}/lnwof{{reptmon}}{{nowk}}.sas7bdat"
BNM_LNWOD_PREFIX   = f"{BASE_DIR}/lnwod{{reptmon}}{{nowk}}.sas7bdat"
SASD_LOAN_PREFIX   = f"{BASE_DIR}/loan{{reptmon}}.sas7bdat"
DISPAY_PREFIX      = f"{BASE_DIR}/dispaymth{{reptmon}}.sas7bdat"
BTBNM_BTRAD_PREFIX = f"{BASE_DIR}/btrad{{reptmon}}{{nowk}}{{reptyear}}.sas7bdat"
LOAN_LNCOMM_SAS    = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/enrh_ln_comm_m{reptmon}.sas7bdat"
FEE_LNFEE_PREFIX   = "/stgsrcsys/host/uat/maa/lnfee{reptmon}{nowk}.sas7bdat"

MFRS_DIR           = f"{BASE_DIR}/mfrs"
MFRS_MAST_BR_SAS   = os.path.join(MFRS_DIR, "mast_br.sas7bdat")
MFRS_ALM_CR_SAS    = os.path.join(MFRS_DIR, "alm_cr.sas7bdat")

REPORT_OUTPUT_DIR  = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMBNM01"
REPORT_TXT         = os.path.join(REPORT_OUTPUT_DIR, "eimbnm01_report.txt")

os.makedirs(MFRS_DIR, exist_ok=True)
os.makedirs(REPORT_OUTPUT_DIR, exist_ok=True)

# =============================================================================
# SAS SESSION
# =============================================================================

SAS_SESSION = saspy.SASsession(cfgname="default")


def write_sas7bdat(df: pd.DataFrame, path: str, table_name: Optional[str] = None):
    if df is None or df.empty:
        SAS_SESSION.submit(f'data _null_; file "{path}"; put; run;')
        return
    if table_name is None:
        table_name = os.path.splitext(os.path.basename(path))[0].upper()
    SAS_SESSION.df2sd(df, table=table_name, libref="WORK")
    out_dir  = os.path.dirname(path).replace('\\', '/')
    out_file = os.path.splitext(os.path.basename(path))[0]
    SAS_SESSION.submit(f'''
    libname _outdir "{out_dir}";
    data _outdir.{out_file};
        set WORK.{table_name};
    run;
    ''')


# =============================================================================
# CONSTANTS
# =============================================================================

ODCORP = {50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,31}
ODRTLA = {68,69,85,86,87,88,89,90,91,100,101,102,103,106,108,109,
          110,111,112,113,114,115,116,117,118,119,120,121,122,123,
          124,125,135,137,138,150,151,152,153,154,155,156,157,158,
          159,170,174,175,176,179,180,181,189,191,192,193,194,195,
          196,197,198,190,30,34,81,82,83,84,77,78}
ODRTLB = {177,178,34,133,134,77,78}
ODFISS = {'0311','0312','0313','0314','0315','0316'}
OTRTLA = {303,306,307,325,330,340,354,355,391,610,611,308,311,367,313,369}
OTRTLB = {4,5,6,7,15,20,25,26,27,28,29,30,31,32,33,34,
          60,61,62,63,70,71,72,73,74,75,76,77,78,79}
FLCORP = {180,181,182,183,193,800,801,802,803,804,818,
          900,901,902,903,904,905,906,907,908,912,922,
          184,909,910,914,915,916,918,919,920,925,950,951,
          631,632,633,634,635,636,637,639,640,641,816,817,
          805,806,807,808,809,810,811,812,813,814,913,917}
HLCORP = {638,911}

DBE_CUSTCDS   = {'41','42','43','44','46','47','48','49','51','52','53','54'}
FBE_CUSTCDS   = {'87','88','89'}
DNBFI_VALS    = {'1','2','3'}
SME_CUSTCDS   = DBE_CUSTCDS | FBE_CUSTCDS
INDIV_CUSTCDS = {'77','78','95','96'}


# =============================================================================
# REPORT WRITER
# =============================================================================

PAGE_LENGTH = 60


class ReportWriter:
    def __init__(self):
        self.lines = []
        self.line_cnt = PAGE_LENGTH + 1

    def _page_eject(self):
        self.line_cnt = 0

    def write_titles(self, title1, title2='', title3=''):
        self._page_eject()
        self.lines.append('1' + title1)
        if title2: self.lines.append(' ' + title2)
        if title3: self.lines.append(' ' + title3)
        self.lines.append(' ')
        self.line_cnt += (3 if title3 else 2) + 1

    def write_line(self, text='', asa=' '):
        self.lines.append(asa + text)
        self.line_cnt += 1

    def blank(self):
        self.write_line()

    def flush(self, filepath):
        with open(filepath, 'w', encoding='utf-8') as fh:
            for ln in self.lines:
                fh.write(ln + '\n')


# =============================================================================
# TIMING
# =============================================================================

_T0 = time.time()
def log(msg):
    print(f"  [+{time.time()-_T0:7.1f}s] {msg}", flush=True)


# =============================================================================
# IO HELPERS
# =============================================================================

def read_sas7bdat(path, usecols=None, row_limit=None) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    kwargs = {}
    if row_limit is not None:
        kwargs['row_limit'] = row_limit
    df, _ = pyreadstat.read_sas7bdat(path, **kwargs)
    df.columns = [c.lower() for c in df.columns]
    if usecols is not None:
        wanted = {c.lower() for c in usecols}
        df = df[[c for c in df.columns if c in wanted]]
    return df


def to_numeric_cols(df, cols) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
        else:
            df[c] = np.nan
    return df


def safe_concat(frames, **kwargs):
    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, **kwargs)


def coalesce_s(val, default=''):
    return str(val).strip() if val is not None else default


def pydate_to_sasdate(d):
    return (d - date(1960, 1, 1)).days


def sas_date_to_pydate(val):
    if val is None or (isinstance(val, float) and val != val):
        return None
    if isinstance(val, (int, float, np.integer, np.floating)):
        return date(1960, 1, 1) + timedelta(days=int(val))
    if isinstance(val, date):
        return val
    return None


def get_report_vars():
    today = date.today()
    reptdate = today.replace(day=1) - timedelta(days=1)
    day = reptdate.day
    if day == 8:
        sdd, wk, wk1, wk2, wk3 = 1, '1', '4', '', ''
    elif day == 15:
        sdd, wk, wk1, wk2, wk3 = 9, '2', '1', '', ''
    elif day == 22:
        sdd, wk, wk1, wk2, wk3 = 16, '3', '2', '', ''
    else:
        sdd, wk, wk1, wk2, wk3 = 23, '4', '3', '2', '1'
    mm = reptdate.month
    mm1 = (mm - 1) if wk != '1' else (mm - 1 if mm > 1 else 12)
    if mm1 == 0: mm1 = 12
    mm2 = mm - 1
    if mm2 == 0: mm2 = 12
    return {
        'reptdate':  reptdate,
        'sdate':     date(reptdate.year, mm, sdd),
        'wk': wk, 'wk1': wk1, 'wk2': wk2, 'wk3': wk3,
        'mm': mm, 'mm1': mm1, 'mm2': mm2, 'sdd': sdd,
        'reptmon':   str(mm).zfill(2),
        'reptmon1':  str(mm1).zfill(2),
        'reptmon2':  str(mm2).zfill(2),
        'reptyear':  reptdate.strftime('%y'),
        'ryear':     str(reptdate.year),
        'reptday':   str(day).zfill(2),
        'nowk':      wk,
        'mdate_int': pydate_to_sasdate(reptdate),
    }


# =============================================================================
# COLUMN SUBSETS
# =============================================================================

KEY_COLS = ['acctno', 'noteno']

LOAN_KEEP = [
    'acctno','noteno','fisspurp','product','noteterm','earnterm',
    'balance','bal_aft_eir','paidind','apprdate','apprlim2','prodcd',
    'custcd','custcode','amtind','sectorcd','sectfiss','acctype','branch',
    'cjfee','dnbfisme','noacct','commno','rleasamt','forate','eir_adj',
    'retailid',
]
DISPAY_KEEP = ['acctno','noteno','disburse','repaid','fisspurp','product',
               'dnbfisme','prodcd','custcd','amtind','sectorcd','branch','acctype']
LNFEE_KEEP  = ['acctno','noteno','duetotal','feeplan']
LNCOMM_KEEP = ['acctno','commno','cusedamt']
BTRAD_KEEP  = ['acctno','subacct','fisspurp','product','noteterm','balance',
               'apprlim2','apprlimt','prodcd','custcd','amtind','transref',
               'sectorcd','disburse','repaid','dnbfisme','retailid','dirctind']


def _ensure_loan_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure BNM_LOAN-derived frames contain the columns build_alm needs.
    Anything missing is synthesised from what's available.
    """
    if df.empty:
        return df
    df = df.copy()

    # custcd <- custcode (if needed)
    if 'custcd' not in df.columns and 'custcode' in df.columns:
        df['custcd'] = df['custcode']
    # sectorcd <- sectfiss (if needed)
    if 'sectorcd' not in df.columns and 'sectfiss' in df.columns:
        df['sectorcd'] = df['sectfiss']
    # amtind default
    if 'amtind' not in df.columns:
        df['amtind'] = 'D'
    # acctype default to 'LN' (BNM_LOAN is a loan book)
    if 'acctype' not in df.columns:
        df['acctype'] = 'LN'
    # prodcd derive from product (SAS stores PRODCD as "34xxx" text)
    if 'prodcd' not in df.columns:
        prod = pd.to_numeric(df.get('product', 0), errors='coerce').fillna(0).astype(int)
        df['prodcd'] = prod.astype(str)
    # retailid default
    if 'retailid' not in df.columns:
        df['retailid'] = ''
    # rleasamt default
    if 'rleasamt' not in df.columns:
        df['rleasamt'] = 0.0
    return df


# =============================================================================
# STAGE 1 — LOAN BASE
# =============================================================================

def build_loan_dataset(rv):
    mm, mm2, wk = rv['reptmon'], rv['reptmon2'], rv['nowk']

    def _read(tmpl, **fmt):
        return _ensure_loan_schema(
            read_sas7bdat(tmpl.format(**fmt), usecols=LOAN_KEEP))

    log("  reading SASD_LOAN ...")
    dloan = _read(SASD_LOAN_PREFIX, reptmon=mm)
    log(f"  SASD_LOAN: {len(dloan):>10,} rows")

    log("  reading BNM_LOAN ...")
    mloan = _read(BNM_LOAN_PREFIX, reptmon=mm, nowk=wk)
    log(f"  BNM_LOAN: {len(mloan):>10,} rows")

    log("  reading BNM_LNWOF ...")
    lnwof = read_sas7bdat(BNM_LNWOF_PREFIX.format(reptmon=mm, nowk=wk),
                          usecols=['acctno','noteno','balance'])
    log(f"  BNM_LNWOF: {len(lnwof):>10,} rows")

    log("  reading BNM_LNWOD ...")
    lnwod = read_sas7bdat(BNM_LNWOD_PREFIX.format(reptmon=mm, nowk=wk),
                          usecols=['acctno','noteno','balance'])
    log(f"  BNM_LNWOD: {len(lnwod):>10,} rows")

    log("  reading BNM_LNWOF (prev) ...")
    plnwof = read_sas7bdat(BNM_LNWOF_PREFIX.format(reptmon=mm2, nowk=wk),
                           usecols=['acctno','noteno','balance'])
    log(f"  BNM_LNWOF(prev): {len(plnwof):>10,} rows")

    log("  reading BNM_LNWOD (prev) ...")
    plnwod = read_sas7bdat(BNM_LNWOD_PREFIX.format(reptmon=mm2, nowk=wk),
                           usecols=['acctno','noteno','balance'])
    log(f"  BNM_LNWOD(prev): {len(plnwod):>10,} rows")

    log("  reading BNM_LOAN (prev) ...")
    loan_prev = _read(BNM_LOAN_PREFIX, reptmon=mm2, nowk=wk)
    log(f"  BNM_LOAN(prev): {len(loan_prev):>10,} rows")

    if not dloan.empty and not mloan.empty:
        mkeys = mloan[KEY_COLS].drop_duplicates()
        mkeys['_b'] = True
        loandm = dloan.merge(mkeys, on=KEY_COLS, how='left')
        loandm = loandm[loandm['_b'].isna()].drop(columns='_b')
    elif not dloan.empty:
        loandm = dloan
    else:
        loandm = pd.DataFrame()

    frames = [f for f in [plnwof, plnwod, loandm, loan_prev, mloan, lnwof, lnwod]
              if not f.empty]
    if not frames:
        return pd.DataFrame(), mloan

    log(f"  merging {len(frames)} frames ...")
    base = frames[0]
    for i, f in enumerate(frames[1:], start=2):
        non_key = [c for c in f.columns if c not in KEY_COLS]
        if not non_key:
            continue
        merged = base.merge(f[KEY_COLS + non_key], on=KEY_COLS,
                            how='outer', suffixes=('', '_r'))
        for col in non_key:
            rc = f"{col}_r"
            if rc in merged.columns:
                if col in merged.columns:
                    merged[col] = merged[rc].where(merged[rc].notna(), merged[col])
                else:
                    merged[col] = merged[rc]
                merged = merged.drop(columns=rc)
        base = merged
        log(f"  merge step {i}/{len(frames)}: {len(base):>10,} rows")

    return base, mloan


# =============================================================================
# STAGE 2 — DISPAY
# =============================================================================

def build_dispay(rv, loan_df):
    if loan_df.empty:
        return pd.DataFrame()
    log("  reading DISPAY ...")
    dispay = read_sas7bdat(DISPAY_PREFIX.format(reptmon=rv['reptmon']),
                           usecols=DISPAY_KEEP)
    log(f"  DISPAY raw: {len(dispay):>10,} rows")
    if dispay.empty:
        return pd.DataFrame()
    dispay = to_numeric_cols(dispay, ['disburse', 'repaid'])
    mask = pd.Series(False, index=dispay.index)
    if 'disburse' in dispay.columns: mask = mask | (dispay['disburse'] > 0)
    if 'repaid'   in dispay.columns: mask = mask | (dispay['repaid']   > 0)
    dispay = dispay[mask]
    log(f"  DISPAY filtered: {len(dispay):>10,} rows")
    result = loan_df.merge(dispay, on=KEY_COLS, how='inner', suffixes=('', '_dp'))
    log(f"  DISPAY merged: {len(result):>10,} rows")
    return result


# =============================================================================
# STAGE 3 — CL_FEE  (SAS-side filter; falls back to pyreadstat)
# =============================================================================

def build_cl_fee(rv):
    path = FEE_LNFEE_PREFIX.format(reptmon=rv['reptmon'], nowk=rv['nowk'])
    log(f"  LNFEE: SAS-side filter on {path}")

    sas_code = f'''
    proc sql;
        create table _lnfee_cl as
        select acctno, noteno, sum(duetotal) as duetotal
        from "{path}"
        where feeplan = "CL"
          and duetotal > 0
        group by acctno, noteno;
    quit;
    '''
    try:
        SAS_SESSION.submit(sas_code)
        df = SAS_SESSION.sd2df('_lnfee_cl', libref='WORK')
    except Exception as e:
        log(f"  LNFEE SAS query failed ({e}); falling back to pyreadstat")
        raw = read_sas7bdat(path, usecols=LNFEE_KEEP)
        if raw.empty:
            return pd.DataFrame(columns=['acctno','noteno','duetotal'])
        raw = to_numeric_cols(raw, ['duetotal'])
        raw = raw[(raw['feeplan'] == 'CL') & (raw['duetotal'] > 0)]
        return raw.groupby(KEY_COLS, as_index=False)['duetotal'].sum()

    if df is None or df.empty:
        log("  LNFEE CL aggregated: 0 rows")
        return pd.DataFrame(columns=['acctno','noteno','duetotal'])

    df.columns = [c.lower() for c in df.columns]
    df = to_numeric_cols(df, ['acctno','noteno','duetotal'])
    log(f"  LNFEE CL aggregated: {len(df):>10,} rows")
    return df


def merge_loan_cl_fee(loan_df, cl_fee):
    if loan_df.empty:
        return loan_df
    if cl_fee.empty:
        out = loan_df.copy()
        if 'clfee' not in out.columns:
            out['clfee'] = 0.0
        return out
    merged = loan_df.merge(cl_fee, on=KEY_COLS, how='left', suffixes=('', '_fee'))
    if 'duetotal_fee' in merged.columns:
        if 'duetotal' in merged.columns:
            merged['duetotal'] = merged['duetotal_fee'].where(
                merged['duetotal_fee'].notna(), merged['duetotal'])
        else:
            merged['duetotal'] = merged['duetotal_fee']
        merged = merged.drop(columns='duetotal_fee')
    if 'forate' in merged.columns and 'duetotal' in merged.columns:
        due = pd.to_numeric(merged['duetotal'], errors='coerce').fillna(0.0)
        rate = pd.to_numeric(merged['forate'],   errors='coerce').fillna(0.0)
        merged['clfee'] = due * rate
    else:
        merged['clfee'] = 0.0
    log(f"  CL_FEE merged: {len(merged):>10,} rows, clfee sum = {merged['clfee'].sum():,.2f}")
    return merged


# =============================================================================
# STAGE 4 — ALM
# =============================================================================

def build_alm(loan_raw, rv):
    if loan_raw.empty:
        return pd.DataFrame()

    log("  reading LNCOMM ...")
    lncomm = read_sas7bdat(LOAN_LNCOMM_SAS.format(reptmon=rv['reptmon']),
                           usecols=LNCOMM_KEEP)
    log(f"  LNCOMM raw: {len(lncomm):>10,} rows")

    if 'balance' in loan_raw.columns:
        loan_raw = loan_raw.rename(columns={'balance': 'oribal'})
    if 'bal_aft_eir' in loan_raw.columns:
        loan_raw = loan_raw.rename(columns={'bal_aft_eir': 'balance'})

    if not lncomm.empty and 'commno' in loan_raw.columns:
        lncomm = to_numeric_cols(lncomm, ['commno', 'cusedamt'])
        merged = loan_raw.merge(lncomm, on=['acctno', 'commno'],
                                how='left', suffixes=('', '_lc'))
        if 'cusedamt_lc' in merged.columns:
            if 'cusedamt' in merged.columns:
                merged['cusedamt'] = merged['cusedamt_lc'].where(
                    merged['cusedamt_lc'].notna(), merged['cusedamt'])
            else:
                merged['cusedamt'] = merged['cusedamt_lc']
            merged = merged.drop(columns='cusedamt_lc')
    else:
        merged = loan_raw

    merged = to_numeric_cols(merged, [
        'oribal','cjfee','cusedamt','rleasamt','clfee',
        'product','commno','noacct','eir_adj','acctno','noteno'])

    for c in ['oribal','cjfee','cusedamt','rleasamt','clfee']:
        merged[c] = merged[c].fillna(0.0)
    for c in ['noacct','product','commno']:
        merged[c] = merged[c].fillna(0).astype(np.int64)
    for c in ['paidind','prodcd','acctype']:
        if c not in merged.columns:
            merged[c] = ''
        merged[c] = merged[c].fillna('').astype(str)
    if 'retailid' not in merged.columns:
        merged['retailid'] = ''
    merged['retailid'] = merged['retailid'].fillna('').astype(str)

    log(f"  ALM pre-filter: {len(merged):>10,} rows")

    paidind  = merged['paidind']
    eir_adj  = merged['eir_adj']
    oribal   = merged['oribal']
    prodcd   = merged['prodcd']
    acctype  = merged['acctype']
    product  = merged['product']
    commno   = merged['commno']
    cusedamt = merged['cusedamt']
    rleasamt = merged['rleasamt']
    cjfee    = merged['cjfee']
    clfee    = merged['clfee']
    noacct   = merged['noacct']

    drop_pc     = paidind.isin(['P','C']) & eir_adj.isna()
    oribal_r    = oribal.round(2)
    drop_zero   = oribal_r.isin([0.0, -0.0])
    prodcd_str  = prodcd.astype(str)
    keep_prodcd = (prodcd_str.str[:2] == '34') | (prodcd_str == '54120')
    base_mask   = ~drop_pc & ~drop_zero & keep_prodcd
    log(f"  ALM post-base mask: {int(base_mask.sum()):>10,} rows")

    in_pc = paidind.isin(['P','C'])
    cond_ln1 = (rleasamt != 0.0) & ~in_pc & (oribal > 0) & (cjfee != oribal)
    cond_ln2 = (rleasamt == 0.0) & ~in_pc & (oribal > 0) & (product >= 600) & (product <= 699)
    cond_ln3 = (rleasamt == 0.0) & ~in_pc & (oribal > 0) & (commno > 0) & (cusedamt > 0)
    ln_eligible = cond_ln1 | cond_ln2 | cond_ln3
    ln_acctype  = (acctype == 'LN')

    noacct_after_ln = noacct.where(~(ln_acctype & ~ln_eligible), 0)
    noacct_after_ln = noacct_after_ln.where(~((rleasamt != 0) & (oribal == clfee)), 0)
    apply_one = (~in_pc) & (cjfee != oribal) & (noacct_after_ln != 0) & \
                (~oribal_r.isin([0.0, -0.0]))
    noacct_final = noacct_after_ln.where(~apply_one, 1)

    merged = merged[base_mask].copy()
    merged['noacct'] = noacct_final[base_mask].values
    if merged.empty:
        return merged

    acctno = pd.to_numeric(merged['acctno'], errors='coerce').fillna(0).astype(np.int64)
    noteno = pd.to_numeric(merged['noteno'], errors='coerce').fillna(0).astype(np.int64)
    product = pd.to_numeric(merged['product'], errors='coerce').fillna(0).astype(np.int64)

    almbt_mask = (((acctno >= 2500000000) & (acctno <= 2599999999) &
                   (noteno >= 40000) & (noteno <= 49999))
                  | (product == 321))
    alm_df   = merged[~almbt_mask].copy()
    almbt_df = merged[almbt_mask].copy()
    log(f"  ALM main: {len(alm_df):>10,}  ALMBT: {len(almbt_df):>10,}")

    if not alm_df.empty and 'commno' in alm_df.columns:
        alm_df = alm_df.sort_values(['acctno', 'commno'])
        grp = alm_df.groupby(['acctno', 'commno'], sort=False)
        is_rev  = alm_df['prodcd'].isin(['34170','34190','34690']).to_numpy()
        cum_no  = grp['noacct'].cumsum().to_numpy()
        zero_m  = is_rev & (cum_no > 1)
        alm_df.loc[zero_m, 'noacct'] = 0

    if not almbt_df.empty:
        almbt_df = almbt_df.sort_values('acctno')
        prev  = almbt_df['acctno'].shift(1)
        first = prev.isna() | (almbt_df['acctno'] != prev)
        almbt_df['noacct'] = first.astype(int)

    return (pd.concat([alm_df, almbt_df], ignore_index=True)
            if not almbt_df.empty else alm_df)


# =============================================================================
# APPLY PRODESC
# =============================================================================

def apply_prodesc(df):
    if df.empty:
        return df
    df = df.copy()
    for c in ['product','prodcd','acctype']:
        if c not in df.columns:
            df[c] = '' if c != 'product' else 0
    product = pd.to_numeric(df['product'], errors='coerce').fillna(0).astype(np.int64)
    acctype = df['acctype'].fillna('').astype(str)
    prodcd  = df['prodcd'].fillna('').astype(str)
    prodesc = pd.Series('', index=df.index, dtype=object)

    m = ((acctype == 'LN') & (prodcd == '34111')) | product.isin({678,679,993,996})
    prodesc = prodesc.where(~m, 'HIRE PURCHASE')

    m = (acctype == 'LN') & (prodcd == '34120')
    prodesc = prodesc.where(~m, 'RETAIL HOUSING LOANS')

    m = (acctype == 'LN') & (prodcd == '34120') & product.isin(HLCORP)
    prodesc = prodesc.where(~m, 'CORP. BANKING HOUSING LOANS')

    m = (acctype == 'OD') & prodcd.isin(['34180','34240']) & product.isin(ODCORP)
    prodesc = prodesc.where(~m, 'OD CORPORATE')
    m = (acctype == 'OD') & prodcd.isin(['34180','34240']) & ~product.isin(ODCORP)
    prodesc = prodesc.where(~m, 'OD RETAIL')

    m_corp = (acctype == 'LN') & ~prodcd.isin(['34111','34120','N','M']) & product.isin(FLCORP)
    prodesc = prodesc.where(~m_corp, 'CORP. BANKING LOANS')
    m_oth = (acctype == 'LN') & ~prodcd.isin(['34111','34120','N','M']) & ~product.isin(FLCORP)
    prodesc = prodesc.where(~m_oth, 'OTHERS RETAIL')

    m = (acctype == 'LN') & (prodcd == '34170')
    prodesc = prodesc.where(~m, 'FLOOR STOCKING LOANS')

    df['prodesc'] = prodesc
    return df


# =============================================================================
# BTRADE
# =============================================================================

def build_btrade(rv):
    mm, wk = rv['reptmon'], rv['nowk']
    path = BTBNM_BTRAD_PREFIX.format(reptmon=mm, nowk=wk, reptyear=rv['reptyear'])

    log("  reading BTRAD ...")
    btrad_raw = read_sas7bdat(path, usecols=BTRAD_KEEP)
    log(f"  BTRAD raw: {len(btrad_raw):>10,} rows")
    if btrad_raw.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    btrad_raw = to_numeric_cols(btrad_raw,
        ['disburse','repaid','balance','apprlimt','apprlim2','product'])

    if 'dirctind' in btrad_raw.columns:
        btrad_raw = btrad_raw[
            (btrad_raw['dirctind'] == 'D') &
            btrad_raw['custcd'].notna() &
            (btrad_raw['custcd'] != ' ')]
        log(f"  BTRAD after DIRCTIND filter: {len(btrad_raw):>10,}")

    if 'apprlimt' in btrad_raw.columns:
        btrad1 = btrad_raw.sort_values(['acctno','apprlimt'], ascending=[True,False])
    else:
        btrad1 = btrad_raw.sort_values('acctno')

    grp_key1 = [c for c in ['acctno','custcd','retailid','sectorcd','dnbfisme']
                if c in btrad1.columns]
    agg_v1 = [c for c in ['disburse','repaid'] if c in btrad1.columns]
    btrad2 = (btrad1.groupby(grp_key1, as_index=False, dropna=False)[agg_v1].sum()
              if grp_key1 and agg_v1 else pd.DataFrame())

    grp_key2 = [c for c in ['acctno','custcd','retailid','sectorcd']
                if c in btrad1.columns]
    btrad1_bal = pd.DataFrame()
    if 'apprlimt' in btrad1.columns and 'balance' in btrad1.columns and grp_key2:
        btrad1_bal = (btrad1[btrad1['apprlimt'] > 0]
                      .groupby(grp_key2, as_index=False, dropna=False)['balance'].sum())

    merge_key = [c for c in ['acctno','custcd','retailid','sectorcd']
                 if c in btrad2.columns]
    if not btrad1_bal.empty and not btrad2.empty:
        mast_m = btrad2.merge(btrad1_bal, on=merge_key, how='left', suffixes=('','_bal'))
        if 'balance_bal' in mast_m.columns:
            mast_m['balance'] = mast_m['balance_bal'].where(
                mast_m['balance_bal'].notna(),
                mast_m['balance'] if 'balance' in mast_m.columns else np.nan)
            mast_m = mast_m.drop(columns='balance_bal')
    else:
        mast_m = btrad2

    mast_m = mast_m.copy()
    mast_m['disbno']  = ((mast_m['disburse'] > 0).astype(int)
                         if 'disburse' in mast_m.columns else 0)
    mast_m['repayno'] = ((mast_m['repaid']   > 0).astype(int)
                         if 'repaid'   in mast_m.columns else 0)
    if 'balance' in mast_m.columns:
        b = mast_m['balance'].round(2)
        mast_m['noacct'] = (b.notna() & (b != 0) & (mast_m['acctno'] != 0)).astype(int)
    else:
        mast_m['noacct'] = 0

    ovc_df = mast_m[[c for c in ['acctno','retailid'] if c in mast_m.columns]].copy()
    mast_keep = [c for c in ['acctno','custcd','balance','retailid','disbno',
                             'repayno','noacct','sectorcd','dnbfisme']
                 if c in mast_m.columns]
    mast_df = mast_m[mast_keep].copy()

    alm_bt_raw = btrad_raw.copy()
    if 'prodcd' in alm_bt_raw.columns:
        alm_bt_raw = alm_bt_raw[alm_bt_raw['prodcd'].astype(str).str[:2] == '34']

    bt_keep = [c for c in ['acctno','subacct','fisspurp','product','noteterm',
                           'balance','apprlim2','prodcd','custcd','amtind',
                           'transref','sectorcd','disburse','repaid',
                           'dnbfisme','retailid']
               if c in alm_bt_raw.columns]
    alm_bt = alm_bt_raw[bt_keep].copy()
    log(f"  BTRAD (prodcd 34*): {len(alm_bt):>10,}")

    if not ovc_df.empty:
        alm_bt = alm_bt.merge(ovc_df, on='acctno', how='inner', suffixes=('','_ovc'))
        if 'retailid_ovc' in alm_bt.columns:
            alm_bt['retailid'] = alm_bt['retailid_ovc'].where(
                alm_bt['retailid_ovc'].notna(),
                alm_bt['retailid'] if 'retailid' in alm_bt.columns else '')
            alm_bt = alm_bt.drop(columns='retailid_ovc')

    bt_grp = [c for c in ['acctno','transref','custcd','fisspurp','sectorcd']
              if c in alm_bt.columns]
    if bt_grp and 'balance' in alm_bt.columns:
        almx = (alm_bt.groupby(bt_grp, as_index=False, dropna=False)['balance']
                .sum().rename(columns={'balance':'balance_sum'}))
        alm_bt = alm_bt.sort_values(bt_grp).drop_duplicates(subset=bt_grp, keep='first')
        alm_bt = alm_bt.drop(columns='balance').merge(almx, on=bt_grp, how='left')
        alm_bt = alm_bt.rename(columns={'balance_sum':'balance'})

    for c in ['disburse','repaid','balance']:
        if c not in alm_bt.columns:
            alm_bt[c] = 0.0
        alm_bt[c] = pd.to_numeric(alm_bt[c], errors='coerce').fillna(0.0)

    if 'retailid' not in alm_bt.columns:
        alm_bt['retailid'] = ''
    alm_bt['prodesc'] = np.where(alm_bt['retailid'].fillna('').astype(str) == 'C',
                                 'BILLS CORPORATE', 'BILLS RETAIL')

    if not mast_df.empty:
        if 'retailid' not in mast_df.columns:
            mast_df['retailid'] = ''
        mast_df['prodesc'] = np.where(mast_df['retailid'].fillna('').astype(str) == 'C',
                                      'BILLS CORPORATE', 'BILLS RETAIL')

    if not mast_df.empty:
        mast_br = mast_df[['acctno', 'prodesc']].copy()
        mast_br['acctno'] = pd.to_numeric(mast_br['acctno'], errors='coerce').fillna(0).astype(np.int64)
        mast_br['noacct'] = mast_df['noacct'].values if 'noacct' in mast_df.columns else 0
        log("  writing MFRS.MAST_BR ...")
        write_sas7bdat(mast_br, MFRS_MAST_BR_SAS, 'MAST_BR')
        log(f"  MFRS.MAST_BR written: {len(mast_br):>10,} rows")

    agg_v = [c for c in ['disburse','repaid','balance'] if c in alm_bt.columns]
    almloan_bt = (alm_bt.groupby('prodesc', as_index=False)[agg_v].sum()
                  if agg_v else pd.DataFrame())

    mast_agg_v = [c for c in ['disbno','repayno','noacct'] if c in mast_df.columns]
    if mast_agg_v and not mast_df.empty:
        mastloan = mast_df.groupby('prodesc', as_index=False)[mast_agg_v].sum()
        almloan_bt = (almloan_bt.merge(mastloan, on='prodesc', how='left')
                      if not almloan_bt.empty else mastloan)

    return alm_bt, almloan_bt, mast_df


# =============================================================================
# REPORT HELPERS
# =============================================================================

NUM_COLS = ['disburse','repaid','disbno','repayno','balance','noacct']


def fmt_num(val, decimals=2):
    if val is None or (isinstance(val, float) and val != val):
        return ' ' * (16 if decimals == 2 else 10)
    if decimals == 2:
        return f"{float(val):16.2f}"
    return f"{float(val):10.0f}"


def print_table(df, rw, title1, title2=''):
    if df is None or df.empty:
        return
    rw.write_titles(title1, title2)
    hdr = (f"{'PRODESC':<35}{'DISBURSE':>16}{'REPAID':>16}"
           f"{'DISBNO':>10}{'REPAYNO':>10}{'BALANCE':>16}{'NOACCT':>10}")
    rw.write_line(' ' + hdr)
    rw.write_line(' ' + '-' * len(hdr))
    tot = {c: 0.0 for c in NUM_COLS}
    for _, row in df.sort_values('prodesc').iterrows():
        line = (f"{coalesce_s(row.get('prodesc'))[:35]:<35}"
                f"{fmt_num(row.get('disburse'))}"
                f"{fmt_num(row.get('repaid'))}"
                f"{fmt_num(row.get('disbno'), 0)}"
                f"{fmt_num(row.get('repayno'), 0)}"
                f"{fmt_num(row.get('balance'))}"
                f"{fmt_num(row.get('noacct'), 0)}")
        rw.write_line(' ' + line)
        for c in NUM_COLS:
            v = row.get(c)
            if v is not None and not (isinstance(v, float) and v != v):
                tot[c] += float(v)
    rw.write_line(' ' + '=' * len(hdr))
    rw.write_line(' ' + f"{'SUM':<35}"
                       f"{fmt_num(tot['disburse'])}"
                       f"{fmt_num(tot['repaid'])}"
                       f"{fmt_num(tot['disbno'], 0)}"
                       f"{fmt_num(tot['repayno'], 0)}"
                       f"{fmt_num(tot['balance'])}"
                       f"{fmt_num(tot['noacct'], 0)}")
    rw.blank()


def print_tabulate_sector(df, rw, title1, title2):
    """
    Reproduce the SAS PROC TABULATE layout for SECTFISS/SECTGROUP:

        +-----------+-----------+------------+----------+
        |SECTFISS   |           |  NO. OF    |          |
        |           |  AMOUNT   |   ACCT     |          |
        +-----------+-----------+------------+----------+
        |1000       |1100       |774044358.60|      1522|
        |           +-----------+------------+----------+
        |           |1200       | 63986978.60|       134|
        ...
        |           |SUB-TOTAL  |925297732.84|      1868|
        +-----------+-----------+------------+----------+
        ...
        |GRAND TOTAL|           |91682120230 |     45123|
        +-----------+-----------+------------+----------+
    """
    if df is None or df.empty:
        return

    if 'secgroup' not in df.columns or 'sectype' not in df.columns:
        return

    agg_cols = {}
    if 'balance' in df.columns: agg_cols['balance'] = 'sum'
    if 'noacct'  in df.columns: agg_cols['noacct']  = 'sum'
    if not agg_cols:
        return

    rw.write_titles(title1, title2)

    grp = (df.groupby(['secgroup','sectype'], as_index=False, dropna=False)
             .agg(agg_cols)
             .sort_values(['secgroup','sectype']))

    if 'balance' in grp.columns:
        grp['balance'] = pd.to_numeric(grp['balance'], errors='coerce').fillna(0.0)
    if 'noacct' in grp.columns:
        grp['noacct'] = pd.to_numeric(grp['noacct'], errors='coerce').fillna(0).astype(int)

    W_GROUP = 11
    W_SUB   = 11
    W_AMT   = 14
    W_NOA   = 10

    def _line(group='', sub='', amt='', noa=''):
        return (f"|{group:<{W_GROUP}}|{sub:<{W_SUB}}|"
                f"{amt:>{W_AMT}}|{noa:>{W_NOA}}|")

    def _border():
        return (f"+{'-'*W_GROUP}+{'-'*W_SUB}+"
                f"{'-'*W_AMT}+{'-'*W_NOA}+")

    def _divider_left():
        return f"|{' '*W_GROUP}+{'-'*W_SUB}+{'-'*W_AMT}+{'-'*W_NOA}+"

    rw.write_line('')
    rw.write_line(_border())
    rw.write_line(_line('SECTFISS', '', 'NO. OF', ''))
    rw.write_line(_line('', 'AMOUNT', 'ACCT', ''))
    rw.write_line(_border())

    grand_bal = 0.0
    grand_noa = 0
    for sg in grp['secgroup'].drop_duplicates():
        sub = grp[grp['secgroup'] == sg].sort_values('sectype')
        sub_bal = 0.0
        sub_noa = 0
        first_row = True
        for _, row in sub.iterrows():
            st  = coalesce_s(row.get('sectype'))
            bal = float(row.get('balance') or 0.0)
            noa = int(row.get('noacct') or 0)
            sub_bal += bal
            sub_noa += noa
            if first_row:
                rw.write_line(_line(sg, st, f"{bal:,.2f}", str(noa)))
                first_row = False
            else:
                rw.write_line(_line('', st, f"{bal:,.2f}", str(noa)))
            rw.write_line(_divider_left())
        rw.write_line(_line('', 'SUB-TOTAL', f"{sub_bal:,.2f}", str(sub_noa)))
        rw.write_line(_border())
        grand_bal += sub_bal
        grand_noa += sub_noa

    rw.write_line(_line('GRAND TOTAL', '', f"{grand_bal:,.2f}", str(grand_noa)))
    rw.write_line(_border())
    rw.blank()


def print_tabulate_product(df, rw, title1, title2):
    if df is None or df.empty:
        return
    rw.write_titles(title1, title2)
    hdr = f"{'FACILITY':<25}{'AMOUNT':>18}{'NO. OF ACCT':>12}"
    rw.write_line(' ' + hdr)
    rw.write_line(' ' + '-' * len(hdr))
    agg = {c: 'sum' for c in ['balance','noacct'] if c in df.columns}
    if not agg:
        return
    grp = df.groupby('type', as_index=False, dropna=False).agg(agg).sort_values('type')
    grand_bal = grand_noa = 0.0
    for _, row in grp.iterrows():
        t   = coalesce_s(row.get('type'))[:25]
        bal = float(row.get('balance') or 0.0)
        noa = float(row.get('noacct') or 0.0)
        rw.write_line(' ' + f"{t:<25}{bal:18.2f}{noa:12.0f}")
        grand_bal += bal
        grand_noa += noa
    rw.write_line(' ' + '=' * len(hdr))
    rw.write_line(' ' + f"{'GRAND TOTAL':<25}{grand_bal:18.2f}{grand_noa:12.0f}")
    rw.blank()


def summarise(df, class_cols):
    if df is None or df.empty:
        return pd.DataFrame()
    agg_v = [c for c in NUM_COLS if c in df.columns]
    if not agg_v:
        return pd.DataFrame()
    return df.groupby(class_cols, as_index=False, dropna=False)[agg_v].sum()


# =============================================================================
# SME MASK HELPERS
# =============================================================================

def _sme_mask(df):
    custcd = df.get('custcd', pd.Series('', index=df.index)).fillna('').astype(str)
    custcx = df.get('custcx', pd.Series('', index=df.index)).fillna('').astype(str)
    dnbfi  = df.get('dnbfisme', pd.Series('', index=df.index)).fillna('').astype(str)
    return (custcd.isin(SME_CUSTCDS) | custcx.isin(SME_CUSTCDS) |
            dnbfi.isin(DNBFI_VALS))

def _dbe_mask(df):
    custcd = df.get('custcd', pd.Series('', index=df.index)).fillna('').astype(str)
    custcx = df.get('custcx', pd.Series('', index=df.index)).fillna('').astype(str)
    return custcd.isin(DBE_CUSTCDS) | custcx.isin(DBE_CUSTCDS)

def _fbe_mask(df):
    custcd = df.get('custcd', pd.Series('', index=df.index)).fillna('').astype(str)
    return custcd.isin(FBE_CUSTCDS)

def _dnbfi_mask(df):
    dnbfi = df.get('dnbfisme', pd.Series('', index=df.index)).fillna('').astype(str)
    return dnbfi.isin(DNBFI_VALS)


# =============================================================================
# MAIN
# =============================================================================

def main():
    log("EIMBNM01: Starting Public Bank Berhad loan summary reports...")
    rv = get_report_vars()
    mm_yr = f"{rv['reptmon']}/{rv['ryear']}"
    log(f"Report date: {rv['reptdate']}  MM={rv['reptmon']} YY={rv['ryear']} "
        f"WK={rv['nowk']}  MM2={rv['reptmon2']}")

    for label, p in [
        ('SASD_LOAN',      SASD_LOAN_PREFIX.format(reptmon=rv['reptmon'])),
        ('BNM_LOAN',       BNM_LOAN_PREFIX.format(reptmon=rv['reptmon'], nowk=rv['nowk'])),
        ('BNM_LNWOF',      BNM_LNWOF_PREFIX.format(reptmon=rv['reptmon'], nowk=rv['nowk'])),
        ('BNM_LNWOD',      BNM_LNWOD_PREFIX.format(reptmon=rv['reptmon'], nowk=rv['nowk'])),
        ('BNM_LNWOF_PREV', BNM_LNWOF_PREFIX.format(reptmon=rv['reptmon2'], nowk=rv['nowk'])),
        ('BNM_LNWOD_PREV', BNM_LNWOD_PREFIX.format(reptmon=rv['reptmon2'], nowk=rv['nowk'])),
        ('BNM_LOAN_PREV',  BNM_LOAN_PREFIX.format(reptmon=rv['reptmon2'], nowk=rv['nowk'])),
        ('DISPAY',         DISPAY_PREFIX.format(reptmon=rv['reptmon'])),
        ('BTRAD',          BTBNM_BTRAD_PREFIX.format(reptmon=rv['reptmon'], nowk=rv['nowk'], reptyear=rv['reptyear'])),
        ('LNCOMM',         LOAN_LNCOMM_SAS.format(reptmon=rv['reptmon'])),
        ('LNFEE',          FEE_LNFEE_PREFIX.format(reptmon=rv['reptmon'], nowk=rv['nowk'])),
        ('MFRS_DIR',       MFRS_DIR),
        ('REPORT_DIR',     REPORT_OUTPUT_DIR),
    ]:
        log(f"input check: {label:15s} exists={str(os.path.exists(p)):5s}  {p}")

    rw = ReportWriter()
    RPT = 'REPORT ID : EIMBNM01'

    # --- Stage 1 -----------------------------------------------------------
    log("STAGE 1: build_loan_dataset")
    loan_base, bnm_loan_cached = build_loan_dataset(rv)
    log(f"loan_base rows: {len(loan_base):,}")

    # --- Stage 2 -----------------------------------------------------------
    log("STAGE 2: build_dispay")
    dispay_df = build_dispay(rv, loan_base)
    log(f"dispay_df rows: {len(dispay_df):,}")

    # --- Stage 3 -----------------------------------------------------------
    log("STAGE 3: build_cl_fee + merge")
    cl_fee = build_cl_fee(rv)
    bnm_loan = merge_loan_cl_fee(bnm_loan_cached, cl_fee)

    # --- Stage 4 -----------------------------------------------------------
    log("STAGE 4: build_alm")
    alm_df = build_alm(bnm_loan, rv)
    log(f"alm_df rows: {len(alm_df):,}")

    # --- Stage 5 -----------------------------------------------------------
    log("STAGE 5: merge DISPAY into ALM")
    if not dispay_df.empty and not alm_df.empty:
        dp_filt = dispay_df.copy()
        if 'prodcd' in dp_filt.columns:
            dp_filt = dp_filt[
                (dp_filt['prodcd'].astype(str).str[:2] == '34') |
                dp_filt['product'].isin([678,679,993,996])]
        dp_sel = dp_filt[[c for c in ['acctno','noteno','disburse','repaid']
                          if c in dp_filt.columns]]
        alm_df = alm_df.merge(dp_sel, on=KEY_COLS, how='left', suffixes=('','_dp'))
        for c in ['disburse','repaid']:
            dc = f"{c}_dp"
            if dc in alm_df.columns:
                if c in alm_df.columns:
                    alm_df[c] = alm_df[dc].where(alm_df[dc].notna(), alm_df[c])
                else:
                    alm_df[c] = alm_df[dc]
                alm_df = alm_df.drop(columns=dc)
    if not alm_df.empty:
        for c in ['disburse','repaid']:
            if c not in alm_df.columns:
                alm_df[c] = 0.0
            alm_df[c] = pd.to_numeric(alm_df[c], errors='coerce').fillna(0.0)
        alm_df['repayno'] = (alm_df['repaid'] > 0).astype(int)
        alm_df['disbno']  = (alm_df['disburse'] > 0).astype(int)
    log(f"alm_df rows after DISPAY merge: {len(alm_df):,}")

    # --- Stage 6 -----------------------------------------------------------
    log("STAGE 6: apply_prodesc")
    alm_df = apply_prodesc(alm_df)
    log(f"alm_df rows after prodesc: {len(alm_df):,}")

    # --- Stage 7 -----------------------------------------------------------
    log("STAGE 7: build_pbif (RDL2PBIF)")
    pbif_pl = build_pbif(rv['reptdate'])
    if pbif_pl is None or pbif_pl.is_empty():
        pbif_df = pd.DataFrame()
    else:
        pbif_df = pbif_pl.to_pandas()
        pbif_df.columns = [c.lower() for c in pbif_df.columns]
    log(f"pbif_df rows: {len(pbif_df):,}")

    if not pbif_df.empty:
        pbif_df = pbif_df.copy()
        pbif_df['prodesc'] = 'FACTORING'
        for c in ['repaid','disburse','balance']:
            if c not in pbif_df.columns:
                pbif_df[c] = 0.0
            pbif_df[c] = pd.to_numeric(pbif_df[c], errors='coerce').fillna(0.0)
        if 'noacct' not in pbif_df.columns:
            pbif_df['noacct'] = 0
        pbif_df['repayno'] = (pbif_df['repaid'] > 0).astype(int)
        pbif_df['disbno']  = (pbif_df['disburse'] > 0).astype(int)
        m = (pbif_df['balance'] > 0) & (pbif_df['noacct'] != 0)
        pbif_df.loc[m, 'noacct'] = 1

    # --- Stage 8 -----------------------------------------------------------
    log("STAGE 8: ALL LOANS summary")
    almnew_df = safe_concat([alm_df, pbif_df], ignore_index=True, sort=False)
    log(f"almnew_df rows: {len(almnew_df):,}")
    print_table(summarise(almnew_df, ['prodesc']), rw,
                f"ALL LOANS AS AT {mm_yr}", RPT)

    # --- Stage 9 -----------------------------------------------------------
    log("STAGE 9: ALM2 / COM3 / ALM2NEW")
    alm2_df = pd.DataFrame()
    com3_df = pd.DataFrame()
    if not alm_df.empty:
        sel = alm_df[alm_df['prodesc'].isin(
            ['OD RETAIL','OTHERS RETAIL','FLOOR STOCKING LOANS'])].copy()
        if not sel.empty:
            prod = pd.to_numeric(sel['product'], errors='coerce').fillna(0).astype(np.int64)
            fiss = sel.get('fisspurp', pd.Series('', index=sel.index)).fillna('').astype(str)
            new_pd = sel['prodesc'].copy()
            tycode = pd.Series(0, index=sel.index, dtype=np.int64)

            m = (sel['prodesc'] == 'OD RETAIL')
            sub_m = m & prod.isin(ODRTLA) & fiss.isin(ODFISS)
            new_pd = new_pd.where(~sub_m, 'PURCHASE OF RESIDENTIAL PROPERTY')
            sub_m = m & prod.isin(ODRTLB)
            new_pd = new_pd.where(~sub_m, 'SHARE MARGIN FINANCING')
            sub_m = m & ~(prod.isin(ODRTLA) & fiss.isin(ODFISS)) & ~prod.isin(ODRTLB)
            new_pd = new_pd.where(~sub_m, 'TOTAL COMMERCIAL RETAILS')
            tycode = tycode.where(~m, 1)

            m = (sel['prodesc'] == 'OTHERS RETAIL')
            sub_m = m & prod.isin(OTRTLA)
            new_pd = new_pd.where(~sub_m, 'PERSONAL LOAN')
            sub_m = m & prod.isin(OTRTLB)
            new_pd = new_pd.where(~sub_m, 'STAFF LOAN')
            sub_m = m & ~prod.isin(OTRTLA) & ~prod.isin(OTRTLB)
            new_pd = new_pd.where(~sub_m, 'TOTAL COMMERCIAL RETAILS')
            tycode = tycode.where(~m, 2)

            m = (sel['prodesc'] == 'FLOOR STOCKING LOANS')
            new_pd = new_pd.where(~m, 'TOTAL COMMERCIAL RETAILS')
            tycode = tycode.where(~m, 3)

            sel['prodesc'] = new_pd
            sel['tycode'] = tycode
            alm2_df = sel
            com3_df = sel.copy()

    pbif1_df = pd.DataFrame()
    if not pbif_df.empty:
        pbif1_df = pbif_df.copy()
        pbif1_df.loc[pbif1_df['prodesc'] == 'FACTORING', 'prodesc'] = 'TOTAL COMMERCIAL RETAILS'

    alm2new_src = safe_concat([alm2_df, pbif1_df], ignore_index=True, sort=False)
    log(f"alm2new_src rows: {len(alm2new_src):,}")

    if not alm2new_src.empty:
        alm_cr_keep = [c for c in ['acctno','noteno','prodesc','noacct']
                       if c in alm2new_src.columns]
        log("  writing MFRS.ALM_CR ...")
        write_sas7bdat(alm2new_src[alm_cr_keep], MFRS_ALM_CR_SAS, 'ALM_CR')
        log(f"  MFRS.ALM_CR written: {len(alm2new_src):,} rows")

    alm2crl_df = pd.DataFrame()
    if not alm2new_src.empty:
        sel = alm2new_src[alm2new_src['prodesc'] == 'TOTAL COMMERCIAL RETAILS'].copy()
        if not sel.empty:
            custcd = sel.get('custcd', pd.Series('', index=sel.index)).fillna('').astype(str)
            sel['prodesc'] = np.where(custcd.isin(INDIV_CUSTCDS),
                                      'COMMERCIAL RETAIL - IND',
                                      'COMMERCIAL RETAIL - NON IND')
            alm2crl_df = sel

    print_table(summarise(alm2new_src, ['prodesc']), rw,
                f"RETAILS LOANS AS AT {mm_yr}", RPT)
    print_table(summarise(alm2crl_df, ['prodesc']), rw,
                f"COMMERCIAL RETAIL LOANS AS AT {mm_yr}", RPT)

    # --- Stage 10 ----------------------------------------------------------
    log("STAGE 10: SME subsets")
    almsme_df = alm_df[_sme_mask(alm_df)].copy() if not alm_df.empty else pd.DataFrame()
    smefac_df = pd.DataFrame()
    if not pbif_df.empty:
        custcx = pbif_df.get('custcx', pd.Series('', index=pbif_df.index)).fillna('').astype(str)
        smefac_df = pbif_df[custcx.isin(SME_CUSTCDS)].copy()
    almsme_all_src = safe_concat([almsme_df, smefac_df], ignore_index=True, sort=False)

    if not almsme_all_src.empty:
        dbe_m  = _dbe_mask(almsme_all_src)
        fbe_m  = _fbe_mask(almsme_all_src)
        dn_m   = _dnbfi_mask(almsme_all_src)
        dbe_df   = almsme_all_src[dbe_m].copy()
        fbe_df   = almsme_all_src[~dbe_m & fbe_m].copy()
        dnbfi_df = almsme_all_src[~dbe_m & ~fbe_m & dn_m].copy()
    else:
        dbe_df = fbe_df = dnbfi_df = pd.DataFrame()

    print_table(summarise(almsme_all_src, ['prodesc']), rw,
                f"SME LOANS AS AT {mm_yr}", RPT)

    almsme2_df = dbe2_df = fbe2_df = dnbfi2_df = pd.DataFrame()
    if not alm2new_src.empty:
        custcd = alm2new_src.get('custcd', pd.Series('', index=alm2new_src.index)).fillna('').astype(str)
        custcx = alm2new_src.get('custcx', pd.Series('', index=alm2new_src.index)).fillna('').astype(str)
        dnbfi  = alm2new_src.get('dnbfisme', pd.Series('', index=alm2new_src.index)).fillna('').astype(str)
        dbe2_m   = custcd.isin(DBE_CUSTCDS) | custcx.isin(DBE_CUSTCDS)
        fbe2_m   = ~dbe2_m & custcd.isin(FBE_CUSTCDS)
        dnbfi2_m = ~dbe2_m & ~fbe2_m & dnbfi.isin(DNBFI_VALS)
        dbe2_df    = alm2new_src[dbe2_m].copy()
        fbe2_df    = alm2new_src[fbe2_m].copy()
        dnbfi2_df  = alm2new_src[dnbfi2_m].copy()
        almsme2_df = safe_concat([dbe2_df, fbe2_df, dnbfi2_df],
                                 ignore_index=True, sort=False)

    print_table(summarise(almsme2_df, ['prodesc']), rw,
                f"RETAILS SME LOANS AS AT {mm_yr}", RPT)
    print_table(summarise(dbe_df, ['prodesc']), rw,
                f"OF WHICH : SME DBE LOANS AS AT {mm_yr}", RPT)
    print_table(summarise(dbe2_df, ['prodesc']), rw,
                f"OF WHICH : RETAILS SME DBE LOANS AS AT {mm_yr}", RPT)
    print_table(summarise(dnbfi_df, ['prodesc']), rw,
                f"OF WHICH : SME DNBFI LOANS AS AT {mm_yr}", RPT)
    print_table(summarise(dnbfi2_df, ['prodesc']), rw,
                f"OF WHICH : RETAILS SME DNBFI LOANS AS AT {mm_yr}", RPT)
    print_table(summarise(fbe_df, ['prodesc']), rw,
                f"OF WHICH : SME FE LOANS AS AT {mm_yr}", RPT)
    print_table(summarise(fbe2_df, ['prodesc']), rw,
                f"OF WHICH : RETAILS SME FE LOANS AS AT {mm_yr}", RPT)

    # --- Stage 11 ----------------------------------------------------------
    log("STAGE 11: build_btrade")
    alm_bt_df, almloan_bt_df, mast_bt_df = build_btrade(rv)
    log(f"alm_bt_df rows: {len(alm_bt_df):,}  mast_bt_df rows: {len(mast_bt_df):,}")
    print_table(almloan_bt_df, rw, f"BANK TRADE AS AT {mm_yr}", RPT)

    almloan_sme_bt = pd.DataFrame()
    dbebt_df = dnbfibt_df = fbebt_df = pd.DataFrame()
    if not alm_bt_df.empty and not mast_bt_df.empty:
        almsme_bt_df  = alm_bt_df[_sme_mask(alm_bt_df)].copy()
        mastsme_bt_df = mast_bt_df[_sme_mask(mast_bt_df)].copy()
        bt_grp_key = [c for c in ['prodesc','custcd','dnbfisme']
                      if c in almsme_bt_df.columns]
        agg_v2 = [c for c in ['disburse','repaid','balance'] if c in almsme_bt_df.columns]
        almsme_bt_sum = (almsme_bt_df.groupby(bt_grp_key, as_index=False, dropna=False)[agg_v2].sum()
                         if bt_grp_key and agg_v2 else pd.DataFrame())
        mbt_grp = [c for c in ['prodesc','custcd','dnbfisme']
                   if c in mastsme_bt_df.columns]
        agg_v3 = [c for c in ['disbno','repayno','noacct'] if c in mastsme_bt_df.columns]
        mastsme_bt_sum = (mastsme_bt_df.groupby(mbt_grp, as_index=False, dropna=False)[agg_v3].sum()
                          if mbt_grp and agg_v3 else pd.DataFrame())
        if not almsme_bt_sum.empty and not mastsme_bt_sum.empty:
            mk = [c for c in bt_grp_key if c in mastsme_bt_sum.columns]
            almloan_sme_bt = almsme_bt_sum.merge(mastsme_bt_sum, on=mk, how='outer')
        elif not almsme_bt_sum.empty:
            almloan_sme_bt = almsme_bt_sum
        else:
            almloan_sme_bt = mastsme_bt_sum
        if not almloan_sme_bt.empty:
            custcd = almloan_sme_bt.get('custcd', pd.Series('', index=almloan_sme_bt.index)).fillna('').astype(str)
            dnbfi  = almloan_sme_bt.get('dnbfisme', pd.Series('', index=almloan_sme_bt.index)).fillna('').astype(str)
            dbebt_df   = almloan_sme_bt[custcd.isin(DBE_CUSTCDS)].copy()
            dnbfibt_df = almloan_sme_bt[dnbfi.isin(DNBFI_VALS)].copy()
            fbebt_df   = almloan_sme_bt[custcd.isin(FBE_CUSTCDS)].copy()

    print_table(summarise(almloan_sme_bt, ['prodesc']), rw,
                f"SME BANK TRADE AS AT {mm_yr}", RPT)
    for df_in, t1 in [
        (dbebt_df,   f"OF WHICH : SME DBE BANK TRADE AS AT {rv['reptmon']}/{rv['reptyear']}"),
        (dnbfibt_df, f"OF WHICH : SME DNBFI BANK TRADE AS AT {rv['reptmon']}/{rv['reptyear']}"),
        (fbebt_df,   f"OF WHICH : SME FE BANK TRADE AS AT {rv['reptmon']}/{rv['reptyear']}"),
    ]:
        print_table(summarise(df_in, ['prodesc']), rw, t1, RPT)

    # --- Stage 12: sector breakdowns ---------------------------------------
    log("STAGE 12: sector breakdowns")

    def _add_sector_labels(frame):
        if frame is None or frame.empty:
            return frame
        frame = frame.copy()
        # ensure noacct flows through — pull from alm_df if missing
        if 'noacct' not in frame.columns and not alm_df.empty:
            keep = [c for c in ['acctno','noteno','noacct'] if c in alm_df.columns]
            if len(keep) == 3:
                frame = frame.merge(alm_df[keep], on=KEY_COLS, how='left',
                                    suffixes=('', '_y'))
                if 'noacct_y' in frame.columns:
                    frame['noacct'] = frame['noacct_y'].fillna(0).astype(int)
                    frame = frame.drop(columns='noacct_y')
        sect = frame.get('sectorcd', pd.Series('', index=frame.index))
        frame['sectype']  = sect.map(format_fisstype)
        frame['secgroup'] = sect.map(format_fissgroup)
        return frame

    pbifsec_df = _add_sector_labels(pbif_df)

    print_tabulate_sector(
        pbifsec_df, rw, 'REPORT ID : EIMBNM01',
        f"OUTSTANDING FACTORING LOANS BY SECTORS AND SUB-SECTORS AS AT"
        f" {rv['reptmon']}{rv['ryear']}"
    )

    comsec_df = pd.DataFrame()
    if not alm2_df.empty:
        comsec_df = _add_sector_labels(
            alm2_df[alm2_df['prodesc'] == 'TOTAL COMMERCIAL RETAILS'])

    print_tabulate_sector(
        comsec_df, rw, 'REPORT ID : EIMBNM01',
        f"OUTSTANDING M&I COMMERCIAL RETAIL LOANS BY SECTORS AND"
        f" SUB-SECTORS AS AT {rv['reptmon']}{rv['ryear']}"
    )

    mast1_df = pd.DataFrame()
    almbt_sec_df = pd.DataFrame()
    if not mast_bt_df.empty:
        mast1_df = _add_sector_labels(
            mast_bt_df[mast_bt_df['prodesc'] == 'BILLS RETAIL'])
    if not alm_bt_df.empty:
        almbt_sec_df = _add_sector_labels(
            alm_bt_df[alm_bt_df['prodesc'] == 'BILLS RETAIL'])

    rebsec_df = pd.DataFrame()
    if not mast1_df.empty:
        sg_key = [c for c in ['secgroup','sectype'] if c in mast1_df.columns]
        if 'noacct' in mast1_df.columns:
            mast1_sum = mast1_df.groupby(sg_key, as_index=False, dropna=False)['noacct'].sum()
        else:
            mast1_sum = mast1_df
        if not almbt_sec_df.empty and 'balance' in almbt_sec_df.columns:
            sg_key2 = [c for c in ['secgroup','sectype'] if c in almbt_sec_df.columns]
            almbt_sec_sum = (almbt_sec_df.groupby(sg_key2, as_index=False, dropna=False)['balance']
                             .sum())
            rebsec_df = mast1_sum.merge(almbt_sec_sum, on=sg_key, how='left')
        else:
            rebsec_df = mast1_sum

    print_tabulate_sector(
        rebsec_df, rw, 'REPORT ID : EIMBNM01',
        f"OUTSTANDING RETAIL BILLS BY SECTORS AND SUB-SECTORS"
        f" AS AT {rv['reptmon']}{rv['ryear']}"
    )

    combysec_df = safe_concat([pbifsec_df, rebsec_df, comsec_df],
                              ignore_index=True, sort=False)
    print_tabulate_sector(
        combysec_df, rw, 'REPORT ID : EIMBNM01',
        f"TOTAL COMMERCIAL RETAIL LOANS BY SECTORS AND SUB-SECTORS"
        f" AS AT {rv['reptmon']}{rv['ryear']}"
    )

    # --- Stage 13 ----------------------------------------------------------
    log("STAGE 13: Total Commercial Retail by product")
    com1_df = pbifsec_df.copy() if not pbifsec_df.empty else pd.DataFrame()
    if not com1_df.empty:
        com1_df['type'] = 'FIXED LOANS'
    com2_df = rebsec_df.copy() if not rebsec_df.empty else pd.DataFrame()
    if not com2_df.empty:
        com2_df['type'] = 'BANKTRADE'

    com3_final_df = pd.DataFrame()
    if not com3_df.empty:
        sel = com3_df[com3_df['prodesc'] == 'TOTAL COMMERCIAL RETAILS'].copy()
        if not sel.empty:
            ty = pd.to_numeric(sel.get('tycode', 0), errors='coerce').fillna(0).astype(int)
            sel['type'] = np.select([ty == 1, ty == 2, ty == 3],
                                    ['OD', 'FIXED LOANS', 'FLOOR STOCKING'],
                                    default='')
            com3_final_df = sel

    combyprod_df = safe_concat([com1_df, com2_df, com3_final_df],
                               ignore_index=True, sort=False)
    print_tabulate_product(
        combyprod_df, rw, 'REPORT ID : EIMBNM01',
        f"TOTAL COMMERCIAL RETAIL LOANS BY TYPE OF PRODUCT"
        f" AS AT {rv['reptmon']}{rv['ryear']}"
    )

    # --- Stage 14 ----------------------------------------------------------
    log("STAGE 14: flush report")
    rw.flush(REPORT_TXT)
    log(f"Written: {REPORT_TXT}")
    log("EIMBNM01: Processing complete.")


if __name__ == '__main__':
    main()
