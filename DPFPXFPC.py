#!/usr/bin/env python3
"""
Program Name: EIMBNM01.py
Purpose: Public Bank Berhad - Monthly Loan Summary Reports (M&I)
         Generates multiple PROC PRINT reports covering:
         - All Loans (disbursement, repayment, outstanding) incl. Factoring
         - Retail Loans breakdown (Personal, Staff, OD, Corp, Factoring)
         - Commercial Retail Loans (individual vs non-individual)
         - SME Loans (DBE, FBE, DNBFI sub-categories)
         - Bank Trade (Bills) summary
         - Sector/sub-sector breakdown tabulations for:
             Factoring, M&I Commercial Retail, Retail Bills, Combined
         - Total Commercial Retail by product type

ESMR: 06-1485
ESMR: 2009-0744
ESMR: 2013-813 (JKA)
ESMR: 2015-606 (TBC)
ESMR: 2016-678 (NSA)

Dependencies:
  %INC PGM(PBBLNFMT):
    The SAS source includes PBBLNFMT as suite-wide boilerplate.  No PBBLNFMT
    converted function (format_lnprod, format_lndenom, etc.) is directly
    called anywhere in this program.

  %INC PGM(RDL2PBIF):
    RDL2PBIF.py defines:
      - build_pbif()        -> builds the PBIF factoring dataset
      - format_fisstype()   -> sector sub-sector label (SAS $FISSTYPE)
      - format_fissgroup()  -> sector group label    (SAS $FISSGROUP)
    All three are imported below.
"""

import os
from datetime import date, timedelta
from typing import Optional

import pandas as pd
import pyreadstat
import saspy

# -----------------------------------------------------------------------------
# %INC PGM(PBBLNFMT);
# %INC PGM(RDL2PBIF);
# -----------------------------------------------------------------------------
import PBBLNFMT          # noqa: F401  (suite-wide boilerplate; no direct calls)
from RDL2PBIF import (  # noqa: F401
    build_pbif,
    format_fisstype,
    format_fissgroup,
)

# =============================================================================
# PATH CONFIGURATION
# =============================================================================

BNM_LOAN_PREFIX       = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMBNM01/loan{reptmon}{nowk}.sas7bdat"
BNM_LNWOF_PREFIX      = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMBNM01/lnwof{reptmon}{nowk}.sas7bdat"
BNM_LNWOD_PREFIX      = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMBNM01/lnwod{reptmon}{nowk}.sas7bdat"
SASD_LOAN_PREFIX      = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMBNM01/loan{reptmon}.sas7bdat"
DISPAY_PREFIX         = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMBNM01/dispaymth{reptmon}.sas7bdat"
LOAN_LNCOMM_SAS       = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/enrh_ln_comm_m{reptmon}.sas7bdat"
FEE_LNFEE_PREFIX      = "/stgsrcsys/host/uat/lnfee{reptmon}{nowk}.sas7bdat"
BTBNM_BTRAD_PREFIX    = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMBNM01/btrad{reptmon}{nowk}{reptyear}.sas7bdat"

OUTPUT_DIR            = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMBNM01"
REPORT_TXT            = os.path.join(OUTPUT_DIR, "eimbnm01_report.txt")
MFRS_DIR              = os.path.join(OUTPUT_DIR, "mfrs")
MFRS_MAST_BR_SAS      = os.path.join(MFRS_DIR, "mast_br.sas7bdat")
MFRS_ALM_CR_SAS       = os.path.join(MFRS_DIR, "alm_cr.sas7bdat")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(MFRS_DIR, exist_ok=True)

# =============================================================================
# SAS SESSION (saspy) — used to write .sas7bdat outputs
# =============================================================================

SAS_SESSION = saspy.SASsession(cfgname="default")   # adjust cfgname as required


def write_sas7bdat(df: pd.DataFrame, path: str, table_name: Optional[str] = None):
    """Write a pandas DataFrame to a .sas7bdat file using saspy."""
    if df is None or df.empty:
        sas_code = f"data _null_; file '{path}'; put; run;"
        SAS_SESSION.submit(sas_code)
        return

    if table_name is None:
        table_name = os.path.splitext(os.path.basename(path))[0].upper()

    SAS_SESSION.df2sd(df, table=table_name, libref="WORK")

    sas_code = f"""
    data "{path}";
        set WORK.{table_name};
    run;
    """
    SAS_SESSION.submit(sas_code)


# =============================================================================
# PRODUCT / CUSTOMER CODE MACRO CONSTANTS
# =============================================================================

ODCORP = {50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 31}

ODRTLA = {68, 69, 85, 86, 87, 88, 89, 90, 91, 100, 101, 102, 103, 106, 108, 109,
          110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123,
          124, 125, 135, 137, 138, 150, 151, 152, 153, 154, 155, 156, 157, 158,
          159, 170, 174, 175, 176, 179, 180, 181, 189, 191, 192, 193, 194, 195,
          196, 197, 198, 190, 30, 34, 81, 82, 83, 84, 77, 78}

ODRTLB = {177, 178, 34, 133, 134, 77, 78}

ODFISS = {'0311', '0312', '0313', '0314', '0315', '0316'}

OTRTLA = {303, 306, 307, 325, 330, 340, 354, 355, 391, 610, 611, 308, 311, 367, 313, 369}

OTRTLB = {4, 5, 6, 7, 15, 20, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34,
          60, 61, 62, 63, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79}

FLCORP = {180, 181, 182, 183, 193, 800, 801, 802, 803, 804, 818,
          900, 901, 902, 903, 904, 905, 906, 907, 908, 912, 922,
          184, 909, 910, 914, 915, 916, 918, 919, 920, 925, 950, 951,
          631, 632, 633, 634, 635, 636, 637, 639, 640, 641, 816, 817,
          805, 806, 807, 808, 809, 810, 811, 812, 813, 814, 913, 917}

HLCORP = {638, 911}

DBE_CUSTCDS   = {'41', '42', '43', '44', '46', '47', '48', '49', '51', '52', '53', '54'}
FBE_CUSTCDS   = {'87', '88', '89'}
DNBFI_VALS    = {'1', '2', '3'}
SME_CUSTCDS   = DBE_CUSTCDS | FBE_CUSTCDS
INDIV_CUSTCDS = {'77', '78', '95', '96'}

# =============================================================================
# ASA CARRIAGE CONTROL / REPORT WRITER
# =============================================================================

PAGE_LENGTH = 60


class ReportWriter:
    """Accumulates ASA carriage-control report lines; flushes to file."""

    def __init__(self):
        self.lines = []
        self.line_cnt = PAGE_LENGTH + 1

    def _page_eject(self):
        self.line_cnt = 0

    def write_titles(self, title1: str, title2: str = '', title3: str = ''):
        self._page_eject()
        self.lines.append('1' + title1)
        if title2:
            self.lines.append(' ' + title2)
        if title3:
            self.lines.append(' ' + title3)
        self.lines.append(' ')
        self.line_cnt += (3 if title3 else 2) + 1

    def write_line(self, text: str = '', asa: str = ' '):
        self.lines.append(asa + text)
        self.line_cnt += 1

    def blank(self):
        self.write_line()

    def flush(self, filepath: str):
        with open(filepath, 'w', encoding='utf-8') as fh:
            for ln in self.lines:
                fh.write(ln + '\n')


# =============================================================================
# DATE / IO / UTILITY HELPERS
# =============================================================================

def read_sas7bdat(path: str) -> pd.DataFrame:
    """Read a SAS7BDAT file with all column names lowercased."""
    if not os.path.exists(path):
        return pd.DataFrame()
    df, _ = pyreadstat.read_sas7bdat(path)
    df.columns = [c.lower() for c in df.columns]
    return df


def sas_date_to_pydate(val) -> Optional[date]:
    if val is None or (isinstance(val, float) and val != val):
        return None
    if isinstance(val, (int, float)):
        return date(1960, 1, 1) + timedelta(days=int(val))
    if isinstance(val, date):
        return val
    return None


def pydate_to_sasdate(d: date) -> int:
    return (d - date(1960, 1, 1)).days


def coalesce_f(val, default: float = 0.0) -> float:
    if val is None or (isinstance(val, float) and val != val):
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def coalesce_s(val, default: str = '') -> str:
    return str(val).strip() if val is not None else default


# =============================================================================
# REPORT DATE VARIABLES  (no REPTDATE column — pure date arithmetic)
# =============================================================================

def get_report_vars() -> dict:
    """
    reptdate = (first day of current month) - 1 day = last day of previous month
    """
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
    if mm1 == 0:
        mm1 = 12
    mm2 = mm - 1
    if mm2 == 0:
        mm2 = 12

    sdate = date(reptdate.year, mm, sdd)
    reptmon  = str(mm).zfill(2)
    reptmon1 = str(mm1).zfill(2)
    reptmon2 = str(mm2).zfill(2)
    reptyear = reptdate.strftime('%y')
    ryear    = str(reptdate.year)
    reptday  = str(day).zfill(2)
    rdate    = reptdate.strftime('%d/%m/%y')
    sdate_s  = sdate.strftime('%d/%m/%y')
    mdate_int = pydate_to_sasdate(reptdate)

    return {
        'reptdate':  reptdate,
        'sdate':     sdate,
        'wk':        wk, 'wk1': wk1, 'wk2': wk2, 'wk3': wk3,
        'mm':        mm, 'mm1': mm1, 'mm2': mm2, 'sdd': sdd,
        'reptmon':   reptmon,
        'reptmon1':  reptmon1,
        'reptmon2':  reptmon2,
        'reptyear':  reptyear,
        'ryear':     ryear,
        'reptday':   reptday,
        'rdate':     rdate,
        'sdate_s':   sdate_s,
        'nowk':      wk,
        'mdate_int': mdate_int,
    }


# =============================================================================
# BUILD BASE LOAN DATASET
# =============================================================================

def build_loan_dataset(rv: dict) -> pd.DataFrame:
    mm, mm2, wk = rv['reptmon'], rv['reptmon2'], rv['nowk']

    dloan     = read_sas7bdat(SASD_LOAN_PREFIX.format(reptmon=mm))
    mloan     = read_sas7bdat(BNM_LOAN_PREFIX.format(reptmon=mm, nowk=wk))
    lnwof     = read_sas7bdat(BNM_LNWOF_PREFIX.format(reptmon=mm, nowk=wk))
    lnwod     = read_sas7bdat(BNM_LNWOD_PREFIX.format(reptmon=mm, nowk=wk))
    plnwof    = read_sas7bdat(BNM_LNWOF_PREFIX.format(reptmon=mm2, nowk=wk))
    plnwod    = read_sas7bdat(BNM_LNWOD_PREFIX.format(reptmon=mm2, nowk=wk))
    loan_prev = read_sas7bdat(BNM_LOAN_PREFIX.format(reptmon=mm2, nowk=wk))

    key = ['acctno', 'noteno']

    if not dloan.empty and not mloan.empty:
        marker = mloan[key].drop_duplicates().assign(_b=True)
        loandm = dloan.merge(marker, on=key, how='left')
        loandm = loandm[loandm['_b'].isna()].drop(columns='_b')
    elif not dloan.empty:
        loandm = dloan
    else:
        loandm = pd.DataFrame()

    frames = [f for f in [plnwof, plnwod, loandm, loan_prev, mloan, lnwof, lnwod]
              if not f.empty]
    if not frames:
        return pd.DataFrame()

    base = frames[0]
    for f in frames[1:]:
        non_key = [c for c in f.columns if c not in key]
        merged = base.merge(f[key + non_key], on=key, how='outer',
                            suffixes=('', '_r'))
        for col in non_key:
            rc = f"{col}_r"
            if rc in merged.columns:
                merged[col] = merged[rc].combine_first(
                    merged[col] if col in merged.columns else pd.Series(index=merged.index)
                )
                merged = merged.drop(columns=rc)
        base = merged

    return base


# =============================================================================
# BUILD DISPAY
# =============================================================================

def build_dispay(rv: dict, loan_df: pd.DataFrame) -> pd.DataFrame:
    path = DISPAY_PREFIX.format(reptmon=rv['reptmon'])
    raw = read_sas7bdat(path)
    if raw.empty or loan_df.empty:
        return pd.DataFrame()

    dispay = raw.copy()
    dispay['disburse'] = dispay['disburse'].round(2)
    dispay['repaid']   = dispay['repaid'].round(2)
    dispay = dispay[(dispay['disburse'] > 0) | (dispay['repaid'] > 0)]

    return loan_df.merge(dispay, on=['acctno', 'noteno'], how='inner',
                         suffixes=('', '_dp'))


# =============================================================================
# BUILD CL_FEE AND AUGMENT LOAN
# =============================================================================

def build_cl_fee(rv: dict) -> pd.DataFrame:
    path = FEE_LNFEE_PREFIX.format(reptmon=rv['reptmon'], nowk=rv['nowk'])
    raw = read_sas7bdat(path)
    if raw.empty:
        return pd.DataFrame(columns=['acctno', 'noteno', 'duetotal'])

    keep = [c for c in ['acctno', 'noteno', 'duetotal', 'feeplan'] if c in raw.columns]
    fee = raw[keep].copy()
    fee = fee[(fee['feeplan'] == 'CL') & (fee['duetotal'] > 0)]
    if fee.empty:
        return pd.DataFrame(columns=['acctno', 'noteno', 'duetotal'])
    return fee.groupby(['acctno', 'noteno'], as_index=False)['duetotal'].sum()


def merge_loan_cl_fee(loan_df: pd.DataFrame, cl_fee: pd.DataFrame) -> pd.DataFrame:
    if loan_df.empty:
        return loan_df
    if cl_fee.empty:
        loan_df = loan_df.copy()
        if 'clfee' not in loan_df.columns:
            loan_df['clfee'] = 0.0
        return loan_df

    merged = loan_df.merge(cl_fee, on=['acctno', 'noteno'], how='left',
                           suffixes=('', '_fee'))
    if 'duetotal_fee' in merged.columns:
        merged['duetotal'] = merged['duetotal_fee'].combine_first(
            merged['duetotal'] if 'duetotal' in merged.columns else pd.Series(index=merged.index)
        )
        merged = merged.drop(columns='duetotal_fee')

    if 'forate' in merged.columns:
        merged['clfee'] = (merged['duetotal'].fillna(0.0) *
                           merged['forate'].fillna(0.0))
    else:
        merged['clfee'] = 0.0

    for col in ['_type_', '_freq_']:
        if col in merged.columns:
            merged = merged.drop(columns=col)
    return merged


# =============================================================================
# BUILD ALM
# =============================================================================

def build_alm(loan_raw: pd.DataFrame, rv: dict) -> pd.DataFrame:
    if loan_raw.empty:
        return pd.DataFrame()

    # LNCOMM file — resolved via the {reptmon} placeholder
    lncomm_path = LOAN_LNCOMM_SAS.format(reptmon=rv['reptmon'])
    lncomm = read_sas7bdat(lncomm_path)

    renames = {}
    if 'balance' in loan_raw.columns:
        renames['balance'] = 'oribal'
    if 'bal_aft_eir' in loan_raw.columns:
        renames['bal_aft_eir'] = 'balance'
    if renames:
        loan_raw = loan_raw.rename(columns=renames)

    # Merge LNCOMM by ACCTNO COMMNO
    if not lncomm.empty and 'commno' in loan_raw.columns:
        lncomm_sel = lncomm[[c for c in ['acctno', 'commno', 'cusedamt']
                             if c in lncomm.columns]]
        merged = loan_raw.merge(lncomm_sel, on=['acctno', 'commno'],
                                how='left', suffixes=('', '_lc'))
        for col in lncomm_sel.columns:
            lc = f"{col}_lc"
            if lc in merged.columns:
                merged[col] = merged[lc].combine_first(
                    merged[col] if col in merged.columns else pd.Series(index=merged.index)
                )
                merged = merged.drop(columns=lc)
    else:
        merged = loan_raw

    keep = ['acctno', 'noteno', 'fisspurp', 'product', 'noteterm', 'earnterm',
            'balance', 'paidind', 'apprdate', 'apprlim2', 'prodcd', 'custcd',
            'amtind', 'sectorcd', 'acctype', 'branch', 'cjfee', 'oribal',
            'dnbfisme', 'noacct', 'commno', 'cusedamt', 'rleasamt', 'clfee',
            'eir_adj', 'retailid']
    avail = [c for c in keep if c in merged.columns]
    merged = merged[avail].copy()

    alm_rows, almbt_rows = [], []
    for _, row in merged.iterrows():
        paidind  = coalesce_s(row.get('paidind'))
        eir_adj  = row.get('eir_adj')
        oribal   = coalesce_f(row.get('oribal'))
        prodcd   = coalesce_s(row.get('prodcd'))
        acctype  = coalesce_s(row.get('acctype'))
        acctno   = int(row.get('acctno') or 0)
        noteno   = int(row.get('noteno') or 0)
        product  = int(row.get('product') or 0)
        commno   = int(row.get('commno') or 0)
        cusedamt = coalesce_f(row.get('cusedamt'))
        rleasamt = coalesce_f(row.get('rleasamt'))
        cjfee    = coalesce_f(row.get('cjfee'))
        clfee    = coalesce_f(row.get('clfee'))

        eir_adj_set = (eir_adj is not None and
                       not (isinstance(eir_adj, float) and eir_adj != eir_adj))
        if paidind in ('P', 'C') and not eir_adj_set:
            continue

        xind = ' '
        if oribal == -0.0 and str(oribal) in ('-0.0', '-0.00'):
            xind = 'Y'
        balx = round(oribal, 2)
        if balx in (0.0, -0.0):
            xind = 'Y'
        if xind == 'Y':
            continue

        if not (prodcd[:2] == '34' or prodcd == '54120'):
            continue

        noacct = int(row.get('noacct') or 0)
        if acctype == 'LN':
            eligible = False
            if (rleasamt != 0.0 and paidind not in ('P', 'C') and
                    oribal > 0 and cjfee != oribal):
                eligible = True
            elif (rleasamt == 0.0 and paidind not in ('P', 'C') and
                  oribal > 0 and 600 <= product <= 699):
                eligible = True
            elif (rleasamt == 0.0 and paidind not in ('P', 'C') and
                  oribal > 0 and commno > 0 and cusedamt > 0):
                eligible = True
            if not eligible:
                noacct = 0
            if rleasamt != 0 and oribal == clfee:
                noacct = 0

        if (paidind not in ('P', 'C') and cjfee != oribal and noacct != 0 and
                round(oribal, 2) not in (0.0, -0.0)):
            noacct = 1

        new_row = row.to_dict()
        new_row['noacct'] = noacct

        if ((2500000000 <= acctno <= 2599999999 and 40000 <= noteno <= 49999) or
                product == 321):
            almbt_rows.append(new_row)
        else:
            alm_rows.append(new_row)

    alm_df = pd.DataFrame(alm_rows)
    almbt_df = pd.DataFrame(almbt_rows)

    if not alm_df.empty and 'commno' in alm_df.columns:
        alm_df = alm_df.sort_values(['acctno', 'commno']).reset_index(drop=True)
        prev_ac = prev_cm = None
        unq = 0
        for i, row in alm_df.iterrows():
            ac, cm = row['acctno'], row.get('commno')
            pd_ = coalesce_s(row.get('prodcd'))
            if ac != prev_ac or cm != prev_cm:
                unq = 0
            if pd_ in ('34170', '34190', '34690'):
                unq += (row.get('noacct') or 0)
                if unq > 1:
                    alm_df.at[i, 'noacct'] = 0
            prev_ac, prev_cm = ac, cm

    if not almbt_df.empty:
        almbt_df = almbt_df.sort_values('acctno').reset_index(drop=True)
        prev_ac = None
        for i, row in almbt_df.iterrows():
            ac = row['acctno']
            almbt_df.at[i, 'noacct'] = 1 if ac != prev_ac else 0
            prev_ac = ac

    if not almbt_df.empty:
        alm_df = pd.concat([alm_df, almbt_df], ignore_index=True)

    return alm_df


# =============================================================================
# APPLY PRODESC
# =============================================================================

def apply_prodesc(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    prodescs = []
    for _, row in df.iterrows():
        product = int(row.get('product') or 0)
        acctype = coalesce_s(row.get('acctype'))
        prodcd  = coalesce_s(row.get('prodcd'))

        prodesc = ''
        if (acctype == 'LN' and prodcd == '34111') or product in {678, 679, 993, 996}:
            prodesc = 'HIRE PURCHASE'
        elif acctype == 'LN' and prodcd == '34120':
            prodesc = 'RETAIL HOUSING LOANS'
            if product in HLCORP:
                prodesc = 'CORP. BANKING HOUSING LOANS'
        elif acctype == 'OD' and prodcd in ('34180', '34240') and product in ODCORP:
            prodesc = 'OD CORPORATE'
        elif acctype == 'OD' and prodcd in ('34180', '34240') and product not in ODCORP:
            prodesc = 'OD RETAIL'
        elif (acctype == 'LN' and prodcd not in ('34111', '34120', 'N', 'M') and
              product in FLCORP):
            prodesc = 'CORP. BANKING LOANS'
        elif (acctype == 'LN' and prodcd not in ('34111', '34120', 'N', 'M') and
              product not in FLCORP):
            prodesc = 'OTHERS RETAIL'
        if acctype == 'LN' and prodcd == '34170':
            prodesc = 'FLOOR STOCKING LOANS'
        prodescs.append(prodesc)
    df['prodesc'] = prodescs
    return df


# =============================================================================
# BUILD BTRADE
# =============================================================================

def build_btrade(rv: dict):
    mm, wk = rv['reptmon'], rv['nowk']
    path = BTBNM_BTRAD_PREFIX.format(reptmon=mm, nowk=wk, reptyear=rv['reptyear'])

    btrad_raw = read_sas7bdat(path)
    if btrad_raw.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    btrad_raw = btrad_raw[
        (btrad_raw['dirctind'] == 'D') &
        (btrad_raw['custcd'].notna()) &
        (btrad_raw['custcd'] != ' ')
    ]

    if 'apprlimt' in btrad_raw.columns:
        btrad1 = btrad_raw.sort_values(['acctno', 'apprlimt'],
                                       ascending=[True, False])
    else:
        btrad1 = btrad_raw.sort_values('acctno')

    grp_key1 = [c for c in ['acctno', 'custcd', 'retailid', 'sectorcd', 'dnbfisme']
                if c in btrad1.columns]
    agg_v1 = [c for c in ['disburse', 'repaid'] if c in btrad1.columns]
    btrad2 = (btrad1.groupby(grp_key1, as_index=False)[agg_v1].sum()
              if grp_key1 and agg_v1 else pd.DataFrame())

    grp_key2 = [c for c in ['acctno', 'custcd', 'retailid', 'sectorcd']
                if c in btrad1.columns]
    btrad1_bal = pd.DataFrame()
    if 'apprlimt' in btrad1.columns and 'balance' in btrad1.columns and grp_key2:
        btrad1_bal = (btrad1[btrad1['apprlimt'] > 0]
                      .groupby(grp_key2, as_index=False)['balance'].sum())

    merge_key = [c for c in ['acctno', 'custcd', 'retailid', 'sectorcd']
                 if c in btrad2.columns]
    if not btrad1_bal.empty and not btrad2.empty:
        mast_m = btrad2.merge(btrad1_bal, on=merge_key, how='left',
                              suffixes=('', '_bal'))
        if 'balance_bal' in mast_m.columns:
            mast_m['balance'] = mast_m['balance_bal'].combine_first(
                mast_m['balance'] if 'balance' in mast_m.columns else pd.Series(index=mast_m.index)
            )
            mast_m = mast_m.drop(columns='balance_bal')
    else:
        mast_m = btrad2

    mast_m = mast_m.copy()
    mast_m['disbno']  = (mast_m['disburse'] > 0).astype(int)
    mast_m['repayno'] = (mast_m['repaid'] > 0).astype(int)
    if 'balance' in mast_m.columns:
        mast_m['noacct'] = (
            mast_m['balance'].round(2).notna() &
            (mast_m['balance'].round(2) != 0) &
            (mast_m['acctno'] != 0)
        ).astype(int)

    ovc_df = mast_m[[c for c in ['acctno', 'retailid'] if c in mast_m.columns]].copy()
    mast_keep = [c for c in ['acctno', 'custcd', 'balance', 'retailid', 'disbno',
                             'repayno', 'noacct', 'sectorcd', 'dnbfisme']
                 if c in mast_m.columns]
    mast_df = mast_m[mast_keep].copy()

    alm_bt_raw = read_sas7bdat(path)
    if alm_bt_raw.empty:
        return pd.DataFrame(), pd.DataFrame(), mast_df
    alm_bt_raw = alm_bt_raw[
        alm_bt_raw['prodcd'].astype(str).str[:2] == '34'
    ]

    bt_keep = [c for c in ['acctno', 'subacct', 'fisspurp', 'product', 'noteterm',
                           'balance', 'apprlim2', 'prodcd', 'custcd', 'amtind',
                           'transref', 'sectorcd', 'disburse', 'repaid',
                           'dnbfisme', 'retailid']
               if c in alm_bt_raw.columns]
    alm_bt = alm_bt_raw[bt_keep].copy()

    if not ovc_df.empty:
        alm_bt = alm_bt.merge(ovc_df, on='acctno', how='inner',
                              suffixes=('', '_ovc'))
        if 'retailid_ovc' in alm_bt.columns:
            alm_bt['retailid'] = alm_bt['retailid_ovc'].combine_first(
                alm_bt['retailid'] if 'retailid' in alm_bt.columns else pd.Series(index=alm_bt.index)
            )
            alm_bt = alm_bt.drop(columns='retailid_ovc')

    bt_grp = [c for c in ['acctno', 'transref', 'custcd', 'fisspurp', 'sectorcd']
              if c in alm_bt.columns]
    if bt_grp and 'balance' in alm_bt.columns:
        almx = (alm_bt.groupby(bt_grp, as_index=False)['balance'].sum()
                .rename(columns={'balance': 'balance_sum'}))
        alm_bt = alm_bt.sort_values(bt_grp).drop_duplicates(subset=bt_grp, keep='first')
        alm_bt = alm_bt.drop(columns='balance').merge(almx, on=bt_grp, how='left')
        alm_bt = alm_bt.rename(columns={'balance_sum': 'balance'})

    for c in ['disburse', 'repaid', 'balance']:
        if c in alm_bt.columns:
            alm_bt[c] = alm_bt[c].astype(float).fillna(0.0)
        else:
            alm_bt[c] = 0.0

    alm_bt['prodesc'] = alm_bt['retailid'].apply(
        lambda x: 'BILLS CORPORATE' if coalesce_s(x) == 'C' else 'BILLS RETAIL'
    )

    mast_rows = mast_df.to_dict('records')
    mast_br_list = []
    for row in mast_rows:
        retailid = coalesce_s(row.get('retailid'))
        row['prodesc'] = 'BILLS CORPORATE' if retailid == 'C' else 'BILLS RETAIL'
        try:
            acctno = int(str(row.get('acctno') or 0))
        except (ValueError, TypeError):
            acctno = 0
        row['acctno'] = acctno
        mast_br_list.append({'acctno': acctno,
                             'prodesc': row['prodesc'],
                             'noacct': row.get('noacct', 0)})
    mast_df = pd.DataFrame(mast_rows)

    if mast_br_list:
        write_sas7bdat(pd.DataFrame(mast_br_list), MFRS_MAST_BR_SAS, 'MAST_BR')

    agg_v = [c for c in ['disburse', 'repaid', 'balance'] if c in alm_bt.columns]
    almloan_bt = (alm_bt.groupby('prodesc', as_index=False)[agg_v].sum()
                  if agg_v else pd.DataFrame())

    mast_agg_v = [c for c in ['disbno', 'repayno', 'noacct'] if c in mast_df.columns]
    if mast_agg_v and not mast_df.empty:
        mastloan = mast_df.groupby('prodesc', as_index=False)[mast_agg_v].sum()
        if not almloan_bt.empty:
            almloan_bt = almloan_bt.merge(mastloan, on='prodesc', how='left')
        else:
            almloan_bt = mastloan

    return alm_bt, almloan_bt, mast_df


# =============================================================================
# REPORT PRINT HELPERS
# =============================================================================

NUM_COLS = ['disburse', 'repaid', 'disbno', 'repayno', 'balance', 'noacct']


def fmt_num(val, decimals: int = 2) -> str:
    if val is None or (isinstance(val, float) and val != val):
        return ' ' * (16 if decimals == 2 else 10)
    if decimals == 2:
        return f"{float(val):16.2f}"
    return f"{float(val):10.0f}"


def print_table(df: pd.DataFrame, rw: ReportWriter, title1: str, title2: str = ''):
    if df is None or df.empty:
        return

    rw.write_titles(title1, title2)

    hdr = (f"{'PRODESC':<35}"
           f"{'DISBURSE':>16}"
           f"{'REPAID':>16}"
           f"{'DISBNO':>10}"
           f"{'REPAYNO':>10}"
           f"{'BALANCE':>16}"
           f"{'NOACCT':>10}")
    rw.write_line(' ' + hdr)
    rw.write_line(' ' + '-' * len(hdr))

    tot = {c: 0.0 for c in NUM_COLS}
    for _, row in df.sort_values('prodesc').iterrows():
        prodesc = coalesce_s(row.get('prodesc'))[:35]
        line = (f"{prodesc:<35}"
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
    sum_line = (f"{'SUM':<35}"
                f"{fmt_num(tot['disburse'])}"
                f"{fmt_num(tot['repaid'])}"
                f"{fmt_num(tot['disbno'], 0)}"
                f"{fmt_num(tot['repayno'], 0)}"
                f"{fmt_num(tot['balance'])}"
                f"{fmt_num(tot['noacct'], 0)}")
    rw.write_line(' ' + sum_line)
    rw.blank()


def print_tabulate_sector(df: pd.DataFrame, rw: ReportWriter,
                          title1: str, title2: str):
    if df is None or df.empty:
        return
    rw.write_titles(title1, title2)
    hdr = f"{'SECTFISS':<25}{'AMOUNT':>18}{'NO. OF ACCT':>12}"
    rw.write_line(' ' + hdr)
    rw.write_line(' ' + '-' * len(hdr))

    agg = {}
    for c in ['balance', 'noacct']:
        if c in df.columns:
            agg[c] = 'sum'
    grp = df.groupby(['secgroup', 'sectype'], as_index=False).agg(agg).sort_values(
        ['secgroup', 'sectype']
    )

    grand_bal = grand_noa = 0.0
    for sg_val in grp['secgroup'].drop_duplicates():
        sub = grp[grp['secgroup'] == sg_val].sort_values('sectype')
        sub_bal = sub_noa = 0.0
        for _, row in sub.iterrows():
            st = coalesce_s(row.get('sectype'))[:25]
            bal = float(row.get('balance') or 0.0)
            noa = float(row.get('noacct') or 0.0)
            rw.write_line(' ' + f"  {st:<23}{bal:18.2f}{noa:12.0f}")
            sub_bal += bal
            sub_noa += noa
        rw.write_line(' ' + f"{'  SUB-TOTAL':<25}{sub_bal:18.2f}{sub_noa:12.0f}")
        grand_bal += sub_bal
        grand_noa += sub_noa

    rw.write_line(' ' + '=' * len(hdr))
    rw.write_line(' ' + f"{'GRAND TOTAL':<25}{grand_bal:18.2f}{grand_noa:12.0f}")
    rw.blank()


def print_tabulate_product(df: pd.DataFrame, rw: ReportWriter,
                           title1: str, title2: str):
    if df is None or df.empty:
        return
    rw.write_titles(title1, title2)
    hdr = f"{'FACILITY':<25}{'AMOUNT':>18}{'NO. OF ACCT':>12}"
    rw.write_line(' ' + hdr)
    rw.write_line(' ' + '-' * len(hdr))

    agg = {}
    for c in ['balance', 'noacct']:
        if c in df.columns:
            agg[c] = 'sum'
    grp = df.groupby('type', as_index=False).agg(agg).sort_values('type')

    grand_bal = grand_noa = 0.0
    for _, row in grp.iterrows():
        t = coalesce_s(row.get('type'))[:25]
        bal = float(row.get('balance') or 0.0)
        noa = float(row.get('noacct') or 0.0)
        rw.write_line(' ' + f"{t:<25}{bal:18.2f}{noa:12.0f}")
        grand_bal += bal
        grand_noa += noa

    rw.write_line(' ' + '=' * len(hdr))
    rw.write_line(' ' + f"{'GRAND TOTAL':<25}{grand_bal:18.2f}{grand_noa:12.0f}")
    rw.blank()


def summarise(df: pd.DataFrame, class_cols: list) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    agg_v = [c for c in NUM_COLS if c in df.columns]
    if not agg_v:
        return pd.DataFrame()
    return df.groupby(class_cols, as_index=False)[agg_v].sum()


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("EIMBNM01: Starting Public Bank Berhad loan summary reports...")

    rv = get_report_vars()
    mm_yr = f"{rv['reptmon']}/{rv['ryear']}"
    print(f"  Report date: {rv['reptdate']}  MM={rv['reptmon']} YY={rv['ryear']} WK={rv['nowk']}")

    rw = ReportWriter()
    RPT = 'REPORT ID : EIMBNM01'

    # -------------------------------------------------------------------------
    # Build base loan dataset
    # -------------------------------------------------------------------------
    loan_base = build_loan_dataset(rv)

    # Build DISPAY
    dispay_df = build_dispay(rv, loan_base)

    # Build CL_FEE and merge into BNM.LOAN<MM><WK>
    mm, wk = rv['reptmon'], rv['nowk']
    bnm_loan = read_sas7bdat(BNM_LOAN_PREFIX.format(reptmon=mm, nowk=wk))
    cl_fee = build_cl_fee(rv)
    bnm_loan = merge_loan_cl_fee(bnm_loan, cl_fee)

    # Build ALM
    alm_df = build_alm(bnm_loan, rv)

    # Merge DISPAY into ALM
    if not dispay_df.empty and not alm_df.empty:
        dp_filt = dispay_df.copy()
        if 'prodcd' in dp_filt.columns:
            dp_filt = dp_filt[
                (dp_filt['prodcd'].astype(str).str[:2] == '34') |
                dp_filt['product'].isin([678, 679, 993, 996])
            ]
        dp_sel = dp_filt[[c for c in ['acctno', 'noteno', 'disburse', 'repaid']
                          if c in dp_filt.columns]]
        alm_df = alm_df.merge(dp_sel, on=['acctno', 'noteno'], how='left',
                              suffixes=('', '_dp'))
        for c in ['disburse', 'repaid']:
            dc = f"{c}_dp"
            if dc in alm_df.columns:
                alm_df[c] = alm_df[dc].combine_first(
                    alm_df[c] if c in alm_df.columns else pd.Series(index=alm_df.index)
                )
                alm_df = alm_df.drop(columns=dc)

    if not alm_df.empty:
        alm_df['repayno'] = (alm_df['repaid'] > 0).astype(int)
        alm_df['disbno']  = (alm_df['disburse'] > 0).astype(int)

    alm_df = apply_prodesc(alm_df)

    # -------------------------------------------------------------------------
    # %INC PGM(RDL2PBIF) — build_pbif()
    # RDL2PBIF derives its own reptyear/reptmon/reptday/mdate_int from reptdate.
    # Pass the same reptdate used by EIMBNM01; lowercase columns on receipt.
    # -------------------------------------------------------------------------
    pbif_df = build_pbif(rv['reptdate'])
    if not pbif_df.empty:
        pbif_df = pbif_df.rename({c: c.lower() for c in pbif_df.columns})

    if not pbif_df.empty:
        pbif_df = pbif_df.copy()
        pbif_df['prodesc'] = 'FACTORING'
        pbif_df['repayno'] = (pbif_df['repaid'].fillna(0) > 0).astype(int)
        pbif_df['disbno']  = (pbif_df['disburse'].fillna(0) > 0).astype(int)
        pbif_df.loc[(pbif_df['balance'].fillna(0) > 0) & (pbif_df['noacct'] != 0),
                    'noacct'] = 1

    # DATA ALMNEW: SET ALM PBIF
    almnew_df = pd.concat([f for f in [alm_df, pbif_df] if not f.empty],
                          ignore_index=True, sort=False)

    almloan_df = summarise(almnew_df, ['prodesc'])
    print_table(almloan_df, rw, f"ALL LOANS AS AT {mm_yr}", RPT)

    # -------------------------------------------------------------------------
    # DATA ALM2 COM3
    # -------------------------------------------------------------------------
    alm2_rows, com3_rows = [], []
    if not alm_df.empty:
        sel = alm_df[alm_df['prodesc'].isin(
            ['OD RETAIL', 'OTHERS RETAIL', 'FLOOR STOCKING LOANS'])]
        for _, row in sel.iterrows():
            row = row.to_dict()
            pd_ = coalesce_s(row.get('prodesc'))
            prod = int(row.get('product') or 0)
            fiss = coalesce_s(row.get('fisspurp'))
            tycode = 0

            if pd_ == 'OD RETAIL':
                if prod in ODRTLA and fiss in ODFISS:
                    row['prodesc'] = 'PURCHASE OF RESIDENTIAL PROPERTY'
                elif prod in ODRTLB:
                    row['prodesc'] = 'SHARE MARGIN FINANCING'
                else:
                    row['prodesc'] = 'TOTAL COMMERCIAL RETAILS'
                tycode = 1
            elif pd_ == 'OTHERS RETAIL':
                if prod in OTRTLA:
                    row['prodesc'] = 'PERSONAL LOAN'
                elif prod in OTRTLB:
                    row['prodesc'] = 'STAFF LOAN'
                else:
                    row['prodesc'] = 'TOTAL COMMERCIAL RETAILS'
                tycode = 2
            elif pd_ == 'FLOOR STOCKING LOANS':
                row['prodesc'] = 'TOTAL COMMERCIAL RETAILS'
                tycode = 3

            row['tycode'] = tycode
            alm2_rows.append(row)
            com3_rows.append(dict(row))

    alm2_df = pd.DataFrame(alm2_rows)
    com3_df = pd.DataFrame(com3_rows)

    pbif1_df = pd.DataFrame()
    if not pbif_df.empty:
        pbif1_df = pbif_df.copy()
        pbif1_df.loc[pbif1_df['prodesc'] == 'FACTORING', 'prodesc'] = 'TOTAL COMMERCIAL RETAILS'

    alm2new_src = pd.concat([f for f in [alm2_df, pbif1_df] if not f.empty],
                            ignore_index=True, sort=False)

    if not alm2new_src.empty:
        alm_cr_keep = [c for c in ['acctno', 'noteno', 'prodesc', 'noacct']
                       if c in alm2new_src.columns]
        write_sas7bdat(alm2new_src[alm_cr_keep], MFRS_ALM_CR_SAS, 'ALM_CR')

    alm2crl_rows = []
    if not alm2new_src.empty:
        for _, row in alm2new_src[alm2new_src['prodesc'] == 'TOTAL COMMERCIAL RETAILS'].iterrows():
            row = row.to_dict()
            custcd = coalesce_s(row.get('custcd'))
            row['prodesc'] = ('COMMERCIAL RETAIL - IND'
                              if custcd in INDIV_CUSTCDS
                              else 'COMMERCIAL RETAIL - NON IND')
            alm2crl_rows.append(row)
    alm2crl_df = pd.DataFrame(alm2crl_rows)

    almloan2_df = summarise(alm2new_src, ['prodesc'])
    print_table(almloan2_df, rw, f"RETAILS LOANS AS AT {mm_yr}", RPT)

    alm2crl_sum = summarise(alm2crl_df, ['prodesc'])
    print_table(alm2crl_sum, rw, f"COMMERCIAL RETAIL LOANS AS AT {mm_yr}", RPT)

    # -------------------------------------------------------------------------
    # SME Datasets
    # -------------------------------------------------------------------------
    def is_dbe(row):
        return (coalesce_s(row.get('custcd')) in DBE_CUSTCDS or
                coalesce_s(row.get('custcx')) in DBE_CUSTCDS)

    def is_fbe(row):
        return coalesce_s(row.get('custcd')) in FBE_CUSTCDS

    def is_dnbfi(row):
        return coalesce_s(row.get('dnbfisme')) in DNBFI_VALS

    almsme_rows = []
    if not alm_df.empty:
        for _, row in alm_df.iterrows():
            row = row.to_dict()
            custcd = coalesce_s(row.get('custcd'))
            dnbfi = coalesce_s(row.get('dnbfisme'))
            if custcd in SME_CUSTCDS or dnbfi in DNBFI_VALS:
                almsme_rows.append(row)
    almsme_df = pd.DataFrame(almsme_rows)

    smefac_rows = []
    if not pbif_df.empty:
        for _, row in pbif_df.iterrows():
            row = row.to_dict()
            if coalesce_s(row.get('custcx')) in SME_CUSTCDS:
                smefac_rows.append(row)
    smefac_df = pd.DataFrame(smefac_rows)

    almsme_all_src = pd.concat([f for f in [almsme_df, smefac_df] if not f.empty],
                               ignore_index=True, sort=False)

    almsme_out, dbe_out, fbe_out, dnbfi_out = [], [], [], []
    if not almsme_all_src.empty:
        for _, row in almsme_all_src.iterrows():
            row = row.to_dict()
            almsme_out.append(row)
            if is_dbe(row):
                dbe_out.append(row)
            elif is_fbe(row):
                fbe_out.append(row)
            elif is_dnbfi(row):
                dnbfi_out.append(row)

    almsme_full = pd.DataFrame(almsme_out)
    dbe_df      = pd.DataFrame(dbe_out)
    fbe_df      = pd.DataFrame(fbe_out)
    dnbfi_df    = pd.DataFrame(dnbfi_out)

    print_table(summarise(almsme_full, ['prodesc']), rw,
                f"SME LOANS AS AT {mm_yr}", RPT)

    almsme2_out, dbe2_out, fbe2_out, dnbfi2_out = [], [], [], []
    if not alm2new_src.empty:
        for _, row in alm2new_src.iterrows():
            row = row.to_dict()
            custcd = coalesce_s(row.get('custcd'))
            custcx = coalesce_s(row.get('custcx'))
            dnbfi = coalesce_s(row.get('dnbfisme'))
            if custcd in DBE_CUSTCDS or custcx in DBE_CUSTCDS:
                dbe2_out.append(row); almsme2_out.append(row)
            elif custcd in FBE_CUSTCDS:
                fbe2_out.append(row); almsme2_out.append(row)
            elif dnbfi in DNBFI_VALS:
                dnbfi2_out.append(row); almsme2_out.append(row)

    almsme2_df = pd.DataFrame(almsme2_out)
    dbe2_df    = pd.DataFrame(dbe2_out)
    fbe2_df    = pd.DataFrame(fbe2_out)
    dnbfi2_df  = pd.DataFrame(dnbfi2_out)

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

    # -------------------------------------------------------------------------
    # BTRADE
    # -------------------------------------------------------------------------
    alm_bt_df, almloan_bt_df, mast_bt_df = build_btrade(rv)

    print_table(almloan_bt_df, rw, f"BANK TRADE AS AT {mm_yr}", RPT)

    almsme_bt, mastsme_bt = [], []
    if not alm_bt_df.empty:
        for _, row in alm_bt_df.iterrows():
            row = row.to_dict()
            if (coalesce_s(row.get('custcd')) in SME_CUSTCDS or
                    coalesce_s(row.get('dnbfisme')) in DNBFI_VALS):
                almsme_bt.append(row)
    if not mast_bt_df.empty:
        for _, row in mast_bt_df.iterrows():
            row = row.to_dict()
            if (coalesce_s(row.get('custcd')) in SME_CUSTCDS or
                    coalesce_s(row.get('dnbfisme')) in DNBFI_VALS):
                mastsme_bt.append(row)

    almsme_bt_df  = pd.DataFrame(almsme_bt)
    mastsme_bt_df = pd.DataFrame(mastsme_bt)

    bt_grp_key = [c for c in ['prodesc', 'custcd', 'dnbfisme']
                  if c in almsme_bt_df.columns]

    almsme_bt_sum = pd.DataFrame()
    mastsme_bt_sum = pd.DataFrame()
    if not almsme_bt_df.empty and bt_grp_key:
        agg_v2 = [c for c in ['disburse', 'repaid', 'balance'] if c in almsme_bt_df.columns]
        if agg_v2:
            almsme_bt_sum = almsme_bt_df.groupby(bt_grp_key, as_index=False)[agg_v2].sum()
    if not mastsme_bt_df.empty:
        mbt_grp = [c for c in ['prodesc', 'custcd', 'dnbfisme']
                   if c in mastsme_bt_df.columns]
        agg_v3 = [c for c in ['disbno', 'repayno', 'noacct'] if c in mastsme_bt_df.columns]
        if mbt_grp and agg_v3:
            mastsme_bt_sum = mastsme_bt_df.groupby(mbt_grp, as_index=False)[agg_v3].sum()

    if not almsme_bt_sum.empty and not mastsme_bt_sum.empty:
        mk = [c for c in bt_grp_key if c in mastsme_bt_sum.columns]
        almloan_sme_bt = almsme_bt_sum.merge(mastsme_bt_sum, on=mk, how='outer')
    elif not almsme_bt_sum.empty:
        almloan_sme_bt = almsme_bt_sum
    else:
        almloan_sme_bt = mastsme_bt_sum

    dbebt_rows, dnbfibt_rows, fbebt_rows = [], [], []
    if not almloan_sme_bt.empty:
        for _, row in almloan_sme_bt.iterrows():
            row = row.to_dict()
            custcd = coalesce_s(row.get('custcd'))
            dnbfi = coalesce_s(row.get('dnbfisme'))
            if custcd in DBE_CUSTCDS:
                dbebt_rows.append(row)
            if dnbfi in DNBFI_VALS:
                dnbfibt_rows.append(row)
            if custcd in FBE_CUSTCDS:
                fbebt_rows.append(row)

    dbebt_df   = pd.DataFrame(dbebt_rows)
    dnbfibt_df = pd.DataFrame(dnbfibt_rows)
    fbebt_df   = pd.DataFrame(fbebt_rows)

    print_table(summarise(almloan_sme_bt, ['prodesc']), rw,
                f"SME BANK TRADE AS AT {mm_yr}", RPT)
    for df_in, t1 in [
        (dbebt_df,   f"OF WHICH : SME DBE BANK TRADE AS AT {rv['reptmon']}/{rv['reptyear']}"),
        (dnbfibt_df, f"OF WHICH : SME DNBFI BANK TRADE AS AT {rv['reptmon']}/{rv['reptyear']}"),
        (fbebt_df,   f"OF WHICH : SME FE BANK TRADE AS AT {rv['reptmon']}/{rv['reptyear']}"),
    ]:
        print_table(summarise(df_in, ['prodesc']), rw, t1, RPT)

    # -------------------------------------------------------------------------
    # Sector breakdowns
    # -------------------------------------------------------------------------
    if not pbif_df.empty:
        pbifsec_df = pbif_df.copy()
        pbifsec_df['sectype']  = pbifsec_df['sectorcd'].apply(format_fisstype)
        pbifsec_df['secgroup'] = pbifsec_df['sectorcd'].apply(format_fissgroup)
    else:
        pbifsec_df = pd.DataFrame()

    print_tabulate_sector(
        pbifsec_df, rw, 'REPORT ID : EIMBNM01',
        f"OUTSTANDING FACTORING LOANS BY SECTORS AND SUB-SECTORS AS AT"
        f" {rv['reptmon']}{rv['ryear']}"
    )

    comsec_df = pd.DataFrame()
    if not alm2_df.empty:
        comsec_df = alm2_df[alm2_df['prodesc'] == 'TOTAL COMMERCIAL RETAILS'].copy()
        comsec_df['sectype']  = comsec_df['sectorcd'].apply(format_fisstype)
        comsec_df['secgroup'] = comsec_df['sectorcd'].apply(format_fissgroup)

    print_tabulate_sector(
        comsec_df, rw, 'REPORT ID : EIMBNM01',
        f"OUTSTANDING M&I COMMERCIAL RETAIL LOANS BY SECTORS AND"
        f" SUB-SECTORS AS AT {rv['reptmon']}{rv['ryear']}"
    )

    mast1_df = pd.DataFrame()
    almbt_sec_df = pd.DataFrame()
    if not mast_bt_df.empty:
        mast1_df = mast_bt_df[mast_bt_df['prodesc'] == 'BILLS RETAIL'].copy()
        mast1_df['sectype']  = mast1_df['sectorcd'].apply(format_fisstype)
        mast1_df['secgroup'] = mast1_df['sectorcd'].apply(format_fissgroup)

    if not alm_bt_df.empty:
        almbt_sec_df = alm_bt_df[alm_bt_df['prodesc'] == 'BILLS RETAIL'].copy()
        almbt_sec_df['sectype']  = almbt_sec_df['sectorcd'].apply(format_fisstype)
        almbt_sec_df['secgroup'] = almbt_sec_df['sectorcd'].apply(format_fissgroup)

    rebsec_df = pd.DataFrame()
    if not mast1_df.empty:
        sg_key = [c for c in ['secgroup', 'sectype'] if c in mast1_df.columns]
        mast1_sum = (mast1_df.groupby(sg_key, as_index=False)['noacct'].sum()
                     if 'noacct' in mast1_df.columns else mast1_df)
        if not almbt_sec_df.empty:
            sg_key2 = [c for c in ['secgroup', 'sectype'] if c in almbt_sec_df.columns]
            almbt_sec_sum = (almbt_sec_df.groupby(sg_key2, as_index=False)['balance'].sum()
                             if 'balance' in almbt_sec_df.columns else almbt_sec_df)
            rebsec_df = mast1_sum.merge(almbt_sec_sum, on=sg_key, how='left')
        else:
            rebsec_df = mast1_sum

    print_tabulate_sector(
        rebsec_df, rw, 'REPORT ID : EIMBNM01',
        f"OUTSTANDING RETAIL BILLS BY SECTORS AND SUB-SECTORS"
        f" AS AT {rv['reptmon']}{rv['ryear']}"
    )

    combysec_df = pd.concat(
        [f for f in [pbifsec_df, rebsec_df, comsec_df] if not f.empty],
        ignore_index=True, sort=False
    ) if any(not f.empty for f in [pbifsec_df, rebsec_df, comsec_df]) else pd.DataFrame()

    print_tabulate_sector(
        combysec_df, rw, 'REPORT ID : EIMBNM01',
        f"TOTAL COMMERCIAL RETAIL LOANS BY SECTORS AND SUB-SECTORS"
        f" AS AT {rv['reptmon']}{rv['ryear']}"
    )

    # -------------------------------------------------------------------------
    # Total Commercial Retail by product
    # -------------------------------------------------------------------------
    com1_df = pbifsec_df.copy() if not pbifsec_df.empty else pd.DataFrame()
    if not com1_df.empty:
        com1_df['type'] = 'FIXED LOANS'

    com2_df = rebsec_df.copy() if not rebsec_df.empty else pd.DataFrame()
    if not com2_df.empty:
        com2_df['type'] = 'BANKTRADE'

    com3_final_rows = []
    if not com3_df.empty:
        for _, row in com3_df[com3_df['prodesc'] == 'TOTAL COMMERCIAL RETAILS'].iterrows():
            row = row.to_dict()
            tycode = int(row.get('tycode') or 0)
            if tycode == 1:
                row['type'] = 'OD'
            elif tycode == 2:
                row['type'] = 'FIXED LOANS'
            elif tycode == 3:
                row['type'] = 'FLOOR STOCKING'
            else:
                row['type'] = ''
            com3_final_rows.append(row)
    com3_final_df = pd.DataFrame(com3_final_rows)

    combyprod_df = pd.concat(
        [f for f in [com1_df, com2_df, com3_final_df] if not f.empty],
        ignore_index=True, sort=False
    ) if any(not f.empty for f in [com1_df, com2_df, com3_final_df]) else pd.DataFrame()

    print_tabulate_product(
        combyprod_df, rw, 'REPORT ID : EIMBNM01',
        f"TOTAL COMMERCIAL RETAIL LOANS BY TYPE OF PRODUCT"
        f" AS AT {rv['reptmon']}{rv['ryear']}"
    )

    # -------------------------------------------------------------------------
    # Flush report
    # -------------------------------------------------------------------------
    rw.flush(REPORT_TXT)
    print(f"  Written: {REPORT_TXT}")
    print("EIMBNM01: Processing complete.")


if __name__ == '__main__':
    main()
