"""
EIIDLCRM - BNM LCR Reporting for Islamic Banking
Faithful Python port of SAS driver PBBELF + PBLCRFMT + KALMLIQ.
Reads sas7bdat (K1TBL/K3TBL, FD/SA/CA/FCYCA, EQUA, CIS, LIST, LCRM, LCR).
Writes sas7bdat + text via saspy.
"""

import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import pyreadstat
import saspy
import pandas as pd

from PBLCRFMT import (
    colid_fmt, lcrcdequ_fmt, lcrcdmni_fmt, lcrcdmniopr_fmt,
    lcrcdigl_fmt, lcrcdiglccy_fmt, remfmt, cmmfmt,
)
from PBBELF import format_ctype
from KALMLIQ import build_kalmliq

# ---------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------
PATHS = {
    'LCR':     '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcr/',
    'LCRM':    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/lcr/',
    'CISDP':   '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/cisdp/',
    'CISCA':   '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/cisca/',
    'CIS':     '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/cis/',
    'EQUA':    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/equa/',
    'LIST':    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/list/',
    'DEPOSIT': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/DEPOSIT/',
    'BNMK':    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/BNMK/',
    'WALK':    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/walk.txt',
    'TEMPL':   '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/templ.txt',
    'OUTPUT':  '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLCRM/',
}
INST = 'PBB'

# ---------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------
CUST_MAP = {
    '08': [76,77,78,95,96],
    '19': [41,42,43,44,46,47,48,49,51,52,53,54,65,66,67,68,69],
    '29': [0,45,57,59,60,61,62,63,64,75,79,85,86,87,88,89,98,99],
    '39': [1,71,72,73,74,90,91,92],
    '49': [2,3,7,12,81,82,83,84],
    '59': [4,5,6,13,20] + list(range(30,41)) + [17],
}
MGIA_PRODUCTS = [302, 315, 394, 396]
SPECIAL_39_NAMES = ['KWSP','KWAP','KWAN','LEMTAB']
SPECIAL_49_NAMES = ['AIM','PBL','PBLEUR','PBLNID','PBLUSD','PIVMYR','PBB','PBBMYR','PBBUSD','CUST']
SPECIAL_39_NUMBERS = [4391161,2115999,12579649,13468207,14300254,
                      14675929,15327497,17104931,12677444,3703533,
                      5978659,16185090,2558344,10819745]
EXCLUDE_CUSTNO = [14094942,16557696,3728510,11335374,16265490,
                  3523050,11880426,16771972,15241330,16500538]

# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------
def get_sas_session():
    return saspy.SASsession(cfgname='default')

def get_report_date():
    """Read DEPOSIT.REPTDATE sas7bdat (SAS original reads DEPOSIT.REPTDATE)."""
    df = read_sas(f"{PATHS['DEPOSIT']}REPTDATE.sas7bdat")
    d = df['REPTDATE'][0]
    if hasattr(d, 'date') and not isinstance(d, (datetime,)):
        d = d.date() if hasattr(d, 'date') else d
    reptdate = datetime(d.year, d.month, d.day)

    day = reptdate.day
    nowk = '1' if day <= 8 else '2' if day <= 15 else '3' if day <= 22 else '4'
    days_in_month = [31,28,31,30,31,30,31,31,30,31,30,31]
    if reptdate.year % 4 == 0:
        days_in_month[1] = 29
    return {
        'date': reptdate, 'nowk': nowk,
        'mon': f"{reptdate.month:02d}", 'day': f"{reptdate.day:02d}",
        'rdate': reptdate.strftime('%d%m%y'), 'rptdt': reptdate.strftime('%y%m%d'),
        'year': reptdate.year, 'month': reptdate.month,
        'day_of_month': day, 'days_in_month': days_in_month,
    }

def get_cust_from_code(code):
    for cat, codes in CUST_MAP.items():
        if code in codes:
            return cat
    return '29'

def read_sas(path):
    df_pd, _ = pyreadstat.read_sas7bdat(path)
    df_pd.columns = [c.lower() for c in df_pd.columns]
    return pl.from_pandas(df_pd)

def read_parquet(path):
    df = pl.read_parquet(path)
    return df.rename({c: c.lower() for c in df.columns})

# ---------------------------------------------------------------------
# TREASURY via KALMLIQ
# ---------------------------------------------------------------------
def process_treasury(rep_date):
    """
    Mirror SAS %INC PGM(KALMLIQ): reads BNMK.K1TBL<MON><NOWK> and
    BNMK.K3TBL<MON><NOWK> sas7bdat, returns the KTBLALL-equivalent rows.
    """
    k1 = Path(f"{PATHS['BNMK']}K1TBL{rep_date['mon']}{rep_date['nowk']}.sas7bdat")
    k3 = Path(f"{PATHS['BNMK']}K3TBL{rep_date['mon']}{rep_date['nowk']}.sas7bdat")
    print(f"  K1TBL: {k1}")
    print(f"  K3TBL: {k3}")

    ktbl, _dist = build_kalmliq(
        k1tbl_path=k1, k3tbl_path=k3,
        reptdate=rep_date['date'].date(),
        rpyr=rep_date['year'], rpmth=rep_date['month'],
        rpday=rep_date['day_of_month'], rd_days=rep_date['days_in_month'],
        inst=INST,
    )

    records = []
    for r in ktbl.iter_rows(named=True):
        bnm = r['BNMCODE']
        is_k3 = bnm[:2] in ('93', '94')
        records.append({
            'src': 'K3TBL' if is_k3 else 'K1TBL',
            'bnmcode': bnm,
            'amt': r['AMOUNT'],
            'amtusd': r.get('AMTUSD', 0.0),
            'amtsgd': r.get('AMTSGD', 0.0),
        })
    return records

def process_cis_equity():
    """CIS.CUST.DAILY.parquet for equity customer mapping."""
    records = {}
    try:
        df = read_parquet(f"{PATHS['CIS']}CIS.CUST.DAILY.parquet")
        df = df.filter((pl.col('acctcode') == 'EQC') & (pl.col('prisec') == 901))
        for row in df.iter_rows(named=True):
            newic = row.get('newic', '') or ''
            if not newic or newic[:5] == '99999':
                icno = f"{row.get('aliaskei', '') or ''}{row.get('custno', 0)}".replace(' ', '')
            else:
                icno = f"{row.get('aliaskei', '') or ''}{row.get('alias', '') or ''}".replace(' ', '')
            records[row['acctno']] = {'cisno': row['custno'], 'cisname': row['custname'], 'icno': icno}
    except Exception as e:
        print(f"  CIS equity warning: {e}")
    return records

def process_utsas(rep_date):
    records = {}
    utvar = ['dealref','dealtype','custfiss','custno','custname','custeqno','custid']
    try:
        for prefix in ['iutms', 'iutfx', 'iutrp']:
            df = read_sas(f"{PATHS['EQUA']}{prefix}{rep_date['rptdt']}.sas7bdat")
            keep = [c for c in utvar if c in df.columns]
            if keep:
                df = df.select(keep)
                if 'custeqno' in df.columns:
                    df = df.rename({'custeqno': 'acctno'})
                for row in df.rows(named=True):
                    records[row['dealref']] = row
    except Exception as e:
        print(f"  UTSAS warning: {e}")
    return records

# ---------------------------------------------------------------------
# BANKING
# ---------------------------------------------------------------------
def process_core_banking(rep_date):
    records = []
    for tbl in ['fd', 'sa', 'ca', 'fcyca']:
        try:
            df = read_sas(f"{PATHS['LCR']}{tbl}{rep_date['day']}.sas7bdat")
            for row in df.iter_rows(named=True):
                custcd = row.get('custcdx' if tbl == 'fd' else 'custcd', 0)
                if tbl == 'fd' and custcd is not None:
                    custcd = f"{int(custcd):02d}"
                cust = get_cust_from_code(custcd)
                rem30d = row.get('rem30d', row.get('remmth', 1)) or row.get('remmth', 1)
                remmth = row.get('remmth', 1)
                bic = row['bnmcode'][:5]
                if bic == '95317' and row.get('product') in MGIA_PRODUCTS:
                    bic = '95315'
                records.append({
                    'src': tbl.upper(), 'bic': bic,
                    'bnmcode': f"{bic}{cust}020000Y",
                    'cmmcode': f"{bic}{cust}{cmmfmt(remmth)}0000Y",
                    'cur': row.get('curcode', 'MYR'), 'amt': row.get('amount', 0),
                    'acctno': row.get('acctno'), 'custno': row.get('custno'),
                    'rem30d': rem30d, 'remmth': remmth, 'ecp': '00',
                    'product': row.get('product'), 'billerind': row.get('billerind', 'N'),
                    'pbmerch': row.get('pbmerch', 'N'), 'intrate': row.get('intrate', 0),
                    'oprrate': row.get('oprrate', 0), 'source': row.get('source', ''),
                    'dtsigned': row.get('dtsigned'), 'intplan': row.get('intplan', 0),
                    'sme_tag': row.get('sme_tag', ''), 'fdhold': row.get('fdhold', 'N'),
                    'trx': row.get('trx', 0), 'sign': '', 'custcd': custcd,
                    'branch': row.get('branch', ''), 'cdno': row.get('cdno', ''),
                    'matdt': row.get('matdt'),
                })
        except Exception as e:
            print(f"  {tbl} warning: {e}")
    return records

# ---------------------------------------------------------------------
# INSURED / UNINSURED SPLIT
# ---------------------------------------------------------------------
def split_insurance(records):
    result = []
    icgrp_totals = {}
    for r in records:
        icgrp = r.get('icgrp', '')
        if icgrp:
            icgrp_totals[icgrp] = icgrp_totals.get(icgrp, 0) + r['amt']
    for r in records:
        toticbal = icgrp_totals.get(r.get('icgrp', ''), 0)
        if toticbal > 250000:
            curbal = r['amt']
            insured = (curbal / toticbal) * 250000
            if r['bnmcode'][5:7] in ('29','39') and r.get('ecp') != '01
