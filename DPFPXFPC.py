"""
EIIDLCRM - BNM LCR Reporting for Islamic Banking
Consolidates Islamic deposits & treasury positions for BNM LCR reporting.
Includes MGIA, TD-I, and Islamic treasury products.

Faithful Python port of SAS driver PBBELF + PBLCRFMT + KALMLIQ.
"""

import polars as pl
from datetime import datetime, timedelta, date
from pathlib import Path
import pyreadstat
import saspy
import pandas as pd

# --- PBLCRFMT / PBBELF / KALMLIQ imports ---
from PBLCRFMT import (
    bnmcd_fmt, lcrcdequ_fmt, lcrcdmniopr_fmt, lcrcdmni_fmt,
    lcrcdgl_fmt, lcrcdgloth_fmt, lcrcdglccy_fmt,
    lcrcdigl_fmt, lcrcdiglccy_fmt, colid_fmt,
    remfmt, cmmfmt, remfmx,
)
from PBBELF import format_ctype
from KALMLIQ import build_kalmliq

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'LCR': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcr/',
    'LCRM': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/lcr/',
    'CISDP': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/cisdp/',
    'CISCA': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/cisca/',
    'CIS': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/cis/',
    'EQUA': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/equa/',
    'LIST': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/list/',
    'DEPOSIT': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/DEPOSIT/',
    'K1TBL_CACHE': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/BNMK/K1TBL.parquet',
    'K3TBL_CACHE': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/BNMK/K3TBL.parquet',
    'WALK': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/walk.txt',
    'TEMPL': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/templ.txt',
    'OUTPUT': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLCRM/',
}

INST = 'PBB'

# =============================================================================
# CUSTOMER CATEGORY MAPPING (from SAS DATA ALLEQU)
# =============================================================================
CUST_MAP = {
    '08': [76, 77, 78, 95, 96],
    '19': [41,42,43,44,46,47,48,49,51,52,53,54,65,66,67,68,69],
    '29': [0,45,57,59,60,61,62,63,64,75,79,85,86,87,88,89,98,99],
    '39': [1,71,72,73,74,90,91,92],
    '49': [2,3,7,12,81,82,83,84],
    '59': [4,5,6,13,20] + list(range(30,41)) + [17],
}

MGIA_PRODUCTS = [302, 315, 394, 396]

SPECIAL_39_NAMES = ['KWSP', 'KWAP', 'KWAN', 'LEMTAB']
SPECIAL_49_NAMES = ['AIM','PBL','PBLEUR','PBLNID','PBLUSD','PIVMYR','PBB','PBBMYR','PBBUSD','CUST']

SPECIAL_39_NUMBERS = [
    4391161, 2115999, 12579649, 13468207, 14300254,
    14675929, 15327497, 17104931, 12677444, 3703533,
    5978659, 16185090, 2558344, 10819745
]

EXCLUDE_CUSTNO = [
    14094942, 16557696, 3728510, 11335374, 16265490,
    3523050, 11880426, 16771972, 15241330, 16500538,
]

# =============================================================================
# HELPERS
# =============================================================================
def get_sas_session():
    return saspy.SASsession(cfgname='default')

def get_report_date():
    d = datetime.now() - timedelta(days=1)
    reptdate = datetime(d.year, d.month, d.day)
    day = reptdate.day
    nowk = '1' if day <= 8 else '2' if day <= 15 else '3' if day <= 22 else '4'
    days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
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

# =============================================================================
# TREASURY (K1/K3)
# =============================================================================
def process_treasury(rep_date):
    """Build KTBLALL via KALMLIQ, then process K1TBL/K3TBL."""
    ktbl, dist_summary = build_kalmliq(
        k1tbl_cache=Path(PATHS['K1TBL_CACHE']),
        k3tbl_cache=Path(PATHS['K3TBL_CACHE']),
        reptdate=rep_date['date'].date(),
        rpyr=rep_date['year'], rpmth=rep_date['month'], rpday=rep_date['day_of_month'],
        rd_days=rep_date['days_in_month'],
        inst=INST,
    )
    # ktbl has BNMCODE, AMOUNT, AMTUSD, AMTSGD
    # SAS KTBLALL also carries a TBL column ('1' or '3') and other fields.
    # We split K1/K3 via BNMCODE prefix ranges — in practice the source
    # K1TBL cache has GW* columns and K3TBL has UT* columns; here we return
    # the KTBLALL-equivalent frame and reconstruct K1TBL/K3TBL by matching
    # BNMCODE prefixes 95/96 (K1) vs 93/94 (K3, alt prefixed).
    records = []
    for r in ktbl.iter_rows(named=True):
        bnm = r['BNMCODE']
        # K1-derived rows use PART 95/96; K3-derived rows are the alt 93/94 copies.
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
    utvar = ['dealref', 'dealtype', 'custfiss', 'custno', 'custname', 'custeqno', 'custid']
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

# =============================================================================
# CORE BANKING
# =============================================================================
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
                    'src': tbl.upper(), 'bic': bic, 'bnmcode': f"{bic}{cust}020000Y",
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

# =============================================================================
# INSURED / UNINSURED SPLIT
# =============================================================================
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
            if r['bnmcode'][5:7] in ('29', '39') and r.get('ecp') != '01':
                r1 = r.copy()
                r1['bnmcode'] = r['bnmcode'][:7] + '10' + r['bnmcode'][10:15]
                result.append(r1)
            else:
                r1 = r.copy(); r1['amt'] = insured; result.append(r1)
                r2 = r.copy(); r2['amt'] = curbal - insured
                r2['bnmcode'] = r['bnmcode'][:7] + '10' + r['bnmcode'][10:15]
                result.append(r2)
        else:
            result.append(r)
    return result

# =============================================================================
# SAS OUTPUT
# =============================================================================
def write_sas7bdat(df_pl, out_path, sas):
    df_pd = df_pl.to_pandas()
    for col in df_pd.columns:
        if df_pd[col].dtype == object:
            df_pd[col] = df_pd[col].astype(str)
    sas.df2sd(df_pd, table='_tmp_out', libref='WORK')
    sas.submit(f"""
    proc export data=WORK._tmp_out outfile="{out_path}"
        dbms=sas7bdat replace;
    run;
    """)

def write_text_file(df_pl, out_path, sas, header_lines=None):
    df_pd = df_pl.to_pandas()
    for col in df_pd.columns:
        if df_pd[col].dtype == object:
            df_pd[col] = df_pd[col].astype(str)
    sas.df2sd(df_pd, table='_tmp_txt', libref='WORK')
    hdr = "\n".join([f'    put "{l}";' for l in (header_lines or [])])
    sas.submit(f"""
    data _null_;
        file "{out_path}";
        {hdr}
        set WORK._tmp_txt;
        put _all_;
    run;
    """)

def read_template():
    items = []
    try:
        with open(PATHS['TEMPL'], 'r') as f:
            for line in f:
                if len(line) >= 7:
                    item = line[0:5].strip()
                    idesc = line[7:127].strip() if len(line) > 7 else ''
                    if item:
                        items.append({'item': item, 'idesc': idesc})
    except Exception as e:
        print(f"  Template warning: {e}")
    return pl.DataFrame(items) if items else pl.DataFrame({'item': [], 'idesc': []})

def read_walker_gl():
    records = []
    try:
        with open(PATHS['WALK'], 'r') as f:
            for line in f:
                if len(line) >= 63:
                    set_id = line[1:20].strip()
                    amount_str = line[41:61].strip().replace(',', '')
                    sign = line[61:62].strip()
                    try:
                        amount = float(amount_str) if amount_str else 0.0
                    except ValueError:
                        amount = 0.0
                    if sign == '':
                        amount = -1 * amount
                    item = lcrcdigl_fmt(set_id)
                    if item != '     ':
                        records.append({'set_id': set_id, 'item': item, 'amount': amount})
    except Exception as e:
        print(f"  Walker GL warning: {e}")
    return records

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIIDLCRM - BNM LCR Reporting (Islamic Banking)")
    print("=" * 60)

    sas = get_sas_session()
    rep_date = get_report_date()
    print(f"\nDate: {rep_date['date'].strftime('%d/%m/%Y')} Week:{rep_date['nowk']} Mon:{rep_date['mon']}")

    template = read_template()
    print(f"Template: {len(template)} items")

    cis_dict = process_cis_equity()
    print(f"CIS: {len(cis_dict)} records")

    # -------- TREASURY --------
    print("\nTreasury...")
    k_records = process_treasury(rep_date)
    utsas_dict = process_utsas(rep_date)

    treasury = []
    for r in k_records:
        if r.get('dealref') in utsas_dict:
            r.update(utsas_dict[r['dealref']])

        custfiss = r.get('custfiss', 0)
        if isinstance(custfiss, str) and custfiss.isdigit():
            custfiss = int(custfiss)
        custno = r.get('custno') or ''
        # SAS: IF CUSTFISS=. AND UTCTP NE '' THEN CUSTFISS=PUT(UTCTP,$CTYPE.);
        if not custfiss and r.get('utctp'):
            custfiss = format_ctype(r['utctp']).strip()
            if custfiss.isdigit():
                custfiss = int(custfiss)

        cust = get_cust_from_code(custfiss) if isinstance(custfiss, int) else '29'
        if custno in SPECIAL_39_NAMES:
            cust = '39'

        dtype = '01' if r.get('dealtype') == 'BQD' else '00'
        bic = r['bnmcode'][:5] if 'bnmcode' in r else '     '
        rem30d = r.get('rem30d', r.get('remmth', 1)) or r.get('remmth', 1)
        remmth = r.get('remmth', 1)
        if rem30d > 1 and remmth > 1:
            rem30d = remmth

        bnmcode = f"{bic}{cust}{remfmt(rem30d)}00{dtype}Y"
        cmmcode = f"{bic}{cust}{cmmfmt(remmth)}00{dtype}Y"

        if custno in SPECIAL_49_NAMES and cust == '49' and bic in ('95840','96840'):
            if remfmt(r.get('ori30d', 0)) > '05' and remfmt(rem30d) > '01':
                bnmcode = bnmcode[:9] + '0200Y'

        icgrp = str(r.get('custid') or r.get('icno') or '').replace(' ', '')

        treasury.append({
            'src': 'TREASURY', 'bic': bic, 'bnmcode': bnmcode, 'cmmcode': cmmcode,
            'cur': r.get('cur', 'MYR'), 'amt': r.get('amt', 0), 'icgrp': icgrp,
            'rem30d': rem30d, 'remmth': remmth, 'custno': custno,
            'dealtype': r.get('dealtype', ''), 'matdt': r.get('matdt'),
        })

    print(f"  Treasury: {len(treasury)} records")

    equtot = {}
    for r in treasury:
        key = (r['bnmcode'], r['cur'])
        equtot[key] = equtot.get(key, 0) + r['amt']

    totequ = {}
    for r in treasury:
        if r['bic'][2:5].startswith('8'):
            icgrp = r.get('icgrp', '')
            if icgrp:
                totequ[icgrp] = totequ.get(icgrp, 0) + r['amt']

    # -------- BANKING --------
    print("\nBanking...")
    banking = process_core_banking(rep_date)

    try:
        cis_info = read_sas(f"{PATHS['LCR']}cisinfo.sas7bdat")
        cis_dict2 = {r['acctno']: r for r in cis_info.rows(named=True)}
    except Exception:
        cis_dict2 = {}

    try:
        ecp_df = read_sas(f"{PATHS['LIST']}lcr_ecp.sas7bdat").unique(subset=['acctno'])
        ecp_dict = {r['acctno']: r['ecp'] for r in ecp_df.rows(named=True)}
    except Exception:
        ecp_dict = {}

    try:
        sme_df = read_sas(f"{PATHS['LCRM']}sme.sas7bdat")
        sme_dict = {r['acctno']: r.get('sme_tag', '') for r in sme_df.rows(named=True)}
    except Exception:
        sme_dict = {}

    enhanced = []
    for r in banking:
        if r['acctno'] in cis_dict2:
            ci = cis_dict2[r['acctno']]
            r['newic'] = ci.get('newic')
            r['oldic'] = ci.get('oldic')
            r['custname'] = ci.get('custname', '')

        if r['acctno'] in ecp_dict:
            r['ecp'] = ecp_dict[r['acctno']]
        if not r['ecp']:
            r['ecp'] = '00'
        if r['ecp'] == '01':
            r['ecp'] = '01' if r['intrate'] < r['oprrate'] else '00'
        if r['billerind'] == 'Y' or r['pbmerch'] == 'Y':
            r['ecp'] = '01'

        if r['acctno'] in sme_dict:
            r['sme_tag'] = sme_dict[r['acctno']]

        prod_list = [106,151,158,97,164,201,215]
        intplan_list = list(range(400,420)) + list(range(600,659)) + \
                       list(range(720,741)) + list(range(864,891)) + list(range(941,968))
        if (r['product'] in prod_list or r['intplan'] in intplan_list or
                (r['source'] != 'PGD' and r['dtsigned'] and
                 (rep_date['date'] - r['dtsigned']).days >= 365)):
            r['sign'] = 'R '

        if r['custno'] in SPECIAL_39_NUMBERS:
            r['cust'] = '39'

        r['icgrp'] = str(r.get('newic') or r.get('oldic') or '').replace(' ', '')
        enhanced.append(r)

    icgrp_totals = {}
    for r in enhanced:
        icgrp_totals[r['icgrp']] = icgrp_totals.get(r['icgrp'], 0) + r['amt']

    for r in enhanced:
        r['toticbal'] = icgrp_totals.get(r['icgrp'], 0)

        if (r['custno'] not in EXCLUDE_CUSTNO and r['bnmcode'][5:7] == '29') or r['custcd'] in ('72','73','74'):
            totdp = r['toticbal'] + totequ.get(r['icgrp'], 0)
            if totdp < 5000000:
                r['bnmcode'] = f"{r['bic']}19{r['bnmcode'][7:]}"
                r['cmmcode'] = f"{r['bic']}19{r['cmmcode'][7:]}"
        elif r['bnmcode'][5:7] == '19' and r.get('sme_tag') == 'N':
            totdp = r['toticbal'] + totequ.get(r['icgrp'], 0)
            if totdp >= 5000000:
                r['bnmcode'] = f"{r['bic']}29{r['bnmcode'][7:]}"
                r['cmmcode'] = f"{r['bic']}29{r['cmmcode'][7:]}"

        if r['bnmcode'][5:7] in ('08', '19'):
            tag = '01' if r.get('trx') == 1 else ('02' if r.get('sign') in ('R','R ') else '03')
            r['bnmcode'] = r['bnmcode'][:7] + tag + '0000Y'

        if r['bic'] in ('95313','96313'):
            r['bnmcode'] = r['bnmcode'][:9] + r['ecp'] + '00Y'
            r['cmmcode'] = r['cmmcode'][:9] + r['ecp'] + '00Y'

    print(f"  Banking: {len(enhanced)} records")

    print("\nInsurance split...")
    banking_split = split_insurance(enhanced)

    all_data = treasury + banking_split
    print(f"Total: {len(all_data)} records")

    if not all_data:
        print("\nWARNING: No records. Skipping report.")
        sas.endsas()
        return

    df = pl.DataFrame(all_data)
    df = df.with_columns((pl.col('amt') / 1000).round(2).alias('amt_k'))
    summary = df.group_by(['bnmcode', 'cur']).agg(pl.col('amt_k').sum())
    print(f"Summary: {len(summary)} codes")

    # -------- REPORT (uses real $COLID / $LCRCDMNI / $LCRCDEQU) --------
    report_data = []
    for row in summary.rows(named=True):
        bic = row['bnmcode'][:5]
        cust = row['bnmcode'][5:7]
        rem = row['bnmcode'][9:11]
        ecp = row['bnmcode'][9:11]
        dltype = row['bnmcode'][11:13]

        colname = colid_fmt(bic).strip()

        item = ''
        if dltype == '01':
            colname = colid_fmt('95830').strip()
            item = lcrcdequ_fmt(cust).strip()
            if item == 'B3.30' and rem == '02':
                item = 'B6.30'
        else:
            combined = f"{cust}{rem}"  # SAS uses SUBSTR(BNMCODE,6,4)
            if bic in ('95313','96313') and ecp == '01':
                item = lcrcdmniopr_fmt(combined).strip()
            if not item or item.strip() == '':
                item = lcrcdmni_fmt(combined).strip()

        if colname and item and item.strip() != '':
            amt = abs(round(row['amt_k'], 2))
            col_final = colname
            if colname[:2] == 'FD' or colname[:3] in ('STD','STQ'):
                col_final = f"{colname}{'1' if rem == '01' else '2'}"
            elif colname[:3] in ('NID','IBB'):
                for i in range(1,7):
                    if remfmt(i) == rem:
                        col_final = f"{colname}V{i}"
                        break
            report_data.append({'item': item, 'col': col_final, 'amt': amt})

    if report_data:
        rep_df = pl.DataFrame(report_data)
        final = rep_df.group_by(['item', 'col']).agg(pl.col('amt').sum())
        pivot = final.pivot(index='item', columns='col', values='amt', aggregate_function='sum')
        pivot = pivot.fill_null(0)

        # Derived columns -- real SAS derivations
        if 'FD95315RM1' in pivot.columns and 'FD95315RM2' in pivot.columns:
            pivot = pivot.with_columns(
                (pl.col('FD95315RM1').fill_null(0) + pl.col('FD95315RM2').fill_null(0)).alias('FD95315RM'))
        if 'FD95317RM1' in pivot.columns and 'FD95317RM2' in pivot.columns:
            pivot = pivot.with_columns(
                (pl.col('FD95317RM1').fill_null(0) + pl.col('FD95317RM2').fill_null(0)).alias('FD95317RM'))

        for pref, target in [('STD95830V', 'STD95830'), ('STQ95830V', 'STQ95830'),
                             ('NID95840V', 'NID95840'), ('IBB9X810V', 'IBB9X810')]:
            cols = [c for c in pivot.columns if c.startswith(pref)]
            if cols:
                pivot = pivot.with_columns(pl.sum_horizontal(cols).alias(target))

        totalv1 = [c for c in ['FD95315RM','FD95317RM1','SA95312RM','CA95313RM','CA96313FX',
                                'STD95830','STQ95830','NID95840','IBB9X810V1','OTHSOURCE']
                   if c in pivot.columns]
        if totalv1:
            pivot = pivot.with_columns(pl.sum_horizontal(totalv1).alias('TOTALV1'))

        totaldp = [c for c in ['FD95315RM','FD95317RM','SA95312RM','CA95313RM','CA96313FX',
                                'STD95830','STQ95830','NID95840','IBB9X810','OTHSOURCE']
                   if c in pivot.columns]
        if totaldp:
            pivot = pivot.with_columns(pl.sum_horizontal(totaldp).alias('TOTALDP'))

        # Walker GL
        gl_records = read_walker_gl()
        if gl_records:
            gl_df = pl.DataFrame(gl_records)
            gl_summary = gl_df.group_by('item').agg(pl.col('amount').sum().alias('othsource'))
            gl_summary = gl_summary.with_columns((pl.col('othsource') / 1000).round(2).alias('othsource'))
            pivot = pivot.join(gl_summary, on='item', how='left', suffix='_gl')
            if 'othsource' in pivot.columns:
                pivot = pivot.rename({'othsource': 'OTHSOURCE'})

        if len(template) > 0:
            merged = template.to_pandas().merge(pivot.to_pandas(), on='item', how='left')
        else:
            merged = pivot.to_pandas()

        out_df = pl.from_pandas(merged)

        sas_out = f"{PATHS['OUTPUT']}lcr{rep_date['day']}.sas7bdat"
        write_sas7bdat(out_df, sas_out, sas)
        print(f"Report (sas7bdat): lcr{rep_date['day']}.sas7bdat")

        txt_out = f"{PATHS['OUTPUT']}lcr{rep_date['day']}.txt"
        write_text_file(out_df, txt_out, sas, header_lines=[
            'PUBLIC ISLAMIC BANK BERHAD',
            f"LIQUIDITY COVERAGE RATIO (LCR) AS AT {rep_date['rdate']}",
            '',
        ])
        print(f"Report (text): lcr{rep_date['day']}.txt")

    print(f"\nTotal: RM {df['amt'].sum()/1000:,.0f}K")
    print("=" * 60)
    print("EIIDLCRM Complete")
    sas.endsas()

if __name__ == "__main__":
    main()
