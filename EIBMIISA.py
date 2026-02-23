"""
EIBMIISA - Monthly Movement of Interest-In-Suspense (IIS)
"""

import polars as pl
from datetime import datetime
from pathlib import Path

# =============================================================================
# CONFIG
# =============================================================================
PATHS = {k: f'data/{v}/' for k, v in {
    'LOAN': 'loan', 'NPL': 'npl', 'PRVLOAN': 'prvloan',
    'SASDATA': 'sasdata', 'ISASDATA': 'isasdata', 'OUTPUT': 'output'
}.items()}
for p in PATHS.values(): Path(p).mkdir(exist_ok=True)

RISK_FMT = {'1': 'SS1', '2': 'SS2', '3': 'D  ', '4': 'B  '}
CONV = lambda x: (x < 3000 or x > 3999) and x not in [4043, 4048]
ISLM = lambda x: (3000 <= x <= 3999) or x in [4043, 4048]
CGC = [517, 521, 522, 523, 528]

# =============================================================================
# DATE
# =============================================================================
def get_dates():
    df = pl.read_parquet(f"{PATHS['LOAN']}REPTDATE.parquet")
    reptdate = df['REPTDATE'][0]
    prev = reptdate.month - 1 or 12
    return {
        'reptdate': reptdate, 'reptmon': f"{reptdate.month:02d}", 'prevmON': f"{prev:02d}",
        'rdate_word': reptdate.strftime("%d %B %Y"), 'rptyr': str(reptdate.year)
    }

# =============================================================================
# IIS CALCULATION
# =============================================================================
def calc_iis(r, rep_date):
    r = r.copy()
    
    # Ageing
    if r.get('BLDATE', 0) > 0:
        try:
            bd = datetime.strptime(str(int(r['BLDATE'])).zfill(8), "%m%d%Y").date()
            r['AGEING'] = (rep_date - bd).days
        except:
            r['AGEING'] = 0
    if r.get('OLDNOTEDAYARR', 0) > 0 and 98000 <= r.get('NOTENO', 0) <= 98999:
        r['AGEING'] = max(0, r.get('AGEING', 0)) + r['OLDNOTEDAYARR']
    
    r['CLASSNEW'] = RISK_FMT.get(str(r.get('RISKRATE', '')).strip(), ' ')
    r['BRANCH'] = f"{int(r.get('NTBRCH', 0)):03d} {r.get('NTBRCH', 0)}"
    
    pv = r.get('PREVBAL', 0) or 0
    cb = r.get('CURBAL', 0) or 0
    sw = r.get('SPWOF', 0) or 0
    iw = r.get('IISW', 0) or 0
    ad = r.get('ADJUST', 0) or 0
    r['DIFFBAL'] = pv - cb - sw - iw - ad
    r['IISP'] = r.get('PREVIIS', 0) or 0
    r['OIP'] = r.get('PREVOI', 0) or 0
    
    n = r.get('NPLIND', '')
    lt = r.get('LOANTYPE', 0)
    lc = r.get('LSTTRNCD', 0)
    ir = r.get('IISR', 0) or 0
    iw = r.get('IISW', 0) or 0
    nt = r.get('NTINT', '')
    
    lst = r.get('LSTTRNAM', 0) or 0
    r['LSTTRNAM'] = (lst / 100000) if lt in CGC and n == 'N' and lc == 166 else 0
    
    if nt == 'A':  # Accrual
        if n in ('O', 'P'):
            r['IIS'] = ir + iw - r['IISP']
        if r.get('IISCLS', 0):
            r['IISP'] = r['IISCLS'] - r['OIP']
        if n in ('O', 'P'):
            r['IIS'] = ir + iw - r['IISP']
        r['IIS'] = r.get('ADONIIS', 0) or 0
        
        oi = (r.get('FEEASSES', 0) or 0) - (r.get('FEEWAIVE', 0) or 0)
        if oi < 0:
            oi = 0
        oir = r.get('FEEPAY', 0) or 0
        b = r.get('BORSTAT', '')
        
        if b in ('W', 'X') and n != 'P':
            r['IIS'] = 0
            oi = r.get('FEELWTOT', 0) or 0
        
        if (n == 'E' and b == 'P') or lc == 652:
            ir = 0
        elif r.get('LOANSTAT', 0) == 1 or r['DIFFBAL'] >= r['IISP']:
            ir = r['IISP'] - iw
        elif r['DIFFBAL'] < r['IISP']:
            ir = r['DIFFBAL'] - iw
        
        if n == 'O':
            r['IIS'] = 0
            oir = (r.get('FEEPAY', 0) or 0) + (r.get('FEELWTOT', 0) or 0)
        
        r['IISR'] = max(0, ir)
        if n == 'P':
            r['IIS'] = r['IIS'] + ir + iw - r['IISP']
        
        r['IISCLOSE'] = r['IISP'] + r['IIS'] - ir - iw
        if r['IISCLOSE'] < 0:
            r['IISCLOSE'] = 0
        
        r['OICLOSE'] = 0 if n == 'O' else (r.get('FEELWTOT', 0) or 0)
        r['TOTOPEN'] = r['IISP'] + r['OIP']
        r['TOTIIS'] = r['IIS'] + oi
        r['TOTIISR'] = ir + oir
        r['TOTWOF'] = iw
        
        if n == 'N':
            r['TOTIIS'] = r['IISCLOSE'] + r['TOTIISR'] + r['TOTWOF']
            r['TOTOPEN'] = 0
        
        r['TOTCLOSE'] = r['IISCLOSE'] + r['OICLOSE']
        if b in ('W', 'X') and n != 'P':
            r['TOTCLOSE'] = r['TOTOPEN'] - r['TOTIISR'] - r['TOTWOF']
    
    else:  # Non-accrual
        if n == 'O':
            ac = r.get('ACTIACCR', 0) or 0
            if ac == 0:
                ia = r.get('INTACTIV', 0) or 0
                fw = r.get('FEELWTOT', 0) or 0
                if ia < 0:
                    ir = ir + fw - ia
                else:
                    ir = ir + fw
                r['IIS'] = ir + iw - r['IISP']
            else:
                r['IIS'] = ir + iw + ac - r['IISP']
                r['OIREC'] = (r.get('FEEPAY', 0) or 0) + (r.get('FEELWTOT', 0) or 0)
        
        r['TOTOPEN'] = r['IISP']
        r['TOTIISR'] = ir
        r['TOTWOF'] = iw
        r['TOTIIS'] = r['IIS']
        
        if n == 'N':
            ic = r.get('IISCLOSE', 0) or 0
            r['TOTIIS'] = ic + r['TOTIISR'] + r['TOTWOF']
            r['TOTOPEN'] = 0
        
        if n == 'O':
            r['TOTCLOSE'] = r['IISP'] + r['IIS'] - ir - iw
        else:
            r['TOTCLOSE'] = r.get('IISCLOSE', 0) or 0
    
    return r

# =============================================================================
# REALISABLE VALUE
# =============================================================================
def calc_realisable(r, rep_date):
    r = r.copy()
    last = None
    for f in ['DLIVRYDT', 'APPRDATE', 'ISSUEDT']:
        if r.get(f, 0) and r[f] > 0:
            try:
                last = datetime.strptime(str(int(r[f])).zfill(11)[:8], "%m%d%Y").date()
                break
            except:
                continue
    
    days = (rep_date - last).days if last else 0
    f1 = r.get('FLAG1', '')
    lb = r.get('LIABCODE', '')
    mv = r.get('MARKETVL', 0) or 0
    av = r.get('APPVALUE', 0) or 0
    lt = r.get('LOANTYPE', 0)
    rv = 0
    
    if f1 == 'F':
        if lb in ('50', 'C2'):
            if mv > 0 and av > 0:
                if mv == av:
                    rv = mv
                else:
                    rv = mv if r.get('DLIVRYDT', 0) > 0 else min(mv, av)
                if days >= 730:
                    rv *= 0.9 if lb == '50' else 0.8
            elif mv > 0:
                rv = mv
            elif av > 0:
                rv = av
            if days >= 730 and (mv > 0 or av > 0):
                rv *= 0.9 if lb == '50' else 0.8
        
        elif lb in ('B8', 'B9', '33', '37', '34', '32', 'C1'):
            if mv > 0 and av > 0:
                if mv == av:
                    rv = mv
                else:
                    rv = mv if r.get('DLIVRYDT', 0) > 0 else min(mv, av)
                if days >= 730:
                    rv *= 0.75
            elif mv > 0:
                rv = mv
            elif av > 0:
                rv = av
            if days >= 730 and (mv > 0 or av > 0):
                rv *= 0.75
        
        else:
            rv = mv
            if lt in (300, 301) and days >= 730:
                rv *= 0.9
    
    elif f1 in ('P', 'I'):
        cu = r.get('CUSEDAMT', 0) or 0
        cc = r.get('CCURAMT', 0) or 0
        ud = cc - cu
        
        if lb in ('50', 'C2'):
            if ud > 0:
                ap = None
                if r.get('APPRDATE', 0) > 0:
                    try:
                        ap = datetime.strptime(str(int(r['APPRDATE'])).zfill(11)[:8], "%m%d%Y").date()
                    except:
                        pass
                adays = (rep_date - ap).days if ap else 0
                if adays <= 730:
                    rv = cu
                else:
                    rv = cu * (0.9 if lb == '50' else 0.8)
            else:
                rv = mv if mv > 0 else av
                if days >= 730:
                    rv *= 0.9 if lb == '50' else 0.8
        else:
            rv = mv
            if lt in (300, 301) and days >= 730:
                rv *= 0.9
    
    if lt == 521:
        rv = r.get('CURRBAL', 0) or 0
    if r.get('BORSTAT', '') == 'F':
        rv = 0
    
    r['REALISVL'] = rv
    return r

# =============================================================================
# REPORT
# =============================================================================
def write_report(df, title, fn, grp=None):
    if df.is_empty():
        return 0
    
    lines = [f"{'=' * 135}", title, f"{'=' * 135}", ""]
    
    if grp and grp in df.columns:
        df = df.sort(grp)
        cur = None
        tot = {}
        gt = {}
        
        for r in df.rows(named=True):
            g = r[grp]
            if g != cur:
                if cur:
                    lines.append(f"{'-' * 135}")
                    sub = f"SUBTOTAL FOR {grp}: {cur}"
                    for k in ['BALANCE', 'TOTOPEN', 'TOTIIS', 'TOTIISR', 'TOTWOF', 'TOTCLOSE']:
                        if k in tot:
                            sub += f"  {k}: {tot[k]:>15,.2f}"
                    lines.append(sub)
                    lines.append(f"{'-' * 135}")
                    lines.append("")
                cur = g
                tot = {}
            
            # Build detail line
            parts = []
            for c in ['BRANCH', 'ACCTNO', 'NOTENO', 'NAME', 'LOANTYPE', 'AGEING', 'CLASSNEW']:
                parts.append(str(r.get(c, '')))
            for c in ['BALANCE', 'TOTOPEN', 'TOTIIS', 'TOTIISR', 'TOTWOF', 'TOTCLOSE']:
                parts.append(f"{r.get(c, 0):>15,.2f}")
            lines.append(" ".join(parts))
            
            # Update totals
            for k in ['BALANCE', 'TOTOPEN', 'TOTIIS', 'TOTIISR', 'TOTWOF', 'TOTCLOSE']:
                if k in r:
                    val = r[k] or 0
                    tot[k] = tot.get(k, 0) + val
                    gt[k] = gt.get(k, 0) + val
    
    # Grand total
    lines.append(f"{'=' * 135}")
    lines.append("GRAND TOTAL")
    grand = ""
    for k in ['BALANCE', 'TOTOPEN', 'TOTIIS', 'TOTIISR', 'TOTWOF', 'TOTCLOSE']:
        if k in gt:
            grand += f"  {k}: {gt[k]:>15,.2f}"
    lines.append(grand)
    lines.append(f"{'=' * 135}")
    
    Path(fn).write_text('\n'.join(lines))
    return len(df)

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIBMIISA - IIS Monthly Movement")
    print("=" * 60)
    
    d = get_dates()
    print(f"Date: {d['rdate_word']}")
    
    # Load data
    print("\nLoading...")
    
    # SASDATA
    sas = pl.DataFrame()
    for p in [f"{PATHS['SASDATA']}LOAN{d['reptmon']}4.parquet", 
              f"{PATHS['ISASDATA']}LOAN{d['reptmon']}4.parquet"]:
        if Path(p).exists():
            df = pl.read_parquet(p)
            if 'BALANCE' in df.columns:
                df_sel = df.select(['ACCTNO', 'NOTENO', 'BALANCE']).unique()
                sas = pl.concat([sas, df_sel]) if not sas.is_empty() else df_sel
    
    # Previous NPL
    prev_path = f"{PATHS['NPL']}TOTIIS{d['prevmON']}.parquet"
    if Path(prev_path).exists():
        prev = pl.read_parquet(prev_path)
        prev = prev.rename({'IISCLOSE': 'PREVIIS', 'OICLOSE': 'PREVOI'})
        prev = prev.select(['ACCTNO', 'NOTENO', 'PREVIIS', 'PREVOI']).unique()
    else:
        prev = pl.DataFrame()
    
    # LNNOTE
    ln = pl.DataFrame()
    for p in [f"{PATHS['LOAN']}LNNOTE.parquet", f"{PATHS['LOAN']}ILOAN.LNNOTE.parquet"]:
        if Path(p).exists():
            df = pl.read_parquet(p)
            ln = pl.concat([ln, df]) if not ln.is_empty() else df
    
    if not ln.is_empty():
        ln = ln.filter(
            (pl.col('REVERSED') != 'Y') & 
            pl.col('NOTENO').is_not_null()
        ).unique(['ACCTNO', 'NOTENO'])
    
    # Previous loan
    prv = pl.DataFrame()
    for p in [f"{PATHS['PRVLOAN']}LNNOTE.parquet", f"{PATHS['PRVLOAN']}IPRVLOAN.LNNOTE.parquet"]:
        if Path(p).exists():
            df = pl.read_parquet(p)
            prv = pl.concat([prv, df]) if not prv.is_empty() else df
    
    if not prv.is_empty():
        prv = prv.filter(
            (pl.col('REVERSED') != 'Y') & 
            pl.col('NOTENO').is_not_null()
        ).select(['ACCTNO', 'NOTENO', 'CURBAL', 'REBATE']).unique()
        prv = prv.rename({'CURBAL': 'PREVBAL', 'REBATE': 'PRVREB'})
    
    # IISCLOSE
    icl_path = f"{PATHS['NPL']}IISCLOSE{d['reptmon']}4.parquet"
    icl = pl.read_parquet(icl_path) if Path(icl_path).exists() else pl.DataFrame()
    if not icl.is_empty() and 'BRANCH' in icl.columns:
        icl = icl.drop('BRANCH')
    
    # IIS
    iis_path = f"{PATHS['NPL']}IIS{d['reptmon']}.parquet"
    if not Path(iis_path).exists():
        print("ERROR: No IIS data")
        return
    
    iis = pl.read_parquet(iis_path)
    iis = iis.filter(pl.col('LOANTYPE') < 981).unique(['ACCTNO', 'NOTENO'])
    
    # ADONIIS
    ado_path = f"{PATHS['NPL']}ADONIIS.parquet"
    ado = pl.read_parquet(ado_path) if Path(ado_path).exists() else pl.DataFrame()
    
    # LNCOMM
    lc = pl.DataFrame()
    for p in [f"{PATHS['LOAN']}LNCOMM.parquet", f"{PATHS['LOAN']}ILOAN.LNCOMM.parquet"]:
        if Path(p).exists():
            df = pl.read_parquet(p)
            lc = pl.concat([lc, df]) if not lc.is_empty() else df
    
    if not lc.is_empty():
        lc = lc.select(['ACCTNO', 'COMMNO', 'CCURAMT', 'CUSEDAMT']).unique(['ACCTNO', 'COMMNO'])
    
    print(f"  IIS: {len(iis)}")
    
    # Merge
    print("\nMerging...")
    if not ln.is_empty():
        if not prv.is_empty():
            ln = ln.join(prv, on=['ACCTNO', 'NOTENO'], how='left')
        if not icl.is_empty():
            ln = ln.join(icl, on=['ACCTNO', 'NOTENO'], how='left')
        iis = iis.join(ln, on=['ACCTNO', 'NOTENO'], how='left')
    
    if not prev.is_empty():
        iis = iis.join(prev, on=['ACCTNO', 'NOTENO'], how='left')
    
    if not ado.is_empty():
        iis = iis.join(ado, on=['ACCTNO', 'NOTENO'], how='left')
    
    # Calculate IIS
    print("\nCalculating IIS...")
    rows = []
    for r in iis.rows(named=True):
        rows.append(calc_iis(r, d['reptdate']))
    iis = pl.DataFrame(rows)
    print(f"  Done: {len(iis)} records")
    
    # Realisable value
    print("\nCalculating realisable...")
    if not lc.is_empty() and not iis.is_empty() and not ln.is_empty():
        ln2 = ln.sort(['ACCTNO', 'COMMNO'])
        lc = lc.sort(['ACCTNO', 'COMMNO'])
        ln2 = ln2.join(lc, on=['ACCTNO', 'COMMNO'], how='left')
        ln2 = ln2.with_columns(
            (pl.col('CCURAMT').fill_null(0) - pl.col('CUSEDAMT').fill_null(0)).alias('UNDRAWN')
        )
        
        big = iis.filter(pl.col('ACCTNO') > 8000000000).sort(['ACCTNO', 'NOTENO'])
        if not big.is_empty():
            ln2 = ln2.sort(['ACCTNO', 'NOTENO'])
            big = big.join(ln2, on=['ACCTNO', 'NOTENO'], how='left')
            
            rv_rows = []
            for r in big.rows(named=True):
                rv_rows.append(calc_realisable(r, d['reptdate']))
            
            if rv_rows:
                rv_df = pl.DataFrame(rv_rows).select(['ACCTNO', 'NOTENO', 'REALISVL'])
                iis = iis.join(rv_df, on=['ACCTNO', 'NOTENO'], how='left')
                print(f"  Realisable: {len(rv_df)} records")
    
    # Generate reports
    print("\nGenerating reports...")
    
    report_configs = [
        ('R1', (~pl.col('LOANTYPE').is_in(CGC)) & pl.col('NPLIND').is_in(['N', 'O', 'P']),
         "MONTHLY MOVEMENT OF INTEREST-IN-SUSPENSE", None),
        ('R2', pl.col('LOANTYPE').is_in(CGC) & pl.col('NPLIND').is_in(['N', 'O', 'P']),
         "MONTHLY MOVEMENT FOR CGC/TUK LOANS", None),
        ('R3', (~pl.col('LOANTYPE').is_in(CGC)) & pl.col('NPLIND').is_in(['E', 'N', 'O', 'P']),
         "MONTHLY MOVEMENT OF IIS FOR NON-PERFORMING TL/FL/HL/RC", 'BRANCH'),
        ('R4', pl.col('LOANTYPE').is_in(CGC) & pl.col('NPLIND').is_in(['E', 'N', 'O', 'P']),
         "MONTHLY MOVEMENT FOR NON-PERFORMING CGC/TUK LOANS", 'BRANCH')
    ]
    
    for name, filt, title, grp in report_configs:
        df = iis.filter(filt)
        write_report(df, title, f"{PATHS['OUTPUT']}IIS_{name}.txt", grp)
        print(f"  {name}: {len(df)} records")
    
    # Split by cost centre
    print("\nSplitting by cost centre...")
    if 'COSTCTR' in iis.columns:
        conv = iis.filter(
            (CONV(pl.col('COSTCTR'))) | pl.col('LOANTYPE').is_in(CGC)
        ).unique(['ACCTNO', 'NOTENO'])
        
        islm = iis.filter(
            ISLM(pl.col('COSTCTR')) & ~pl.col('LOANTYPE').is_in(CGC)
        ).unique(['ACCTNO', 'NOTENO'])
        
        if not conv.is_empty():
            conv.write_parquet(f"{PATHS['NPL']}TOTIIS{d['reptmon']}.parquet")
        if not islm.is_empty():
            islm.write_parquet(f"{PATHS['NPL']}NPLI.TOTIIS{d['reptmon']}.parquet")
        
        print(f"  Conv: {len(conv)}, Islm: {len(islm)}")
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    if not iis.is_empty():
        total_bal = iis['BALANCE'].sum() if 'BALANCE' in iis.columns else 0
        print(f"\nTotal Balance: RM {total_bal:,.2f}")
        
        for f in ['TOTIIS', 'TOTIISR', 'TOTWOF']:
            if f in iis.columns:
                print(f"Total {f}: RM {iis[f].sum():,.2f}")
        
        if 'REALISVL' in iis.columns and not iis['REALISVL'].is_null().all():
            print(f"Total Realisable: RM {iis['REALISVL'].sum():,.2f}")
    
    print("\n" + "=" * 60)
    print("✓ EIBMIISA Complete")

if __name__ == "__main__":
    main()
