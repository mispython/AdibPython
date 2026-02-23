"""
EIBMIISA - Monthly Movement of Interest-In-Suspense (IIS)
Comprehensive IIS reporting for NPL accounts including opening/closing balances,
IIS recognized, recovered, written-off, and realisable value calculation.
"""

import polars as pl
from datetime import datetime
from pathlib import Path
from collections import defaultdict

# =============================================================================
# CONFIG
# =============================================================================
PATHS = {
    'LOAN': 'data/loan/', 'NPL': 'data/npl/', 'PRVLOAN': 'data/prvloan/',
    'SASDATA': 'data/sasdata/', 'ISASDATA': 'data/isasdata/', 'OUTPUT': 'output/'
}
for p in PATHS.values(): Path(p).mkdir(exist_ok=True)

# Risk format
RISK_FMT = {'1': 'SS1', '2': 'SS2', '3': 'D  ', '4': 'B  '}

# Cost centre rules
CONV = lambda x: (x < 3000 or x > 3999) and x not in [4043, 4048]
ISLM = lambda x: (3000 <= x <= 3999) or x in [4043, 4048]

CGC_TYPES = [517, 521, 522, 523, 528]

# =============================================================================
# DATE
# =============================================================================
def get_dates():
    df = pl.read_parquet(f"{PATHS['LOAN']}REPTDATE.parquet")
    reptdate = df['REPTDATE'][0]
    prev_month = reptdate.month - 1 or 12
    return {
        'reptdate': reptdate, 'reptmon': f"{reptdate.month:02d}", 'prevmON': f"{prev_month:02d}",
        'rdate_word': reptdate.strftime("%d %B %Y"), 'rdate_dmy': reptdate.strftime("%d/%m/%Y"),
        'rptyr': str(reptdate.year), 'rptdy': f"{reptdate.day:02d}"
    }

# =============================================================================
# IIS CALCULATION ENGINE
# =============================================================================
def calc_iis(r, rep_date):
    """Core IIS calculation - exact SAS logic"""
    r = r.copy()
    
    # Ageing
    if r.get('BLDATE', 0) > 0:
        try:
            bd = datetime.strptime(str(int(r['BLDATE'])).zfill(8), "%m%d%Y").date()
            r['AGEING'] = (rep_date - bd).days
        except: pass
    if r.get('OLDNOTEDAYARR', 0) > 0 and 98000 <= r.get('NOTENO', 0) <= 98999:
        r['AGEING'] = max(0, r.get('AGEING', 0)) + r['OLDNOTEDAYARR']
    
    # Risk
    r['CLASSNEW'] = RISK_FMT.get(str(r.get('RISKRATE', '')).strip(), ' ')
    
    # Branch
    nb = r.get('NTBRCH', 0)
    r['BRANCH'] = f"{int(nb):03d} {nb}"
    
    # Diff balance
    pv = r.get('PREVBAL', 0) or 0
    cb = r.get('CURBAL', 0) or 0
    sw = r.get('SPWOF', 0) or 0
    iw = r.get('IISW', 0) or 0
    ad = r.get('ADJUST', 0) or 0
    r['DIFFBAL'] = pv - cb - sw - iw - ad
    
    # Previous IIS/OI
    r['IISP'] = r.get('PREVIIS', 0) or 0
    r['OIP'] = r.get('PREVOI', 0) or 0
    
    n = r.get('NPLIND', '')
    lt = r.get('LOANTYPE', 0)
    lc = r.get('LSTTRNCD', 0)
    ir = r.get('IISR', 0) or 0
    iw = r.get('IISW', 0) or 0
    nt = r.get('NTINT', '')
    
    # LSTTRNAM
    r['LSTTRNAM'] = (r.get('LSTTRNAM', 0) / 100000) if lt in CGC_TYPES and n == 'N' and lc == 166 else 0
    
    # ACCRUAL (NTINT='A')
    if nt == 'A':
        if n in ('O','P'): r['IIS'] = ir + iw - r['IISP']
        if r.get('IISCLS', 0): r['IISP'] = r['IISCLS'] - r['OIP']
        if n in ('O','P'): r['IIS'] = ir + iw - r['IISP']
        r['IIS'] = r.get('ADONIIS', 0) or 0
        
        oi = (r.get('FEEASSES',0)or0) - (r.get('FEEWAIVE',0)or0)
        if oi < 0: oi = 0
        oir = r.get('FEEPAY',0)or0
        b = r.get('BORSTAT', '')
        
        if b in ('W','X') and n != 'P':
            r['IIS'] = 0
            oi = r.get('FEELWTOT',0)or0
        
        if (n == 'E' and b == 'P') or lc == 652:
            ir = 0
        elif r.get('LOANSTAT',0) == 1 or r['DIFFBAL'] >= r['IISP']:
            ir = r['IISP'] - iw
        elif r['DIFFBAL'] < r['IISP']:
            ir = r['DIFFBAL'] - iw
        
        if n == 'O':
            r['IIS'] = 0
            oir = (r.get('FEEPAY',0)or0) + (r.get('FEELWTOT',0)or0)
        
        if ir < 0: ir = 0
        r['IISR'] = ir
        
        if n == 'P':
            r['IIS'] = r['IIS'] + ir + iw - r['IISP']
        
        r['IISCLOSE'] = r['IISP'] + r['IIS'] - ir - iw
        if r['IISCLOSE'] < 0: r['IISCLOSE'] = 0
        
        r['OICLOSE'] = 0 if n == 'O' else (r.get('FEELWTOT',0)or0)
        r['TOTOPEN'] = r['IISP'] + r['OIP']
        r['TOTIIS'] = r['IIS'] + oi
        r['TOTIISR'] = ir + oir
        r['TOTWOF'] = iw
        
        if n == 'N':
            r['TOTIIS'] = r['IISCLOSE'] + r['TOTIISR'] + r['TOTWOF']
            r['TOTOPEN'] = 0
        
        r['TOTCLOSE'] = r['IISCLOSE'] + r['OICLOSE']
        if b in ('W','X') and n != 'P':
            r['TOTCLOSE'] = r['TOTOPEN'] - r['TOTIISR'] - r['TOTWOF']
    
    # NON-ACCRUAL
    else:
        if n == 'O':
            ac = r.get('ACTIACCR',0)or0
            if ac == 0:
                ia = r.get('INTACTIV',0)or0
                if ia < 0:
                    ir = ir + (r.get('FEELWTOT',0)or0) - ia
                else:
                    ir = ir + (r.get('FEELWTOT',0)or0)
                r['IIS'] = ir + iw - r['IISP']
            else:
                r['IIS'] = ir + iw + ac - r['IISP']
                r['OIREC'] = (r.get('FEEPAY',0)or0) + (r.get('FEELWTOT',0)or0)
        
        r['TOTOPEN'] = r['IISP']
        r['TOTIISR'] = ir
        r['TOTWOF'] = iw
        r['TOTIIS'] = r['IIS']
        
        if n == 'N':
            ic = r.get('IISCLOSE',0)or0
            r['TOTIIS'] = ic + r['TOTIISR'] + r['TOTWOF']
            r['TOTOPEN'] = 0
        
        if n == 'O':
            r['TOTCLOSE'] = r['IISP'] + r['IIS'] - ir - iw
        else:
            r['TOTCLOSE'] = r.get('IISCLOSE',0)or0
    
    return r

# =============================================================================
# REALISABLE VALUE (%SPRO)
# =============================================================================
def calc_realisable(r, rep_date):
    """Calculate realisable value for collateral"""
    r = r.copy()
    
    # Last update date
    last = None
    for f in ['DLIVRYDT', 'APPRDATE', 'ISSUEDT']:
        if r.get(f, 0) > 0:
            try:
                last = datetime.strptime(str(int(r[f])).zfill(11)[:8], "%m%d%Y").date()
                break
            except: pass
    
    days = (rep_date - last).days if last else 0
    f1 = r.get('FLAG1', '')
    lb = r.get('LIABCODE', '')
    mv = r.get('MARKETVL',0) or 0
    av = r.get('APPVALUE',0) or 0
    lt = r.get('LOANTYPE', 0)
    
    # FLAG1 = 'F'
    if f1 == 'F':
        if lb in ('50','C2'):
            if mv > 0 and av > 0:
                rv = mv if mv == av else (mv if r.get('DLIVRYDT',0)>0 else min(mv,av))
                if days >= 730: rv *= 0.9 if lb == '50' else 0.8
            elif mv > 0: rv = mv
            elif av > 0: rv = av
            if days >= 730 and mv > 0 and av == 0: rv *= 0.9 if lb == '50' else 0.8
            if days >= 730 and mv == 0 and av > 0: rv *= 0.9 if lb == '50' else 0.8
        
        elif lb in ('B8','B9','33','37','34','32','C1'):
            if mv > 0 and av > 0:
                rv = mv if mv == av else (mv if r.get('DLIVRYDT',0)>0 else min(mv,av))
                if days >= 730: rv *= 0.75
            elif mv > 0: rv = mv
            elif av > 0: rv = av
            if days >= 730 and (mv > 0 or av > 0): rv *= 0.75
        
        else:
            rv = mv
            if lt in (300,301) and days >= 730: rv *= 0.9
    
    # FLAG1 in ('P','I')
    elif f1 in ('P','I'):
        cu = r.get('CUSEDAMT',0) or 0
        cc = r.get('CCURAMT',0) or 0
        ud = cc - cu
        
        if lb in ('50','C2'):
            if ud > 0:
                ap = None
                if r.get('APPRDATE',0) > 0:
                    try: ap = datetime.strptime(str(int(r['APPRDATE'])).zfill(11)[:8], "%m%d%Y").date()
                    except: pass
                adays = (rep_date - ap).days if ap else 0
                if adays <= 730: rv = cu
                else: rv = cu * (0.9 if lb == '50' else 0.8)
            else:
                if mv > 0: rv = mv
                else: rv = av
                if days >= 730: rv *= 0.9 if lb == '50' else 0.8
        else:
            rv = mv
            if lt in (300,301) and days >= 730: rv *= 0.9
    
    else: rv = 0
    
    # Overrides
    if lt == 521: rv = r.get('CURRBAL',0) or 0
    if r.get('BORSTAT','') == 'F': rv = 0
    
    r['REALISVL'] = rv
    return r

# =============================================================================
# REPORT GENERATOR
# =============================================================================
def write_report(df, title, footer, fn, grp=None, cols=None):
    """Generate formatted report"""
    if df.is_empty(): return 0
    
    lines = [f"{'='*135}", title, f"{'='*135}", ""]
    
    if grp:
        df = df.sort(grp)
        cur = None
        tot = defaultdict(float)
        gt = defaultdict(float)
        
        for r in df.rows(named=True):
            gv = r[grp]
            if gv != cur:
                if cur:
                    lines += [f"{'-'*135}", f"SUBTOTAL FOR {grp}: {cur}"]
                    for k in ['BALANCE','TOTOPEN','TOTIIS','TOTIISR','TOTWOF','TOTCLOSE']:
                        if k in r: lines[-1] += f"  {k}: {tot[k]:>15,.2f}"
                    lines += [f"{'-'*135}", ""]
                    tot = defaultdict(float)
                cur = gv
            
            # Detail line
            if cols:
                line = ""
                for c in cols:
                    if c == 'NAME': line += f"{str(r.get(c,''))[:30]:<30} "
                    elif c in ['BALANCE','TOTOPEN','TOTIIS','TOTIISR','TOTWOF','TOTCLOSE','LSTTRNAM']:
                        line += f"{r.get(c,0):>15,.2f} "
                    else:
                        line += f"{r.get(c,'')} "
                lines.append(line)
            
            # Update totals
            for k in tot:
                if k in r: tot[k] += r.get(k,0)
                gt[k] += r.get(k,0)
    
    Path(fn).write_text('\n'.join(lines))
    return len(df)

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("="*60+"\nEIBMIISA - IIS Monthly Movement\n"+"="*60)
    d = get_dates()
    print(f"Date: {d['rdate_word']}")
    
    # Load all data
    print("\nLoading...")
    
    # SASDATA
    sas = pl.DataFrame()
    for p in [f"{PATHS['SASDATA']}LOAN{d['reptmon']}4.parquet", f"{PATHS['ISASDATA']}LOAN{d['reptmon']}4.parquet"]:
        if Path(p).exists():
            df = pl.read_parquet(p)
            if 'BALANCE' in df.columns:
                sas = pl.concat([sas, df.select(['ACCTNO','NOTENO','BALANCE']).unique()]) if not sas.is_empty() else df.select(['ACCTNO','NOTENO','BALANCE']).unique()
    
    # Previous NPL
    prev = pl.read_parquet(f"{PATHS['NPL']}TOTIIS{d['prevmON']}.parquet") if Path(f"{PATHS['NPL']}TOTIIS{d['prevmON']}.parquet").exists() else pl.DataFrame()
    if not prev.is_empty():
        prev = prev.rename({'IISCLOSE':'PREVIIS','OICLOSE':'PREVOI'}).select(['ACCTNO','NOTENO','PREVIIS','PREVOI']).unique()
    
    # LNNOTE
    ln = pl.DataFrame()
    for p in [f"{PATHS['LOAN']}LNNOTE.parquet", f"{PATHS['LOAN']}ILOAN.LNNOTE.parquet"]:
        if Path(p).exists():
            df = pl.read_parquet(p)
            ln = pl.concat([ln, df]) if not ln.is_empty() else df
    if not ln.is_empty():
        ln = ln.filter((pl.col('REVERSED')!='Y') & pl.col('NOTENO').is_not_null()).unique(['ACCTNO','NOTENO'])
    
    # Previous loan
    prv = pl.DataFrame()
    for p in [f"{PATHS['PRVLOAN']}LNNOTE.parquet", f"{PATHS['PRVLOAN']}IPRVLOAN.LNNOTE.parquet"]:
        if Path(p).exists():
            df = pl.read_parquet(p)
            prv = pl.concat([prv, df]) if not prv.is_empty() else df
    if not prv.is_empty():
        prv = prv.filter((pl.col('REVERSED')!='Y') & pl.col('NOTENO').is_not_null()).select(['ACCTNO','NOTENO','CURBAL','REBATE']).unique()
        prv = prv.rename({'CURBAL':'PREVBAL','REBATE':'PRVREB'})
    
    # IISCLOSE
    icl = pl.read_parquet(f"{PATHS['NPL']}IISCLOSE{d['reptmon']}4.parquet") if Path(f"{PATHS['NPL']}IISCLOSE{d['reptmon']}4.parquet").exists() else pl.DataFrame()
    if not icl.is_empty() and 'BRANCH' in icl.columns: icl = icl.drop('BRANCH')
    
    # IIS
    iis = pl.read_parquet(f"{PATHS['NPL']}IIS{d['reptmon']}.parquet") if Path(f"{PATHS['NPL']}IIS{d['reptmon']}.parquet").exists() else pl.DataFrame()
    if iis.is_empty():
        print("ERROR: No IIS data")
        return
    iis = iis.filter(pl.col('LOANTYPE') < 981).unique(['ACCTNO','NOTENO'])
    
    # ADONIIS
    ado = pl.read_parquet(f"{PATHS['NPL']}ADONIIS.parquet") if Path(f"{PATHS['NPL']}ADONIIS.parquet").exists() else pl.DataFrame()
    
    # LNCOMM
    lc = pl.DataFrame()
    for p in [f"{PATHS['LOAN']}LNCOMM.parquet", f"{PATHS['LOAN']}ILOAN.LNCOMM.parquet"]:
        if Path(p).exists():
            df = pl.read_parquet(p)
            lc = pl.concat([lc, df]) if not lc.is_empty() else df
    if not lc.is_empty():
        lc = lc.select(['ACCTNO','COMMNO','CCURAMT','CUSEDAMT']).unique(['ACCTNO','COMMNO'])
    
    print(f"  IIS: {len(iis)}")
    
    # Merge
    print("\nMerging...")
    ln = ln.join(prv, on=['ACCTNO','NOTENO'], how='left')
    if not icl.is_empty(): ln = ln.join(icl, on=['ACCTNO','NOTENO'], how='left')
    iis = iis.join(ln, on=['ACCTNO','NOTENO'], how='left')
    if not prev.is_empty(): iis = iis.join(prev, on=['ACCTNO','NOTENO'], how='left')
    if not ado.is_empty(): iis = iis.join(ado, on=['ACCTNO','NOTENO'], how='left')
    
    # Calculate IIS
    print("\nCalculating IIS...")
    res = [calc_iis(r, d['reptdate']) for r in iis.rows(named=True)]
    iis = pl.DataFrame(res)
    print(f"  Done: {len(iis)} records")
    
    # Realisable value
    print("\nCalculating realisable values...")
    if not lc.is_empty() and not iis.is_empty():
        # Merge with LNCOMM
        ln2 = ln.sort(['ACCTNO','COMMNO'])
        lc = lc.sort(['ACCTNO','COMMNO'])
        ln2 = ln2.join(lc, on=['ACCTNO','COMMNO'], how='left')
        ln2 = ln2.with_columns([(pl.col('CCURAMT').fill_null(0) - pl.col('CUSEDAMT').fill_null(0)).alias('UNDRAWN')])
        
        # Large accounts
        big = iis.filter(pl.col('ACCTNO') > 8000000000).sort(['ACCTNO','NOTENO'])
        if not big.is_empty():
            ln2 = ln2.sort(['ACCTNO','NOTENO'])
            big = big.join(ln2, on=['ACCTNO','NOTENO'], how='left')
            rv = [calc_realisable(r, d['reptdate']) for r in big.rows(named=True)]
            rv_df = pl.DataFrame(rv).select(['ACCTNO','NOTENO','REALISVL'])
            iis = iis.join(rv_df, on=['ACCTNO','NOTENO'], how='left')
            print(f"  Realisable: {len(rv_df)} records")
    
    # Generate reports
    print("\nGenerating reports...")
    
    # Report 1: Newly surfaced / Normalised / Fully settled (non-CGC)
    w1 = iis.filter(~pl.col('LOANTYPE').is_in(CGC_TYPES) & pl.col('NPLIND').is_in(['N','O','P']))
    write_report(w1, "MONTHLY MOVEMENT OF INTEREST-IN-SUSPENSE",
                "NPL: N-NEWLY SURFACED, O-NORMALIZE, P-SETTLE",
                f"{PATHS['OUTPUT']}IIS_R1.txt")
    print(f"  R1: {len(w1)} records")
    
    # Report 2: CGC/TUK loans
    w2 = iis.filter(pl.col('LOANTYPE').is_in(CGC_TYPES) & pl.col('NPLIND').is_in(['N','O','P']))
    write_report(w2, "MONTHLY MOVEMENT FOR CGC/TUK LOANS",
                "NPL: N-NEWLY SURFACED, O-NORMALIZE, P-SETTLE",
                f"{PATHS['OUTPUT']}IIS_R2.txt")
    print(f"  R2: {len(w2)} records")
    
    # Report 3: All NPL by branch (non-CGC)
    w3 = iis.filter(~pl.col('LOANTYPE').is_in(CGC_TYPES) & pl.col('NPLIND').is_in(['E','N','O','P']))
    write_report(w3, "MONTHLY MOVEMENT OF IIS FOR NON-PERFORMING TL/FL/HL/RC",
                "NPL: E-EXISTING, N-NEWLY SURFACED, O-NORMALIZE, P-SETTLE",
                f"{PATHS['OUTPUT']}IIS_R3.txt", grp='BRANCH')
    print(f"  R3: {len(w3)} records")
    
    # Report 4: CGC/TUK by branch
    w4 = iis.filter(pl.col('LOANTYPE').is_in(CGC_TYPES) & pl.col('NPLIND').is_in(['E','N','O','P']))
    write_report(w4, "MONTHLY MOVEMENT FOR NON-PERFORMING CGC/TUK LOANS",
                "NPL: E-EXISTING, N-NEWLY SURFACED, O-NORMALIZE, P-SETTLE",
                f"{PATHS['OUTPUT']}IIS_R4.txt", grp='BRANCH')
    print(f"  R4: {len(w4)} records")
    
    # Split by cost centre
    print("\nSplitting by cost centre...")
    if 'COSTCTR' in iis.columns:
        conv = iis.filter((CONV(pl.col('COSTCTR'))) | pl.col('LOANTYPE').is_in(CGC_TYPES)).unique(['ACCTNO','NOTENO'])
        islm = iis.filter(ISLM(pl.col('COSTCTR')) & ~pl.col('LOANTYPE').is_in(CGC_TYPES)).unique(['ACCTNO','NOTENO'])
        
        conv.write_parquet(f"{PATHS['NPL']}TOTIIS{d['reptmon']}.parquet")
        islm.write_parquet(f"{PATHS['NPL']}NPLI.TOTIIS{d['reptmon']}.parquet")
        print(f"  Conv: {len(conv)}, Islm: {len(islm)}")
    
    # Summary
    print("\n"+"="*60+"\nSUMMARY\n"+"="*60)
    if not iis.is_empty():
        print(f"\nTotal Balance: RM {iis['BALANCE'].sum():,.2f}")
        if 'TOTIIS' in iis.columns: print(f"Total IIS: RM {iis['TOTIIS'].sum():,.2f}")
        if 'TOTIISR' in iis.columns: print(f"Total IISR: RM {iis['TOTIISR'].sum():,.2f}")
        if 'TOTWOF' in iis.columns: print(f"Total WOFF: RM {iis['TOTWOF'].sum():,.2f}")
        if 'REALISVL' in iis.columns: print(f"Total Realisable: RM {iis['REALISVL'].sum():,.2f}")
    
    print("\n"+"="*60+"\n✓ EIBMIISA Complete")

if __name__ == "__main__":
    main()
