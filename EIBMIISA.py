"""
EIBMIISA - Monthly Movement of Interest-In-Suspense (IIS)
"""

import polars as pl
from datetime import datetime
from pathlib import Path

# =============================================================================
# CONFIG
# =============================================================================
PATHS = {k: f'data/{v}/' for k,v in {
    'LOAN':'loan', 'NPL':'npl', 'PRVLOAN':'prvloan',
    'SASDATA':'sasdata', 'ISASDATA':'isasdata', 'OUTPUT':'output'
}.items()}
for p in PATHS.values(): Path(p).mkdir(exist_ok=True)

RISK_FMT = {'1':'SS1','2':'SS2','3':'D  ','4':'B  '}
CONV = lambda x: (x<3000 or x>3999) and x not in [4043,4048]
ISLM = lambda x: (3000<=x<=3999) or x in [4043,4048]
CGC = [517,521,522,523,528]

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
    if r.get('BLDATE',0)>0:
        try: r['AGEING'] = (rep_date - datetime.strptime(str(int(r['BLDATE'])).zfill(8),"%m%d%Y").date()).days
        except: r['AGEING'] = 0
    if r.get('OLDNOTEDAYARR',0)>0 and 98000<=r.get('NOTENO',0)<=98999:
        r['AGEING'] = max(0,r.get('AGEING',0)) + r['OLDNOTEDAYARR']
    
    r['CLASSNEW'] = RISK_FMT.get(str(r.get('RISKRATE','')).strip(), ' ')
    r['BRANCH'] = f"{int(r.get('NTBRCH',0)):03d} {r.get('NTBRCH',0)}"
    
    pv, cb = r.get('PREVBAL',0)or0, r.get('CURBAL',0)or0
    sw, iw, ad = r.get('SPWOF',0)or0, r.get('IISW',0)or0, r.get('ADJUST',0)or0
    r['DIFFBAL'] = pv - cb - sw - iw - ad
    r['IISP'], r['OIP'] = r.get('PREVIIS',0)or0, r.get('PREVOI',0)or0
    
    n, lt, lc = r.get('NPLIND',''), r.get('LOANTYPE',0), r.get('LSTTRNCD',0)
    ir, iw, nt = r.get('IISR',0)or0, r.get('IISW',0)or0, r.get('NTINT','')
    r['LSTTRNAM'] = (r.get('LSTTRNAM',0)/100000) if lt in CGC and n=='N' and lc==166 else 0
    
    if nt == 'A':  # Accrual
        if n in ('O','P'): r['IIS'] = ir + iw - r['IISP']
        if r.get('IISCLS',0): r['IISP'] = r['IISCLS'] - r['OIP']
        if n in ('O','P'): r['IIS'] = ir + iw - r['IISP']
        r['IIS'] = r.get('ADONIIS',0) or 0
        
        oi = max(0, (r.get('FEEASSES',0)or0) - (r.get('FEEWAIVE',0)or0))
        oir, b = (r.get('FEEPAY',0)or0), r.get('BORSTAT','')
        
        if b in ('W','X') and n != 'P':
            r['IIS'], oi = 0, r.get('FEELWTOT',0)or0
        
        if (n=='E' and b=='P') or lc==652: ir = 0
        elif r.get('LOANSTAT',0)==1 or r['DIFFBAL']>=r['IISP']: ir = r['IISP'] - iw
        elif r['DIFFBAL'] < r['IISP']: ir = r['DIFFBAL'] - iw
        
        if n == 'O':
            r['IIS'] = 0
            oir = (r.get('FEEPAY',0)or0) + (r.get('FEELWTOT',0)or0)
        
        r['IISR'] = max(0, ir)
        if n == 'P': r['IIS'] = r['IIS'] + ir + iw - r['IISP']
        
        r['IISCLOSE'] = max(0, r['IISP'] + r['IIS'] - ir - iw)
        r['OICLOSE'] = 0 if n=='O' else (r.get('FEELWTOT',0)or0)
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
    
    else:  # Non-accrual
        if n == 'O':
            ac = r.get('ACTIACCR',0)or0
            if ac == 0:
                ia = r.get('INTACTIV',0)or0
                fw = r.get('FEELWTOT',0)or0
                ir = ir + fw - ia if ia<0 else ir + fw
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
        
        r['TOTCLOSE'] = (r['IISP'] + r['IIS'] - ir - iw) if n=='O' else (r.get('IISCLOSE',0)or0)
    
    return r

# =============================================================================
# REALISABLE VALUE
# =============================================================================
def calc_realisable(r, rep_date):
    r = r.copy()
    last = None
    for f in ['DLIVRYDT','APPRDATE','ISSUEDT']:
        if r.get(f,0) and r[f]>0:
            try: last = datetime.strptime(str(int(r[f])).zfill(11)[:8], "%m%d%Y").date(); break
            except: continue
    
    days = (rep_date - last).days if last else 0
    f1, lb = r.get('FLAG1',''), r.get('LIABCODE','')
    mv, av, lt = (r.get('MARKETVL',0)or0), (r.get('APPVALUE',0)or0), r.get('LOANTYPE',0)
    rv = 0
    
    if f1 == 'F':
        if lb in ('50','C2'):
            if mv>0 and av>0:
                rv = mv if mv==av else (mv if r.get('DLIVRYDT',0)>0 else min(mv,av))
                if days>=730: rv *= 0.9 if lb=='50' else 0.8
            elif mv>0: rv = mv
            elif av>0: rv = av
            if days>=730 and (mv>0 or av>0): rv *= 0.9 if lb=='50' else 0.8
        elif lb in ('B8','B9','33','37','34','32','C1'):
            if mv>0 and av>0:
                rv = mv if mv==av else (mv if r.get('DLIVRYDT',0)>0 else min(mv,av))
                if days>=730: rv *= 0.75
            elif mv>0: rv = mv
            elif av>0: rv = av
            if days>=730 and (mv>0 or av>0): rv *= 0.75
        else:
            rv = mv
            if lt in (300,301) and days>=730: rv *= 0.9
    
    elif f1 in ('P','I'):
        cu, cc = (r.get('CUSEDAMT',0)or0), (r.get('CCURAMT',0)or0)
        ud = cc - cu
        if lb in ('50','C2'):
            if ud>0:
                ap = None
                if r.get('APPRDATE',0)>0:
                    try: ap = datetime.strptime(str(int(r['APPRDATE'])).zfill(11)[:8], "%m%d%Y").date()
                    except: pass
                adays = (rep_date - ap).days if ap else 0
                rv = cu if adays<=730 else cu*(0.9 if lb=='50' else 0.8)
            else:
                rv = mv if mv>0 else av
                if days>=730: rv *= 0.9 if lb=='50' else 0.8
        else:
            rv = mv
            if lt in (300,301) and days>=730: rv *= 0.9
    
    if lt == 521: rv = r.get('CURRBAL',0) or 0
    if r.get('BORSTAT','') == 'F': rv = 0
    
    r['REALISVL'] = rv
    return r

# =============================================================================
# REPORT
# =============================================================================
def write_report(df, title, fn, grp=None):
    if df.is_empty(): return 0
    lines = [f"{'='*135}", title, f"{'='*135}", ""]
    if grp and grp in df.columns:
        df = df.sort(grp)
        cur, tot, gt = None, {}, {}
        for r in df.rows(named=True):
            g = r[grp]
            if g != cur:
                if cur:
                    lines += [f"{'-'*135}", f"SUBTOTAL FOR {grp}: {cur}"]
                    for k in ['BALANCE','TOTOPEN','TOTIIS','TOTIISR','TOTWOF','TOTCLOSE']:
                        if k in tot: lines[-1] += f"  {k}: {tot[k]:>15,.2f}"
                    lines += [f"{'-'*135}", ""]
                cur, tot = g, {}
            lines.append(" ".join([f"{r.get(c,'')}" for c in ['BRANCH','ACCTNO','NOTENO','NAME','LOANTYPE','AGEING','CLASSNEW']] +
                                 [f"{r.get(c,0):>15,.2f}" for c in ['BALANCE','TOTOPEN','TOTIIS','TOTIISR','TOTWOF','TOTCLOSE']]))
            for k in tot: 
                if k in r: tot[k] = tot.get(k,0) + (r[k] or 0); gt[k] = gt.get(k,0) + (r[k] or 0)
    
    lines += [f"{'='*135}", "GRAND TOTAL"] + [f"  {k}: {gt[k]:>15,.2f}" for k in gt if k in gt] + [f"{'='*135}"]
    Path(fn).write_text('\n'.join(lines))
    return len(df)

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("="*60+"\nEIBMIISA - IIS Monthly Movement\n"+"="*60)
    d = get_dates()
    print(f"Date: {d['rdate_word']}")
    
    # Load
    print("\nLoading...")
    sas = pl.DataFrame()
    for p in [f"{PATHS['SASDATA']}LOAN{d['reptmon']}4.parquet", f"{PATHS['ISASDATA']}LOAN{d['reptmon']}4.parquet"]:
        if Path(p).exists(): 
            df = pl.read_parquet(p)
            if 'BALANCE' in df.columns: 
                sas = pl.concat([sas, df.select(['ACCTNO','NOTENO','BALANCE']).unique()]) if not sas.is_empty() else df.select(['ACCTNO','NOTENO','BALANCE']).unique()
    
    prev = pl.read_parquet(f"{PATHS['NPL']}TOTIIS{d['prevmON']}.parquet").rename({'IISCLOSE':'PREVIIS','OICLOSE':'PREVOI'}).select(['ACCTNO','NOTENO','PREVIIS','PREVOI']).unique() if Path(f"{PATHS['NPL']}TOTIIS{d['prevmON']}.parquet").exists() else pl.DataFrame()
    
    ln = pl.DataFrame()
    for p in [f"{PATHS['LOAN']}LNNOTE.parquet", f"{PATHS['LOAN']}ILOAN.LNNOTE.parquet"]:
        if Path(p).exists(): ln = pl.concat([ln, pl.read_parquet(p)]) if not ln.is_empty() else pl.read_parquet(p)
    if not ln.is_empty(): ln = ln.filter((pl.col('REVERSED')!='Y') & pl.col('NOTENO').is_not_null()).unique(['ACCTNO','NOTENO'])
    
    prv = pl.DataFrame()
    for p in [f"{PATHS['PRVLOAN']}LNNOTE.parquet", f"{PATHS['PRVLOAN']}IPRVLOAN.LNNOTE.parquet"]:
        if Path(p).exists(): prv = pl.concat([prv, pl.read_parquet(p)]) if not prv.is_empty() else pl.read_parquet(p)
    if not prv.is_empty(): prv = prv.filter((pl.col('REVERSED')!='Y') & pl.col('NOTENO').is_not_null()).select(['ACCTNO','NOTENO','CURBAL','REBATE']).unique().rename({'CURBAL':'PREVBAL','REBATE':'PRVREB'})
    
    icl = pl.read_parquet(f"{PATHS['NPL']}IISCLOSE{d['reptmon']}4.parquet") if Path(f"{PATHS['NPL']}IISCLOSE{d['reptmon']}4.parquet").exists() else pl.DataFrame()
    if not icl.is_empty() and 'BRANCH' in icl.columns: icl = icl.drop('BRANCH')
    
    iis = pl.read_parquet(f"{PATHS['NPL']}IIS{d['reptmon']}.parquet").filter(pl.col('LOANTYPE')<981).unique(['ACCTNO','NOTENO']) if Path(f"{PATHS['NPL']}IIS{d['reptmon']}.parquet").exists() else pl.DataFrame()
    if iis.is_empty(): print("ERROR: No IIS data"); return
    
    ado = pl.read_parquet(f"{PATHS['NPL']}ADONIIS.parquet") if Path(f"{PATHS['NPL']}ADONIIS.parquet").exists() else pl.DataFrame()
    
    lc = pl.DataFrame()
    for p in [f"{PATHS['LOAN']}LNCOMM.parquet", f"{PATHS['LOAN']}ILOAN.LNCOMM.parquet"]:
        if Path(p).exists(): lc = pl.concat([lc, pl.read_parquet(p)]) if not lc.is_empty() else pl.read_parquet(p)
    if not lc.is_empty(): lc = lc.select(['ACCTNO','COMMNO','CCURAMT','CUSEDAMT']).unique(['ACCTNO','COMMNO'])
    
    print(f"  IIS: {len(iis)}")
    
    # Merge
    print("\nMerging...")
    if not ln.is_empty():
        if not prv.is_empty(): ln = ln.join(prv, on=['ACCTNO','NOTENO'], how='left')
        if not icl.is_empty(): ln = ln.join(icl, on=['ACCTNO','NOTENO'], how='left')
        iis = iis.join(ln, on=['ACCTNO','NOTENO'], how='left')
    if not prev.is_empty(): iis = iis.join(prev, on=['ACCTNO','NOTENO'], how='left')
    if not ado.is_empty(): iis = iis.join(ado, on=['ACCTNO','NOTENO'], how='left')
    
    # Calculate
    print("\nCalculating IIS...")
    iis = pl.DataFrame([calc_iis(r, d['reptdate']) for r in iis.rows(named=True)])
    print(f"  Done: {len(iis)} records")
    
    # Realisable
    print("\nCalculating realisable...")
    if not lc.is_empty() and not iis.is_empty() and not ln.is_empty():
        ln2 = ln.sort(['ACCTNO','COMMNO']).join(lc.sort(['ACCTNO','COMMNO']), on=['ACCTNO','COMMNO'], how='left')
        ln2 = ln2.with_columns((pl.col('CCURAMT').fill_null(0)-pl.col('CUSEDAMT').fill_null(0)).alias('UNDRAWN'))
        big = iis.filter(pl.col('ACCTNO')>8000000000).sort(['ACCTNO','NOTENO'])
        if not big.is_empty():
            big = big.join(ln2.sort(['ACCTNO','NOTENO']), on=['ACCTNO','NOTENO'], how='left')
            rv = pl.DataFrame([calc_realisable(r, d['reptdate']) for r in big.rows(named=True)]).select(['ACCTNO','NOTENO','REALISVL'])
            iis = iis.join(rv, on=['ACCTNO','NOTENO'], how='left')
            print(f"  Realisable: {len(rv)} records")
    
    # Reports
    print("\nGenerating reports...")
    for name, filt, title in [
        ('R1', (~pl.col('LOANTYPE').is_in(CGC)) & pl.col('NPLIND').is_in(['N','O','P']), "MONTHLY MOVEMENT OF INTEREST-IN-SUSPENSE"),
        ('R2', pl.col('LOANTYPE').is_in(CGC) & pl.col('NPLIND').is_in(['N','O','P']), "MONTHLY MOVEMENT FOR CGC/TUK LOANS"),
        ('R3', (~pl.col('LOANTYPE').is_in(CGC)) & pl.col('NPLIND').is_in(['E','N','O','P']), "MONTHLY MOVEMENT OF IIS FOR NON-PERFORMING TL/FL/HL/RC"),
        ('R4', pl.col('LOANTYPE').is_in(CGC) & pl.col('NPLIND').is_in(['E','N','O','P']), "MONTHLY MOVEMENT FOR NON-PERFORMING CGC/TUK LOANS")
    ]:
        df = iis.filter(filt)
        write_report(df, title, f"{PATHS['OUTPUT']}IIS_{name}.txt", grp='BRANCH' if 'R3' in name or 'R4' in name else None)
        print(f"  {name}: {len(df)} records")
    
    # Split by cost centre
    print("\nSplitting by cost centre...")
    if 'COSTCTR' in iis.columns:
        conv = iis.filter((CONV(pl.col('COSTCTR'))) | pl.col('LOANTYPE').is_in(CGC)).unique(['ACCTNO','NOTENO'])
        islm = iis.filter(ISLM(pl.col('COSTCTR')) & ~pl.col('LOANTYPE').is_in(CGC)).unique(['ACCTNO','NOTENO'])
        if not conv.is_empty(): conv.write_parquet(f"{PATHS['NPL']}TOTIIS{d['reptmon']}.parquet")
        if not islm.is_empty(): islm.write_parquet(f"{PATHS['NPL']}NPLI.TOTIIS{d['reptmon']}.parquet")
        print(f"  Conv: {len(conv)}, Islm: {len(islm)}")
    
    # Summary
    print("\n"+"="*60+"\nSUMMARY\n"+"="*60)
    if not iis.is_empty():
        print(f"\nTotal Balance: RM {iis['BALANCE'].sum() if 'BALANCE' in iis.columns else 0:,.2f}")
        for f in ['TOTIIS','TOTIISR','TOTWOF']:
            if f in iis.columns: print(f"Total {f}: RM {iis[f].sum():,.2f}")
        if 'REALISVL' in iis.columns and not iis['REALISVL'].is_null().all():
            print(f"Total Realisable: RM {iis['REALISVL'].sum():,.2f}")
    
    print("\n"+"="*60+"\n✓ EIBMIISA Complete")

if __name__ == "__main__":
    main()
