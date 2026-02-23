"""
EIBMIISX - IIS GL Interface Generator
Creates GL entries for IIS movements (normalisation, write-back/off) for PBB & PIBB
"""

import polars as pl
from datetime import datetime
from pathlib import Path
from collections import defaultdict

# =============================================================================
# CONFIG
# =============================================================================
PATHS = {'LOAN': 'data/loan/', 'NPL': 'data/npl/', 'PRVLOAN': 'data/prvloan/', 'OUTPUT': 'output/'}
for p in PATHS.values(): Path(p).mkdir(exist_ok=True)

# Risk format
RISK_FMT = {'1': 'SS1', '2': 'SS2', '3': 'D  ', '4': 'B  '}

# Cost centre rules
CONV = lambda x: (x < 3000 or x > 3999) and x not in [4043, 4048]
ISLM = lambda x: (3000 <= x <= 3999) or x in [4043, 4048]

CGC_TYPES = [517, 521, 522, 523, 528]
CGC_SPECIAL = [147, 148, 173, 174]

# =============================================================================
# DATE
# =============================================================================
def get_dates():
    df = pl.read_parquet(f"{PATHS['LOAN']}REPTDATE.parquet")
    reptdate = df['REPTDATE'][0]
    prev_month = reptdate.month - 1 or 12
    now = datetime.now()
    return {
        'reptdate': reptdate, 'reptmon': f"{reptdate.month:02d}", 'prevmON': f"{prev_month:02d}",
        'rptyr': str(reptdate.year), 'rptdy': f"{reptdate.day:02d}",
        'curyr': str(now.year), 'curmth': f"{now.month:02d}", 'curday': f"{now.day:02d}",
        'rptdte': now.strftime("%Y%m%d"), 'rpttime': now.strftime("%H%M%S")
    }

# =============================================================================
# IIS CALC (from EIBMIISA)
# =============================================================================
def calc_iis(r, rep_date):
    r = r.copy()
    # Ageing
    if r.get('BLDATE', 0) > 0:
        try: r['AGEING'] = (rep_date - datetime.strptime(str(int(r['BLDATE'])).zfill(8), "%m%d%Y").date()).days
        except: pass
    if r.get('OLDNOTEDAYARR', 0) > 0 and 98000 <= r.get('NOTENO', 0) <= 98999:
        r['AGEING'] = max(0, r.get('AGEING', 0)) + r['OLDNOTEDAYARR']
    
    # Risk
    r['CLASSNEW'] = RISK_FMT.get(str(r.get('RISKRATE', '')).strip(), ' ')
    
    # Diff balance
    pv = r.get('PREVBAL', 0) or 0; cb = r.get('CURBAL', 0) or 0
    sw = r.get('SPWOF', 0) or 0; iw = r.get('IISW', 0) or 0; ad = r.get('ADJUST', 0) or 0
    r['DIFFBAL'] = pv - cb - sw - iw - ad
    
    r['IISP'] = r.get('PREVIIS', 0) or 0
    r['OIP'] = r.get('PREVOI', 0) or 0
    
    n = r.get('NPLIND', ''); lt = r.get('LOANTYPE', 0); lc = r.get('LSTTRNCD', 0)
    ir = r.get('IISR', 0) or 0; iw = r.get('IISW', 0) or 0; nt = r.get('NTINT', '')
    
    # LSTTRNAM
    r['LSTTRNAM'] = (r.get('LSTTRNAM', 0) / 100000) if lt in CGC_TYPES and n == 'N' and lc == 166 else 0
    
    if nt == 'A':  # Accrual
        if n in ('O','P'): r['IIS'] = ir + iw - r['IISP']
        if r.get('IISCLS', 0): r['IISP'] = r['IISCLS'] - r['OIP']
        if n in ('O','P'): r['IIS'] = ir + iw - r['IISP']
        r['IIS'] = r.get('ADONIIS', 0) or 0
        oi = (r.get('FEEASSES',0)or0) - (r.get('FEEWAIVE',0)or0)
        if oi < 0: oi = 0
        oir = r.get('FEEPAY',0)or0
        b = r.get('BORSTAT','')
        if b in ('W','X') and n != 'P':
            r['IIS'] = 0
            oi = r.get('FEELWTOT',0)or0
        if (n == 'E' and b == 'P') or lc == 652: ir = 0
        elif r.get('LOANSTAT',0) == 1 or r['DIFFBAL'] >= r['IISP']: ir = r['IISP'] - iw
        elif r['DIFFBAL'] < r['IISP']: ir = r['DIFFBAL'] - iw
        if n == 'O':
            r['IIS'] = 0
            oir = (r.get('FEEPAY',0)or0) + (r.get('FEELWTOT',0)or0)
        if ir < 0: ir = 0
        r['IISR'] = ir
        if n == 'P': r['IIS'] = r['IIS'] + ir + iw - r['IISP']
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
    else:  # Non-accrual
        if n == 'O':
            ac = r.get('ACTIACCR',0)or0
            if ac == 0:
                ia = r.get('INTACTIV',0)or0
                if ia < 0: ir = ir + (r.get('FEELWTOT',0)or0) - ia
                else: ir = ir + (r.get('FEELWTOT',0)or0)
                r['IIS'] = ir + iw - r['IISP']
            else:
                r['IIS'] = ir + iw + ac - r['IISP']
                r['OIREC'] = (r.get('FEEPAY',0)or0) + (r.get('FEELWTOT',0)or0)
        r['TOTOPEN'] = r['IISP']; r['TOTIISR'] = ir; r['TOTWOF'] = iw; r['TOTIIS'] = r['IIS']
        if n == 'N':
            ic = r.get('IISCLOSE',0)or0
            r['TOTIIS'] = ic + r['TOTIISR'] + r['TOTWOF']
            r['TOTOPEN'] = 0
        if n == 'O': r['TOTCLOSE'] = r['IISP'] + r['IIS'] - ir - iw
        else: r['TOTCLOSE'] = r.get('IISCLOSE',0)or0
    return r

# =============================================================================
# GL GENERATOR
# =============================================================================
class GLGen:
    def __init__(self, d): self.d = d; self.entries = []
    
    def add(self, c, a, d, s='+', v=0): self.entries.append({'c':c, 'a':a, 'd':d, 's':s, 'v':abs(v)})
    
    def nxday(self, r, isl):
        lt = r.get('LOANTYPE', 0)
        ni = r.get('NOTEINT', '')
        amt = r.get('ADONIIS' if ni == 'A' else 'IIS_AMT', 0)
        # PY1/PY4
        self.add(r['COSTCTR'], f"II{lt:03d}PY1", 'IISL-NORMALISE NEW NPL', '+' if amt>=0 else '-', amt)
        self.add(r['COSTCTR'], f"II{lt:03d}PY4", 'IISL-NORMALISE EXISTING NPL', '+' if amt>=0 else '-', amt)
        # PY2/PY5 (non-accrual only)
        if ni != 'A':
            amt2 = r.get('IIS_AMT', 0)
            self.add(r['COSTCTR'], f"II{lt:03d}PY2", 'IISL-REVERSE IIS CHARGE', '+' if amt2>=0 else '-', amt2)
            self.add(r['COSTCTR'], f"II{lt:03d}PY5", 'IISL-REVERSE IIS CHARGE', '+' if amt2>=0 else '-', amt2)
    
    def proc(self, r, isl):
        lt = r.get('LOANTYPE', 0)
        if 600 <= lt <= 609 or 631 <= lt <= 637 or 650 <= lt <= 699: return
        nt = r.get('NTINT', '')
        pre = 'I' if isl else 'C'
        
        if nt != 'A':  # Non-accrual
            if (ia := r.get('INTACTIV',0)or0) > 0:
                self.add(r['COSTCTR'], f"II{lt:03d}{pre}WB", f'IISL-{pre}ONV LOAN WRITE BACK', '+' if ia>=0 else '-', ia)
            if (iw := r.get('IISW',0)or0) > 0:
                self.add(r['COSTCTR'], f"II{lt:03d}{pre}WO", f'IISL-{pre}ONV LOAN WRITE OFF', '+' if iw>=0 else '-', iw)
            if (sw := r.get('SPWOF',0)or0) > 0:
                self.add(r['COSTCTR'], f"SP{lt:03d}{pre}WO", f'SP WRITTEN-OFF - {"CONV" if not isl else "ISLM"}', '+' if sw>=0 else '-', sw)
        else:  # Accrual
            if (ir := r.get('IISR',0)or0) > 0:
                self.add(r['COSTCTR'], f"II{lt:03d}{pre}WB", f'IISL-{pre}SL LOAN WRITE BACK', '+' if ir>=0 else '-', ir)
            if (iw := r.get('IISW',0)or0) > 0:
                self.add(r['COSTCTR'], f"II{lt:03d}{pre}WO", f'IISL-{pre}SL LOAN WRITE OFF', '+' if iw>=0 else '-', iw)
            if (sw := r.get('SPWOF',0)or0) > 0:
                self.add(r['COSTCTR'], f"SP{lt:03d}{pre}WO", f'SP WRITTEN-OFF - {"CONV" if not isl else "ISLM"}', '+' if sw>=0 else '-', sw)
        
        # Fee corrections
        if r.get('NPLIND') == 'O' and (fw := r.get('FEELWTOT',0)or0) > 0:
            self.add(r['COSTCTR'], f"II{lt:03d}COR", 'IISL-CORR FOR KYWD FEE ASSESS', '+' if fw>=0 else '-', fw)
            self.add(r['COSTCTR'], f"II{lt:03d}FWB", 'IISL-NORMALIZATION OF FEETYPE LW', '+', 0)
    
    def write(self, fn, isl):
        lines = []
        # Header
        hdr = 'INTFISL2' if isl else 'INTFIISL'
        lines.append(f"{hdr:<8}{'0':<9}{self.d['rptyr']:<4}{self.d['reptmon']:<2}{self.d['rptdy']:<2}{self.d['rptdte']:<8}{self.d['rpttime']:<6}")
        
        # Group by ARID
        grp = defaultdict(list)
        for e in self.entries: grp[e['a']].append(e)
        
        cnt = tc = td = 0
        for arid, es in grp.items():
            tot = sum(e['v'] for e in es)
            sgn = next((e['s'] for e in es if e['v'] > 0), '+')
            if sgn == '+': td += tot
            else: tc += tot
            cnt += 1
            
            use_cur = any(x in arid for x in ['COR','PY2','PY3','PY5','PY6'])
            dt = f"{self.d['curyr']}{self.d['curmth']}{self.d['curday']}" if use_cur else f"{self.d['rptyr']}{self.d['reptmon']}{self.d['rptdy']}"
            amt = f"{tot:017.2f}".replace('.', '')
            
            lines.append(f"{hdr:<8}{'1':<9}{arid:<8}{'PIBB' if isl else 'PBBH':<4}{es[0]['c']:04d}{'MYR':<3}{dt}{sgn}{amt}{es[0]['d']:<60}{arid:<8}{'IISL':<4}{'+':<80}{dt}{'+':<4}{'0'*16}")
        
        # Trailer
        lines.append(f"{hdr:<8}{'9':<9}{self.d['rptyr']:<4}{self.d['reptmon']:<2}{self.d['rptdy']:<2}{self.d['rptdte']:<8}{self.d['rpttime']:<6}"
                    f"{'+':<1}{f'{td:017.2f}'.replace('.','')}{'-':<1}{f'{tc:017.2f}'.replace('.','')}"
                    f"{'+':<1}{'0'*16}{'-':<1}{'0'*16}{cnt:09d}")
        
        Path(fn).write_text('\n'.join(lines))
        return cnt

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("="*60+"\nEIBMIISX - IIS GL Interface\n"+"="*60)
    d = get_dates()
    print(f"Date: {d['reptmon']}/{d['rptdy']}/{d['rptyr']}")
    
    # Load all data (simplified - assumes parquet exists)
    iis = pl.read_parquet(f"{PATHS['NPL']}IIS{d['reptmon']}.parquet").filter(pl.col('LOANTYPE') < 981).unique(['ACCTNO','NOTENO'])
    adon = pl.read_parquet(f"{PATHS['NPL']}ADONIIS.parquet") if Path(f"{PATHS['NPL']}ADONIIS.parquet").exists() else pl.DataFrame()
    prev = pl.read_parquet(f"{PATHS['NPL']}TOTIIS{d['prevmON']}.parquet").rename({'IISCLOSE':'PREVIIS','OICLOSE':'PREVOI'}).select(['ACCTNO','NOTENO','PREVIIS','PREVOI']).unique()
    ln = pl.read_parquet(f"{PATHS['LOAN']}LNNOTE.parquet").filter((pl.col('REVERSED')!='Y') & pl.col('NOTENO').is_not_null()).unique(['ACCTNO','NOTENO'])
    prv = pl.read_parquet(f"{PATHS['PRVLOAN']}LNNOTE.parquet").filter((pl.col('REVERSED')!='Y') & pl.col('NOTENO').is_not_null()).select(['ACCTNO','NOTENO','CURBAL','REBATE']).unique().rename({'CURBAL':'PREVBAL','REBATE':'PRVREB'})
    nxt = pl.read_parquet(f"{PATHS['NPL']}NEXTDAY.parquet") if Path(f"{PATHS['NPL']}NEXTDAY.parquet").exists() else pl.DataFrame()
    
    # Merge
    ln = ln.join(prv, on=['ACCTNO','NOTENO'], how='left').join(adon, on=['ACCTNO','NOTENO'], how='left')
    iis = iis.join(ln, on=['ACCTNO','NOTENO'], how='left').join(prev, on=['ACCTNO','NOTENO'], how='left')
    
    # Calculate IIS
    res = [calc_iis(r, d['reptdate']) for r in iis.rows(named=True)]
    iis = pl.DataFrame(res)
    
    # Output IISCLOSE
    Path(f"{PATHS['OUTPUT']}IISCLOSE.txt").write_text('\n'.join(
        f"{r['ACCTNO']:011d}{r['NOTENO']:05d}{r.get('IISCLOSE',0):016.2f}" 
        for r in iis.filter(pl.col('NTINT')=='A').rows(named=True)
    ))
    
    # PBLEAN processing
    filt = ((pl.col('LOANTYPE') < 4) | (pl.col('LOANTYPE') > 99)) & ~pl.col('LOANTYPE').is_in([100,199,380,381,390,500,516]+CGC_TYPES)
    ti = iis.filter(filt).sort(['ACCTNO','NOTENO'])
    
    if not nxt.is_empty():
        nx = ti.join(nxt, on=['ACCTNO','NOTENO'], how='inner')
        nx = nx.with_columns(pl.when(pl.col('ACCTNO').is_not_null()).then(pl.lit('E')).otherwise(pl.lit('N')).alias('PREV_IND'))
        if not adon.is_empty(): nx = nx.join(adon, on=['ACCTNO','NOTENO'], how='left')
        
        g1, g2 = GLGen(d), GLGen(d)
        for r in nx.rows(named=True):
            cc = r['COSTCTR']
            if CONV(cc) or r['LOANTYPE'] in CGC_SPECIAL: g1.nxday(r, False)
            elif ISLM(cc) and r['LOANTYPE'] not in CGC_SPECIAL: g2.nxday(r, True)
        
        if g1.entries: print(f"  GLNXDAY: {g1.write(f"{PATHS['OUTPUT']}GLNXDAY.txt", False)} entries")
        if g2.entries: print(f"  GLNXDAY2: {g2.write(f"{PATHS['OUTPUT']}GLNXDAY2.txt", True)} entries")
        ti = ti.join(nxt, on=['ACCTNO','NOTENO'], how='anti')
    
    # Regular GL
    g3, g4 = GLGen(d), GLGen(d)
    for r in ti.rows(named=True):
        cc = r['COSTCTR']
        if CONV(cc) or r['LOANTYPE'] in CGC_SPECIAL: g3.proc(r, False)
        elif ISLM(cc) and r['LOANTYPE'] not in CGC_SPECIAL: g4.proc(r, True)
    
    # Combine with nxday entries
    if 'g1' in locals() and g1.entries: g3.entries.extend(g1.entries)
    if 'g2' in locals() and g2.entries: g4.entries.extend(g2.entries)
    
    if g3.entries: print(f"  GLINTER: {g3.write(f"{PATHS['OUTPUT']}GLINTER.txt", False)} entries")
    if g4.entries: print(f"  GLINTER2: {g4.write(f"{PATHS['OUTPUT']}GLINTER2.txt", True)} entries")
    
    print(f"\nTotal: Conv={len(g3.entries)}, Islm={len(g4.entries)}")
    print("="*60+"\n✓ EIBMIISX Complete")

if __name__ == "__main__":
    main()
