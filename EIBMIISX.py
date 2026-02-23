"""
EIBMIISX - IIS GL Interface Generator
Creates GL entries for IIS movements (normalisation, write-back/off)
"""

import polars as pl
from datetime import datetime
from pathlib import Path
from collections import defaultdict

# =============================================================================
# CONFIG
# =============================================================================
P = {k:f'data/{v}/' for k,v in {'LOAN':'loan','NPL':'npl','PRVLOAN':'prvloan','OUTPUT':'output'}.items()}
for p in P.values(): Path(p).mkdir(exist_ok=True)

RISK_FMT = {'1':'SS1','2':'SS2','3':'D  ','4':'B  '}
CONV = lambda x: (x < 3000 or x > 3999) and x not in [4043, 4048]
ISLM = lambda x: (3000 <= x <= 3999) or x in [4043, 4048]
CGC, CGC_SP = [517,521,522,523,528], [147,148,173,174]

# =============================================================================
# DATE
# =============================================================================
def d():
    df = pl.read_parquet(f"{P['LOAN']}REPTDATE.parquet")
    r, n = df['REPTDATE'][0], datetime.now()
    p = r.month - 1 or 12
    return {
        'reptdate': r, 'reptmon': f"{r.month:02d}", 'prevmON': f"{p:02d}",
        'rptyr': str(r.year), 'rptdy': f"{r.day:02d}",
        'curyr': str(n.year), 'curmth': f"{n.month:02d}", 'curday': f"{n.day:02d}",
        'rptdte': n.strftime("%Y%m%d"), 'rpttime': n.strftime("%H%M%S")
    }

# =============================================================================
# IIS CALC
# =============================================================================
def calc(r, rd):
    r = r.copy()
    if r.get('BLDATE', 0) > 0:
        try:
            r['AGEING'] = (rd - datetime.strptime(str(int(r['BLDATE'])).zfill(8), "%m%d%Y").date()).days
        except:
            pass
    if r.get('OLDNOTEDAYARR', 0) > 0 and 98000 <= r.get('NOTENO', 0) <= 98999:
        r['AGEING'] = max(0, r.get('AGEING', 0)) + r['OLDNOTEDAYARR']
    
    r['CLASSNEW'] = RISK_FMT.get(str(r.get('RISKRATE', '')).strip(), ' ')
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
        
        if ir < 0:
            ir = 0
        r['IISR'] = ir
        
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
# GL GENERATOR
# =============================================================================
class GL:
    def __init__(self, d):
        self.d = d
        self.e = []
    
    def add(self, c, a, desc, s='+', v=0):
        self.e.append({'c': c, 'a': a, 'd': desc, 's': s, 'v': abs(v)})
    
    def nxday(self, r, _):
        lt = r.get('LOANTYPE', 0)
        ni = r.get('NOTEINT', '')
        amt = r.get('ADONIIS' if ni == 'A' else 'IIS_AMT', 0)
        for p in ['PY1', 'PY4']:
            self.add(r['COSTCTR'], f"II{lt:03d}{p}", 'IISL-NORMALISE NEW/EXISTING NPL',
                    '+' if amt >= 0 else '-', amt)
        if ni != 'A':
            amt2 = r.get('IIS_AMT', 0)
            for p in ['PY2', 'PY5']:
                self.add(r['COSTCTR'], f"II{lt:03d}{p}", 'IISL-REVERSE IIS CHARGE',
                        '+' if amt2 >= 0 else '-', amt2)
    
    def proc(self, r, isl):
        lt = r.get('LOANTYPE', 0)
        nt = r.get('NTINT', '')
        if 600 <= lt <= 609 or 631 <= lt <= 637 or 650 <= lt <= 699:
            return
        pre = 'I' if isl else 'C'
        
        # INTACTIV
        ia = r.get('INTACTIV', 0) or 0
        if ia > 0:
            self.add(r['COSTCTR'], f"II{lt:03d}{pre}WB", f'IISL-{pre}ONV LOAN WRITE BACK',
                    '+' if ia >= 0 else '-', ia)
        
        # IISW
        iw = r.get('IISW', 0) or 0
        if iw > 0:
            self.add(r['COSTCTR'], f"II{lt:03d}{pre}WO", f'IISL-{pre}ONV LOAN WRITE OFF',
                    '+' if iw >= 0 else '-', iw)
        
        # SPWOF
        sw = r.get('SPWOF', 0) or 0
        if sw > 0:
            desc = f'SP WRITTEN-OFF - {"CONV" if not isl else "ISLM"}'
            self.add(r['COSTCTR'], f"SP{lt:03d}{pre}WO", desc,
                    '+' if sw >= 0 else '-', sw)
        
        # IISR for accrual
        if nt == 'A':
            ir = r.get('IISR', 0) or 0
            if ir > 0:
                self.add(r['COSTCTR'], f"II{lt:03d}{pre}WB", f'IISL-{pre}SL LOAN WRITE BACK',
                        '+' if ir >= 0 else '-', ir)
        
        # Fee corrections
        if r.get('NPLIND') == 'O':
            fw = r.get('FEELWTOT', 0) or 0
            if fw > 0:
                self.add(r['COSTCTR'], f"II{lt:03d}COR", 'IISL-CORR FOR KYWD FEE ASSESS',
                        '+' if fw >= 0 else '-', fw)
                self.add(r['COSTCTR'], f"II{lt:03d}FWB", 'IISL-NORMALIZATION OF FEETYPE LW', '+', 0)
    
    def write(self, fn, isl):
        h = 'INTFISL2' if isl else 'INTFIISL'
        lines = [
            f"{h:<8}{'0':<9}{self.d['rptyr']:<4}{self.d['reptmon']:<2}{self.d['rptdy']:<2}"
            f"{self.d['rptdte']:<8}{self.d['rpttime']:<6}"
        ]
        
        grp = defaultdict(list)
        for e in self.e:
            grp[e['a']].append(e)
        
        cnt = tc = td = 0
        for a, es in grp.items():
            tot = sum(e['v'] for e in es)
            sgn = next((e['s'] for e in es if e['v'] > 0), '+')
            if sgn == '+':
                td += tot
            else:
                tc += tot
            cnt += 1
            
            use_cur = any(x in a for x in ['COR', 'PY2', 'PY3', 'PY5', 'PY6'])
            if use_cur:
                dt = f"{self.d['curyr']}{self.d['curmth']}{self.d['curday']}"
            else:
                dt = f"{self.d['rptyr']}{self.d['reptmon']}{self.d['rptdy']}"
            
            amt = f"{tot:017.2f}".replace('.', '')
            
            lines.append(
                f"{h:<8}{'1':<9}{a:<8}{'PIBB' if isl else 'PBBH':<4}{es[0]['c']:04d}{'MYR':<3}{dt}"
                f"{sgn}{amt}{es[0]['d']:<60}{a:<8}{'IISL':<4}{'+':<80}{dt}{'+':<4}{'0'*16}"
            )
        
        td_str = f"{td:017.2f}".replace('.', '')
        tc_str = f"{tc:017.2f}".replace('.', '')
        lines.append(
            f"{h:<8}{'9':<9}{self.d['rptyr']:<4}{self.d['reptmon']:<2}{self.d['rptdy']:<2}"
            f"{self.d['rptdte']:<8}{self.d['rpttime']:<6}"
            f"{'+':<1}{td_str}{'-':<1}{tc_str}{'+':<1}{'0'*16}{'-':<1}{'0'*16}{cnt:09d}"
        )
        
        Path(fn).write_text('\n'.join(lines))
        return cnt

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIBMIISX - IIS GL Interface")
    print("=" * 60)
    
    D = d()
    print(f"Date: {D['reptmon']}/{D['rptdy']}/{D['rptyr']}")
    
    # Load data
    iis = pl.read_parquet(f"{P['NPL']}IIS{D['reptmon']}.parquet")
    iis = iis.filter(pl.col('LOANTYPE') < 981).unique(['ACCTNO', 'NOTENO'])
    
    adon_path = f"{P['NPL']}ADONIIS.parquet"
    adon = pl.read_parquet(adon_path) if Path(adon_path).exists() else pl.DataFrame()
    
    prev_path = f"{P['NPL']}TOTIIS{D['prevmON']}.parquet"
    if Path(prev_path).exists():
        prev = pl.read_parquet(prev_path)
        prev = prev.rename({'IISCLOSE': 'PREVIIS', 'OICLOSE': 'PREVOI'})
        prev = prev.select(['ACCTNO', 'NOTENO', 'PREVIIS', 'PREVOI']).unique()
    else:
        prev = pl.DataFrame()
    
    ln = pl.read_parquet(f"{P['LOAN']}LNNOTE.parquet")
    ln = ln.filter((pl.col('REVERSED') != 'Y') & pl.col('NOTENO').is_not_null())
    ln = ln.unique(['ACCTNO', 'NOTENO'])
    
    prv = pl.read_parquet(f"{P['PRVLOAN']}LNNOTE.parquet")
    prv = prv.filter((pl.col('REVERSED') != 'Y') & pl.col('NOTENO').is_not_null())
    prv = prv.select(['ACCTNO', 'NOTENO', 'CURBAL', 'REBATE']).unique()
    prv = prv.rename({'CURBAL': 'PREVBAL', 'REBATE': 'PRVREB'})
    
    nxt_path = f"{P['NPL']}NEXTDAY.parquet"
    nxt = pl.read_parquet(nxt_path) if Path(nxt_path).exists() else pl.DataFrame()
    
    # Merge and calculate
    ln = ln.join(prv, on=['ACCTNO', 'NOTENO'], how='left')
    if not adon.is_empty():
        ln = ln.join(adon, on=['ACCTNO', 'NOTENO'], how='left')
    
    iis = iis.join(ln, on=['ACCTNO', 'NOTENO'], how='left')
    if not prev.is_empty():
        iis = iis.join(prev, on=['ACCTNO', 'NOTENO'], how='left')
    
    # Calculate IIS
    rows = []
    for r in iis.rows(named=True):
        rows.append(calc(r, D['reptdate']))
    iis = pl.DataFrame(rows)
    
    # Output IISCLOSE
    close_rows = []
    for r in iis.filter(pl.col('NTINT') == 'A').rows(named=True):
        close_rows.append(f"{r['ACCTNO']:011d}{r['NOTENO']:05d}{r.get('IISCLOSE', 0):016.2f}")
    Path(f"{P['OUTPUT']}IISCLOSE.txt").write_text('\n'.join(close_rows))
    
    # PBLEAN processing
    filt = ((pl.col('LOANTYPE') < 4) | (pl.col('LOANTYPE') > 99))
    excl = [100, 199, 380, 381, 390, 500, 516] + CGC
    filt = filt & ~pl.col('LOANTYPE').is_in(excl)
    
    ti = iis.filter(filt).sort(['ACCTNO', 'NOTENO'])
    g1, g2 = GL(D), GL(D)
    
    if not nxt.is_empty():
        nx = ti.join(nxt, on=['ACCTNO', 'NOTENO'], how='inner')
        nx = nx.with_columns(pl.lit('E').alias('PREV_IND'))
        if not adon.is_empty():
            nx = nx.join(adon, on=['ACCTNO', 'NOTENO'], how='left')
        
        for r in nx.rows(named=True):
            cc = r['COSTCTR']
            lt = r['LOANTYPE']
            if CONV(cc) or lt in CGC_SP:
                g1.nxday(r, False)
            elif ISLM(cc) and lt not in CGC_SP:
                g2.nxday(r, True)
        
        ti = ti.join(nxt, on=['ACCTNO', 'NOTENO'], how='anti')
    
    # Regular GL
    for r in ti.rows(named=True):
        cc = r['COSTCTR']
        lt = r['LOANTYPE']
        if CONV(cc) or lt in CGC_SP:
            g1.proc(r, False)
        elif ISLM(cc) and lt not in CGC_SP:
            g2.proc(r, True)
    
    # Write outputs
    if g1.e:
        print(f"  GLINTER: {g1.write(f"{P['OUTPUT']}GLINTER.txt", False)} entries")
    if g2.e:
        print(f"  GLINTER2: {g2.write(f"{P['OUTPUT']}GLINTER2.txt", True)} entries")
    
    print(f"\nTotal: Conv={len(g1.e)}, Islm={len(g2.e)}")
    print("=" * 60)
    print("✓ EIBMIISX Complete")

if __name__ == "__main__":
    main()
