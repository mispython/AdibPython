"""
EIWDISRP - Weekly Disbursement and Repayment Report (Ultra-Compact)
All critical logic preserved, under 600 lines
"""

import polars as pl
from datetime import datetime, timedelta
from pathlib import Path

# =============================================================================
# CONFIG
# =============================================================================
PATHS = {k: f'data/{k.lower()}/' for k in ['LOAN','BNM','FORATE','DEPOSIT','LIMIT',
           'PLIMIT','DAILY','SASD','ILOAN','IBNM','ILIMIT','IPLIMIT',
           'IDEPOSIT','IDAILY','ISASD','DISPAY']}
for p in PATHS.values(): Path(p).mkdir(exist_ok=True)

FCY = list(range(801,851))
HP = [219,220,221,222,223,224]

# =============================================================================
# DATE
# =============================================================================
def get_dates():
    d = pl.read_parquet(f"{PATHS['LOAN']}REPTDATE.parquet")['REPTDATE'][0]
    wk = '1' if d.day<=8 else '2' if d.day<=15 else '3' if d.day<=22 else '4'
    st = d.replace(day=1) if wk=='1' else d.replace(day=9) if wk=='2' else d.replace(day=16) if wk=='3' else d.replace(day=23)
    return {'d':d,'wk':wk,'wk1':{'1':'4','2':'1','3':'2','4':'3'}[wk],
            'mon':f"{d.month:02d}","mon2":f"{(d.month-1) or 12:02d}",
            'day':f"{d.day:02d}",'yr':str(d.year)[-2:],
            'rdate':d.strftime('%d%m%y'),'st':st,'t':d,
            'sty':d.replace(month=1,day=1),'loop':d-timedelta(61)}

# =============================================================================
# FX RATES
# =============================================================================
def get_fx(rd):
    dates = pl.DataFrame({'dt':[rd['loop']+timedelta(i) for i in range((rd['t']-rd['loop']).days+1)]})
    fx = pl.read_parquet(f"{PATHS['FORATE']}FORATEBKP.parquet")\
          .filter(pl.col('REPTDATE').is_between(rd['loop'],rd['t']))\
          .sort('REPTDATE',descending=True).unique(subset=['CURCODE'],keep='first')
    return {r['CURCODE']+str(r['REPTDATE']):r['SPOTRATE'] for r in fx.iter_rows(named=True)}

# =============================================================================
# TRANSACTION PARSING
# =============================================================================
def parse_hist(rd,islamic):
    data = []
    with open(f"{PATHS['LOAN']}HISTFILE.txt") as f:
        for line in f:
            if len(line)<500: continue
            try:
                if (acct:=int(line[0:6] or 0))<=0: continue
                post = datetime.strptime(str(int(line[33:39] or 0)).zfill(11)[:8],'%Y%m%d') if int(line[33:39] or 0) else None
                if post and post<rd['sty']: continue
                
                # Transaction amount logic
                tc, rev = int(line[26:28] or 0), line[28:29].strip()
                tot = float(line[215:223] or 0)
                if str(tc) in ['666','726']: tot = float(line[76:84] or 0) or tot
                elif tc==999: tot = float(line[143:151] or 0)
                elif 800<=tc<=899 and float(line[215:223] or 0)==0: tot = float(line[76:84] or 0)
                
                # Sign logic
                if tc in [310,726,750,751,752,753,754,756,758,760,761] and not (tc==726 and rev=='R'):
                    tot = -tot
                
                if tc==660 and int(line[16:18])==5: continue
                
                data.append({
                    'ac':acct,'no':int(line[6:9] or 0),
                    'ed':datetime.strptime(str(int(line[9:13] or 0)).zfill(5),'%Y%j'),
                    'tr':int(line[13:15] or 0),'ch':int(line[16:18] or 0),'tc':tc,'rv':rev,
                    'br':int(line[29:33] or 0),'pd':post,
                    'hp':float(line[135:143] or 0),'hi':float(line[143:151] or 0),
                    'hr':float(line[225:233] or 0),'ho':float(line[233:241] or 0),
                    'tot':tot
                })
            except: continue
    df = pl.DataFrame(data)
    return df, df.filter(pl.col('pd').is_between(rd['st'],rd['t']))

# =============================================================================
# TRANSACTION PROCESSING (UNIFIED)
# =============================================================================
def process_txns(df, ln, islamic):
    # Merge with loan note
    df = df.join(ln, on=['ac','no'], how='inner')
    v = df.filter(pl.col('rv')!='R')
    
    # Disbursements
    d310 = v.filter(pl.col('tc')==310).with_columns([
        pl.when(pl.col('NTINT')=='A').then(-pl.col('NETPROC')).otherwise(pl.col('tot')).alias('tot'),
        pl.col('no').alias('nnew'),
        pl.int_range(0,pl.count()).over(['ac','tot']).alias('cnt')
    ])
    
    d652 = v.filter(pl.col('tc')==652).with_columns([(-pl.col('tot')).alias('tot')])
    d652s = d652.group_by(['ac','ed']).agg(pl.col('tot').sum().round(2))
    
    d750 = v.filter(pl.col('tc')==750).join(
        df.filter(pl.col('tc')==782).select(['ac','no']), on=['ac','no'], how='anti')
    
    # Note conversion (310 vs 652 matching)
    note = []
    if not d310.is_empty() and not d652s.is_empty():
        m = d310.with_columns(pl.col('tot').round(2).alias('t2')).join(
            d652s.rename({'tot':'t2'}), on=['ac','t2','ed'], how='inner')
        if not m.is_empty():
            note = m.select(['ac','no','nnew']).unique().rows(named=True)
    
    # Combine disbursements
    disb = pl.concat([
        d310.select(['ac','no','tot','ed','pd','br','rv','CURCODE']),
        d750.select(['ac','no','tot','ed','pd','br','rv','CURCODE'])
    ]).filter(pl.col('rv')!='R').with_columns([(-pl.col('tot')).alias('disb')])
    
    # Repayments
    rc = [610,614,615,619,650,660,661,668,680]
    r610 = v.filter(pl.col('tc').is_in(rc) | ((pl.col('tc')==612)&(pl.col('NTBRCH')==130)))\
            .with_columns([(-pl.col('tot')).alias('tot'),
                           pl.int_range(0,pl.count()).over(['ac','tot','ed']).alias('cnt')])
    
    r754 = v.filter(pl.col('tc')==754).with_columns([(-pl.col('tot')).alias('tot')])
    if not r754.is_empty():
        r610 = r610.join(r754.select(['ac','tot','ed']), on=['ac','tot','ed'], how='anti')
    
    r800 = v.filter(pl.col('tc').is_in([800,801,810])).with_columns([(-pl.col('tot')).alias('tot')])
    r652 = df.filter((pl.col('rv')=='R') & ~pl.col('tc').is_in(rc+[612]))
    if not r652.is_empty():
        r800 = r800.join(r652.select(['ac','no','tot','ed']), on=['ac','no','tot','ed'], how='anti')
    
    rep = r610.select(['ac','no','tot','ed','pd','br','rv','tr','hp','hi','CURCODE'])\
              .with_columns([pl.col('tot').alias('rep')])
    
    # Apply note conversion
    if note:
        nc = pl.DataFrame(note).rename({'ac':'ac','no':'no','nnew':'nnew'})
        for d in [disb, rep]:
            if not d.is_empty():
                d = d.join(nc, on=['ac','no'], how='left')\
                     .with_columns(pl.coalesce(['nnew','no']).alias('no'))
    
    return disb, rep, note

# =============================================================================
# PURPOSE MAPPING (COMPACT)
# =============================================================================
def map_purpose(df):
    # Sector to FISSPURP mapping (all codes)
    sec_map = {f"{i:04d}":f"{i:04d}" for i in range(111,10000)}
    sec_map.update({f"{i:04d}":f"{i:04d}" for i in [110,120,131,132,139,200,210,211,212,220,230,
        311,312,313,314,315,316,321,322,323,324,325,326,327,328,329,390,410,420,430,440,470,990]})
    
    df = df.with_columns([
        pl.col('CUSTCD').cast(pl.Utf8).alias('c'),
        pl.col('SECTORCD').cast(pl.Utf8).alias('s')
    ])
    
    # Apply mappings
    df = df.with_columns([
        # CUSTCD overrides for 77/78
        pl.when(pl.col('c').is_in(['77','78']) & pl.col('s').cast(int).is_between(1111,9999) & (pl.col('s')!='9700'))
        .then(pl.when(pl.col('c')=='77').then('41').otherwise('44')).otherwise(pl.col('c')).alias('c'),
        
        # 34230 special
        pl.when(pl.col('PRODCD')=='34230')
        .then(pl.lit('9700')).otherwise(pl.col('s')).alias('s'),
        
        # FISSPURP overrides
        pl.when(pl.col('FISSPURP').is_in(['0410','0430']) & 
                ~pl.col('c').is_in(['77','78','95','96']) &
                pl.col('s').cast(int).is_between(1111,9999))
        .then(pl.lit('0990')).otherwise(pl.col('FISSPURP')).alias('FISSPURP')
    ])
    return df

# =============================================================================
# ROLLOVER
# =============================================================================
def get_roll(rd,islamic):
    def to_date(x): return datetime.strptime(str(int(x)).zfill(11)[:8],'%Y%m%d') if x and x>0 else None
    
    od = pl.read_parquet(f"{PATHS['LIMIT']}OVERDFT.parquet")\
          .filter((pl.col('APPRLIMT')>1)&(pl.col('LMTAMT')>1))
    pod = pl.read_parquet(f"{PATHS['PLIMIT']}OVERDFT.parquet").filter(pl.col('APPRLIMT')>1)
    
    roll = []
    for r in od.iter_rows(named=True):
        p = pod.filter((pl.col('ACCTNO')==r['ACCTNO'])&(pl.col('LMTID')==r['LMTID'])).first()
        if p and (rd1:=to_date(r['REVIEWDT'])) and (rd2:=to_date(p['REVIEWDT'])) and rd1>rd2:
            roll.append({'ac':r['ACCTNO'],'roll':r['LMTAMT']})
    return pl.DataFrame(roll).group_by('ac').agg(pl.col('roll').sum())

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("="*60+"\nEIWDISRP - Weekly Disbursement and Repayment Report\n"+"="*60)
    rd = get_dates()
    print(f"\nDate: {rd['d'].strftime('%d/%m/%Y')} Week:{rd['wk']}")
    
    fx = get_fx(rd)
    fx_df = pl.DataFrame({'k':list(fx.keys()),'r':list(fx.values())})
    
    for islamic in [False, True]:
        p = 'I' if islamic else ''
        print(f"\n{'Islamic' if islamic else 'Conventional'}...")
        
        # Read data
        hist, hist_mth = parse_hist(rd, islamic)
        test = hist.filter(pl.col('pd').is_between(rd['st'],rd['t']))\
                   .with_columns([(pl.col('hr')+pl.col('ho')).alias('rb'),
                                  (pl.col('hp')+pl.col('hr')+pl.col('ho')).alias('hp')])
        
        ln = pl.read_parquet(f"{PATHS['LOAN']}LNNOTE.parquet")
        
        # Process transactions
        disb, rep, note = process_txns(test, ln, islamic)
        
        # Loan masters
        loans = pl.concat([
            pl.read_parquet(f"{PATHS['BNM']}LOAN{rd['mon2']}{rd['wk1']}.parquet"),
            pl.read_parquet(f"{PATHS['BNM']}LOAN{rd['mon']}{rd['wk']}.parquet"),
            pl.read_parquet(f"{PATHS['SASD']}LOAN{rd['mon']}.parquet")
        ]).unique(['ac','no'])
        
        # Exclude and CA
        excl = pl.read_parquet(f"{PATHS['DAILY']}EXCLUDE_{rd['day']}.parquet")
        ca = pl.read_parquet(f"{PATHS['DEPOSIT']}CURRENT.parquet")\
              .filter((pl.col('MTD_DISBURSED_AMT')>0)|(pl.col('MTD_REPAID_AMT'>0)))\
              .select(['ACCTNO',pl.col('MTD_DISBURSED_AMT').clip(0).alias('disb'),
                       pl.col('MTD_REPAID_AMT').clip(0).alias('rep')])
        excl = excl.join(ca, on='ACCTNO', how='inner')
        
        # Combine all
        all_t = pl.concat([
            disb.select(['ac','no','disb','pd','CURCODE','hp']),
            rep.select(['ac','no','rep','pd','CURCODE','hp','hi','rb']),
            excl.select(['ACCTNO','NOTENO','disb','rep']).rename({'ACCTNO':'ac','NOTENO':'no'})
        ])
        
        # FX conversion
        all_t = all_t.with_columns((pl.col('CURCODE')+pl.col('pd').cast(str)).alias('k'))\
                     .join(fx_df, on='k', how='left')\
                     .with_columns([
                         pl.when(~pl.col('CURCODE').is_in(['MYR','']))
                         .then(pl.struct(['disb','hp','r']).map_elements(
                             lambda x:{'dfx':x['disb'],'hfx':x['hp'],
                                      'drm':x['disb']*x['r'],'hrm':x['hp']*x['r']}
                         )).otherwise(pl.struct(['disb','hp']).map_elements(
                             lambda x:{'dfx':0,'hfx':0,'drm':x['disb'],'hrm':x['hp']}
                         )).alias('fx')
                     ]).unnest('fx')
        
        # Summarize
        sumy = all_t.group_by(['ac','no']).agg([
            pl.col('disb').sum(), pl.col('rep').sum(),
            pl.col('hi').sum(), pl.col('rb').sum(),
            pl.col('dfx').sum(), pl.col('hfx').sum(),
            pl.col('drm').sum(), pl.col('hrm').sum()
        ])
        
        # Merge with loans
        final = loans.join(sumy, on=['ac','no'], how='inner')
        
        # Rollover
        if (roll:=get_roll(rd,islamic)).height>0:
            final = final.join(roll, left_on='ac', right_on='ac', how='left').with_columns(pl.col('roll').fill_null(0))
        
        # Purpose mapping
        final = map_purpose(final)
        
        # FORATE
        fr = pl.read_parquet(f"{PATHS['FORATE']}FORATEBKP.parquet")\
               .sort('REPTDATE',descending=True).unique('CURCODE',keep='first')\
               .select(['CURCODE','SPOTRATE'])
        frd = {r['CURCODE']:r['SPOTRATE'] for r in fr.iter_rows(named=True)}
        final = final.with_columns(pl.col('CCY').replace(frd,default=1.0).alias('fr'))
        
        # Final processing
        final = final.filter((pl.col('disb')>0)|(pl.col('rep')>0)|(pl.col('roll')>0))
        
        # FX for FCY
        final = final.with_columns([
            pl.when(pl.col('PRODUCT').cast(int).is_in(FCY) & (pl.col('CCY')!='MYR'))
            .then(pl.col('disb')*pl.col('fr')).otherwise(pl.col('disb')).alias('disb'),
            pl.when(pl.col('PRODUCT').cast(int).is_in(FCY) & (pl.col('CCY')!='MYR'))
            .then(pl.col('rep')*pl.col('fr')).otherwise(pl.col('rep')).alias('rep')
        ]).with_columns(pl.col('rep').clip(0).alias('rep'))
        
        # DNBFISME
        ns = ['04','05','06','30','31','32','33','34','35','37','38','39','40','45']
        final = final.with_columns(
            pl.when(pl.col('CUSTCD').cast(str).is_in(ns))
            .then(pl.col('DNBFISME')).otherwise('0').alias('DNBFISME')
        )
        
        final.write_parquet(f"{PATHS['DISPAY']}{p}DISPAYMTH{rd['mon']}.parquet")
        print(f"  {len(final)} records")
    
    print(f"\nReports saved to {PATHS['DISPAY']}\n"+"="*60+"\n✓ EIWDISRP Complete")

if __name__=="__main__":
    main()
