"""
SAS to Python Translation - EIMDISRP
Processes loan disbursement and repayment data for monthly reporting
"""

import polars as pl
from pathlib import Path
from datetime import datetime, timedelta, date

BASE_DIR = Path("./")
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"

# Create output directories
for d in [OUTPUT_DIR / "disp ay", OUTPUT_DIR / "idispay"]:
    d.mkdir(parents=True, exist_ok=True)

# Library paths
LOAN_DIR = INPUT_DIR / "loan"
ILOAN_DIR = INPUT_DIR / "iloan"
FORATE_DIR = INPUT_DIR / "forate"
FEE_DIR = INPUT_DIR / "fee"
BNM_DIR = INPUT_DIR / "bnm"
IBNM_DIR = INPUT_DIR / "ibnm"
SASD_DIR = INPUT_DIR / "sasd"
ISASD_DIR = INPUT_DIR / "isasd"
LIMIT_DIR = INPUT_DIR / "limit"
ILIMIT_DIR = INPUT_DIR / "ilimit"
DEPOSIT_DIR = INPUT_DIR / "deposit"
IDEPOSIT_DIR = INPUT_DIR / "ideposit"
DISPAY_DIR = OUTPUT_DIR / "disp ay"
IDISPAY_DIR = OUTPUT_DIR / "idispay"

def sas_date_to_dt(sas_date):
    return datetime(1960, 1, 1) + timedelta(days=int(sas_date)) if sas_date and sas_date > 0 else None

def dt_to_sas_date(dt):
    return (dt - datetime(1960, 1, 1).date()).days if dt else 0

def parse_mmddyyyy(s):
    return datetime(int(s[4:8]), int(s[0:2]), int(s[2:4])).date() if s and len(s) == 8 else None

def julian_to_date(j):
    if not j or j <= 0: return None
    j_str = str(int(j)).zfill(7)
    return datetime(int(j_str[0:4]), 1, 1) + timedelta(days=int(j_str[4:7]) - 1)

def setup_dates():
    reptdate = sas_date_to_dt(pl.read_parquet(LOAN_DIR / "REPTDATE.parquet").select(pl.first()).item())
    mm2 = reptdate.month - 1 or 12
    stdate = date(reptdate.year, reptdate.month, 1)
    return {
        'REPTDATE': reptdate, 'MM': reptdate.month, 'MM2': mm2,
        'NOWK': '4', 'NOWK1': '1', 'NOWK2': '2', 'NOWK3': '3',
        'REPTYEAR': reptdate.strftime('%y'), 'REPTMON': str(reptdate.month).zfill(2),
        'REPTDAY': str(reptdate.day).zfill(2), 'REPTMON2': str(mm2).zfill(2),
        'RDATE': reptdate.strftime('%d%m%Y'), 'SDATE': stdate,
        'SDATE_SAS': dt_to_sas_date(stdate), 'TDATE': reptdate,
        'TDATE_SAS': dt_to_sas_date(reptdate), 'STYEAR': date(reptdate.year, 1, 1),
        'STYEAR_SAS': dt_to_sas_date(date(reptdate.year, 1, 1)),
        'LOOPDT': reptdate - timedelta(days=61), 'LOOPDT_SAS': dt_to_sas_date(reptdate - timedelta(days=61))
    }

def get_forex_rates(mv):
    dates = pl.DataFrame({'REPTDATE': [dt_to_sas_date(mv['LOOPDT'] + timedelta(days=i)) 
                                       for i in range((mv['TDATE'] - mv['LOOPDT']).days + 1)]})
    fx = pl.read_parquet(FORATE_DIR / "FORATEBKP.parquet").filter(
        pl.col('REPTDATE').is_between(mv['LOOPDT_SAS'], mv['TDATE_SAS'])
    ).sort(['CURCODE', 'REPTDATE'], descending=True).unique(subset=['CURCODE'])
    
    work = dates.with_columns(pl.lit(None).alias('REPXDATE'))
    work = work.join(fx.select('REPTDATE').unique(), on='REPTDATE', how='left', indicator='m')
    work = work.with_columns(pl.when(pl.col('m') == 'both').then(pl.col('REPTDATE')).otherwise(None).alias('REPXDATE')).drop('m')
    work = work.with_columns(pl.col('REPXDATE').forward_fill())
    
    return work.join(fx.select(['REPTDATE', 'CURCODE', 'SPOTRATE']), 
                     left_on='REPXDATE', right_on='REPTDATE', how='left').drop_nulls('SPOTRATE')

def process_histfile(mv):
    hist = pl.read_parquet(INPUT_DIR / "HISTFILE.parquet").filter(
        ~((pl.col('TRANCODE') == 660) & (pl.col('CHANNEL') == 5))
    ).with_columns([
        pl.when(pl.col('TRANCODE').is_in([666, 726]))
          .then(pl.when(pl.col('ACCRADJ') == 0).then(pl.col('TAMT')).otherwise(pl.col('ACCRADJ')))
          .when(pl.col('TRANCODE') == 999).then(pl.col('HISTINT'))
          .otherwise(pl.col('TAMT')).alias('TOTAMT')
    ])
    
    neg = [310, 726, 750, 751, 752, 753, 754, 756, 758, 760, 761]
    hist = hist.with_columns(
        pl.when(pl.col('TRANCODE').is_in(neg) & ~((pl.col('TRANCODE') == 726) & (pl.col('REVERSED') == 'R')))
          .then(pl.col('TOTAMT') * -1).otherwise(pl.col('TOTAMT')).alias('TOTAMT')
    ).with_columns([
        pl.col('EFFDATE').apply(julian_to_date).alias('EFFDATE'),
        pl.when(pl.col('POSTDT').is_not_null() & (pl.col('POSTDT') != 0))
          .then(pl.col('POSTDT').cast(str).str.zfill(11).str.slice(0, 8).str.strptime(pl.Date, '%m%d%Y'))
          .otherwise(None).alias('POSTDATE')
    ])
    
    return (hist.filter((pl.col('ACCTNO') > 0) & (pl.col('POSTDATE') >= mv['STYEAR'])),
            hist.filter((pl.col('ACCTNO') > 0) & ((pl.col('EFFDATE') >= mv['SDATE']) | (pl.col('POSTDATE') >= mv['SDATE']))))

def get_feetran(mv):
    f = FEE_DIR / f"FEETRAN{mv['REPTMON']}{mv['NOWK']}{mv['REPTYEAR']}.parquet"
    return pl.read_parquet(f).filter((pl.col('TRANCODE') == 680) & (pl.col('REVERSED') != 'R')).with_columns([
        pl.lit('10').alias('REPAY_TYPE_CD'), pl.col('ACTDATE').alias('POSTDATE'), pl.col('TRANAMT').alias('TOTAMT')
    ]) if f.exists() else None

def add_counts(df, by):
    return df.with_columns(pl.cum_count(by[0]).over(by).alias('COUNT')) if df is not None else None

def process_disbursements(df, ln):
    d310 = df.filter((pl.col('TRANCODE') == 310) & (pl.col('REVERSED') != 'R')).with_columns(pl.col('NOTENO').alias('NOTENEW'))
    d310 = add_counts(d310.sort(['ACCTNO', 'TOTAMT', 'EFFDATE']), ['ACCTNO', 'TOTAMT', 'EFFDATE'])
    d310 = d310.with_columns(pl.when(pl.col('NTINT') == 'A').then(-pl.col('NETPROC')).otherwise(pl.col('TOTAMT')).alias('TOTAMT'))
    if 'LOANTYPE' in d310.columns:
        d310 = d310.with_columns(pl.when(pl.col('LOANTYPE').is_in([700, 705, 128, 130]))
                                  .then((-(pl.col('NETPROC') + pl.col('FEEAMT').fill_null(0))).round(2)).alias('TOTAMT_HP'))
    
    d652 = df.filter((pl.col('TRANCODE') == 652) & (pl.col('REVERSED') != 'R')).with_columns((pl.col('TOTAMT') * -1).alias('TOTAMT'))
    d652 = add_counts(d652.sort(['ACCTNO', 'TOTAMT', 'EFFDATE']), ['ACCTNO', 'TOTAMT', 'EFFDATE'])
    d652_sum = d652.group_by(['ACCTNO', 'EFFDATE']).agg(pl.col('TOTAMT').sum().round(2).alias('TOTAMT')).with_columns(pl.lit(1).alias('COUNT'))
    
    d750 = df.filter((pl.col('TRANCODE') == 750) & (pl.col('REVERSED') != 'R')).with_columns(pl.col('NOTENO').alias('NOTENEW'))
    
    # Merge logic
    m1 = d310.sort(['ACCTNO', 'TOTAMT', 'EFFDATE', 'COUNT']).join(
        d652_sum.select(['ACCTNO', 'TOTAMT', 'EFFDATE', 'COUNT']), on=['ACCTNO', 'TOTAMT', 'EFFDATE', 'COUNT'], how='left', indicator='m')
    d310_out = m1.filter(pl.col('m') == 'left_only').drop('m')
    d652t = m1.filter(pl.col('m') == 'both').drop('m')
    
    note1 = d652t.join(d652.select(['ACCTNO', 'EFFDATE', 'NOTENO']), on=['ACCTNO', 'EFFDATE'], how='inner'
                      ).select(['ACCTNO', 'NOTENO', pl.col('NOTENO_right').alias('NOTENEW')])
    d652 = d652.join(note1.select('ACCTNO'), on='ACCTNO', how='anti')
    
    m2 = d310_out.sort(['ACCTNO', 'TOTAMT', 'EFFDATE']).join(
        d652.select(['ACCTNO', 'TOTAMT', 'EFFDATE']), on=['ACCTNO', 'TOTAMT', 'EFFDATE'], how='left', indicator='m')
    d310_out2 = m2.filter(pl.col('m') == 'left_only').drop('m')
    note2 = m2.filter(pl.col('m') == 'both').drop('m').select(['ACCTNO', 'NOTENO']).with_columns(pl.col('NOTENO').alias('NOTENEW'))
    d652 = d652.join(note2.select('ACCTNO'), on='ACCTNO', how='anti')
    
    m3 = d310_out2.sort(['ACCTNO', 'TOTAMT_HP']).join(
        d652.rename({'TOTAMT': 'TOTAMT_HP'}).select(['ACCTNO', 'TOTAMT_HP']), on=['ACCTNO', 'TOTAMT_HP'], how='left', indicator='m')
    d310_final = m3.filter(pl.col('m') == 'left_only').drop('m')
    note3 = m3.filter(pl.col('m') == 'both').drop('m').select(['ACCTNO', 'NOTENO']).with_columns(pl.col('NOTENO').alias('NOTENEW'))
    d652_final = m3.filter(pl.col('m') == 'both').drop('m').rename({'TOTAMT_HP': 'TOTAMT'})
    
    notes = pl.concat([n for n in [note1, note2, note3] if n is not None and len(n) > 0]).unique(['ACCTNO', 'NOTENO'])
    return d310_final, d652_final, d750, notes

def process_repayments(df, d750, notes):
    r610 = df.filter((pl.col('TRANCODE').is_in([610,614,615,619,650,660,661,668,680]) | 
                     ((pl.col('TRANCODE') == 612) & (pl.col('NTBRCH') == 130)))
    ).with_columns(pl.when(pl.col('REVERSED') == 'R').then(1).otherwise(0).alias('IND_R'))
    r610 = add_counts(r610.sort(['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'IND_R'], descending=True).drop('IND_R'),
                     ['ACCTNO', 'NOTENO', 'TOTAMT_CNT'])
    
    r652 = df.filter((pl.col('REVERSED') == 'R') & ~pl.col('TRANCODE').is_in([610,612,614,615,619,650,660,661,668,680])
                    ).sort(['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'EFFDATE'])
    r652 = add_counts(r652, ['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'EFFDATE'])
    
    r800 = df.filter(pl.col('TRANCODE').is_in([800,801,810])).with_columns((pl.col('TOTAMT') * -1).alias('TOTAMT'))
    r800 = add_counts(r800.sort(['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'EFFDATE']), ['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'EFFDATE'])
    r800 = r800.join(r652.select(['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'EFFDATE', 'COUNT']), 
                     on=['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'EFFDATE', 'COUNT'], how='anti')
    r800 = add_counts(r800, ['ACCTNO', 'NOTENO', 'TOTAMT_CNT'])
    
    r610 = r610.join(r800.select(['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'COUNT']), 
                     on=['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'COUNT'], how='anti')
    
    r754 = df.filter(pl.col('TRANCODE').is_in([726,754,757]) & (pl.col('REVERSED') != 'R'))
    if d750 is not None: r754 = pl.concat([r754, d750])
    r754 = add_counts(r754.sort(['ACCTNO', 'NOTENO', 'TOTAMT_CNT']), ['ACCTNO', 'NOTENO', 'TOTAMT_CNT'])
    
    r610 = add_counts(r610, ['ACCTNO', 'NOTENO', 'TOTAMT_CNT'])
    m = r610.join(r754.select(['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'COUNT']), 
                  on=['ACCTNO', 'NOTENO', 'TOTAMT_CNT', 'COUNT'], how='left', indicator='m')
    r_final = m.filter(pl.col('m') == 'left_only').drop('m')
    r754x = m.filter(pl.col('m') == 'both').drop('m')
    
    r_final = r_final.join(notes, on=['ACCTNO', 'NOTENO'], how='left').with_columns(
        pl.when(pl.col('NOTENEW').is_not_null()).then(pl.col('NOTENEW')).otherwise(pl.col('NOTENO')).alias('NOTENO'))
    
    return r_final, r754x.filter(pl.col('TRANCODE') == 750)

def process_od_rollover(path, deposit_path, mv):
    od = pl.read_parquet(path / "OVERDFT.parquet").filter((pl.col('APPRLIMT') > 1) & (pl.col('LMTAMT') > 1)
                         ).select(['ACCTNO', 'LMTID', 'REVIEWDT', 'LMTAMT'])
    pod = pl.read_parquet(path / "POVERDFT.parquet").filter((pl.col('APPRLIMT') > 1) & ~pl.col('REVIEWDT').is_in([0, None])
                          ).select(['ACCTNO', 'LMTID', 'REVIEWDT'])
    
    lim = od.join(pod.rename({'REVIEWDT': 'PREVIEWDT'}), on=['ACCTNO', 'LMTID'], how='inner')
    for c in ['REVIEWDT', 'PREVIEWDT']:
        lim = lim.with_columns(pl.when(pl.col(c) > 0).then(
            pl.col(c).cast(str).str.zfill(11).str.slice(0,8).str.strptime(pl.Date, '%m%d%Y')).otherwise(None).alias(c))
    lim = lim.filter(pl.col('REVIEWDT') > pl.col('PREVIEWDT'))
    lim_sum = lim.group_by('ACCTNO').agg(pl.col('LMTAMT').sum().alias('ROLLOVER'))
    
    cur = pl.read_parquet(deposit_path / "CURRENT.parquet").rename({'SECTOR': 'SECTORC'}).filter(~pl.col('OPENIND').is_in(['B','C','P']))
    cur = cur.with_columns([
        pl.col('SECTORC').cast(float).cast(int).cast(str).str.zfill(4).alias('SECTOR'),
        pl.when(pl.col('PRODUCT') == 104).then('02').when(pl.col('PRODUCT') == 105).then('81')
          .otherwise(pl.col('CUSTCODE').cast(str)).alias('CUSTCD'),
        pl.col('CURCODE').alias('CCY')
    ])
    return cur.join(lim_sum, on='ACCTNO', how='inner')

def process_loan_type(mv, is_islamic=False):
    prefix = 'I' if is_islamic else ''
    loan_dir = ILOAN_DIR if is_islamic else LOAN_DIR
    bnm_dir = IBNM_DIR if is_islamic else BNM_DIR
    sasd_dir = ISASD_DIR if is_islamic else SASD_DIR
    deposit_dir = IDEPOSIT_DIR if is_islamic else DEPOSIT_DIR
    limit_dir = ILIMIT_DIR if is_islamic else LIMIT_DIR
    out_dir = IDISPAY_DIR if is_islamic else DISPAY_DIR
    
    print(f"\nProcessing {'Islamic' if is_islamic else 'Conventional'} Loans")
    
    # Base data
    hist_yr, hist = process_histfile(mv)
    feetran = get_feetran(mv)
    disb = pl.concat([d for d in [hist_yr, feetran] if d is not None]).filter(
        (pl.col('POSTDATE') >= mv['SDATE']) & (pl.col('POSTDATE') <= mv['TDATE'])
    ).with_columns([
        (pl.col('HISTREB').fill_null(0) + pl.col('HISTOREB').fill_null(0)).alias('REBATE'),
        (pl.col('HISTPRIN').fill_null(0) + pl.col('HISTREB').fill_null(0) + pl.col('HISTOREB').fill_null(0)).alias('HISTPRIN'),
        pl.when(pl.col('REPAY_TYPE_CD') == '10').then(pl.col('TOTAMT')).alias('REPAID10'),
        pl.col('TOTAMT').abs().alias('TOTAMT_CNT')
    ])
    
    ln = pl.read_parquet(loan_dir / "LNNOTE.parquet").select(
        ['ACCTNO', 'NOTENO', 'NETPROC', 'NTINT', 'DELQCD', 'NTBRCH', 'DNBFISME', 'CURCODE', 'LOANTYPE', 'FEEAMT', 'BONUSANO'])
    disb = disb.join(ln, on=['ACCTNO', 'NOTENO'], how='inner').sort(['ACCTNO', 'TOTAMT', 'EFFDATE', 'BONUSANO'], descending=True)
    
    # Disbursements
    d310, d652, d750, notes = process_disbursements(disb, ln)
    
    # Repayments
    r_final, d750_extra = process_repayments(disb, d750, notes)
    
    # Disbursed final
    disb_final = pl.concat([
        d310.with_columns((pl.col('TOTAMT') * -1).alias('DISBURSE')),
        d750_extra.with_columns((pl.col('TOTAMT') * -1).alias('DISBURSE'))
    ]).filter(pl.col('REVERSED') != 'R')
    disb_final = disb_final.join(notes, on=['ACCTNO', 'NOTENO'], how='left').with_columns(
        pl.when(pl.col('NOTENEW').is_not_null()).then(pl.col('NOTENEW')).otherwise(pl.col('NOTENO')).alias('NOTENO'))
    
    # Loan files
    files = [pl.read_parquet(bnm_dir / f) for f in [
        f"LNWOF{mv['REPTMON']}{mv['NOWK']}.parquet",
        f"LNWOD{mv['REPTMON']}{mv['NOWK']}.parquet",
        f"LNWOF{mv['REPTMON2']}{mv['NOWK']}.parquet",
        f"LNWOD{mv['REPTMON2']}{mv['NOWK']}.parquet",
        f"LOAN{mv['REPTMON2']}{mv['NOWK']}.parquet"
    ] if (bnm_dir / f).exists()]
    if (sasd_dir / f"LOAN{mv['REPTMON']}.parquet").exists():
        files.append(pl.read_parquet(sasd_dir / f"LOAN{mv['REPTMON']}.parquet"))
    loan_all = pl.concat(files).unique(['ACCTNO', 'NOTENO'])
    
    # Summarize
    disb_sum = disb_final.group_by(['ACCTNO', 'NOTENO']).agg([
        pl.col('DISBURSE').sum(),
        pl.col('HISTINT').fill_null(0).sum().alias('HISTINT'),
        pl.col('REBATE').fill_null(0).sum().alias('REBATE'),
        pl.col('REPAID10').fill_null(0).sum().alias('REPAID10')
    ])
    repay_sum = r_final.group_by(['ACCTNO', 'NOTENO']).agg(pl.col('REPAID').sum())
    loan = loan_all.join(disb_sum.join(repay_sum, on=['ACCTNO','NOTENO'], how='outer'), on=['ACCTNO','NOTENO'], how='inner')
    
    # ALM processing
    prod_filter = pl.col('PRODCD').str.slice(0,3).is_in(['341','342','343','344','346']) | pl.col('PRODUCT').is_in([225,226])
    if is_islamic:
        prod_filter = prod_filter | pl.col('PRODUCT').is_in([698,699,983]) | (pl.col('PRODCD') == '54120')
    
    alm1 = pl.read_parquet(bnm_dir / f"LOAN{mv['REPTMON2']}{mv['NOWK']}.parquet").filter(prod_filter).select(
        ['ACCTNO','NOTENO','FISSPURP','PRODUCT','NOTETERM','BALANCE','PRODCD','CUSTCD','AMTIND','SECTORCD','BRANCH','CCY','FORATE','DNBFISME']
    ).rename({'BALANCE':'LASTBAL','NOTETERM':'LASTNOTE'})
    
    alm = pl.read_parquet(bnm_dir / f"LOAN{mv['REPTMON']}{mv['NOWK']}.parquet").filter(prod_filter).select(
        ['ACCTNO','NOTENO','FISSPURP','PRODUCT','NOTETERM','EARNTERM','BALANCE','APPRDATE','APPRLIM2','PRODCD','CUSTCD','AMTIND','SECTORCD','BRANCH','CCY','FORATE','DNBFISME']
    )
    
    # Rate changes
    lnhist = pl.read_parquet(DISPAY_DIR / "LNHIST.parquet").filter((pl.col('TRANCODE') == 400) & (pl.col('RATECHG') != 0)).unique(['ACCTNO','NOTENO'])
    alm = alm.join(ln.select(['ACCTNO','NOTENO','DELQCD']), on=['ACCTNO','NOTENO'], how='left')
    alm = alm.join(lnhist, on=['ACCTNO','NOTENO'], how='left')
    alm = alm.with_columns(
        pl.when(pl.col('DELQCD').is_null() & (pl.col('ISSDTE') < mv['SDATE_SAS'])).then('Y').otherwise('N').alias('RATECHG')
    ).with_columns(
        pl.when((pl.col('PRODCD').is_in(['34190','34690'])) & (pl.col('RATECHG') == 'Y')).then(pl.col('CURBAL')).otherwise(0).alias('ROLLOVER')
    )
    
    # OD Rollover
    ovdft = process_od_rollover(limit_dir, deposit_dir, mv)
    alm = alm.join(ovdft, on='ACCTNO', how='left')
    
    # Forex
    forate = pl.read_parquet(FORATE_DIR / "FORATEBKP.parquet").sort(['CURCODE','REPTDATE'], descending=True).unique('CURCODE').select(['CURCODE','SPOTRATE'])
    fr_dict = {r['CURCODE']: r['SPOTRATE'] for r in forate.rows()}
    
    # Final
    result = alm.filter((pl.col('DISBURSE') > 0) | (pl.col('REPAID') > 0) | (pl.col('ROLLOVER') > 0)).with_columns(
        pl.col('CCY').apply(lambda c: fr_dict.get(c, 1.0)).alias('FORATE')
    )
    
    prod_range = (800, 851) if not is_islamic else (851, 900)
    result = result.with_columns([
        pl.when((pl.col('PRODUCT').is_between(*prod_range)) & (pl.col('CCY') != 'MYR'))
          .then(pl.col('DISBURSE') * pl.col('FORATE')).otherwise(pl.col('DISBURSE')).alias('DISBURSE'),
        pl.when((pl.col('PRODUCT').is_between(*prod_range)) & (pl.col('CCY') != 'MYR'))
          .then(pl.col('REPAID') * pl.col('FORATE')).otherwise(pl.col('REPAID')).alias('REPAID')
    ]).with_columns([
        pl.when(pl.col('REPAID') <= 0).then(0).otherwise(pl.col('REPAID')).alias('REPAID'),
        pl.when(pl.col('REPAID10') <= 0).then(0).otherwise(pl.col('REPAID10')).alias('REPAID10')
    ])
    
    cust_excl = ['04','05','06','30','31','32','33','34','35','37','38','39','40','45']
    result = result.with_columns(
        pl.when(~pl.col('CUSTCD').cast(str).is_in(cust_excl)).then('0').otherwise(pl.col('DNBFISME')).alias('DNBFISME')
    )
    
    out_file = out_dir / f"{'I' if is_islamic else ''}DISPAYMTH{mv['REPTMON']}.parquet"
    result.write_parquet(out_file)
    print(f"Saved: {out_file}")
    return result

def main():
    print(f"Starting EIMDISRP at {datetime.now()}")
    mv = setup_dates()
    print(f"Report Date: {mv['RDATE']}, REPTMON: {mv['REPTMON']}")
    
    process_loan_type(mv, False)
    process_loan_type(mv, True)
    
    print(f"\nCompleted at {datetime.now()}")

if __name__ == "__main__":
    main()
    
