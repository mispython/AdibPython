import polars as pl
from pathlib import Path
from datetime import datetime
import sys

# Product codes (equivalent to SAS macro variables)
PBBPROD = [200,201,204,205,209,210,211,212,214,215,219,220,225,226,227,228,230,
           233,234,235,236,237,238,239,240,241,242,243,300,301,304,305,359,361,
           363,213,216,217,218,231,232,244,245,246,247,315,568,248,249,250,348,349,368]

PIBPROD = [152,153,154,155,423,424,425,426,175,176,177,178,400,401,402,406,407,
           408,409,410,411,412,413,414,415,416,419,420,422,429,430,464]

def calculate_report_dates(reptdate: datetime) -> dict:
    """Calculate report date variables."""
    day = reptdate.day
    
    # Determine week number
    if 1 <= day <= 8:
        nowk = '1'
    elif 9 <= day <= 15:
        nowk = '2'
    elif 16 <= day <= 22:
        nowk = '3'
    else:
        nowk = '4'
    
    # Previous month date
    prevdate = reptdate.replace(day=1) - pl.duration(days=1)
    
    # Calculate PDATE (2.5 years back)
    month = reptdate.month
    year = reptdate.year
    if month >= 6:
        pmth = month - 5
        pyear = year - 2
    else:
        pmth = month + 7
        pyear = year - 3
    
    pdate = datetime(pyear, pmth, 1).date()
    
    return {
        'nowk': nowk,
        'reptmon': f"{reptdate.month:02d}",
        'prevmon': f"{prevdate.month:02d}",
        'rdate': reptdate.strftime('%d%m%Y'),
        'reptdt': reptdate.date(),
        'prdate': pdate,
        'prevdate': prevdate.date()
    }

def process_eiqprom2(
    reptdate: datetime,
    lnnote_pbb_path: Path,
    lnnote_pib_path: Path,
    loan_pbb_path: Path,
    loan_pib_path: Path,
    billbad_path: Path,
    lncomm_pbb_path: Path,
    lncomm_pib_path: Path,
    collater_pbb_path: Path,
    collater_pib_path: Path,
    cis_loan_path: Path,
    elds_path: Path,
    output_loan_path: Path,
    output_summary_path: Path
) -> None:
    """
    Process EIQPROM2 - Filter customers with 2.5 years prompt repayment.
    """
    
    # Calculate dates
    dates = calculate_report_dates(reptdate)
    print(f"Report Date: {dates['reptdt']}")
    print(f"PDATE (2.5 years back): {dates['prdate']}")
    print(f"Week: {dates['nowk']}, Month: {dates['reptmon']}")
    print("-" * 80)
    
    # 1. Read and filter LNNOTE
    lnnote_pbb = pl.read_parquet(lnnote_pbb_path)
    lnnote_pib = pl.read_parquet(lnnote_pib_path)
    
    lnnote = pl.concat([lnnote_pbb, lnnote_pib]).filter(
        ((pl.col('LOANTYPE').is_in(PBBPROD)) | (pl.col('LOANTYPE').is_in(PIBPROD))) &
        (pl.col('LOANSTAT') != 3) &
        (pl.col('CURBAL') > 0) &
        (pl.col('LSTTRNCD') != 661)
    ).with_columns([
        pl.col('VINNO').str.slice(0, 13).str.replace_all(r'[@*(#)\-]', '').alias('AANO')
    ]).filter(
        pl.col('AANO').str.len_chars() == 13
    ).select(['ACCTNO','NOTENO','NAME','FLAG1','ORGBAL','NETPROC','AANO',
              'COLLMAKE','COLLYEAR','NTINDEX','SPREAD','MODELDES','SCORE1',
              'IA_LRU','BORSTAT','DELQCD','GUAREND','MAILCODE'])
    
    print(f"LNNOTE records: {len(lnnote):,}")
    
    # 2. Read and process LOAN
    loan_pbb = pl.read_parquet(loan_pbb_path)
    loan_pib = pl.read_parquet(loan_pib_path)
    
    loan = pl.concat([loan_pbb, loan_pib]).filter(
        ((pl.col('PRODUCT').is_in(PBBPROD)) | (pl.col('PRODUCT').is_in(PIBPROD))) &
        (pl.col('LOANSTAT') != 3) &
        (pl.col('CURBAL') > 0)
    ).with_columns([
        pl.when(pl.col('BLDATE') > 0)
          .then(pl.lit(dates['reptdt']) - pl.col('BLDATE'))
          .otherwise(0)
          .alias('DAYDIFF'),
        pl.when(pl.col('EXPRDATE') > 0)
          .then(pl.col('EXPRDATE').dt.year() - pl.lit(dates['reptdt'].year))
          .otherwise(0)
          .alias('REMTERM'),
        pl.col('NOTENO').cast(pl.Utf8).str.slice(0, 1).alias('NOTE1'),
        pl.col('NOTENO').cast(pl.Utf8).str.slice(1, 1).alias('NOTE2')
    ]).select(['ACCTNO','NOTENO','COMMNO','ISSDTE','DAYDIFF','PRODUCT',
               'NOTE1','NOTE2','BRANCH','APPRLIMT','BALANCE','EXPRDATE','REMTERM'])
    
    print(f"LOAN records: {len(loan):,}")
    
    # 3. Remove bad bills
    billbad = pl.read_parquet(billbad_path).unique(subset=['ACCTNO','NOTENO'])
    loan = loan.join(billbad, on=['ACCTNO','NOTENO'], how='anti')
    print(f"After removing BILLBAD: {len(loan):,}")
    
    # 4. Merge LNNOTE
    loan = loan.join(lnnote, on=['ACCTNO','NOTENO'], how='inner')
    print(f"After merging LNNOTE: {len(loan):,}")
    
    # 5. Merge LNCOMM
    lncomm_pbb = pl.read_parquet(lncomm_pbb_path)
    lncomm_pib = pl.read_parquet(lncomm_pib_path)
    lncomm = pl.concat([lncomm_pbb, lncomm_pib]).unique(subset=['ACCTNO','COMMNO'])
    
    loan = loan.join(lncomm.select(['ACCTNO','COMMNO','CORGAMT']), 
                     on=['ACCTNO','COMMNO'], how='left')
    print(f"After merging LNCOMM: {len(loan):,}")
    
    # 6. Apply main filters
    loan = loan.filter(
        (pl.col('BALANCE') > 30000) &
        (pl.col('ISSDTE') < dates['prdate']) &
        (pl.col('FLAG1') == 'F') &
        (pl.col('DAYDIFF') <= 0) &
        ((pl.col('PRODUCT').is_in(PBBPROD)) | (pl.col('PRODUCT').is_in(PIBPROD))) &
        (pl.col('NOTE1') != '1') &
        (pl.col('NOTE2') != '2')
    ).with_columns([
        pl.max_horizontal(['CORGAMT','ORGBAL','NETPROC','APPRLIMT']).alias('LMTAPPR')
    ]).with_columns([
        (pl.col('LMTAPPR') - pl.col('BALANCE')).alias('REPAID')
    ])
    
    print(f"After main filters: {len(loan):,}")
    
    # 7. Exclude based on NTINDEX, SPREAD, MODELDES, SCORE1, etc.
    loan = loan.filter(
        ~(
            ((pl.col('NTINDEX') == 1) & (pl.col('SPREAD') == 0.00)) |
            ((pl.col('NTINDEX') == 1) & (pl.col('SPREAD') == 3.50)) |
            ((pl.col('NTINDEX') == 38) & (pl.col('SPREAD') == 3.20)) |
            ((pl.col('NTINDEX') == 38) & (pl.col('SPREAD') == 6.70)) |
            (pl.col('MODELDES').str.slice(0, 1).is_in(['S','T','R','C'])) |
            (pl.col('MODELDES') == 'Z') |
            (pl.col('MODELDES').str.slice(4, 1) == 'F') |
            (pl.col('SCORE1').str.slice(0, 1).is_in(['D','E','F','G','H','I'])) |
            (pl.col('IA_LRU') == 'I') |
            (pl.col('BORSTAT') == 'K') |
            (pl.col('DELQCD').is_in(['9','09','10','11','12','13','14','15',
                                     '16','17','18','19','20']))
        )
    )
    
    print(f"After exclusion rules: {len(loan):,}")
    
    # 8. Process COLLATER and exclude
    coll_pbb = pl.read_parquet(collater_pbb_path)
    coll_pib = pl.read_parquet(collater_pib_path)
    
    coll = pl.concat([coll_pbb, coll_pib]).with_columns([
        pl.when(
            ((pl.col('CPRPROPD').is_in(['10','11','32','33','34','35'])) &
             (pl.col('CPRLANDU').is_in(['10','11','32','33','34','35']))) |
            (pl.col('MRESERVE') == 'Y')
        ).then(pl.lit('Y')).otherwise(pl.lit(None)).alias('EXCL')
    ]).filter(
        (pl.col('EXCL') == 'Y') | (pl.col('HOLDEXPD') == 'L')
    ).select(['ACCTNO','NOTENO','EXCL','HOLDEXPD','EXPDATE'])
    
    loan = loan.join(coll, on=['ACCTNO','NOTENO'], how='left')
    loan = loan.filter(
        ~(
            (pl.col('EXCL') == 'Y') |
            ((pl.col('HOLDEXPD') == 'L') & 
             ((pl.col('EXPRDATE') - pl.col('EXPDATE')).dt.total_days() / 365 < 30))
        )
    )
    
    print(f"After COLLATER exclusion: {len(loan):,}")
    
    # 9. CIS exclusions
    cis = pl.read_parquet(cis_loan_path)
    
    # Split CIS into loan and bill accounts
    cisbill = cis.filter(
        ((pl.col('ACCTNO') >= 2500000000) & (pl.col('ACCTNO') <= 2599999999)) |
        ((pl.col('ACCTNO') >= 2850000000) & (pl.col('ACCTNO') <= 2859999999))
    ).select(['CUSTNO']).unique()
    
    cisln = cis.filter(
        ~(((pl.col('ACCTNO') >= 2500000000) & (pl.col('ACCTNO') <= 2599999999)) |
          ((pl.col('ACCTNO') >= 2850000000) & (pl.col('ACCTNO') <= 2859999999)))
    )
    
    cisln = cisln.join(cisbill, on='CUSTNO', how='inner').select(['ACCTNO']).unique()
    loan = loan.join(cisln, on='ACCTNO', how='anti')
    
    # Exclude SECCUST = 901
    cis_901 = cis.filter(pl.col('SECCUST') == '901').select(['ACCTNO']).unique()
    loan = loan.join(cis_901, on='ACCTNO', how='anti')
    
    print(f"After CIS exclusion: {len(loan):,}")
    
    # 10. ELDS exclusion
    elds = pl.read_parquet(elds_path).select(['AANO','REINPROD'])
    loan = loan.join(elds, on='AANO', how='left')
    loan = loan.filter(pl.col('REINPROD') != 'Y')
    
    print(f"After ELDS exclusion: {len(loan):,}")
    
    # 11. Add NEW flag (compare with previous month)
    try:
        prev_month_loan = pl.read_parquet(
            output_loan_path.parent / f"LOAN{dates['prevmon']}.parquet"
        ).select(['ACCTNO','NOTENO'])
        
        loan = loan.join(
            prev_month_loan.with_columns(pl.lit('N').alias('_prev')),
            on=['ACCTNO','NOTENO'],
            how='left'
        ).with_columns([
            pl.when(pl.col('_prev').is_null()).then(pl.lit('Y')).otherwise(pl.lit('')).alias('NEW')
        ]).drop('_prev')
    except:
        loan = loan.with_columns(pl.lit('Y').alias('NEW'))
    
    # 12. Sort and save
    loan = loan.sort(['BRANCH','ACCTNO','NOTENO'])
    
    # Save main output
    loan.write_parquet(output_loan_path)
    print(f"\nFinal output: {len(loan):,} records")
    print(f"Saved to: {output_loan_path}")
    
    # 13. Create summary by branch
    summary = loan.group_by('BRANCH').agg([
        pl.count().alias('NOACCT'),
        pl.sum('LMTAPPR').alias('LMTAPPR'),
        pl.sum('BALANCE').alias('BALANCE'),
        pl.sum('REPAID').alias('REPAID')
    ]).sort('BRANCH')
    
    summary.write_parquet(output_summary_path)
    print(f"Summary saved to: {output_summary_path}")
    
    # Print summary
    print("\n" + "="*80)
    print("SUMMARY BY BRANCH")
    print("="*80)
    print(summary)

if __name__ == "__main__":
    # Set report date
    report_date = datetime.now()  # Replace with actual REPTDATE
    
    # Define input paths
    input_dir = Path(".")
    
    # Process EIQPROM2
    process_eiqprom2(
        reptdate=report_date,
        lnnote_pbb_path=input_dir / "PBBLN_LNNOTE.parquet",
        lnnote_pib_path=input_dir / "PIBLN_LNNOTE.parquet",
        loan_pbb_path=input_dir / f"PBBSAS_LOAN{report_date.month:02d}.parquet",
        loan_pib_path=input_dir / f"PIBSAS_LOAN{report_date.month:02d}.parquet",
        billbad_path=input_dir / "LNBILL_BILL.parquet",  # From EIQPROM1
        lncomm_pbb_path=input_dir / "PBBLN_LNCOMM.parquet",
        lncomm_pib_path=input_dir / "PIBLN_LNCOMM.parquet",
        collater_pbb_path=input_dir / "COLL_COLLATER.parquet",
        collater_pib_path=input_dir / "COLLI_COLLATER.parquet",
        cis_loan_path=input_dir / "CIS_LOAN.parquet",
        elds_path=input_dir / "ELDS_ELBNMAX.parquet",
        output_loan_path=input_dir / f"LOAN{report_date.month:02d}.parquet",
        output_summary_path=input_dir / "BILLSUM.parquet"
    )
