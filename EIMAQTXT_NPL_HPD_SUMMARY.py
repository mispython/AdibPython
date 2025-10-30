import polars as pl
import duckdb
from pathlib import Path
import datetime
import sys

# Configuration
loan_path = Path("LOAN")
npl6_path = Path("NPL6")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

# DATA REPTDATE;
reptdate_df = pl.read_parquet(loan_path / "REPTDATE.parquet")

# Extract parameters - CALL SYMPUT equivalent
first_row = reptdate_df.row(0)
MM = f"{first_row['REPTDATE'].month:02d}"  # Z2.
YY = str(first_row['REPTDATE'].year)       # YEAR4.
DD = f"{first_row['REPTDATE'].day:02d}"    # Z2.
RDATE = first_row['REPTDATE'].strftime('%d%m%y')  # DDMMYY8.

print(f"MM: {MM}, YY: {YY}, DD: {DD}, RDATE: {RDATE}")

# DATA _NULL_ for NPLDATE
npl_reptdate_df = pl.read_parquet(npl6_path / "REPTDATE.parquet")
NPLDATE = npl_reptdate_df.row(0)['REPTDATE'].strftime('%d%m%y')  # DDMMYY8.

print(f"NPLDATE: {NPLDATE}")
print(f"RDATE: {RDATE}")

# MACRO %PROCESS equivalent
if NPLDATE == RDATE:
    print("EXTRACT FROM AQ, SP2 & IIS")
    
    # PROC SORT DATA=NPL6.AQ OUT=AQ; BY ACCTNO NOTENO;
    aq_df = pl.read_parquet(npl6_path / "AQ.parquet").sort(['ACCTNO', 'NOTENO'])
    
    # PROC SORT DATA=NPL6.SP2 OUT=SP2(KEEP=ACCTNO NOTENO SP SPPL MARKETVL);
    sp2_df = pl.read_parquet(npl6_path / "SP2.parquet").select([
        'ACCTNO', 'NOTENO', 'SP', 'SPPL', 'MARKETVL'
    ]).sort(['ACCTNO', 'NOTENO'])
    
    # PROC SORT DATA=NPL6.IIS OUT=IIS(KEEP=ACCTNO NOTENO TOTIIS);
    iis_df = pl.read_parquet(npl6_path / "IIS.parquet").select([
        'ACCTNO', 'NOTENO', 'TOTIIS'
    ]).sort(['ACCTNO', 'NOTENO'])
    
    # DATA AQ; MERGE AQ(IN=A) SP2 IIS; BY ACCTNO NOTENO;
    aq_merged = aq_df.join(sp2_df, on=['ACCTNO', 'NOTENO'], how='left')\
                     .join(iis_df, on=['ACCTNO', 'NOTENO'], how='left')
    
    # Apply filters and transformations
    aq_processed = aq_merged.filter(
        pl.col('LOANTYP').str.slice(0, 3) == 'HPD'
    ).with_columns([
        # IF SUBSTR(RISK,1,1) = 'S' THEN DO;
        pl.when(pl.col('RISK').str.slice(0, 1) == 'S')
        .then(
            pl.when(pl.col('RISK') == 'SUBSTANDARD-1')
            .then(pl.lit('S1'))
            .otherwise(pl.lit('S2'))
        )
        .otherwise(pl.col('RISK').str.slice(0, 1))
        .alias('RISKCD'),
        
        # SPAMT = SP;
        pl.col('SP').alias('SPAMT'),
        
        # SPPLAMT = SPPL;
        pl.col('SPPL').alias('SPPLAMT'),
        
        # IF SUBSTR(LOANTYP,1,9) = 'HPD AITAB' THEN LOANDESC = 'AITAB';
        pl.when(pl.col('LOANTYP').str.slice(0, 9) == 'HPD AITAB')
        .then(pl.lit('AITAB'))
        .otherwise(pl.lit('HPD'))
        .alias('LOANDESC'),
        
        # RPTDATE = MDY(&MM,&DD,&YY);
        pl.datetime(int(YY), int(MM), int(DD)).alias('RPTDATE')
    ])
    
    # PROC SORT DATA=AQ OUT=AQ; BY BRANCH LOANDESC RISKCD RPTDATE;
    aq_sorted = aq_processed.sort(['BRANCH', 'LOANDESC', 'RISKCD', 'RPTDATE'])
    
    # PROC SUMMARY DATA=AQ NWAY;
    summary_columns = [
        'NETBALP', 'NEWNPL', 'RECOVER', 'PL', 'NPLW', 'NPL', 
        'TOTIIS', 'SPAMT', 'SPPLAMT', 'MARKETVL', 'ADJUST'
    ]
    
    aq_summary = aq_sorted.group_by(['BRANCH', 'LOANDESC', 'RISKCD', 'RPTDATE']).agg([
        pl.count().alias('NOACCT'),  # _FREQ_ renamed to NOACCT
        *[pl.col(col).sum().alias(col) for col in summary_columns]
    ])
    
    # Save summary output
    aq_summary.write_csv(output_path / "AQ.csv")
    aq_summary.write_parquet(output_path / "AQ.parquet")
    
    # DATA _NULL_; SET AQ; FILE AQTXT NOTITLES;
    with open(output_path / "AQTXT.txt", "w") as f:
        for row in aq_summary.iter_rows(named=True):
            # Format each field according to SAS PUT specifications
            line = (
                f"{row['RPTDATE'].strftime('%d%m%Y'):>10}"      # @001 RPTDATE DDMMYY10.
                f"{str(row.get('BRANCH', '')):>7}"              # @014 BRANCH $7.
                f"{str(row.get('LOANDESC', '')):>5}"            # @023 LOANDESC $5.
                f"{str(row.get('RISKCD', '')):>2}"              # @029 RISKCD $2.
                f"{row.get('NETBALP', 0):13.2f}"               # @031 NETBALP 13.2
                f"{row.get('NEWNPL', 0):13.2f}"                # @044 NEWNPL 13.2
                f"{row.get('RECOVER', 0):13.2f}"               # @057 RECOVER 13.2
                f"{row.get('PL', 0):13.2f}"                    # @070 PL 13.2
                f"{row.get('NPLW', 0):13.2f}"                  # @083 NPLW 13.2
                f"{row.get('NPL', 0):13.2f}"                   # @096 NPL 13.2
                f"{row.get('TOTIIS', 0):13.2f}"                # @109 TOTIIS 13.2
                f"{row.get('SPAMT', 0):13.2f}"                 # @122 SPAMT 13.2
                f"{row.get('SPPLAMT', 0):13.2f}"               # @135 SPPLAMT 13.2
                f"{row.get('MARKETVL', 0):13.2f}"              # @148 MARKETVL 13.2
                f"{row.get('ADJUST', 0):13.2f}"                # @161 ADJUST 13.2
            )
            f.write(line + "\n")
    
    print("AQTXT file generated successfully")
    
else:
    # %ELSE %DO;
    if NPLDATE != RDATE:
        print(f"SAP.PBB.NPL.HP.SASDATA IS NOT DATED {RDATE}")
    
    # DATA A; ABORT 77;
    print("ABORTING WITH CODE 77")
    sys.exit(77)

print("PROCESSING COMPLETED SUCCESSFULLY")
