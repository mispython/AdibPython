# !/usr/bin/env python3
"""
Program  : EIBWP124
Purpose  : Weekly run (after EIBWWKLY) for PIBB - Report on Domestic Assets
           and Liabilities Part I (M&I Loan / Cagamas L124).
           - Derives REPTDATE week/month variables for PIBB MNILN.
           - Runs LALWP124 to produce BNM.LALW{REPTMON}{NOWK}.
           - Copies BNMX.ALW{REPTMON}{NOWK} to BNM.ALW{REPTMON}{NOWK}.
           - Runs P124RDAL to produce the RDAL semicolon-delimited output.

           SMR 2007-0925. RUN AFTER EIBWWKLY.

Dependency: LALWP124 - produces BNM.LALW{REPTMON}{NOWK} from L124/UL124 data.
            P124RDAL - merges BIC codes with ALW data and writes RDAL output.
"""

import duckdb
import polars as pl
from pathlib import Path
import datetime
import shutil

# ---------------------------------------------------------------------------
# Dependency: LALWP124 (%INC PGM(LALWP124))
# ---------------------------------------------------------------------------
from LALWP124 import main as run_lalwp124

# ---------------------------------------------------------------------------
# Dependency: P124RDAL (%INC PGM(P124RDAL))
# ---------------------------------------------------------------------------
from P124RDAL import main as run_p124rdal

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

# PIBB MNILN - current generation (0) and prior (-4) reptdate/lnnote
PIBB_LOAN_PATH      = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m{reptmon}.sas7bdat")   # SAP.PIBB.MNILN(0)  - REPTDATE

# BNM output library  (SAP.PBB.P124)
BNM_PATH            = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm")                           # BNM library (DD BNM)

# BNMX library - source of ALW data keyed by REPTYEAR
# SAP.PIBB.D&REPTYEAR  -> resolved at runtime once REPTYEAR is known
# BNMX is set dynamically below based on REPTYEAR.

# BNM1 library - PIBB sasdata (SAP.PIBB.SASDATA)
BNM1_PATH           = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1")                       # SAP.PIBB.SASDATA

# RDAL output (SAP.PBB.FISS.RDAL124)
# RDAL_OUTPUT_PATH is managed by P124RDAL; defined here for reference.
RDAL_OUTPUT_PATH    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWP124")                # SAP.PBB.FISS.RDAL124

# Program library (SAP.BNM.PROGRAM) - used by %INC PGM(); handled via imports above.
# PGM_PATH          = BASE_DIR / "bnm/program"

# ============================================================================
# HELPER: Derive all date/week macro variables from REPTDATE
# ============================================================================

def get_date_variables(reptdate_parquet: Path) -> dict:
    """
    Replicate SAS DATA REPTDATE step with SELECT(DAY(REPTDATE)) logic.
    Returns a dict of all macro variable equivalents:
      NOWK, NOWK1, NOWK2, NOWK3,
      REPTMON, REPTMON1, REPTMON2,
      REPTYEAR, REPTDAY, RDATE, SDATE
    """
    con = duckdb.connect()
    df  = con.execute(
        f"SELECT REPTDATE FROM read_parquet('{reptdate_parquet}') LIMIT 1"
    ).fetchdf()
    con.close()

    reptdate = df['REPTDATE'].iloc[0]
    if hasattr(reptdate, 'date'):
        reptdate = reptdate.date()

    day  = reptdate.day
    mm   = reptdate.month
    yyyy = reptdate.year

    # SELECT(DAY(REPTDATE))
    if day == 8:
        sdd  = 1
        wk   = '1';  wk1 = '4'
        wk2  = None; wk3 = None
    elif day == 15:
        sdd  = 9
        wk   = '2';  wk1 = '1'
        wk2  = None; wk3 = None
    elif day == 22:
        sdd  = 16
        wk   = '3';  wk1 = '2'
        wk2  = None; wk3 = None
    else:
        sdd  = 23
        wk   = '4';  wk1 = '3'
        wk2  = '2';  wk3 = '1'

    # MM1: prior month for WK='1', else current month
    if wk == '1':
        mm1 = mm - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = mm

    # MM2: prior month (for all weeks)
    mm2 = mm - 1
    if mm2 == 0:
        mm2 = 12

    # SDATE = MDY(MM, SDD, YEAR(REPTDATE))
    sdate = datetime.date(yyyy, mm, sdd)

    return {
        'NOWK':     wk,
        'NOWK1':    wk1,
        'NOWK2':    wk2,
        'NOWK3':    wk3,
        'REPTMON':  f"{mm:02d}",
        'REPTMON1': f"{mm1:02d}",
        'REPTMON2': f"{mm2:02d}",
        'REPTYEAR': str(yyyy),
        'REPTDAY':  f"{day:02d}",
        'RDATE':    reptdate.strftime('%d/%m/%y'),   # DDMMYY8.
        'SDATE':    sdate.strftime('%d/%m/%y'),      # DDMMYY8.
        '_reptdate_obj': reptdate,
    }


# ============================================================================
# MAIN
# ============================================================================

def main():
    # -----------------------------------------------------------------------
    # DATA REPTDATE: SET LOAN.REPTDATE (SAP.PIBB.MNILN(0))
    # Derive all week/month/year macro variables
    # -----------------------------------------------------------------------
    dvars = get_date_variables(PIBB_LOAN_PATH)

    nowk     = dvars['NOWK']
    nowk1    = dvars['NOWK1']
    nowk2    = dvars['NOWK2']
    nowk3    = dvars['NOWK3']
    reptmon  = dvars['REPTMON']
    reptmon1 = dvars['REPTMON1']
    reptmon2 = dvars['REPTMON2']
    reptyear = dvars['REPTYEAR']
    reptday  = dvars['REPTDAY']
    rdate    = dvars['RDATE']
    sdate    = dvars['SDATE']

    print(f"REPTMON={reptmon}, NOWK={nowk}, REPTYEAR={reptyear}, RDATE={rdate}, SDATE={sdate}")

    # -----------------------------------------------------------------------
    # LIBNAME BNM1 "SAP.PIBB.SASDATA"
    # LIBNAME BNMX "SAP.PIBB.D&REPTYEAR"
    # Resolve BNMX path dynamically using REPTYEAR
    # -----------------------------------------------------------------------
    # LIBNAME BNM1 "SAP.PIBB.SASDATA" DISP=SHR
    bnm1_path = BNM1_PATH

    # LIBNAME BNMX "SAP.PIBB.D&REPTYEAR" DISP=SHR
    bnmx_path = BASE_DIR / f"pibb/d{reptyear}"

    # -----------------------------------------------------------------------
    # %INC PGM(LALWP124)
    # Runs the LALWP124 dependency which internally:
    #   - runs L124PBBD to produce BNM.L124&REPTMON&NOWK and BNM.UL124&REPTMON&NOWK
    #   - summarises by CUSTCD, PRODCD and Cagamas, appends to BNM.LALW&REPTMON&NOWK
    # -----------------------------------------------------------------------
    run_lalwp124()

    # -----------------------------------------------------------------------
    # DATA BNM.ALW&REPTMON&NOWK;
    #   SET BNMX.ALW&REPTMON&NOWK;
    # Copy ALW parquet from BNMX library into BNM library
    # -----------------------------------------------------------------------
    bnmx_alw_path = bnmx_path / f"alw{reptmon}{nowk}.parquet"
    bnm_alw_path  = BNM_PATH  / f"alw{reptmon}{nowk}.parquet"

    BNM_PATH.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    alw_df = con.execute(
        f"SELECT * FROM read_parquet('{bnmx_alw_path}')"
    ).pl()
    con.close()

    alw_df.write_parquet(bnm_alw_path)
    print(f"ALW copied from {bnmx_alw_path} to {bnm_alw_path}  ({len(alw_df)} rows)")

    # -----------------------------------------------------------------------
    # %INC PGM(P124RDAL)
    # Runs the P124RDAL dependency which:
    #   - loads BIC codes (weekly or monthly)
    #   - merges with BNM.ALW&REPTMON&NOWK
    #   - appends Cagamas loan data
    #   - splits into AL / OB / SP sections
    #   - writes semicolon-delimited RDAL output file
    # -----------------------------------------------------------------------
    run_p124rdal()


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    main()


BELOW IS SAS ORIGINAL CODE:

OPTIONS SORTDEV=3390 YEARCUTOFF=1950 NOCENTER;

DATA REPTDATE;
  SET LOAN.REPTDATE;
  SELECT(DAY(REPTDATE));
    WHEN (8)  DO; SDD = 1;  WK = '1'; WK1 = '4'; END;
    WHEN(15)  DO; SDD = 9;  WK = '2'; WK1 = '1'; END;
    WHEN(22)  DO; SDD = 16; WK = '3'; WK1 = '2'; END;
    OTHERWISE DO; SDD = 23; WK = '4'; WK1 = '3';
                            WK2= '2'; WK3 = '1'; END;
  END;
  MM = MONTH(REPTDATE);
  IF WK = '1' THEN DO;
     MM1 = MM - 1;
     IF MM1 = 0 THEN MM1 = 12;
  END;
  ELSE MM1 = MM;
  MM2 = MM - 1;
  IF MM2 = 0 THEN MM2 = 12;
  SDATE = MDY(MM,SDD,YEAR(REPTDATE));
  CALL SYMPUT('NOWK',PUT(WK,$1.));
  CALL SYMPUT('NOWK1',PUT(WK1,$1.));
  CALL SYMPUT('NOWK2',PUT(WK2,$1.));
  CALL SYMPUT('NOWK3',PUT(WK3,$1.));
  CALL SYMPUT('REPTMON',PUT(MM,Z2.));
  CALL SYMPUT('REPTMON1',PUT(MM1,Z2.));
  CALL SYMPUT('REPTMON2',PUT(MM2,Z2.));
  CALL SYMPUT('REPTYEAR',PUT(REPTDATE,YEAR4.));
  CALL SYMPUT('REPTDAY',PUT(DAY(REPTDATE),Z2.));
  CALL SYMPUT('RDATE',PUT(REPTDATE,DDMMYY8.));
  CALL SYMPUT('SDATE',PUT(SDATE,DDMMYY8.));
RUN;
LIBNAME BNM1 "SAP.PIBB.SASDATA" DISP=SHR;
LIBNAME BNMX "SAP.PIBB.D&REPTYEAR" DISP=SHR;
RUN;
   %INC PGM(LALWP124);
   DATA BNM.ALW&REPTMON&NOWK;
        SET BNMX.ALW&REPTMON&NOWK;
   %INC PGM(P124RDAL);



for LNNOTE, need to add filter of "WHERE ENTITY_CD = 'PIBB'" (islamic)
all inputs are in sas7bdat sas dataset and need to be in all lowercase.
use pyreadstat to read.
remove reptdate, use datetime timedelta - 1 instead. 
output in sas7bdat and TEXT files. 
write out using saspy
make sure to include pgm files (already existed in .py)

