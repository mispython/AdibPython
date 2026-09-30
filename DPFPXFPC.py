#!/usr/bin/env python3
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

import datetime
from pathlib import Path

import pyreadstat
import saspy
import pandas as pd

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
PIBB_LOAN_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/"
    "enrh_ln_note_m{reptmon}.sas7bdat"
)  # SAP.PIBB.MNILN(0) - REPTDATE

# BNM output library (SAP.PBB.P124)
BNM_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm"
)  # BNM library (DD BNM)

# BNM1 library - PIBB sasdata (SAP.PIBB.SASDATA)
BNM1_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1"
)  # SAP.PIBB.SASDATA

# RDAL output (SAP.PBB.FISS.RDAL124)
# RDAL_OUTPUT_PATH is managed by P124RDAL; defined here for reference.
RDAL_OUTPUT_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWP124"
)  # SAP.PBB.FISS.RDAL124

# Base directory (for BNMX dynamic path resolution)
BASE_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod")


# ============================================================================
# HELPER: Derive all date/week macro variables from "today - 1"
# ============================================================================

def get_date_variables() -> dict:
    """
    Replicate SAS DATA REPTDATE step with SELECT(DAY(REPTDATE)) logic.

    Instead of reading REPTDATE from a SAS dataset, we use:
        reptdate = (today - 1 day)

    Returns a dict of all macro variable equivalents:
      NOWK, NOWK1, NOWK2, NOWK3,
      REPTMON, REPTMON1, REPTMON2,
      REPTYEAR, REPTDAY, RDATE, SDATE
    """
    reptdate = datetime.date.today() - datetime.timedelta(days=1)

    day = reptdate.day
    mm = reptdate.month
    yyyy = reptdate.year

    # SELECT(DAY(REPTDATE))
    if day == 8:
        sdd = 1
        wk, wk1 = '1', '4'
        wk2, wk3 = None, None
    elif day == 15:
        sdd = 9
        wk, wk1 = '2', '1'
        wk2, wk3 = None, None
    elif day == 22:
        sdd = 16
        wk, wk1 = '3', '2'
        wk2, wk3 = None, None
    else:
        sdd = 23
        wk, wk1 = '4', '3'
        wk2, wk3 = '2', '1'

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
        'NOWK': wk,
        'NOWK1': wk1,
        'NOWK2': wk2,
        'NOWK3': wk3,
        'REPTMON': f"{mm:02d}",
        'REPTMON1': f"{mm1:02d}",
        'REPTMON2': f"{mm2:02d}",
        'REPTYEAR': str(yyyy),
        'REPTDAY': f"{day:02d}",
        'RDATE': reptdate.strftime('%d/%m/%y'),   # DDMMYY8.
        'SDATE': sdate.strftime('%d/%m/%y'),      # DDMMYY8.
        '_reptdate_obj': reptdate,
    }


# ============================================================================
# HELPER: Read SAS7BDAT with pyreadstat (lowercase columns)
# ============================================================================

def read_sas7bdat(path: Path, where: str | None = None) -> pd.DataFrame:
    """
    Read a SAS7BDAT file using pyreadstat, lowercasing all column names.

    Parameters
    ----------
    path : Path
        Path to the .sas7bdat file.
    where : str, optional
        A pandas query string to filter rows (e.g. "entity_cd == 'PIBB'").

    Returns
    -------
    pd.DataFrame
        DataFrame with lowercase column names.
    """
    df, _meta = pyreadstat.read_sas7bdat(str(path))
    df.columns = [c.lower() for c in df.columns]

    if where:
        df = df.query(where)

    return df


# ============================================================================
# HELPER: Write SAS7BDAT + TEXT via saspy
# ============================================================================

def write_outputs(df: pd.DataFrame, out_dir: Path, base_name: str) -> None:
    """
    Write the DataFrame to both a .sas7bdat file and a semicolon-delimited
    .txt file using saspy.

    Parameters
    ----------
    df : pd.DataFrame
        Data to write.
    out_dir : Path
        Output directory.
    base_name : str
        Base file name (without extension), e.g. "alw1220".
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    sas7bdat_path = out_dir / f"{base_name}.sas7bdat"
    text_path = out_dir / f"{base_name}.txt"

    # --- Write SAS7BDAT via saspy ---
    sas = saspy.SASsession(cfgname='default')
    sas.df2sd(df, table=base_name, libref='WORK')

    # Save the SAS dataset to a .sas7bdat file on disk
    sas.submit(
        f"""
        PROC EXPORT DATA=WORK.{base_name}
            OUTFILE="{sas7bdat_path}"
            DBMS=SAS7BDAT REPLACE;
        RUN;
        """
    )

    # --- Write semicolon-delimited TEXT file via saspy ---
    sas.submit(
        f"""
        PROC EXPORT DATA=WORK.{base_name}
            OUTFILE="{text_path}"
            DBMS=DLM REPLACE;
            DELIMITER=';';
        RUN;
        """
    )

    # Optionally, close the SAS session
    sas.endsas()

    print(f"Wrote {sas7bdat_path} and {text_path} ({len(df)} rows)")


# ============================================================================
# MAIN
# ============================================================================

def main():
    # -----------------------------------------------------------------------
    # DATA REPTDATE: derive all week/month/year macro variables
    # NOTE: REPTDATE is NOT read from a SAS dataset; we use (today - 1).
    # -----------------------------------------------------------------------
    dvars = get_date_variables()

    nowk = dvars['NOWK']
    nowk1 = dvars['NOWK1']
    nowk2 = dvars['NOWK2']
    nowk3 = dvars['NOWK3']
    reptmon = dvars['REPTMON']
    reptmon1 = dvars['REPTMON1']
    reptmon2 = dvars['REPTMON2']
    reptyear = dvars['REPTYEAR']
    reptday = dvars['REPTDAY']
    rdate = dvars['RDATE']
    sdate = dvars['SDATE']

    print(
        f"REPTMON={reptmon}, NOWK={nowk}, REPTYEAR={reptyear}, "
        f"RDATE={rdate}, SDATE={sdate}"
    )

    # -----------------------------------------------------------------------
    # LIBNAME BNM1 "SAP.PIBB.SASDATA"
    # LIBNAME BNMX "SAP.PIBB.D&REPTYEAR"
    # Resolve BNMX path dynamically using REPTYEAR
    # -----------------------------------------------------------------------
    bnm1_path = BNM1_PATH
    bnmx_path = BASE_DIR / f"pibb/d{reptyear}"

    # -----------------------------------------------------------------------
    # Read PIBB MNILN with LNNOTE filter: ENTITY_CD = 'PIBB' (Islamic)
    # (lowercase column: entity_cd)
    # -----------------------------------------------------------------------
    loan_df = read_sas7bdat(
        PIBB_LOAN_PATH,
        where="entity_cd == 'PIBB'",
    )
    print(f"PIBB MNILN (filtered) rows: {len(loan_df)}")

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
    # Copy ALW from BNMX library into BNM library
    # -----------------------------------------------------------------------
    bnmx_alw_path = bnmx_path / f"alw{reptmon}{nowk}.sas7bdat"
    bnm_alw_base = f"alw{reptmon}{nowk}"

    alw_df = read_sas7bdat(bnmx_alw_path)

    # Write out via saspy (both .sas7bdat and .txt)
    write_outputs(alw_df, BNM_PATH, bnm_alw_base)

    print(
        f"ALW copied from {bnmx_alw_path} to "
        f"{BNM_PATH / (bnm_alw_base + '.sas7bdat')} ({len(alw_df)} rows)"
    )

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
