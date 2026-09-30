#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : EIBWP124
Purpose  : Weekly run (after EIBWWKLY) for PIBB - Report on Domestic Assets
           and Liabilities Part I (M&I Loan / Cagamas L124).
           - Derives REPTDATE week/month variables from (today - 1).
           - Runs LALWP124 to produce BNM.LALW{REPTMON}{NOWK}.
           - Copies BNMX.ALW{REPTMON}{NOWK} to BNM.ALW{REPTMON}{NOWK}.
           - Runs P124RDAL to produce the RDAL semicolon-delimited output.

Dependency: LALWP124 - produces BNM.LALW{REPTMON}{NOWK} from L124/UL124 data.
            P124RDAL - merges BIC codes with ALW data and writes RDAL output.
"""

import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import pyreadstat
import saspy

# ---------------------------------------------------------------------------
# Dependencies (%INC PGM(...))
# ---------------------------------------------------------------------------
from LALWP124 import main as run_lalwp124
from P124RDAL import main as run_p124rdal


# ============================================================================
# PATH CONFIGURATION (absolute paths, no BASE_DIR)
# ============================================================================

PIBB_LOAN_DIR = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS"
)

BNM_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm"
)
BNM1_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1"
)
BNMX_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnmx"
)

RDAL_OUTPUT_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWP124"
)


# ============================================================================
# HELPER: Derive date/week macro variables from (today - 1)
# ============================================================================

def get_date_variables() -> dict:
    reptdate = datetime.date.today() - datetime.timedelta(days=1)

    day  = reptdate.day
    mm   = reptdate.month
    yyyy = reptdate.year

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

    if wk == '1':
        mm1 = mm - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = mm

    mm2 = mm - 1
    if mm2 == 0:
        mm2 = 12

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
        'RDATE':    reptdate.strftime('%d/%m/%y'),
        'SDATE':    sdate.strftime('%d/%m/%y'),
        '_reptdate_obj': reptdate,
    }


# ============================================================================
# HELPERS: Read via pyreadstat / Write via saspy
# ============================================================================

def read_sas7bdat(path: Path, where: Optional[str] = None) -> pd.DataFrame:
    df, _meta = pyreadstat.read_sas7bdat(str(path))
    df.columns = [c.lower() for c in df.columns]
    if where:
        df = df.query(where)
    return df


def write_outputs(df: pd.DataFrame, out_dir: Path, base_name: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    sas7bdat_path = out_dir / f"{base_name}.sas7bdat"
    text_path     = out_dir / f"{base_name}.txt"

    sas = saspy.SASsession(cfgname='default')
    sas.df2sd(df, table=base_name, libref='WORK')

    sas.submit(
        f"""
        PROC EXPORT DATA=WORK.{base_name}
            OUTFILE="{sas7bdat_path}"
            DBMS=SAS7BDAT REPLACE;
        RUN;
        """
    )
    sas.submit(
        f"""
        PROC EXPORT DATA=WORK.{base_name}
            OUTFILE="{text_path}"
            DBMS=DLM REPLACE;
            DELIMITER=';';
        RUN;
        """
    )
    sas.endsas()

    print(f"Wrote {sas7bdat_path} and {text_path} ({len(df)} rows)")


# ============================================================================
# MAIN
# ============================================================================

def main():
    dvars    = get_date_variables()
    nowk     = dvars['NOWK']
    reptmon  = dvars['REPTMON']
    reptyear = dvars['REPTYEAR']
    rdate    = dvars['RDATE']
    sdate    = dvars['SDATE']

    print(
        f"REPTMON={reptmon}, NOWK={nowk}, REPTYEAR={reptyear}, "
        f"RDATE={rdate}, SDATE={sdate}"
    )

    # LIBNAME paths
    bnm_path  = BNM_PATH
    bnmx_path = BNMX_PATH

    # -----------------------------------------------------------------------
    # Sanity check: LNNOTE exists (do NOT load it here; P124RDAL streams it).
    # -----------------------------------------------------------------------
    loan_file = PIBB_LOAN_DIR / f"enrh_ln_note_m{reptmon}.sas7bdat"
    if not loan_file.exists():
        raise FileNotFoundError(
            f"PIBB MNILN monthly file not found for REPTMON={reptmon}: "
            f"{loan_file}"
        )

    # -----------------------------------------------------------------------
    # %INC PGM(LALWP124)
    # -----------------------------------------------------------------------
    run_lalwp124()

    # -----------------------------------------------------------------------
    # DATA BNM.ALW&REPTMON&NOWK; SET BNMX.ALW&REPTMON&NOWK;
    # -----------------------------------------------------------------------
    bnmx_alw_path = bnmx_path / f"alw{reptmon}{nowk}.sas7bdat"
    bnm_alw_base  = f"alw{reptmon}{nowk}"

    if not bnmx_alw_path.exists():
        raise FileNotFoundError(f"BNMX ALW not found: {bnmx_alw_path}")

    alw_df = read_sas7bdat(bnmx_alw_path)
    write_outputs(alw_df, bnm_path, bnm_alw_base)

    print(
        f"ALW copied from {bnmx_alw_path} to "
        f"{bnm_path / (bnm_alw_base + '.sas7bdat')} ({len(alw_df)} rows)"
    )

    # -----------------------------------------------------------------------
    # %INC PGM(P124RDAL)
    # -----------------------------------------------------------------------
    run_p124rdal()


if __name__ == '__main__':
    main()
