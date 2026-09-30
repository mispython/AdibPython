#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : L124PBBD
Purpose  : Filter LOAN and ULOAN datasets for PRODUCT IN (124, 145),
           assign PRODCD='34120' and AMTIND='I', then write out
           BNM.L124{REPTMON}{NOWK} and BNM.UL124{REPTMON}{NOWK}
           as .sas7bdat (+ .txt) via saspy.

Public API:
    get_reptmon_nowk() -> (reptmon, nowk)   # derived from today - 1
    main()                                   # run the extraction
"""

import datetime
from pathlib import Path

import pandas as pd
import pyreadstat
import saspy


# ============================================================================
# PATH CONFIGURATION (absolute paths, no BASE_DIR)
# ============================================================================

BNM1_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1"
)
BNM_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm"
)


# ============================================================================
# DATE VARIABLES (from today - 1)
# ============================================================================

def get_reptmon_nowk() -> tuple:
    reptdate = datetime.date.today() - datetime.timedelta(days=1)
    day = reptdate.day

    if day == 8:
        wk = '1'
    elif day == 15:
        wk = '2'
    elif day == 22:
        wk = '3'
    else:
        wk = '4'

    return f"{reptdate.month:02d}", wk


# ============================================================================
# HELPERS
# ============================================================================

def read_sas7bdat(path: Path) -> pd.DataFrame:
    df, _meta = pyreadstat.read_sas7bdat(str(path))
    df.columns = [c.lower() for c in df.columns]
    return df


def write_via_saspy(df: pd.DataFrame, out_dir: Path, base_name: str) -> None:
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


def make_l124(src_path: Path) -> pd.DataFrame:
    """
    DATA ... ; SET ... ; IF PRODUCT IN (124,145) ;
                 PRODCD='34120' ; AMTIND='I' ;
    """
    df = read_sas7bdat(src_path)
    df = df[df['product'].isin([124, 145])].copy()
    df['prodcd'] = '34120'
    df['amtind'] = 'I'
    return df


# ============================================================================
# MAIN
# ============================================================================

def main():
    reptmon, nowk = get_reptmon_nowk()

    loan_path  = BNM1_PATH / f"loan{reptmon}{nowk}.sas7bdat"
    uloan_path = BNM1_PATH / f"uloan{reptmon}{nowk}.sas7bdat"

    if not loan_path.exists():
        raise FileNotFoundError(f"L124PBBD: input not found: {loan_path}")
    if not uloan_path.exists():
        raise FileNotFoundError(f"L124PBBD: input not found: {uloan_path}")

    # DATA BNM.L124{MM}{WK}
    print(f"L124PBBD: reading {loan_path} ...")
    l124_df = make_l124(loan_path)
    write_via_saspy(l124_df, BNM_PATH, f"l124{reptmon}{nowk}")
    print(f"L124 written: {BNM_PATH / ('l124' + reptmon + nowk + '.sas7bdat')}  "
          f"({len(l124_df)} rows)")
    del l124_df

    # DATA BNM.UL124{MM}{WK}
    print(f"L124PBBD: reading {uloan_path} ...")
    ul124_df = make_l124(uloan_path)
    write_via_saspy(ul124_df, BNM_PATH, f"ul124{reptmon}{nowk}")
    print(f"UL124 written: {BNM_PATH / ('ul124' + reptmon + nowk + '.sas7bdat')}  "
          f"({len(ul124_df)} rows)")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    main()
