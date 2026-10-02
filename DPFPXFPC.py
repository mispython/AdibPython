#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : L124PBBD
Purpose  : Produce BNM.L124{sfx} and BNM.UL124{sfx} from BNM1.LOAN{sfx}
           and BNM1.ULOAN{sfx}, where sfx = '0' + single-digit month + week.
           For REPTMON='09', NOWK='4' -> sfx = '094'.

Convention:
    REPTMON = '09' (2-digit)
    NOWK    = '4'  (single digit)
    Suffix  = '094'  -> f"0{int(REPTMON)}{NOWK}"

No ENTITY_CD filter here — that's LNNOTE-only (P124RDAL).
"""

import datetime
from pathlib import Path

import pandas as pd
import pyreadstat
import saspy


# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BNM1_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1"
)
BNM_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm"
)


# ============================================================================
# SUFFIX HELPER
# ============================================================================

def bnm_suffix(reptmon: str, nowk: str) -> str:
    """'09' + '4' -> '094'."""
    return f"0{int(reptmon)}{nowk}"


# ============================================================================
# DATE VARIABLES
# ============================================================================

def get_reptmon_nowk() -> tuple:
    """
    REPTMON -> '09' (2-digit)
    NOWK    -> '4'  (single digit, NEVER '04')
    """
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
    if df is None or len(df.columns) == 0:
        raise ValueError(f"Refusing to write schema-less dataset '{base_name}'.")
    if df.empty:
        print(f"WARNING: '{base_name}' has 0 rows — writing empty dataset.")

    out_dir.mkdir(parents=True, exist_ok=True)
    sas7bdat_path = out_dir / f"{base_name}.sas7bdat"
    text_path     = out_dir / f"{base_name}.txt"

    sas = saspy.SASsession(cfgname='default')
    sas.df2sd(df, table=base_name, libref='WORK')

    sas.submit(f"""
        PROC EXPORT DATA=WORK.{base_name}
            OUTFILE="{sas7bdat_path}"
            DBMS=SAS7BDAT REPLACE;
        RUN;
    """)
    sas.submit(f"""
        PROC EXPORT DATA=WORK.{base_name}
            OUTFILE="{text_path}"
            DBMS=DLM REPLACE;
            DELIMITER=';';
        RUN;
    """)
    sas.endsas()


def make_l124(src_path: Path) -> pd.DataFrame:
    """
    Mirrors:
        SET BNM1.LOAN{sfx};
        IF PRODUCT IN (124,145);
        PRODCD='34120'; AMTIND='I';
    """
    df = read_sas7bdat(src_path)

    prod_col = next(
        (c for c in ('product', 'prodcd', 'product_cd') if c in df.columns),
        None,
    )
    if prod_col is None:
        raise KeyError(
            f"No product column in {src_path.name}. "
            f"Columns: {list(df.columns)[:20]}..."
        )

    vals = df[prod_col]
    if vals.dtype == object:
        mask = vals.astype(str).isin(['124', '145'])
    else:
        mask = vals.isin([124, 145])

    df = df[mask].copy()
    df['prodcd'] = '34120'
    df['amtind'] = 'I'
    return df


# ============================================================================
# MAIN
# ============================================================================

def main():
    reptmon, nowk = get_reptmon_nowk()

    assert len(reptmon) == 2 and reptmon.isdigit(), (
        f"REPTMON must be 2-digit, got {reptmon!r}"
    )
    assert nowk in {'1', '2', '3', '4'}, (
        f"NOWK must be single-digit, got {nowk!r}"
    )

    sfx = bnm_suffix(reptmon, nowk)

    print(f"L124PBBD DEBUG: reptmon={reptmon!r} nowk={nowk!r} sfx={sfx!r}")

    loan_path  = BNM1_PATH / f"loan{sfx}.sas7bdat"
    uloan_path = BNM1_PATH / f"uloan{sfx}.sas7bdat"

    if not loan_path.exists():
        raise FileNotFoundError(f"L124PBBD: input not found: {loan_path}")
    if not uloan_path.exists():
        raise FileNotFoundError(f"L124PBBD: input not found: {uloan_path}")

    # DATA BNM.L124{sfx}
    print(f"L124PBBD: reading {loan_path} ...")
    l124_df = make_l124(loan_path)
    write_via_saspy(l124_df, BNM_PATH, f"l124{sfx}")
    print(f"L124 written: {BNM_PATH / ('l124' + sfx + '.sas7bdat')}  ({len(l124_df)} rows)")
    del l124_df

    # DATA BNM.UL124{sfx}
    print(f"L124PBBD: reading {uloan_path} ...")
    ul124_df = make_l124(uloan_path)
    write_via_saspy(ul124_df, BNM_PATH, f"ul124{sfx}")
    print(f"UL124 written: {BNM_PATH / ('ul124' + sfx + '.sas7bdat')}  ({len(ul124_df)} rows)")


if __name__ == '__main__':
    main()
