#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : L124PBBD
Purpose  : Produce BNM.L124{sfx} and BNM.UL124{sfx} from BNM1.LOAN{sfx}
           and BNM1.ULOAN{sfx}, where sfx = f"0{int(reptmon)}{nowk}".

           No ENTITY_CD filter here — that is LNNOTE-only (P124RDAL).

Convention:
    REPTMON = '09' (2-digit)
    NOWK    = '4'  (single digit)
    Suffix  = '094'

reptmon/nowk may be passed by LALWP124 (single source of truth) or
derived from (today - 1) when invoked standalone.
"""

import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from bnm_io import (
    read_sas7bdat,
    read_sas7bdat_ci,
    resolve_ci,
    write_sas_and_txt,
)


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
# DATE VARIABLES (fallback for standalone invocation)
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
# L124/UL124 BUILDER
# ============================================================================

def make_l124(src_path: Path) -> pd.DataFrame:
    """
    Mirrors:
        DATA BNM.L124{sfx};
            SET BNM1.LOAN{sfx};
            IF PRODUCT IN (124,145);
            PRODCD='34120'; AMTIND='I';

    No ENTITY_CD filter (that column only exists in LNNOTE).
    """
    df = read_sas7bdat(src_path)

    prod_col = next(
        (c for c in ('product', 'prodcd', 'product_cd') if c in df.columns),
        None,
    )
    if prod_col is None:
        raise KeyError(
            f"No product column in {src_path.name}. "
            f"Columns present: {list(df.columns)[:20]}..."
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

def main(
    reptmon: Optional[str] = None,
    nowk: Optional[str] = None,
) -> None:
    # Resolve date variables
    if reptmon is None or nowk is None:
        reptmon, nowk = get_reptmon_nowk()
        print(
            f"L124PBBD: no explicit date passed — derived "
            f"reptmon={reptmon!r} nowk={nowk!r}"
        )

    assert len(reptmon) == 2 and reptmon.isdigit(), (
        f"REPTMON must be 2-digit, got {reptmon!r}"
    )
    assert nowk in {'1', '2', '3', '4'}, (
        f"NOWK must be single-digit, got {nowk!r}"
    )

    sfx = bnm_suffix(reptmon, nowk)
    print(f"L124PBBD DEBUG: reptmon={reptmon!r} nowk={nowk!r} sfx={sfx!r}")

    # Input paths (case-insensitive)
    loan_path = resolve_ci(BNM1_PATH, f"loan{sfx}.sas7bdat")
    uloan_path = resolve_ci(BNM1_PATH, f"uloan{sfx}.sas7bdat")

    if loan_path is None:
        raise FileNotFoundError(
            f"L124PBBD: input not found (case-insensitive) for "
            f"loan{sfx}.sas7bdat in {BNM1_PATH}"
        )
    if uloan_path is None:
        raise FileNotFoundError(
            f"L124PBBD: input not found (case-insensitive) for "
            f"uloan{sfx}.sas7bdat in {BNM1_PATH}"
        )

    # -----------------------------------------------------------------------
    # DATA BNM.L124{sfx}
    # -----------------------------------------------------------------------
    print(f"L124PBBD: reading {loan_path} ...")
    l124_df = make_l124(loan_path)
    l124_base = f"l124{sfx}"
    write_sas_and_txt(l124_df, BNM_PATH, l124_base)
    print(
        f"L124 written: {BNM_PATH / (l124_base + '.sas7bdat')}  "
        f"({len(l124_df)} rows)"
    )
    del l124_df

    # -----------------------------------------------------------------------
    # DATA BNM.UL124{sfx}
    # -----------------------------------------------------------------------
    print(f"L124PBBD: reading {uloan_path} ...")
    ul124_df = make_l124(uloan_path)
    ul124_base = f"ul124{sfx}"
    write_sas_and_txt(ul124_df, BNM_PATH, ul124_base)
    print(
        f"UL124 written: {BNM_PATH / (ul124_base + '.sas7bdat')}  "
        f"({len(ul124_df)} rows)"
    )


if __name__ == '__main__':
    main()
