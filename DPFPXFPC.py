#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : EIBWP124
Purpose  : Weekly run for PIBB - Report on Domestic Assets and Liabilities.
           Single source of truth for REPTMON/NOWK, passed to child modules
           so they can't drift across midnight.

Convention:
    REPTMON = '09' (2-digit, zero-padded)
    NOWK    = '4'  (single digit — NEVER '04')
    BNM/BNM1/BNMX suffix = '094' -> f"0{int(REPTMON)}{NOWK}"
    LNNOTE filename uses REPTMON directly: enrh_ln_note_m09.sas7bdat
"""

import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from bnm_io import read_sas7bdat_ci, write_sas_and_txt, resolve_ci

from LALWP124 import main as run_lalwp124
from P124RDAL import main as run_p124rdal


# ============================================================================
# PATH CONFIGURATION
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
# SUFFIX HELPER
# ============================================================================

def bnm_suffix(reptmon: str, nowk: str) -> str:
    """'09' + '4' -> '094'."""
    return f"0{int(reptmon)}{nowk}"


# ============================================================================
# DATE VARIABLES
# ============================================================================

def get_date_variables() -> dict:
    """
    REPTMON -> '09' (2-digit)
    NOWK    -> '4'  (single digit)
    """
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
# MAIN
# ============================================================================

def main():
    dvars    = get_date_variables()
    nowk     = dvars['NOWK']
    reptmon  = dvars['REPTMON']
    reptyear = dvars['REPTYEAR']
    rdate    = dvars['RDATE']
    sdate    = dvars['SDATE']

    assert len(reptmon) == 2 and reptmon.isdigit(), (
        f"REPTMON must be 2-digit, got {reptmon!r}"
    )
    assert nowk in {'1', '2', '3', '4'}, (
        f"NOWK must be single-digit, got {nowk!r}"
    )

    sfx = bnm_suffix(reptmon, nowk)

    print(
        f"REPTMON={reptmon}, NOWK={nowk}, REPTYEAR={reptyear}, "
        f"RDATE={rdate}, SDATE={sdate}, SUFFIX={sfx}"
    )

    # LNNOTE existence check (case-insensitive)
    loan_file = resolve_ci(
        PIBB_LOAN_DIR, f"enrh_ln_note_m{reptmon}.sas7bdat"
    )
    if loan_file is None:
        raise FileNotFoundError(
            f"PIBB LNNOTE not found for REPTMON={reptmon} in "
            f"{PIBB_LOAN_DIR}"
        )

    # --- %INC PGM(LALWP124) — pass reptmon/nowk explicitly ---
    run_lalwp124(reptmon=reptmon, nowk=nowk)

    # -----------------------------------------------------------------------
    # ALW copy: BNMX -> BNM
    # Note: bnm_io.read_sas7bdat_ci takes (directory, filename).
    # -----------------------------------------------------------------------
    alw_df = read_sas7bdat_ci(
        BNMX_PATH, f"alw{sfx}.sas7bdat", required=True
    )

    write_sas_and_txt(alw_df, BNM_PATH, f"alw{sfx}")

    print(
        f"ALW copied: {BNMX_PATH / ('alw' + sfx + '.sas7bdat')} -> "
        f"{BNM_PATH / ('alw' + sfx + '.sas7bdat')} ({len(alw_df)} rows)"
    )

    # --- %INC PGM(P124RDAL) — pass reptmon/nowk explicitly ---
    run_p124rdal(reptmon=reptmon, nowk=nowk)


if __name__ == '__main__':
    main()
