#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : LALWP124
Purpose  : Report on Domestic Assets and Liabilities - Part I (M&I Loan).
           Reads BNM.L124{sfx} and BNM.UL124{sfx}, summarises by customer
           code / approved limit / Cagamas, appends to BNM.LALW{sfx}.

Convention:
    REPTMON = '09'  (2-digit)
    NOWK    = '4'   (single digit)
    BNM/BNM1/BNMX suffix = '094' -> f"0{int(REPTMON)}{NOWK}"

reptmon/nowk may be passed by EIBWP124 (single source of truth) or
derived from (today - 1) when invoked standalone.
"""

from pathlib import Path
from typing import Optional

import pandas as pd

import PBBLNFMT  # noqa: F401  %INC PGM(PBBLNFMT)

from bnm_io import (
    read_sas7bdat_ci,
    write_sas_and_txt,
    resolve_ci,
)

from L124PBBD import main as run_l124pbbd, get_reptmon_nowk


# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BNM_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm"
)
BNM1_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1"
)
BNMX_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnmx"
)


# ============================================================================
# SUFFIX HELPER
# ============================================================================

def bnm_suffix(reptmon: str, nowk: str) -> str:
    return f"0{int(reptmon)}{nowk}"


# ============================================================================
# BNMCODE MAPPING
# ============================================================================

def get_bnmcodes_for_custcd(custcd: str) -> list:
    grp1 = {'02', '03', '11', '12', '71', '72', '73', '74', '79'}
    grp2 = {'20', '13', '17', '30', '32', '33', '34', '35',
            '36', '37', '38', '39', '40', '04', '05', '06'}
    grp3 = {'41', '42', '43', '44', '46', '47', '48', '49', '51',
            '52', '53', '54', '60', '61', '62', '63', '64', '65',
            '59', '75', '57'}
    grp4 = {'76', '77', '78'}
    grp5 = {'81', '82', '83', '84'}
    grp6 = {'85', '86', '90', '91', '92', '95', '96', '98', '99'}

    codes = []
    if custcd in grp1:
        codes.append(f'34100{custcd}000000Y')
    elif custcd in grp2:
        codes.append('3410020000000Y')
        if custcd in ('13', '17'):
            codes.append(f'34100{custcd}000000Y')
    elif custcd in grp3:
        codes.append('3410060000000Y')
    elif custcd in grp4:
        codes.append('3410076000000Y')
    elif custcd in grp5:
        codes.append('3410081000000Y')
    elif custcd in grp6:
        codes.append('3410085000000Y')
    return codes


# ============================================================================
# APPEND HELPER
# ============================================================================

def append_to_output(
    new_df: pd.DataFrame,
    target_dir: Path,
    target_name: str,
) -> pd.DataFrame:
    """
    PROC APPEND equivalent.
    Reads any existing target_dir/target_name (case-insensitive), concatenates
    new_df, returns the combined DataFrame.
    """
    existing = resolve_ci(target_dir, target_name)
    if existing is not None:
        existing_df, _ = __import__('pyreadstat').read_sas7bdat(str(existing))
        existing_df.columns = [c.lower() for c in existing_df.columns]
        return pd.concat([existing_df, new_df], ignore_index=True, sort=False)
    return new_df


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
            f"LALWP124: no explicit date passed — derived "
            f"reptmon={reptmon!r} nowk={nowk!r}"
        )

    assert len(reptmon) == 2 and reptmon.isdigit(), (
        f"REPTMON must be 2-digit, got {reptmon!r}"
    )
    assert nowk in {'1', '2', '3', '4'}, (
        f"NOWK must be single-digit, got {nowk!r}"
    )

    sfx = bnm_suffix(reptmon, nowk)

    # -----------------------------------------------------------------------
    # %INC PGM(L124PBBD) — pass reptmon/nowk so it doesn't recompute
    # -----------------------------------------------------------------------
    run_l124pbbd(reptmon=reptmon, nowk=nowk)

    # -----------------------------------------------------------------------
    # PROC DATASETS: DELETE LALW / LALM / LALQ for this period
    # -----------------------------------------------------------------------
    lalw_name = f"lalw{sfx}.sas7bdat"
    lalm_name = f"lalm{sfx}.sas7bdat"
    lalq_name = f"lalq{sfx}.sas7bdat"

    for nm in (lalw_name, f"lalw{sfx}.txt", lalm_name, lalq_name):
        p = resolve_ci(BNM_PATH, nm)
        if p is not None:
            p.unlink()

    # -----------------------------------------------------------------------
    # DATA LOAN / ULOAN — no entity_cd filter (column isn't in L124/UL124)
    # -----------------------------------------------------------------------
    loan_df  = read_sas7bdat_ci(BNM_PATH, f"l124{sfx}.sas7bdat",  required=True)
    uloan_df = read_sas7bdat_ci(BNM_PATH, f"ul124{sfx}.sas7bdat", required=True)

    # -----------------------------------------------------------------------
    # SECTION 1: RM LOANS - BY CUSTOMER CODE
    # -----------------------------------------------------------------------
    if not loan_df.empty and 'prodcd' in loan_df.columns:
        alw1_df = (
            loan_df[
                loan_df['prodcd'].astype(str).str.slice(0, 3)
                    .isin(['341', '342', '343', '344'])
            ]
            .groupby(['custcd', 'prodcd', 'amtind'], dropna=False, as_index=False)
            .agg(amount=('balance', 'sum'))
        )
    else:
        alw1_df = pd.DataFrame(
            columns=['custcd', 'prodcd', 'amtind', 'amount']
        )

    alwloan1_rows = []
    for row in alw1_df.to_dict('records'):
        custcd = str(row.get('custcd', '') or '').strip()
        amtind = row.get('amtind')
        amount = row.get('amount')
        for bnmcode in get_bnmcodes_for_custcd(custcd):
            alwloan1_rows.append({
                'BNMCODE': bnmcode,
                'AMTIND':  amtind,
                'AMOUNT':  amount,
            })

    alwloan1_df = pd.DataFrame(
        alwloan1_rows,
        columns=['BNMCODE', 'AMTIND', 'AMOUNT'],
    )

    BNM_PATH.mkdir(parents=True, exist_ok=True)
    lalw_df = append_to_output(alwloan1_df, BNM_PATH, lalw_name)

    # -----------------------------------------------------------------------
    # SECTION 2: GROSS LOAN - BY APPROVED LIMIT
    # -----------------------------------------------------------------------
    if not loan_df.empty and 'prodcd' in loan_df.columns:
        mask2 = (
            (loan_df['prodcd'].astype(str).str.slice(0, 2) == '34') |
            (loan_df['prodcd'].astype(str) == '54120')
        )
        alw2_df = (
            loan_df[mask2]
            .groupby(['prodcd', 'amtind'], dropna=False, as_index=False)
            .agg(amount=('balance', 'sum'))
        )
    else:
        alw2_df = pd.DataFrame(columns=['prodcd', 'amtind', 'amount'])

    alwloan2_df = pd.DataFrame({
        'BNMCODE': '3051000000000Y',
        'AMTIND':  alw2_df['amtind'],
        'AMOUNT':  alw2_df['amount'],
    })

    lalw_df = append_to_output(alwloan2_df, BNM_PATH, lalw_name)

    # -----------------------------------------------------------------------
    # SECTION 3: LOANS SOLD TO CAGAMAS
    # -----------------------------------------------------------------------
    if not loan_df.empty and 'product' in loan_df.columns:
        alw3_df = (
            loan_df[loan_df['product'].isin([124, 145])]
            .groupby(['prodcd', 'amtind'], dropna=False, as_index=False)
            .agg(amount=('balance', 'sum'))
        )
    else:
        alw3_df = pd.DataFrame(columns=['prodcd', 'amtind', 'amount'])

    alwloan3_df = pd.DataFrame({
        'BNMCODE': '7511100000000Y',
        'AMTIND':  alw3_df['amtind'],
        'AMOUNT':  alw3_df['amount'],
    })

    lalw_df = append_to_output(alwloan3_df, BNM_PATH, lalw_name)

    # -----------------------------------------------------------------------
    # FINAL CONSOLIDATION
    # -----------------------------------------------------------------------
    if lalw_df.empty:
        lalw_final = pd.DataFrame(columns=['BNMCODE', 'AMTIND', 'AMOUNT'])
    else:
        lalw_final = (
            lalw_df
            .groupby(['BNMCODE', 'AMTIND'], dropna=False, as_index=False)
            .agg(AMOUNT=('AMOUNT', 'sum'))
        )

    write_sas_and_txt(lalw_final, BNM_PATH, f"lalw{sfx}")
    print(
        f"LALW written: {BNM_PATH / ('lalw' + sfx + '.sas7bdat')}  "
        f"({len(lalw_final)} rows)"
    )


if __name__ == '__main__':
    main()
