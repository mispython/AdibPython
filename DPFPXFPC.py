#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : P124RDAL
Purpose  : Report on Domestic Assets and Liabilities - Part I (Cagamas/L124).

            1. Loads PBBRDAL.sas7bdat (case-insensitive; SAS writes lowercase).
            2. Loads BNM.ALW{sfx}.sas7bdat (case-insensitive).
            3. %MRGBIC merges them by ITCODE/AMTIND.
            4. Streams LOAN.LNNOTE in chunks (9+ GB file), applies
               ENTITY_CD='PIBB' (conventional-vs-Islamic split) plus
               LOANTYPE IN (124,145) and PZIPCODE filters, aggregates.
            5. Splits result into AL / OB / SP sections.
            6. Writes RDAL semicolon-delimited text file.

Convention:
    REPTMON = '09' (2-digit)
    NOWK    = '4'  (single digit)
    BNM/BNM1/BNMX suffix = '094' -> f"0{int(REPTMON)}{NOWK}"
    LNNOTE  uses REPTMON directly: enrh_ln_note_m09.sas7bdat

reptmon/nowk/reptday/reptyear are passed in by EIBWP124 (single source
of truth). If omitted, they are derived from (today - 1).
"""

import datetime
import math
import os
from pathlib import Path
from typing import Optional

import pandas as pd
import pyreadstat
import saspy

import PBBLNFMT  # noqa: F401  %INC PGM(PBBLNFMT)

from bnm_io import (
    read_sas7bdat,
    read_sas7bdat_ci,
    resolve_ci,
    write_sas_and_txt,
)


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
LOAN_DIR = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS"
)
PBBRDAL_DIR = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output"
)
PBBRDAL_NAME = "PBBRDAL.sas7bdat"

RDAL_OUTPUT_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWP124/rdal.txt"
)


# ============================================================================
# SUFFIX HELPER
# ============================================================================

def bnm_suffix(reptmon: str, nowk: str) -> str:
    """'09' + '4' -> '094'."""
    return f"0{int(reptmon)}{nowk}"


# ============================================================================
# CONSTANTS
# ============================================================================

PZIPCODE_LIST = {
    2002, 2013, 3039, 3047, 800003098, 800003114,
    800004016, 800004022, 800004029, 800040050,
    800040053, 800050024, 800060024, 800060045,
    800060081, 80060085,
}

LNNOTE_USECOLS   = ['entity_cd', 'loantype', 'pzipcode', 'balance']
LNNOTE_CHUNKSIZE = 1_000_000


# ============================================================================
# DATE VARIABLES (fallback for standalone invocation)
# ============================================================================

def get_rept_vars() -> dict:
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

    return {
        'REPTMON':  f"{reptdate.month:02d}",
        'NOWK':     wk,
        'REPTDAY':  f"{day:02d}",
        'REPTYEAR': str(reptdate.year),
    }


# ============================================================================
# HELPERS
# ============================================================================

def round_div1000(value) -> int:
    """Equivalent to SAS ROUND(AMOUNT/1000)."""
    if value is None:
        return 0
    return int(math.floor(float(value) / 1000.0 + 0.5))


# ============================================================================
# STREAMING LNNOTE -> CAGAMAS SUMMARY
# ============================================================================

def build_cag_summary(
    loan_file: Path,
    chunksize: int = LNNOTE_CHUNKSIZE,
) -> pd.DataFrame:
    """
    Stream LOAN.LNNOTE in chunks. ENTITY_CD filter applies ONLY here.

    Per chunk:
        entity_cd == 'PIBB'
        loantype IN (124, 145)
        pzipcode IN PZIPCODE_LIST
    Aggregate: sum(balance) grouped by amtind.

    Returns DataFrame with columns: itcode, amtind, amount.
    """
    partials = []
    total_rows = 0

    reader = pyreadstat.read_file_in_chunks(
        pyreadstat.read_sas7bdat,
        str(loan_file),
        chunksize=chunksize,
        usecols=LNNOTE_USECOLS,
        disable_datetime_conversion=True,
    )

    for i, (chunk, _meta) in enumerate(reader, start=1):
        chunk.columns = [c.lower() for c in chunk.columns]
        total_rows += len(chunk)

        chunk = chunk[
            (chunk['entity_cd'] == 'PIBB') &
            (chunk['loantype'].isin([124, 145])) &
            (chunk['pzipcode'].isin(PZIPCODE_LIST))
        ]
        if chunk.empty:
            continue

        chunk['loantype'] = chunk['loantype'].astype('int16')
        chunk['pzipcode'] = chunk['pzipcode'].astype('int32')
        chunk['balance']  = chunk['balance'].astype('float32')

        partials.append(
            chunk.groupby('amtind', dropna=False, as_index=False)
                 .agg(amount=('balance', 'sum'))
        )
        del chunk

        if i % 10 == 0:
            print(f"  ... LNNOTE chunk {i}, rows scanned: {total_rows:,}")

    if not partials:
        return pd.DataFrame(
            columns=['itcode', 'amtind', 'amount'],
            dtype=object,
        )

    cag_summary = (
        pd.concat(partials, ignore_index=True)
          .groupby('amtind', dropna=False, as_index=False)
          .agg(amount=('amount', 'sum'))
    )
    cag_summary['itcode'] = '7511100000000Y'
    cag_summary['prodcd'] = '34120'
    return cag_summary[['itcode', 'amtind', 'amount']]


# ============================================================================
# %MACRO MRGBIC
# ============================================================================

def macro_mrgbic(
    pbbrdal_df: pd.DataFrame,
    alw_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    DATA PBBRDAL1:
        IF SUBSTR(ITCODE,2,1)='0' THEN AMTIND=' ' ELSE AMTIND='D';
        AMOUNT=0;

    MERGE ALW (AMOUNT->AMT1) PBBRDAL1 (AMOUNT->AMT2) BY ITCODE AMTIND;
        A AND B     -> AMOUNT = AMT1
        NOT A AND B -> AMOUNT = AMT2
        A AND NOT B -> AMOUNT = AMT1

    REMOVE UNWANTED ITCODE ranges.
    """
    pbbrdal_df = pbbrdal_df.copy()
    alw_df     = alw_df.copy()
    pbbrdal_df.columns = [c.lower() for c in pbbrdal_df.columns]
    alw_df.columns     = [c.lower() for c in alw_df.columns]

    pbbrdal1 = pbbrdal_df.copy()
    pbbrdal1['amtind'] = pbbrdal1['itcode'].str.slice(1, 2).apply(
        lambda c: ' ' if c == '0' else 'D'
    )
    pbbrdal1['amount'] = 0.0

    merged = pd.merge(
        alw_df.rename(columns={'amount': 'amt1'}),
        pbbrdal1.rename(columns={'amount': 'amt2'}),
        on=['itcode', 'amtind'],
        how='outer',
    )
    merged['amount'] = merged['amt1'].where(merged['amt1'].notna(), merged['amt2'])
    merged = merged.drop(
        columns=[c for c in ('amt1', 'amt2') if c in merged.columns]
    )

    it5 = merged['itcode'].str.slice(0, 5)
    mask = ~(
        ((it5 >= '30221') & (it5 <= '30228')) |
        ((it5 >= '30231') & (it5 <= '30238')) |
        ((it5 >= '30091') & (it5 <= '30098')) |
        ((it5 >= '40151') & (it5 <= '40158')) |
        (it5 == 'NSSTS')
    )
    return merged[mask]


# ============================================================================
# MAIN
# ============================================================================

def main(
    reptmon: Optional[str] = None,
    nowk: Optional[str] = None,
    reptday: Optional[str] = None,
    reptyear: Optional[str] = None,
) -> None:
    # -----------------------------------------------------------------------
    # Resolve date variables (fallback for standalone invocation)
    # -----------------------------------------------------------------------
    if reptmon is None or nowk is None:
        fallback = get_rept_vars()
        reptmon  = reptmon  or fallback['REPTMON']
        nowk     = nowk     or fallback['NOWK']
        reptday  = reptday  or fallback['REPTDAY']
        reptyear = reptyear or fallback['REPTYEAR']
        print(
            f"P124RDAL: no explicit date passed — derived "
            f"reptmon={reptmon!r} nowk={nowk!r}"
        )

    if reptday is None:
        reptday = f"{(datetime.date.today() - datetime.timedelta(days=1)).day:02d}"
    if reptyear is None:
        reptyear = str(
            (datetime.date.today() - datetime.timedelta(days=1)).year
        )

    sfx = bnm_suffix(reptmon, nowk)
    print(f"P124RDAL DEBUG: REPTMON={reptmon!r} NOWK={nowk!r} sfx={sfx!r}")

    # -----------------------------------------------------------------------
    # %GET_BICS — NOWK governs weekly vs monthly PBBRDAL list
    # -----------------------------------------------------------------------
    if nowk == '4':
        import PBBMRDLF  # noqa: F401
    else:
        import PBBWRDLF  # noqa: F401

    # -----------------------------------------------------------------------
    # Load PBBRDAL (case-insensitive; SAS writes lowercase here)
    # -----------------------------------------------------------------------
    pbbrdal_df = read_sas7bdat_ci(PBBRDAL_DIR, PBBRDAL_NAME, required=True)

    # -----------------------------------------------------------------------
    # Load BNM.ALW{sfx} (case-insensitive)
    # -----------------------------------------------------------------------
    alw_df = read_sas7bdat_ci(BNM_PATH, f"alw{sfx}.sas7bdat", required=True)

    # %MRGBIC
    rdal_df = macro_mrgbic(pbbrdal_df, alw_df)

    # -----------------------------------------------------------------------
    # DATA CAG — stream LNNOTE (ENTITY_CD filter only here)
    # -----------------------------------------------------------------------
    loan_file = LOAN_DIR / f"enrh_ln_note_m{reptmon}.sas7bdat"
    if not loan_file.exists():
        # also try case-insensitive
        alt = resolve_ci(LOAN_DIR, f"enrh_ln_note_m{reptmon}.sas7bdat")
        if alt is None:
            raise FileNotFoundError(
                f"LNNOTE not found for REPTMON={reptmon}: {loan_file}"
            )
        loan_file = alt

    print(f"Streaming LNNOTE: {loan_file}  (chunksize={LNNOTE_CHUNKSIZE:,})")
    cag_summary = build_cag_summary(loan_file)
    print(f"Cagamas summary rows: {len(cag_summary)}")

    rdal_df = pd.concat([rdal_df, cag_summary], ignore_index=True, sort=False)

    # IF SUBSTR(ITCODE,1,3) IN ('331','421','426','431') THEN DELETE;
    it3 = rdal_df['itcode'].str.slice(0, 3)
    rdal_df = rdal_df[~it3.isin(['331', '421', '426', '431'])]

    # PROC SORT BY ITCODE AMTIND
    rdal_df = rdal_df.sort_values(['itcode', 'amtind']).reset_index(drop=True)

    # -----------------------------------------------------------------------
    # DATA AL OB SP: SET RDAL
    # -----------------------------------------------------------------------
    al_rows, ob_rows, sp_rows = [], [], []

    for row in rdal_df.to_dict('records'):
        itcode = str(row.get('itcode', '') or '')
        amtind = str(row.get('amtind', '') or '')

        it1, it3_, it4, it5, it2_1 = (
            itcode[0:1], itcode[0:3], itcode[0:4],
            itcode[0:5], itcode[1:2],
        )

        if amtind != ' ':
            if it3_ == '307':
                sp_rows.append(row)
            elif it5 == '40190':
                sp_rows.append(row)
            elif it4 == 'SSTS':
                new_row = dict(row)
                new_row['itcode'] = '4017000000000Y'
                sp_rows.append(new_row)
            elif it1 != '5':
                if it3_ in ('685', '785'):
                    sp_rows.append(row)
                else:
                    al_rows.append(row)
            else:
                ob_rows.append(row)
        elif it2_1 == '0':
            sp_rows.append(row)

    al_df = pd.DataFrame(al_rows) if al_rows else pd.DataFrame(columns=rdal_df.columns)
    ob_df = pd.DataFrame(ob_rows) if ob_rows else pd.DataFrame(columns=rdal_df.columns)
    sp_df = pd.DataFrame(sp_rows) if sp_rows else pd.DataFrame(columns=rdal_df.columns)

    sp_df = sp_df.sort_values('itcode').reset_index(drop=True)

    # -----------------------------------------------------------------------
    # Write RDAL text file
    # -----------------------------------------------------------------------
    RDAL_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    phead = f"RDAL{reptday}{reptmon}{reptyear}"

    with open(RDAL_OUTPUT_PATH, 'w', encoding='utf-8', newline='\n') as f:
        # --- AL ---
        al_sorted = al_df.sort_values(['itcode', 'amtind']).reset_index(drop=True)
        first_al, amountd, amounti, prev_itcode = True, 0, 0, None

        for row in al_sorted.to_dict('records'):
            itcode = str(row.get('itcode', '') or '')
            amtind = str(row.get('amtind', '') or '')
            amount = float(row.get('amount', 0) or 0)

            if first_al:
                f.write(phead + '\n')
                f.write('AL\n')
                first_al = False

            if prev_itcode is not None and itcode != prev_itcode:
                amountd = amountd + amounti
                f.write(f"{prev_itcode};{amountd};{amounti}\n")
                amountd, amounti = 0, 0

            amt_rounded = round_div1000(amount)
            if amtind == 'D':
                amountd += amt_rounded
            elif amtind == 'I':
                amounti += amt_rounded

            prev_itcode = itcode

        if prev_itcode is not None:
            amountd = amountd + amounti
            f.write(f"{prev_itcode};{amountd};{amounti}\n")

        # --- OB ---
        ob_sorted = ob_df.sort_values(['itcode', 'amtind']).reset_index(drop=True)
        first_ob, amountd, amounti, prev_itcode = True, 0, 0, None

        for row in ob_sorted.to_dict('records'):
            itcode = str(row.get('itcode', '') or '')
            amtind = str(row.get('amtind', '') or '')
            amount = float(row.get('amount', 0) or 0)

            if first_ob:
                f.write('OB\n')
                first_ob = False

            if prev_itcode is not None and itcode != prev_itcode:
                amountd = amountd + amounti
                f.write(f"{prev_itcode};{amountd};{amounti}\n")
                amountd, amounti = 0, 0

            if amtind == 'D':
                amountd += round_div1000(amount)
            elif amtind == 'I':
                amounti += round_div1000(amount)

            prev_itcode = itcode

        if prev_itcode is not None:
            amountd = amountd + amounti
            f.write(f"{prev_itcode};{amountd};{amounti}\n")

        # --- SP ---
        first_sp, amountd, prev_itcode = True, 0.0, None

        for row in sp_df.to_dict('records'):
            itcode = str(row.get('itcode', '') or '')
            amount = float(row.get('amount', 0) or 0)

            if first_sp:
                f.write('SP\n')
                first_sp = False

            if prev_itcode is not None and itcode != prev_itcode:
                f.write(f"{prev_itcode};{round_div1000(amountd)}\n")
                amountd = 0.0

            amountd += amount
            prev_itcode = itcode

        if prev_itcode is not None:
            f.write(f"{prev_itcode};{round_div1000(amountd)}\n")

    print(f"RDAL output written to: {RDAL_OUTPUT_PATH}")


if __name__ == '__main__':
    main()
