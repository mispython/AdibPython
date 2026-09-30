#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : P124RDAL.py
Purpose  : Report on Domestic Assets and Liabilities - Part I (Cagamas/L124).
           - Loads BIC reference codes (weekly or monthly depending on NOWK).
           - Merges with BNM.ALW summary data.
           - Filters and appends Cagamas loan data (LOANTYPE 124/145).
           - Splits result into AL (assets/loans), OB (off-balance), SP (special).
           - Writes semicolon-delimited output text file RDAL with sections
             AL, OB, SP each prefixed by section header and PHEAD on first line.

Dependency: PBBLNFMT  - format/mapping functions
            PBBWRDLF  - weekly  ITCODE reference list -> PBBRDAL
            PBBMRDLF  - monthly ITCODE reference list -> PBBRDAL
"""

import datetime
import math
from pathlib import Path
from typing import Optional

import pandas as pd
import pyreadstat
import saspy

# %INC PGM(PBBLNFMT)
import PBBLNFMT  # noqa: F401


# ============================================================================
# PATH CONFIGURATION (absolute paths, no BASE_DIR)
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

# PIBB MNILN directory (monthly file: enrh_ln_note_m<MM>.sas7bdat)
LOAN_DIR = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS"
)

# PBBRDAL reference output (produced by PBBWRDLF or PBBMRDLF at import time)
PBBRDAL_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/"
    "output/PBBRDAL.sas7bdat"
)

# RDAL output text file (semicolon-delimited)
RDAL_OUTPUT_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWP124/rdal.txt"
)


# ============================================================================
# DATE VARIABLES (derived from today - 1)
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


def read_sas7bdat(path: Path, where: Optional[str] = None) -> pd.DataFrame:
    df, _meta = pyreadstat.read_sas7bdat(str(path))
    df.columns = [c.lower() for c in df.columns]
    if where:
        df = df.query(where)
    return df


def write_sas_and_txt(df: pd.DataFrame, out_dir: Path, base_name: str) -> None:
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


# ============================================================================
# %MACRO MRGBIC
# ============================================================================

def macro_mrgbic(pbbrdal_df: pd.DataFrame, alw_df: pd.DataFrame) -> pd.DataFrame:
    pbbrdal_df = pbbrdal_df.copy()
    alw_df = alw_df.copy()
    pbbrdal_df.columns = [c.lower() for c in pbbrdal_df.columns]
    alw_df.columns = [c.lower() for c in alw_df.columns]

    # DATA PBBRDAL1
    pbbrdal1 = pbbrdal_df.copy()
    pbbrdal1['amtind'] = pbbrdal1['itcode'].str.slice(1, 2).apply(
        lambda c: ' ' if c == '0' else 'D'
    )
    pbbrdal1['amount'] = 0.0

    # MERGE ALW (AMOUNT->AMT1) PBBRDAL1 (AMOUNT->AMT2)
    merged = pd.merge(
        alw_df.rename(columns={'amount': 'amt1'}),
        pbbrdal1.rename(columns={'amount': 'amt2'}),
        on=['itcode', 'amtind'],
        how='outer',
    )

    merged['amount'] = merged['amt1'].where(merged['amt1'].notna(), merged['amt2'])
    merged = merged.drop(columns=[c for c in ('amt1', 'amt2') if c in merged.columns])

    # Remove unwanted ITCODE ranges
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

def main():
    dvars    = get_rept_vars()
    REPTMON  = dvars['REPTMON']
    NOWK     = dvars['NOWK']
    REPTDAY  = dvars['REPTDAY']
    REPTYEAR = dvars['REPTYEAR']

    # %GET_BICS
    if NOWK == '4':
        import PBBMRDLF  # noqa: F401
    else:
        import PBBWRDLF  # noqa: F401

    # Load PBBRDAL
    if not PBBRDAL_PATH.exists():
        raise FileNotFoundError(f"PBBRDAL not found: {PBBRDAL_PATH}")
    pbbrdal_df = read_sas7bdat(PBBRDAL_PATH)

    # Load BNM.ALW&REPTMON&NOWK
    alw_path = BNM_PATH / f"alw{REPTMON}{NOWK}.sas7bdat"
    if not alw_path.exists():
        raise FileNotFoundError(f"BNM ALW not found: {alw_path}")
    alw_df = read_sas7bdat(alw_path)

    # %MRGBIC
    rdal_df = macro_mrgbic(pbbrdal_df, alw_df)

    # DATA CAG: SET LOAN.LNNOTE (entity_cd = 'PIBB')
    loan_file = LOAN_DIR / f"enrh_ln_note_m{REPTMON}.sas7bdat"
    if not loan_file.exists():
        raise FileNotFoundError(
            f"PIBB LNNOTE monthly file not found for REPTMON={REPTMON}: {loan_file}"
        )
    loan_df = read_sas7bdat(loan_file, where="entity_cd == 'PIBB'")

    pzipcode_list = [
        2002, 2013, 3039, 3047, 800003098, 800003114,
        800004016, 800004022, 800004029, 800040050,
        800040053, 800050024, 800060024, 800060045,
        800060081, 80060085,
    ]

    cag_df = loan_df[loan_df['loantype'].isin([124, 145])].copy()
    cag_df['prodcd'] = '34120'
    cag_df['amtind'] = 'I'
    cag_df = cag_df[cag_df['pzipcode'].isin(pzipcode_list)].copy()
    cag_df['itcode'] = '7511100000000Y'

    cag_summary = (
        cag_df
        .groupby(['itcode', 'amtind'], dropna=False, as_index=False)
        .agg(amount=('balance', 'sum'))
    )

    # DATA RDAL: SET RDAL CAG;
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

        it1   = itcode[0:1]
        it3   = itcode[0:3]
        it4   = itcode[0:4]
        it5   = itcode[0:5]
        it2_1 = itcode[1:2]

        if amtind != ' ':
            if it3 == '307':
                sp_rows.append(row)
            elif it5 == '40190':
                sp_rows.append(row)
            elif it4 == 'SSTS':
                new_row = dict(row)
                new_row['itcode'] = '4017000000000Y'
                sp_rows.append(new_row)
            elif it1 != '5':
                if it3 in ('685', '785'):
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
    # Write RDAL output text file
    # -----------------------------------------------------------------------
    RDAL_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    phead = f"RDAL{REPTDAY}{REPTMON}{REPTYEAR}"

    with open(RDAL_OUTPUT_PATH, 'w', encoding='utf-8', newline='\n') as f:

        # --- AL SECTION ---
        al_sorted = al_df.sort_values(['itcode', 'amtind']).reset_index(drop=True)
        first_al    = True
        amountd     = 0
        amounti     = 0
        prev_itcode = None

        for row in al_sorted.to_dict('records'):
            itcode = str(row.get('itcode', '') or '')
            amtind = str(row.get('amtind', '') or '')
            amount = float(row.get('amount', 0) or 0)

            if first_al:
                f.write(phead + '\n')
                f.write('AL\n')
                amountd  = 0
                amounti  = 0
                first_al = False

            if prev_itcode is not None and itcode != prev_itcode:
                amountd = amountd + amounti
                f.write(f"{prev_itcode};{amountd};{amounti}\n")
                amountd = 0
                amounti = 0

            amt_rounded = round_div1000(amount)
            if amtind == 'D':
                amountd += amt_rounded
            elif amtind == 'I':
                amounti += amt_rounded

            prev_itcode = itcode

        if prev_itcode is not None:
            amountd = amountd + amounti
            f.write(f"{prev_itcode};{amountd};{amounti}\n")

        # --- OB SECTION ---
        ob_sorted = ob_df.sort_values(['itcode', 'amtind']).reset_index(drop=True)
        first_ob    = True
        amountd     = 0
        amounti     = 0
        prev_itcode = None

        for row in ob_sorted.to_dict('records'):
            itcode = str(row.get('itcode', '') or '')
            amtind = str(row.get('amtind', '') or '')
            amount = float(row.get('amount', 0) or 0)

            if first_ob:
                f.write('OB\n')
                amountd  = 0
                amounti  = 0
                first_ob = False

            if prev_itcode is not None and itcode != prev_itcode:
                amountd = amountd + amounti
                f.write(f"{prev_itcode};{amountd};{amounti}\n")
                amountd = 0
                amounti = 0

            if amtind == 'D':
                amountd += round_div1000(amount)
            elif amtind == 'I':
                amounti += round_div1000(amount)

            prev_itcode = itcode

        if prev_itcode is not None:
            amountd = amountd + amounti
            f.write(f"{prev_itcode};{amountd};{amounti}\n")

        # --- SP SECTION ---
        first_sp    = True
        amountd     = 0.0
        prev_itcode = None

        for row in sp_df.to_dict('records'):
            itcode = str(row.get('itcode', '') or '')
            amount = float(row.get('amount', 0) or 0)

            if first_sp:
                f.write('SP\n')
                amountd  = 0.0
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
