#!/usr/bin/env python3
"""
Program  : LALWP124
Purpose  : Report on Domestic Assets and Liabilities - Part I (M&I Loan).
           Filters L124/UL124 loan data (entity_cd = 'PIBB', Islamic),
           summarises by customer code, approved limit, and Cagamas sold
           loans, then appends results to BNM.LALW{REPTMON}{NOWK}.

Dependency: PBBLNFMT  - format/mapping functions (format_lnprod, etc.)
            L124PBBD  - produces BNM.L124{REPTMON}{NOWK} and
                        BNM.UL124{REPTMON}{NOWK} from BNM1 source data.
"""

from pathlib import Path

import pandas as pd
import pyreadstat
import saspy

# ---------------------------------------------------------------------------
# Dependency: PBBLNFMT  (%INC PGM(PBBLNFMT))
# ---------------------------------------------------------------------------
from PBBLNFMT import (
    format_lnprod,
    format_lncustcd,
    format_lndenom,
    format_apprlimt,
)

# ---------------------------------------------------------------------------
# Dependency: L124PBBD  (%INC PGM(L124PBBD))
# ---------------------------------------------------------------------------
from L124PBBD import main as run_l124pbbd, get_reptmon_nowk

# ============================================================================
# PATH CONFIGURATION (absolute paths, no BASE_DIR)
# ============================================================================

# BNM library - input (produced by L124PBBD) and output
BNM_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm"
)

# BNM1 library - PIBB sasdata (SAP.PIBB.SASDATA)
BNM1_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1"
)

# BNMX library - source of ALW data keyed by REPTYEAR
BNMX_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnmx"
)


# ============================================================================
# BNMCODE MAPPING LOGIC
# ============================================================================

def get_bnmcodes_for_custcd(custcd: str) -> list:
    """
    Replicate SAS SELECT(CUSTCD) logic.
    Returns a list of BNMCODE strings (one or two entries per custcd).
    """
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
# HELPERS: Read / Append / Write via pyreadstat + saspy
# ============================================================================

def read_sas7bdat(path: Path, where: str | None = None) -> pd.DataFrame:
    """Read .sas7bdat via pyreadstat, lowercase columns, optional filter."""
    df, _meta = pyreadstat.read_sas7bdat(str(path))
    df.columns = [c.lower() for c in df.columns]
    if where:
        df = df.query(where)
    return df


def write_sas_and_txt(df: pd.DataFrame, out_dir: Path, base_name: str) -> None:
    """Write DataFrame as .sas7bdat and semicolon-delimited .txt via saspy."""
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


def append_to_output(new_df: pd.DataFrame, target_base: Path) -> pd.DataFrame:
    """
    PROC APPEND DATA=new_df BASE=target_base equivalent.
    """
    target_sas = target_base.with_suffix('.sas7bdat')

    if target_sas.exists():
        existing_df = read_sas7bdat(target_sas)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True, sort=False)
    else:
        combined_df = new_df

    return combined_df


# ============================================================================
# MAIN PROCESSING
# ============================================================================

def main():
    # -----------------------------------------------------------------------
    # Derive REPTMON and NOWK
    # -----------------------------------------------------------------------
    reptmon, nowk = get_reptmon_nowk()

    # -----------------------------------------------------------------------
    # %INC PGM(L124PBBD)
    # -----------------------------------------------------------------------
    run_l124pbbd()

    # -----------------------------------------------------------------------
    # PROC DATASETS LIB=BNM NOLIST;
    #   DELETE LALW&REPTMON&NOWK LALM&REPTMON&NOWK LALQ&REPTMON&NOWK;
    # -----------------------------------------------------------------------
    lalw_base = BNM_PATH / f"lalw{reptmon}{nowk}"
    lalm_path = BNM_PATH / f"lalm{reptmon}{nowk}.sas7bdat"
    lalq_path = BNM_PATH / f"lalq{reptmon}{nowk}.sas7bdat"

    for p in (lalw_base.with_suffix('.sas7bdat'),
              lalw_base.with_suffix('.txt'),
              lalm_path, lalq_path):
        if p.exists():
            p.unlink()

    # -----------------------------------------------------------------------
    # DATA LOAN&REPTMON&NOWK: SET BNM.L124&REPTMON&NOWK
    # DATA ULOAN&REPTMON&NOWK: SET BNM.UL124&REPTMON&NOWK
    # ENTITY_CD = 'PIBB' (Islamic) filter applied.
    # -----------------------------------------------------------------------
    l124_path  = BNM_PATH / f"l124{reptmon}{nowk}.sas7bdat"
    ul124_path = BNM_PATH / f"ul124{reptmon}{nowk}.sas7bdat"

    loan_df  = read_sas7bdat(l124_path,  where="entity_cd == 'PIBB'")
    uloan_df = read_sas7bdat(ul124_path, where="entity_cd == 'PIBB'")

    # -----------------------------------------------------------------------
    # SECTION 1: RM LOANS - BY CUSTOMER CODE
    # -----------------------------------------------------------------------
    alw1_df = (
        loan_df[
            loan_df['prodcd'].str.slice(0, 3).isin(['341', '342', '343', '344'])
        ]
        .groupby(['custcd', 'prodcd', 'amtind'], dropna=False, as_index=False)
        .agg(amount=('balance', 'sum'))
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
    lalw_df = append_to_output(alwloan1_df, lalw_base)

    # -----------------------------------------------------------------------
    # SECTION 2: GROSS LOAN - BY APPROVED LIMIT
    # -----------------------------------------------------------------------
    mask2 = (
        (loan_df['prodcd'].str.slice(0, 2) == '34') |
        (loan_df['prodcd'] == '54120')
    )
    alw2_df = (
        loan_df[mask2]
        .groupby(['prodcd', 'amtind'], dropna=False, as_index=False)
        .agg(amount=('balance', 'sum'))
    )

    alwloan2_df = pd.DataFrame({
        'BNMCODE': '3051000000000Y',
        'AMTIND':  alw2_df['amtind'],
        'AMOUNT':  alw2_df['amount'],
    })

    lalw_df = append_to_output(alwloan2_df, lalw_base)

    # -----------------------------------------------------------------------
    # SECTION 3: LOANS SOLD TO CAGAMAS BERHAD WITH RECOURSE
    # -----------------------------------------------------------------------
    alw3_df = (
        loan_df[loan_df['product'].isin([124, 145])]
        .groupby(['prodcd', 'amtind'], dropna=False, as_index=False)
        .agg(amount=('balance', 'sum'))
    )

    alwloan3_df = pd.DataFrame({
        'BNMCODE': '7511100000000Y',
        'AMTIND':  alw3_df['amtind'],
        'AMOUNT':  alw3_df['amount'],
    })

    lalw_df = append_to_output(alwloan3_df, lalw_base)

    # -----------------------------------------------------------------------
    # FINAL CONSOLIDATION
    # -----------------------------------------------------------------------
    lalw_final = (
        lalw_df
        .groupby(['BNMCODE', 'AMTIND'], dropna=False, as_index=False)
        .agg(AMOUNT=('AMOUNT', 'sum'))
    )

    write_sas_and_txt(lalw_final, BNM_PATH, f"lalw{reptmon}{nowk}")
    print(f"LALW written to: {BNM_PATH / ('lalw' + reptmon + nowk + '.sas7bdat')}  "
          f"({len(lalw_final)} rows)")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    main()
