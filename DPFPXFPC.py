#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : P124RDAL.py
Purpose  : Report on Domestic Assets and Liabilities - Part I (Cagamas/L124).
           - Loads BIC reference codes (weekly or monthly depending on NOWK).
           - Merges with BNM.ALW{MM}{WK} summary data.
           - Streams LOAN.LNNOTE in chunks (9+ GB), applies
             ENTITY_CD='PIBB' (conventional vs Islamic split), filters
             loantype IN (124,145) & pzipcode IN (...), aggregates.
           - Splits result into AL / OB / SP sections.
           - Writes semicolon-delimited RDAL text file.

Note: ENTITY_CD filter is applied HERE only — it exists only in LNNOTE.
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

PBBRDAL_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/"
    "output/PBBRDAL.sas7bdat"
)

RDAL_OUTPUT_PATH = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWP124/rdal.txt"
)


# ============================================================================
# CONSTANTS
# ============================================================================

PZIPCODE_LIST = {
    2002, 2013, 3039, 3047, 800003098, 800003114,
    800004016, 800004022, 800004029, 800040050,
    800040053, 800050024, 800060024, 800060045,
    800060081, 80060085,
}

LNNOTE_USECOLS     = ['entity_cd', 'loantype', 'pzipcode', 'balance']
LNNOTE_CHUNKSIZE   = 1_000_000


# ============================================================================
# DATE VARIABLES (from today - 1)
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
    if value is None:
        return 0
    return int(math.floor(float(value) / 1000.0 + 0.5))


def read_sas7bdat(path: Path, where: Optional[str] = None) -> pd.DataFrame:
    df, _meta = pyreadstat.read_sas7bdat(str(path))
    df.columns = [c.lower() for c in df.columns]
    if where and not df.empty:
        col = where.split()[0]
        if col in df.columns:
            df = df.query(where)
    return df


def write_sas_and_txt(df: pd.DataFrame, out_dir: Path, base_name: str) -> None:
    if df is None or len(df.columns) == 0:
        raise ValueError(
            f"Refusing to write schema-less dataset '{base_name}'."
        )
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


# ============================================================================
# STREAMING LNNOTE -> CAGAMAS SUMMARY
# ============================================================================

def build_cag_summary(loan_file: Path,
                      chunksize: int = LNNOTE_CHUNKSIZE) -> pd.DataFrame:
    """
    Stream LOAN.LNNOTE in chunks:
      - entity_cd == 'PIBB'  (Islamic)
      - loantype IN (124, 145)
      - pzipcode IN PZIPCODE_LIST
    Aggregate: sum(balance) per amtind.

    Returns small DataFrame with columns: itcode, amtind, amount
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
            print(f"  ... LNNOTE chunk {i}, total rows scanned: {total_rows:,}")

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


# =========================================================================
