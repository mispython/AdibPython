#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : bnm_io
Purpose  : Shared read/write helpers for the EIBWP124 pipeline.

           write_sas_and_txt  — write a DataFrame to .sas7bdat and
                                semicolon-delimited .txt via saspy.
                                Uses LIBNAME + DATA (not PROC EXPORT
                                DBMS=SAS7BDAT — that engine is not
                                available in this SAS install).

           read_sas7bdat      — read .sas7bdat via pyreadstat,
                                lowercasing column names, optional
                                pandas.query() filter.
"""

from pathlib import Path
from typing import Optional

import pandas as pd
import pyreadstat
import saspy


def read_sas7bdat(path: Path, where: Optional[str] = None) -> pd.DataFrame:
    """Read .sas7bdat via pyreadstat, lowercase columns, optional filter."""
    df, _meta = pyreadstat.read_sas7bdat(str(path))
    df.columns = [c.lower() for c in df.columns]
    if where and not df.empty:
        col = where.split()[0]
        if col in df.columns:
            df = df.query(where)
    return df


def write_sas_and_txt(
    df: pd.DataFrame,
    out_dir: Path,
    base_name: str,
    *,
    verbose: bool = True,
) -> None:
    """
    Write DataFrame to out_dir/base_name.sas7bdat and out_dir/base_name.txt
    (semicolon-delimited) via saspy.

    - Allows 0-row datasets (with schema).
    - Refuses schema-less datasets.
    - Uses LIBNAME + DATA for the .sas7bdat (PROC EXPORT DBMS=SAS7BDAT
      is not supported on this SAS install).
    """
    if df is None or len(df.columns) == 0:
        raise ValueError(f"Refusing to write schema-less dataset '{base_name}'.")
    if df.empty:
        print(f"WARNING: '{base_name}' has 0 rows — writing empty dataset.")

    out_dir.mkdir(parents=True, exist_ok=True)
    sas7bdat_path = out_dir / f"{base_name}.sas7bdat"
    text_path     = out_dir / f"{base_name}.txt"

    sas = saspy.SASsession(cfgname='default')
    try:
        sas.df2sd(df, table=base_name, libref='WORK')

        # .sas7bdat — via LIBNAME + DATA (portable, no SAS7BDAT engine needed)
        log1 = sas.submit(f"""
            LIBNAME _outlib "{out_dir}";
            DATA _outlib.{base_name};
                SET WORK.{base_name};
            RUN;
            LIBNAME _outlib CLEAR;
        """)

        # .txt — semicolon-delimited
        log2 = sas.submit(f"""
            PROC EXPORT DATA=WORK.{base_name}
                OUTFILE="{text_path}"
                DBMS=DLM REPLACE;
                DELIMITER=';';
            RUN;
        """)

        if verbose:
            print(f"=== SAS log: LIBNAME+DATA -> {sas7bdat_path.name} ===")
            print(log1.get('LOG', ''))
            print(f"=== SAS log: PROC EXPORT txt -> {text_path.name} ===")
            print(log2.get('LOG', ''))
    finally:
        sas.endsas()

    if not sas7bdat_path.exists():
        raise RuntimeError(
            f"Failed to create {sas7bdat_path}. See SAS log above."
        )


def write_sas_only(df: pd.DataFrame, out_dir: Path, base_name: str) -> None:
    """Write only the .sas7bdat (no .txt)."""
    if df is None or len(df.columns) == 0:
        raise ValueError(f"Refusing to write schema-less dataset '{base_name}'.")
    if df.empty:
        print(f"WARNING: '{base_name}' has 0 rows — writing empty dataset.")

    out_dir.mkdir(parents=True, exist_ok=True)
    sas7bdat_path = out_dir / f"{base_name}.sas7bdat"

    sas = saspy.SASsession(cfgname='default')
    try:
        sas.df2sd(df, table=base_name, libref='WORK')
        sas.submit(f"""
            LIBNAME _outlib "{out_dir}";
            DATA _outlib.{base_name};
                SET WORK.{base_name};
            RUN;
            LIBNAME _outlib CLEAR;
        """)
    finally:
        sas.endsas()

    if not sas7bdat_path.exists():
        raise RuntimeError(f"Failed to create {sas7bdat_path}.")
