#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : bnm_io
Purpose  : Shared read/write helpers for the EIBWP124 pipeline.

           read_sas7bdat        — read .sas7bdat via pyreadstat,
                                  lowercased columns, optional filter.
           resolve_ci           — case-insensitive file lookup in a directory.
           read_sas7bdat_ci     — read with case-insensitive lookup.
           write_sas_and_txt    — write DataFrame to .sas7bdat + .txt via
                                  saspy using SAS LIBNAME + DATA (this SAS
                                  install rejects DBMS=SAS7BDAT for PROC
                                  EXPORT).

Convention on this filesystem:
    SAS writes member filenames in LOWERCASE (e.g. 'pbbrdal.sas7bdat',
    'l124094.sas7bdat'). All reads go through case-insensitive lookup.
"""

import os
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import pyreadstat
import saspy


# ============================================================================
# CASE-INSENSITIVE FILE LOOKUP
# ============================================================================

def resolve_ci(directory: Path, filename: str) -> Optional[Path]:
    """
    Return the actual Path in `directory` whose name matches `filename`
    case-insensitively, or None if not found.
    """
    target = filename.lower()
    try:
        for name in os.listdir(directory):
            if name.lower() == target:
                return directory / name
    except OSError:
        pass
    return None


def wait_for_file_ci(
    directory: Path,
    filename: str,
    timeout: float = 30.0,
    interval: float = 0.5,
) -> Optional[Path]:
    """
    Poll for a case-insensitive file match up to `timeout` seconds.
    Returns the actual Path or None if not found in time.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        p = resolve_ci(directory, filename)
        if p is not None:
            return p
        time.sleep(interval)
    return None


# ============================================================================
# READ HELPERS
# ============================================================================

def read_sas7bdat(path: Path, where: Optional[str] = None) -> pd.DataFrame:
    """Read .sas7bdat via pyreadstat, lowercase columns, optional filter."""
    df, _meta = pyreadstat.read_sas7bdat(str(path))
    df.columns = [c.lower() for c in df.columns]
    if where and not df.empty:
        col = where.split()[0]
        if col in df.columns:
            df = df.query(where)
    return df


def read_sas7bdat_ci(
    directory: Path,
    filename: str,
    where: Optional[str] = None,
    required: bool = True,
) -> Optional[pd.DataFrame]:
    """
    Case-insensitive read of directory/filename.

    - If `required=True` and no match, raises FileNotFoundError.
    - If `required=False` and no match, returns None.
    """
    actual = resolve_ci(directory, filename)
    if actual is None:
        if required:
            raise FileNotFoundError(
                f"No case-insensitive match for '{filename}' in {directory}"
            )
        return None
    return read_sas7bdat(actual, where=where)


# ============================================================================
# WRITE HELPERS
# ============================================================================

def write_sas_and_txt(
    df: pd.DataFrame,
    out_dir: Path,
    base_name: str,
    *,
    verbose: bool = False,
) -> Path:
    """
    Write DataFrame to out_dir/base_name.sas7bdat and out_dir/base_name.txt
    via saspy.

    Uses SAS LIBNAME + DATA for the .sas7bdat (this SAS install does not
    support DBMS=SAS7BDAT in PROC EXPORT), and PROC EXPORT DBMS=DLM for
    the semicolon-delimited .txt.

    Waits for the .sas7bdat to appear on disk (SAS writes filenames in
    lowercase on this filesystem, and there may be an NFS sync delay).

    Returns the actual on-disk path to the .sas7bdat.
    """
    if df is None or len(df.columns) == 0:
        raise ValueError(
            f"Refusing to write schema-less dataset '{base_name}'."
        )
    if df.empty:
        print(f"WARNING: '{base_name}' has 0 rows — writing empty dataset.")

    out_dir.mkdir(parents=True, exist_ok=True)
    sas7bdat_name = f"{base_name}.sas7bdat"
    text_name     = f"{base_name}.txt"
    text_path     = out_dir / text_name

    # Delete any existing (case-insensitively) outputs.
    for nm in (sas7bdat_name, text_name):
        p = resolve_ci(out_dir, nm)
        if p is not None:
            p.unlink()

    sas = saspy.SASsession(cfgname='default')
    try:
        sas.df2sd(df, table=base_name, libref='WORK')

        log1 = sas.submit(f"""
            LIBNAME _outlib "{out_dir}";
            DATA _outlib.{base_name};
                SET WORK.{base_name};
            RUN;
            LIBNAME _outlib CLEAR;
        """)

        log2 = sas.submit(f"""
            PROC EXPORT DATA=WORK.{base_name}
                OUTFILE="{text_path}"
                DBMS=DLM REPLACE;
                DELIMITER=';';
            RUN;
        """)

        if verbose:
            print(f"=== SAS log: LIBNAME+DATA -> {sas7bdat_name} ===")
            print(log1.get('LOG', ''))
            print(f"=== SAS log: PROC EXPORT txt -> {text_name} ===")
            print(log2.get('LOG', ''))

    finally:
        sas.endsas()

    actual = wait_for_file_ci(out_dir, sas7bdat_name, timeout=30.0)
    if actual is None:
        print(f"DEBUG: directory listing of {out_dir}:")
        try:
            for name in sorted(os.listdir(out_dir)):
                print(f"   {name}")
        except OSError:
            pass
        raise RuntimeError(
            f"Failed to create {sas7bdat_name} in {out_dir}. "
            f"See SAS log above."
        )

    return actual
