#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : PBBMRDLF
Purpose  : Monthly ITCODE reference list -> PBBRDAL.sas7bdat (+ .txt).

Builds in polars. Writes:
  - PBBRDAL.sas7bdat via SAS LIBNAME + DATA.
  - PBBRDAL.txt     via saspy PROC EXPORT DBMS=DLM.

Resolves the output filename case-insensitively (SAS writes lowercase
on this filesystem: 'pbbrdal.sas7bdat').
"""

import os
import time
from pathlib import Path

import polars as pl
import saspy


OUTPUT_DIR  = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output"
)
OUTPUT_BASE = "PBBRDAL"


ITCODE_DATA = [
    "3313002000000Y", "3313003000000Y", "4019000000000Y", "4216060000000Y",
    "4261076000000Y", "4261085000000Y", "4263076000000Y", "4263085000000Y",
    "4269981000000Y", "4313002000000Y", "4313003000000Y", "7200000008310Y",
    "7300000003000Y", "7300000006100Y", "7300000008310Y", "7300000008320Y",
    "5422000000000Y", "4017000000000Y", "3051577000000Y", "3054077000000Y",
    "3055060000000Y", "3055061000000Y", "3055076000000Y", "3055077000000Y",
    "3056000000000Y", "3400010000310Y", "3400010008100Y", "3400020000100Y",
    "3400020000110Y", "3400000000132Y", "3400077000420Y", "3400078000132Y",
    "3415100000000Y", "3415200000000Y", "3415900000000Y", "3416000000000Y",
    "3420000000420Y", "7211500000000Y", "7312000000000Y", "7318000000000Y",
    "7411000000000Y", "7412000000000Y", "7413000000000Y", "7414000000000Y",
]


def _resolve_ci(directory: Path, filename: str):
    """Case-insensitive file lookup in `directory`. Returns Path or None."""
    target = filename.lower()
    try:
        for name in os.listdir(directory):
            if name.lower() == target:
                return directory / name
    except OSError:
        pass
    return None


def _wait_for_file_ci(directory: Path, filename: str, timeout: float = 30.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        p = _resolve_ci(directory, filename)
        if p is not None:
            return p
        time.sleep(0.5)
    return None


def build() -> Path:
    if not ITCODE_DATA:
        raise ValueError("PBBMRDLF.ITCODE_DATA is empty.")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    sas7bdat_name = f"{OUTPUT_BASE}.sas7bdat"   # desired name
    text_name     = f"{OUTPUT_BASE}.txt"

    # Delete stale outputs (case-insensitive)
    for nm in (sas7bdat_name, text_name):
        p = _resolve_ci(OUTPUT_DIR, nm)
        if p is not None:
            p.unlink()

    pl_df = pl.DataFrame({
        "itcode": ITCODE_DATA,
        "amount": [0.0] * len(ITCODE_DATA),
    })
    pd_df = pl_df.to_pandas()

    sas = saspy.SASsession(cfgname='default')
    try:
        sas.df2sd(pd_df, table=OUTPUT_BASE, libref='WORK')

        log1 = sas.submit(f"""
            LIBNAME _outlib "{OUTPUT_DIR}";
            DATA _outlib.{OUTPUT_BASE};
                SET WORK.{OUTPUT_BASE};
            RUN;
            LIBNAME _outlib CLEAR;
        """)
        print("=== SAS log: LIBNAME+DATA -> PBBRDAL.sas7bdat ===")
        print(log1.get('LOG', ''))

        log2 = sas.submit(f"""
            PROC EXPORT DATA=WORK.{OUTPUT_BASE}
                OUTFILE="{OUTPUT_DIR}/{text_name}"
                DBMS=DLM REPLACE;
                DELIMITER=';';
            RUN;
        """)
        print("=== SAS log: PROC EXPORT txt -> PBBRDAL.txt ===")
        print(log2.get('LOG', ''))

    finally:
        sas.endsas()

    sas7bdat_path = _wait_for_file_ci(OUTPUT_DIR, sas7bdat_name, timeout=30.0)
    if sas7bdat_path is None:
        print(f"DEBUG: no case-insensitive match for {sas7bdat_name} in {OUTPUT_DIR}")
        try:
            for name in sorted(os.listdir(OUTPUT_DIR)):
                print(f"   {name}")
        except OSError:
            pass
        raise RuntimeError(
            f"PBBMRDLF: {sas7bdat_name} never appeared on disk."
        )

    print(f"PBBMRDLF: wrote {sas7bdat_path} ({pl_df.height} records)")
    return sas7bdat_path


build()


if __name__ == '__main__':
    pass
