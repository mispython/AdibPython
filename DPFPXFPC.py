#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : PBBWRDLF
Purpose  : Weekly ITCODE reference list -> PBBRDAL.sas7bdat (+ .txt).

Builds in polars; writes via SAS LIBNAME + DATA and PROC EXPORT DBMS=DLM.
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


# ============================================================================
# Weekly ITCODE list (from SAS %WEEKLY CARDS block).
# ============================================================================
ITCODE_DATA = [
    "3313002000000Y",
    "3313003000000Y",
    "4017000000000Y",
    "4019000000000Y",
    "4216060000000Y",
    "4261076000000Y",
    "4261085000000Y",
    "4263076000000Y",
    "4263085000000Y",
    "4269981000000Y",
    "4313002000000Y",
    "4313003000000Y",
    "5422000000000Y",
    "7200000008310Y",
    "7300000003000Y",
    "7300000006100Y",
    "7300000008310Y",
    "7300000008320Y",
]


def _wait_for_file(path: Path, timeout: float = 30.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists():
            return True
        time.sleep(0.5)
    return False


def _list_dir(path: Path) -> None:
    print(f"DEBUG: directory listing of {path}:")
    try:
        for name in sorted(os.listdir(path)):
            if 'PBBRDAL' in name.upper() or name.lower().endswith('.sas7bdat'):
                print(f"   {name}")
    except OSError as e:
        print(f"   (cannot list: {e})")


def build() -> Path:
    if not ITCODE_DATA:
        raise ValueError("PBBWRDLF.ITCODE_DATA is empty.")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    sas7bdat_path = OUTPUT_DIR / f"{OUTPUT_BASE}.sas7bdat"
    text_path     = OUTPUT_DIR / f"{OUTPUT_BASE}.txt"

    for p in (sas7bdat_path, text_path):
        if p.exists():
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
                OUTFILE="{text_path}"
                DBMS=DLM REPLACE;
                DELIMITER=';';
            RUN;
        """)
        print("=== SAS log: PROC EXPORT txt -> PBBRDAL.txt ===")
        print(log2.get('LOG', ''))

    finally:
        sas.endsas()

    if not _wait_for_file(sas7bdat_path, timeout=30.0):
        print(f"DEBUG: {sas7bdat_path} did not appear after 30s.")
        _list_dir(OUTPUT_DIR)
        raise RuntimeError(
            f"PBBWRDLF: {sas7bdat_path} never appeared on disk."
        )

    print(f"PBBWRDLF: wrote {sas7bdat_path} ({pl_df.height} records)")
    return sas7bdat_path


build()


if __name__ == '__main__':
    pass
