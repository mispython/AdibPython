#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : PBBMRDLF
Purpose  : Monthly ITCODE reference list -> PBBRDAL.sas7bdat (+ .txt).

Builds the DataFrame in polars. Writes:
  - PBBRDAL.sas7bdat via SAS LIBNAME + DATA (confirmed working).
  - PBBRDAL.txt     via saspy PROC EXPORT DBMS=DLM.

Waits for the .sas7bdat to appear on disk (handles NFS sync delay).
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
# Monthly ITCODE list (from SAS %MONTHLY CARDS block).
# ============================================================================
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
        raise ValueError("PBBMRDLF.ITCODE_DATA is empty.")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    sas7bdat_path = OUTPUT_DIR / f"{OUTPUT_BASE}.sas7bdat"
    text_path     = OUTPUT_DIR / f"{OUTPUT_BASE}.txt"

    # Delete stale outputs
    for p in (sas7bdat_path, text_path):
        if p.exists():
            p.unlink()

    # --- Build with polars ---
    pl_df = pl.DataFrame({
        "itcode": ITCODE_DATA,
        "amount": [0.0] * len(ITCODE_DATA),
    })

    # Convert to pandas at the saspy boundary (df2sd expects pandas)
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
            f"PBBMRDLF: {sas7bdat_path} never appeared on disk."
        )

    print(f"PBBMRDLF: wrote {sas7bdat_path} ({pl_df.height} records)")
    return sas7bdat_path


build()


if __name__ == '__main__':
    pass
