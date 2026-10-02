#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : PBBWRDLF
Purpose  : Weekly ITCODE reference list -> PBBRDAL.sas7bdat (+ .txt).

Writes PBBRDAL.sas7bdat via pandas.DataFrame.to_sas.
The .txt file is written via saspy PROC EXPORT DBMS=DLM (confirmed working).
"""

from pathlib import Path

import pandas as pd
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


def build() -> Path:
    if not ITCODE_DATA:
        raise ValueError("PBBWRDLF.ITCODE_DATA is empty.")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    sas7bdat_path = OUTPUT_DIR / f"{OUTPUT_BASE}.sas7bdat"
    text_path     = OUTPUT_DIR / f"{OUTPUT_BASE}.txt"

    for p in (sas7bdat_path, text_path):
        if p.exists():
            p.unlink()

    df = pd.DataFrame({
        "itcode": ITCODE_DATA,
        "amount": [0.0] * len(ITCODE_DATA),
    })

    # Write .sas7bdat via pandas.to_sas
    df.to_sas(str(sas7bdat_path), format='sas7bdat', index=False)

    if not sas7bdat_path.exists():
        raise RuntimeError(
            f"pandas.to_sas did not create {sas7bdat_path}."
        )

    print(f"PBBWRDLF: wrote {sas7bdat_path} ({len(df)} records)")

    # Write .txt via saspy PROC EXPORT DBMS=DLM
    sas = saspy.SASsession(cfgname='default')
    try:
        sas.df2sd(df, table=OUTPUT_BASE, libref='WORK')
        sas.submit(f"""
            PROC EXPORT DATA=WORK.{OUTPUT_BASE}
                OUTFILE="{text_path}"
                DBMS=DLM REPLACE;
                DELIMITER=';';
            RUN;
        """)
    finally:
        sas.endsas()

    if not text_path.exists():
        print(f"WARNING: {text_path} was not created — check SAS log.")

    return sas7bdat_path


build()


if __name__ == '__main__':
    pass
