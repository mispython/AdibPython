#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : PBBWRDLF
Purpose  : Weekly ITCODE reference list -> PBBRDAL.sas7bdat
           Import executes the build (%INC PGM(PBBWRDLF) behaviour).
"""

from pathlib import Path

import pandas as pd
import saspy


OUTPUT_DIR  = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output"
)
OUTPUT_BASE = "PBBRDAL"


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

    df = pd.DataFrame({"itcode": ITCODE_DATA})

    sas = saspy.SASsession(cfgname='default')
    sas.df2sd(df, table=OUTPUT_BASE, libref='WORK')

    sas.submit(f"""
        PROC EXPORT DATA=WORK.{OUTPUT_BASE}
            OUTFILE="{sas7bdat_path}"
            DBMS=SAS7BDAT REPLACE;
        RUN;
    """)
    sas.submit(f"""
        PROC EXPORT DATA=WORK.{OUTPUT_BASE}
            OUTFILE="{text_path}"
            DBMS=DLM REPLACE;
            DELIMITER=';';
        RUN;
    """)
    sas.endsas()

    if not sas7bdat_path.exists():
        raise RuntimeError(
            f"PBBWRDLF: PROC EXPORT did not create {sas7bdat_path}."
        )

    print(f"PBBWRDLF: wrote {sas7bdat_path} ({len(df)} records)")
    return sas7bdat_path


build()


if __name__ == '__main__':
    pass
