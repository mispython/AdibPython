#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : PBBMRDLF
Purpose  : Monthly ITCODE reference list -> PBBRDAL.sas7bdat
"""

from pathlib import Path

import pandas as pd
import saspy


OUTPUT_DIR  = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output"
)
OUTPUT_BASE = "PBBRDAL"


# Replace with the monthly SAS CARDS contents.
ITCODE_DATA = [
    # ... monthly ITCODEs here ...
]


def build() -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    sas7bdat_path = OUTPUT_DIR / f"{OUTPUT_BASE}.sas7bdat"
    text_path     = OUTPUT_DIR / f"{OUTPUT_BASE}.txt"

    df = pd.DataFrame({"ITCODE": ITCODE_DATA})

    sas = saspy.SASsession(cfgname='default')
    sas.df2sd(df, table=OUTPUT_BASE, libref='WORK')

    sas.submit(
        f"""
        PROC EXPORT DATA=WORK.{OUTPUT_BASE}
            OUTFILE="{sas7bdat_path}"
            DBMS=SAS7BDAT REPLACE;
        RUN;
        """
    )
    sas.submit(
        f"""
        PROC EXPORT DATA=WORK.{OUTPUT_BASE}
            OUTFILE="{text_path}"
            DBMS=DLM REPLACE;
            DELIMITER=';';
        RUN;
        """
    )
    sas.endsas()

    print(f"PBBMRDLF: wrote {sas7bdat_path} ({len(df)} records)")
    return sas7bdat_path


build()


if __name__ == '__main__':
    pass
