#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : PBBMRDLF
Purpose  : Monthly ITCODE reference list -> PBBRDAL.sas7bdat
           Importing this module executes the build at import time,
           mirroring %INC PGM(PBBMRDLF).
"""

from pathlib import Path

import pandas as pd
import saspy


OUTPUT_DIR  = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output"
)
OUTPUT_BASE = "PBBRDAL"


# ============================================================================
# Monthly ITCODE list — paste the SAS CARDS contents here.
# ============================================================================
ITCODE_DATA = [
    # "3313002000000Y",
    # "3313003000000Y",
    # ... (populate from the SAS %MONTHLY CARDS block) ...
]


def build() -> Path:
    if not ITCODE_DATA:
        raise ValueError(
            "PBBMRDLF.ITCODE_DATA is empty. Populate it with the monthly "
            "ITCODE list from the SAS CARDS block before running. "
            "Writing an empty PBBRDAL would make downstream P124RDAL fail."
        )

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
            f"PBBMRDLF: PROC EXPORT did not create {sas7bdat_path}. "
            f"Check the SAS log — the DataFrame may have been rejected."
        )

    print(f"PBBMRDLF: wrote {sas7bdat_path} ({len(df)} records)")
    return sas7bdat_path


# Import triggers build (%INC behaviour)
build()


if __name__ == '__main__':
    pass
