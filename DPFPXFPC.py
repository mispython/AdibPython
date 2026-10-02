#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : PBBMRDLF
Purpose  : Monthly ITCODE reference list -> PBBRDAL.sas7bdat.
           Import executes the build (%INC PGM(PBBMRDLF) behaviour).

Writes columns itcode + amount (amount=0.0) so that
P124RDAL.macro_mrgbic's MERGE (on itcode, amtind) works.
"""

from pathlib import Path

import pandas as pd
import saspy


OUTPUT_DIR  = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output"
)
OUTPUT_BASE = "PBBRDAL"


# ============================================================================
# Monthly ITCODE list (from SAS %MONTHLY CARDS block).
# ============================================================================
ITCODE_DATA = [
    "3313002000000Y",
    "3313003000000Y",
    "4019000000000Y",
    "4216060000000Y",
    "4261076000000Y",
    "4261085000000Y",
    "4263076000000Y",
    "4263085000000Y",
    "4269981000000Y",
    "4313002000000Y",
    "4313003000000Y",
    "7200000008310Y",
    "7300000003000Y",
    "7300000006100Y",
    "7300000008310Y",
    "7300000008320Y",
    "5422000000000Y",
    "4017000000000Y",
    "3051577000000Y",
    "3054077000000Y",
    "3055060000000Y",
    "3055061000000Y",
    "3055076000000Y",
    "3055077000000Y",
    "3056000000000Y",
    "3400010000310Y",
    "3400010008100Y",
    "3400020000100Y",
    "3400020000110Y",
    "3400000000132Y",
    "3400077000420Y",
    "3400078000132Y",
    "3415100000000Y",
    "3415200000000Y",
    "3415900000000Y",
    "3416000000000Y",
    "3420000000420Y",
    "7211500000000Y",
    "7312000000000Y",
    "7318000000000Y",
    "7411000000000Y",
    "7412000000000Y",
    "7413000000000Y",
    "7414000000000Y",
]


def build() -> Path:
    if not ITCODE_DATA:
        raise ValueError(
            "PBBMRDLF.ITCODE_DATA is empty. Populate it with the monthly "
            "ITCODE list (SAS %MONTHLY CARDS) before running — otherwise "
            "PBBRDAL will not be written and P124RDAL will fail."
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    sas7bdat_path = OUTPUT_DIR / f"{OUTPUT_BASE}.sas7bdat"
    text_path     = OUTPUT_DIR / f"{OUTPUT_BASE}.txt"

    # Schema P124RDAL expects: itcode + amount
    df = pd.DataFrame({
        "itcode": ITCODE_DATA,
        "amount": [0.0] * len(ITCODE_DATA),
    })

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
            f"Check the SAS log for ERROR lines."
        )

    print(f"PBBMRDLF: wrote {sas7bdat_path} ({len(df)} records)")
    return sas7bdat_path


build()


if __name__ == '__main__':
    pass
