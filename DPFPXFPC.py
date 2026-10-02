#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : PBBMRDLF
Purpose  : Monthly ITCODE reference list -> PBBRDAL.sas7bdat (+ .txt).

Writes PBBRDAL.sas7bdat via pandas.DataFrame.to_sas (which uses the
bundled SAS7BDAT writer; no SAS session and no pyreadstat writer needed).

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


def build() -> Path:
    if not ITCODE_DATA:
        raise ValueError("PBBMRDLF.ITCODE_DATA is empty.")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    sas7bdat_path = OUTPUT_DIR / f"{OUTPUT_BASE}.sas7bdat"
    text_path     = OUTPUT_DIR / f"{OUTPUT_BASE}.txt"

    # Delete stale outputs so we prove the new ones are created.
    for p in (sas7bdat_path, text_path):
        if p.exists():
            p.unlink()

    df = pd.DataFrame({
        "itcode": ITCODE_DATA,
        "amount": [0.0] * len(ITCODE_DATA),
    })

    # ------------------------------------------------------------------
    # 1. Write .sas7bdat directly via pandas.to_sas
    #    (no SAS session needed; uses bundled sas7bdat writer)
    # ------------------------------------------------------------------
    df.to_sas(str(sas7bdat_path), format='sas7bdat', index=False)

    if not sas7bdat_path.exists():
        raise RuntimeError(
            f"pandas.to_sas did not create {sas7bdat_path}."
        )

    print(f"PBBMRDLF: wrote {sas7bdat_path} ({len(df)} records)")

    # ------------------------------------------------------------------
    # 2. Write .txt via saspy PROC EXPORT DBMS=DLM (confirmed working)
    # ------------------------------------------------------------------
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
