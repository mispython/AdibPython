#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : PBBMRDLF
Purpose  : Monthly ITCODE reference list -> PBBRDAL.sas7bdat + .txt
           via the shared bnm_io.write_sas_and_txt helper.
"""

from pathlib import Path

import pandas as pd

from bnm_io import write_sas_and_txt


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


def build() -> Path:
    if not ITCODE_DATA:
        raise ValueError("PBBMRDLF.ITCODE_DATA is empty.")

    df = pd.DataFrame({
        "itcode": ITCODE_DATA,
        "amount": [0.0] * len(ITCODE_DATA),
    })

    write_sas_and_txt(df, OUTPUT_DIR, OUTPUT_BASE, verbose=False)
    print(f"PBBMRDLF: wrote {OUTPUT_DIR / (OUTPUT_BASE + '.sas7bdat')} "
          f"({len(df)} records)")
    return OUTPUT_DIR / f"{OUTPUT_BASE}.sas7bdat"


build()


if __name__ == '__main__':
    pass
