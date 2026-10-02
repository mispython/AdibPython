#!/usr/bin/env python3
from __future__ import annotations

"""
Program  : PBBWRDLF
Purpose  : Weekly ITCODE reference list -> PBBRDAL.sas7bdat + .txt
"""

from pathlib import Path

import pandas as pd

from bnm_io import write_sas_and_txt


OUTPUT_DIR  = Path(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output"
)
OUTPUT_BASE = "PBBRDAL"


ITCODE_DATA = [
    "3313002000000Y", "3313003000000Y", "4017000000000Y", "4019000000000Y",
    "4216060000000Y", "4261076000000Y", "4261085000000Y", "4263076000000Y",
    "4263085000000Y", "4269981000000Y", "4313002000000Y", "4313003000000Y",
    "5422000000000Y", "7200000008310Y", "7300000003000Y", "7300000006100Y",
    "7300000008310Y", "7300000008320Y",
]


def build() -> Path:
    if not ITCODE_DATA:
        raise ValueError("PBBWRDLF.ITCODE_DATA is empty.")

    df = pd.DataFrame({
        "itcode": ITCODE_DATA,
        "amount": [0.0] * len(ITCODE_DATA),
    })

    write_sas_and_txt(df, OUTPUT_DIR, OUTPUT_BASE, verbose=False)
    print(f"PBBWRDLF: wrote {OUTPUT_DIR / (OUTPUT_BASE + '.sas7bdat')} "
          f"({len(df)} records)")
    return OUTPUT_DIR / f"{OUTPUT_BASE}.sas7bdat"


build()


if __name__ == '__main__':
    pass
