#!/usr/bin/env python3
"""
Program: EIAWOF14
Purpose: Select WIIS/WSP2/WAQ from NPL and append WOFF transactions into WOFFTOT.

SAS parity implemented:
1) Copy NPL.WIIS, NPL.WSP2, NPL.WAQ -> NPL1 library.
2) Read fixed-width WMIS file and build WOFF dataset.
3) Apply SAS filtering logic and SPWOFF overwrite.
4) Append into NPL1.WOFFTOT and de-duplicate by ACCTNO/NOTENO.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable

import pandas as pd
import pyreadstat
import saspy


# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_PATH = Path(".")
INPUT_PATH = BASE_PATH / "sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIAWOF14"
OUTPUT_PATH = BASE_PATH / "sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIAWOF14"

NPL_INPUT_PATH = INPUT_PATH / "npl"
NPL1_OUTPUT_PATH = OUTPUT_PATH / "npl1"
WMIS_INPUT_PATH = INPUT_PATH / "wmis.txt"

WIIS_INPUT = NPL_INPUT_PATH / "wiis.sas7bdat"
WSP2_INPUT = NPL_INPUT_PATH / "wsp2.sas7bdat"
WAQ_INPUT = NPL_INPUT_PATH / "waq.sas7bdat"
WOFFTOT_OUTPUT_PARQUET = NPL1_OUTPUT_PATH / "WOFFTOT.parquet"
WOFFTOT_OUTPUT_SAS = NPL1_OUTPUT_PATH / "WOFFTOT.sas7bdat"

NPL1_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# Initialize SAS session
sas = saspy.SASsession()


@dataclass(frozen=True)
class FieldSpec:
    name: str
    start: int  # 1-based, inclusive (SAS column pointer semantics)
    width: int


WMIS_LAYOUT: tuple[FieldSpec, ...] = (
    FieldSpec("ACCTNO", 142, 10),
    FieldSpec("NOTENO", 152, 5),
    FieldSpec("IISWOFF", 162, 16),
    FieldSpec("SPWOFF", 178, 16),
    FieldSpec("DDWOFF", 210, 2),
    FieldSpec("MMWOFF", 213, 2),
    FieldSpec("YYWOFF", 216, 4),
    FieldSpec("CAPBAL", 220, 16),
    FieldSpec("COSTCTR", 236, 4),
)


def read_sas7bdat(file_path: Path) -> pd.DataFrame:
    """Read SAS7BDAT file using pyreadstat."""
    if not file_path.exists():
        raise FileNotFoundError(f"Required input SAS dataset not found: {file_path}")
    
    df, meta = pyreadstat.read_sas7bdat(str(file_path))
    
    # Remove REPTDATE if it exists and replace with current date - 1
    if 'REPTDATE' in df.columns:
        df = df.drop(columns=['REPTDATE'])
    
    # Add REPTDATE as current date minus 1 day
    df['REPTDATE'] = datetime.now() - timedelta(days=1)
    
    return df


def write_sas7bdat(df: pd.DataFrame, file_path: Path, table_name: str = None) -> None:
    """Write DataFrame to SAS7BDAT using saspy."""
    # Upload DataFrame to SAS
    sas_df = sas.df2sd(df, table_name or file_path.stem)
    
    # Write to SAS7BDAT using SAS
    sas_code = f"""
    LIBNAME outlib "{str(file_path.parent)}";
    DATA outlib.{file_path.stem};
        SET {table_name or file_path.stem};
    RUN;
    """
    sas.submit(sas_code)


def copy_base_tables() -> None:
    """Copy WIIS/WSP2/WAQ from NPL input to NPL1 output."""
    copies = {
        WIIS_INPUT: "WIIS",
        WSP2_INPUT: "WSP2",
        WAQ_INPUT: "WAQ",
    }

    for src, table_name in copies.items():
        # Read with pyreadstat
        df = read_sas7bdat(src)
        
        # Write as both parquet and SAS7BDAT
        parquet_path = NPL1_OUTPUT_PATH / f"{table_name}.parquet"
        sas_path = NPL1_OUTPUT_PATH / f"{table_name}.sas7bdat"
        
        df.to_parquet(parquet_path)
        write_sas7bdat(df, sas_path, table_name)


def _slice_text(line: str, start: int, width: int) -> str:
    """Return fixed-width field by 1-based start and width."""
    zero_based = start - 1
    return line[zero_based : zero_based + width]


def _to_int(value: str) -> int | None:
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _to_decimal(value: str) -> Decimal | None:
    value = value.strip()
    if not value:
        return None
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError):
        return None


def parse_wmis_records(lines: Iterable[str]) -> list[dict]:
    """Parse fixed-width WMIS lines into SAS-equivalent WOFF rows."""
    records: list[dict] = []

    for line in lines:
        parsed = {spec.name: _slice_text(line, spec.start, spec.width) for spec in WMIS_LAYOUT}

        acctno = _to_int(parsed["ACCTNO"])
        noteno = _to_int(parsed["NOTENO"])
        iiswoff = _to_decimal(parsed["IISWOFF"])
        ddwoff = _to_int(parsed["DDWOFF"])
        mmwoff = _to_int(parsed["MMWOFF"])
        yywoff = _to_int(parsed["YYWOFF"])
        capbal = _to_decimal(parsed["CAPBAL"])
        costctr = _to_int(parsed["COSTCTR"])

        if costctr is None:
            continue

        if (3000 <= costctr <= 3999) or costctr in {4043, 4048}:
            continue

        woffdt = f"{(mmwoff or 0):02d}/{(yywoff or 0):04d}"

        records.append(
            {
                "ACCTNO": acctno,
                "NOTENO": noteno,
                "IISWOFF": iiswoff,
                "SPWOFF": capbal,  # SAS: SPWOFF=CAPBAL;
                "WOFFDT": woffdt,
                "CAPBAL": capbal,
            }
        )

    return records


def build_woff_from_wmis() -> pd.DataFrame:
    """Create WOFF dataframe from raw WMIS fixed-width input."""
    if not WMIS_INPUT_PATH.exists():
        raise FileNotFoundError(f"Required WMIS input not found: {WMIS_INPUT_PATH}")

    with WMIS_INPUT_PATH.open("r", encoding="latin-1") as f:
        records = parse_wmis_records(f)

    if not records:
        return pd.DataFrame(columns=["ACCTNO", "NOTENO", "IISWOFF", "SPWOFF", "WOFFDT", "CAPBAL"])

    df = pd.DataFrame(records)
    
    # Convert decimal columns to float for SAS compatibility
    for col in ["IISWOFF", "SPWOFF", "CAPBAL"]:
        df[col] = df[col].apply(lambda x: float(x) if x is not None else None)
    
    return df


def append_and_deduplicate_wofftot(woff: pd.DataFrame) -> pd.DataFrame:
    """Append WOFF to WOFFTOT then deduplicate on ACCTNO/NOTENO."""
    if WOFFTOT_OUTPUT_PARQUET.exists():
        existing = pd.read_parquet(WOFFTOT_OUTPUT_PARQUET)
        combined = pd.concat([existing, woff], ignore_index=True)
    else:
        combined = woff

    # Sort and deduplicate
    deduped = (
        combined
        .sort_values(["ACCTNO", "NOTENO"])
        .drop_duplicates(subset=["ACCTNO", "NOTENO"], keep="first")
        .reset_index(drop=True)
    )

    # Write outputs
    deduped.to_parquet(WOFFTOT_OUTPUT_PARQUET)
    write_sas7bdat(deduped, WOFFTOT_OUTPUT_SAS, "WOFFTOT")
    
    return deduped


def main() -> None:
    try:
        copy_base_tables()
        woff = build_woff_from_wmis()
        result = append_and_deduplicate_wofftot(woff)
        print(f"Successfully processed {len(result)} records")
    finally:
        # Clean up SAS session
        sas.endsas()


if __name__ == "__main__":
    main()
