"""
FIX for: polars.exceptions.ColumnNotFoundError: unable to find column "OPENDT"
Root cause: the original script builds column lists via set-intersection
(DEPO vs IDEPO per product, then again across SAVING/CURRENT/FD). If ANY one
source file is missing/renames a column, it silently disappears from ALL
downstream frames, even ones that do have it. align_to_schema() below fixes
that by adding missing columns as nulls instead of dropping shared ones.

---
Drop-in replacement for the LN_NOTE / LOAN section of the pipeline.

Why this is faster and won't crash:
  - pyreadstat.read_sas7bdat() reads ALL columns into memory before you can
    filter or select anything. For a 20GB / 6M-row file that's the crash.
  - usecols=[...] tells the underlying reader to only materialize the columns
    you actually need (VINNO, ESCRACCT), which for a wide loan-note table can
    cut bytes read by 10-50x on its own.
  - read_file_in_chunks() streams the file in row-batches (generator), so you
    filter (VINNO != "") and drop each batch immediately. Peak memory becomes
    "one chunk" + "accumulated filtered rows" instead of "whole file".
"""

from __future__ import annotations
import gc
import multiprocessing
import polars as pl
import pyreadstat


def read_sas_chunked(
    path,
    usecols: list[str] | None = None,
    chunksize: int = 500_000,
    filter_expr: pl.Expr | None = None,
) -> pl.DataFrame:
    """Stream a large .sas7bdat in row-chunks, selecting only usecols and
    applying filter_expr per chunk so memory stays bounded."""
    chunks: list[pl.DataFrame] = []
    reader = pyreadstat.read_file_in_chunks(
        pyreadstat.read_sas7bdat,
        str(path),
        chunksize=chunksize,
        usecols=usecols,
    )
    for df_chunk, _meta in reader:
        pl_chunk = pl.from_pandas(df_chunk)
        if filter_expr is not None:
            pl_chunk = pl_chunk.filter(filter_expr)
        if pl_chunk.height:
            chunks.append(pl_chunk)
        del df_chunk, pl_chunk
    gc.collect()
    return pl.concat(chunks, how="vertical", rechunk=True) if chunks else pl.DataFrame(schema=usecols)


def read_sas_parallel(
    path,
    usecols: list[str] | None = None,
    num_processes: int | None = None,
) -> pl.DataFrame:
    """Alternative: parallel read across CPU cores. Use when the file fits in
    RAM once column-pruned, and you want wall-clock speed rather than a hard
    memory ceiling. Don't combine both chunked+parallel unless you also
    filter afterwards, since this one materializes the full (pruned) result
    at once."""
    df, _meta = pyreadstat.read_file_multiprocessing(
        pyreadstat.read_sas7bdat,
        str(path),
        usecols=usecols,
        num_processes=num_processes or multiprocessing.cpu_count(),
    )
    return pl.from_pandas(df)


def align_to_schema(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    """Select `cols` from df, adding any missing ones as typed nulls instead
    of silently dropping columns that OTHER frames in the union still have.
    This replaces the "find common columns" pattern that caused the
    ColumnNotFoundError -- one source missing a column no longer wipes it
    out for everyone."""
    exprs = []
    for c in cols:
        if c in df.columns:
            exprs.append(pl.col(c))
        else:
            exprs.append(pl.lit(None).alias(c))
    return df.select(exprs)


# ---- Replace the SAVING/CURRENT/FD column-selection logic like this ----
#
# Instead of:
#   common_saving_cols = [c for c in COMMON_COLS if c in SAVING_DEPO.columns and c in SAVING_IDEPO.columns]
#   SAVING_DEPO = SAVING_DEPO.select(common_saving_cols)
#   SAVING_IDEPO = SAVING_IDEPO.select(common_saving_cols)
#
# Do:
#   SAVING_DEPO  = align_to_schema(SAVING_DEPO,  COMMON_COLS)
#   SAVING_IDEPO = align_to_schema(SAVING_IDEPO, COMMON_COLS)
#   (repeat for CURRENT_DEPO/IDEPO and FD_DEPO/IDEPO)
#
# Then SAVING, CURRENT, FD all guaranteed to have every column in COMMON_COLS
# (nulls where a source lacked it) -- no more surprise drops in the later
# "common_deposit_cols = set(SAVING.columns) & set(CURRENT.columns) & ..."
# step either; you can just union COMMON_COLS + key_cols directly there too:
#
#   common_deposit_cols = key_cols + [c for c in COMMON_COLS if c not in key_cols]
#   SAVING  = align_to_schema(SAVING,  common_deposit_cols)
#   CURRENT = align_to_schema(CURRENT, common_deposit_cols)
#   FD      = align_to_schema(FD,      common_deposit_cols)


# ---- replace the original LOAN block with this ----
LN_NOTE_COLS = ["VINNO", "ESCRACCT"]

def load_loan(ln_note_path, iln_note_path, chunksize: int = 500_000) -> pl.DataFrame:
    conv_loan = read_sas_chunked(
        ln_note_path,
        usecols=LN_NOTE_COLS,
        chunksize=chunksize,
        filter_expr=pl.col("VINNO") != "",
    )
    islamic_loan = read_sas_chunked(
        iln_note_path,
        usecols=LN_NOTE_COLS,
        chunksize=chunksize,
        filter_expr=pl.col("VINNO") != "",
    )
    return (
        pl.concat([conv_loan, islamic_loan], how="vertical", rechunk=True)
        .rename({"VINNO": "AANO"})
        .unique(subset=["AANO"], keep="first")
    )

# usage in the main script:
# LOAN = load_loan(LN_NOTE, ILN_NOTE)
# EXTCRMA = EXTCRMA.join(LOAN, on="AANO", how="left")
