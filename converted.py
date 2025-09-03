




EIBWBTMS**********************************************************************************

import polars as pl
from pathlib import Path
from datetime import datetime

# ======================================
# 0. CONFIG
# ======================================
BASE_INPUT = Path("data/input")
BASE_OUTPUT = Path("data/output")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

SAVE_INTERMEDIATE = True  # set False to only save final outputs

def out(name: str):
    p = BASE_OUTPUT / f"{name}"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def save(df: pl.DataFrame, name: str):
    df.write_parquet(out(f"{name}.parquet"))
    df.write_csv(out(f"{name}.csv"))

def read_parquet_or_empty(path: Path, schema: dict | None = None) -> pl.DataFrame:
    if path.exists():
        return pl.read_parquet(path)
    return pl.DataFrame(schema=schema) if schema else pl.DataFrame()

def ds(name: str) -> Path:
    """Helper to build a parquet path by dataset name (no library dots)."""
    # e.g. "BTRSA_MAST1508.parquet"
    return BASE_INPUT / f"{name}.parquet"


# ======================================
# 1. MACRO VARS FROM LOAN.REPTDATE
# ======================================
loan_reptdate = read_parquet_or_empty(ds("LOAN_REPTDATE"))
if loan_reptdate.is_empty():
    raise FileNotFoundError("Missing LOAN_REPTDATE parquet.")

reptdate_value = loan_reptdate["REPTDATE"][0]  # expecting a date/datetime type
day_val = reptdate_value.day
month_val = reptdate_value.month
year_val = reptdate_value.year

# SAS logic for SDD/WK/WK1
if day_val == 8:
    SDD, WK, WK1 = 1, "1", "4"
elif day_val == 15:
    SDD, WK, WK1 = 9, "2", "1"
elif day_val == 22:
    SDD, WK, WK1 = 16, "3", "2"
else:
    SDD, WK, WK1 = 23, "4", "3"

# MM1 depends on WK==1
if WK == "1":
    MM1 = month_val - 1 if month_val > 1 else 12
else:
    MM1 = month_val

NOWK     = WK
NOWK1    = WK1
REPTMON  = f"{month_val:02d}"
REPTMON1 = f"{MM1:02d}"
REPTYEAR = f"{year_val:04d}"
REPTDAY  = f"{day_val:02d}"
SDATE    = datetime(year_val, month_val, SDD)
RDATE    = reptdate_value.strftime("%d%m%y")

# ======================================
# 2. FORMATS / LOOKUPS
# ======================================
# $FACX (subset used here for DIRCTIND inference)
FACX_D = {
    "TRX","TRL","TRC","BAX","BAL","FBP","FBD","FBC",
    "DBP","DBD","FTB","ITB","POS","PRE","PCX","PDB",
    "ABL","ABX","FTL","FTI","PFT","MCP","MCT","IEF"
}
FACX_I = {
    "LCX","LCB","LCD","LCL","ALC","SGX","SGC","SGL",
    "BGX","BGF","UMB","APG"
}
# Anything else -> ' '

# $VALID (GLMNEMO validity)
VALID_N = {
    "00620","00560","00530","00640","00660",
    "00120","00140","00160","00590","00470",
    "00490","00440","00420","00340"
}
# 'N' means drop; OTHER = 'Y' (keep)

# GLMNEMO values for INTRECV calc
INTRECV_SET = {"01010","01020","01070"}

# ======================================
# 3. LOAD BTDTLmmW (from previous pipeline)
#    We assume file produced by your earlier job: BTRWH1.BTDTL&REPTMON&NOWK
# ======================================
btdtl_path = ds(f"BTRWH1_BTDTL{REPTMON}{NOWK}")
BTDTL = read_parquet_or_empty(btdtl_path)
if BTDTL.is_empty():
    raise FileNotFoundError(f"Missing {btdtl_path.name}. Produce BTDTL first.")

# Keep only GLMNEMO VALID='Y'
BTDTL = BTDTL.filter(~pl.col("GLMNEMO").cast(pl.Utf8).is_in(list(VALID_N)))
# DIRCTIN1 based on DIRCTIND
BTDTL = BTDTL.with_columns(
    pl.when(pl.col("DIRCTIND") == "D").then(pl.lit("DCURBAL"))
     .when(pl.col("DIRCTIND") == "I").then(pl.lit("ICURBAL"))
     .otherwise(pl.lit(None))
     .alias("DIRCTIN1"),
    # if SUBACCT like _SGL then BTREL = 'OV'
    pl.when(pl.col("SUBACCT").str.slice(1, 3) == "SGL")
      .then(pl.lit("OV"))
      .otherwise(pl.col("BTREL"))
      .alias("BTREL")
)

# ======================================
# 4. DIRECT/INDIRECT CURRENT BALANCE (PROC SUMMARY + TRANSPOSE)
#    WHERE SUBSTR(GLMNEMO,1,2)='00'
# ======================================
DIRECT = (
    BTDTL.filter(pl.col("GLMNEMO").str.slice(0, 2) == "00")
         .groupby(["ACCTNO", "SUBACCT", "BTREL", "DIRCTIN1"])
         .agg(pl.col("GLTRNTOT").sum().alias("GLTRNTOT"))
)

# Pivot DIRCTIN1 to columns DCURBAL / ICURBAL
DIRECT_pivot = (
    DIRECT.pivot(values="GLTRNTOT", index=["ACCTNO","SUBACCT","BTREL"], columns="DIRCTIN1")
          .rename({"DCURBAL": "DCURBAL", "ICURBAL": "ICURBAL"})
)

# ======================================
# 5. INTRECV (PROC SUMMARY WHERE GLMNEMO in INTRECV_SET)
# ======================================
INTRECV = (
    BTDTL.filter(pl.col("GLMNEMO").is_in(list(INTRECV_SET)))
         .groupby(["ACCTNO", "SUBACCT"])
         .agg(pl.col("GLTRNTOT").sum().alias("INTRECV"))
)

# ======================================
# 6. MAST(WIP) = DIRECT + INTRECV  (DBALANCE = DCURBAL + INTRECV)
#    (Rename FICODE->BRANCH like SAS later when we enrich)
# ======================================
MAST_wip = (
    DIRECT_pivot.join(INTRECV, on=["ACCTNO","SUBACCT"], how="outer")
                .with_columns([
                    pl.coalesce([pl.col("DCURBAL"), pl.lit(None)]).alias("DCURBAL"),
                    pl.coalesce([pl.col("ICURBAL"), pl.lit(None)]).alias("ICURBAL"),
                    pl.coalesce([pl.col("INTRECV"), pl.lit(None)]).alias("INTRECV"),
                ])
                .with_columns(
                    pl.sum_horizontal(
                        [pl.col("DCURBAL"), pl.col("INTRECV")]
                    ).alias("DBALANCE")
                )
)

if SAVE_INTERMEDIATE:
    save(MAST_wip, f"_step_mast_wip_{REPTMON}{NOWK}")

# ======================================
# 7. FAC1 / FAC2 split and rollup (as per SAS)
#    FAC1: SUBACCT := BTREL for certain rows, then sum by ACCTNO,SUBACCT,BTREL
# ======================================
FAC2 = MAST_wip  # start; FAC2 keeps original rows unless reclassified

FAC1_cond = (
    (~pl.col("BTREL").str.starts_with(("F","O"))) &
    (pl.col("SUBACCT").str.slice(1,3) != "SGL")
)

FAC1 = (
    MAST_wip.filter(FAC1_cond)
            .with_columns(pl.col("BTREL").alias("SUBACCT"))
            .rename({
                "ICURBAL": "XICURBAL",
                "DCURBAL": "XDCURBAL",
                "DBALANCE":"XBALANCE"
            })
            .groupby(["ACCTNO","SUBACCT","BTREL"])  # BTREL kept as key to match SAS rollup
            .agg([
                pl.col("XICURBAL").sum(),
                pl.col("XDCURBAL").sum(),
                pl.col("XBALANCE").sum(),
                pl.col("INTRECV").sum()
            ])
)

# Rebuild MAST after FAC merge (ICURBAL/DCURBAL/DBALANCE augmented)
MAST_ext = (
    FAC2.join(
        FAC1.rename({"SUBACCT":"SUBACCT_F1"}),  # avoid collision then adjust
        left_on=["ACCTNO","SUBACCT","BTREL"],
        right_on=["ACCTNO","SUBACCT_F1","BTREL"],
        how="left"
    )
    .with_columns([
        pl.coalesce([pl.col("ICURBAL"), pl.lit(0)]) + pl.coalesce([pl.col("XICURBAL"), pl.lit(0)]),
        pl.coalesce([pl.col("DCURBAL"), pl.lit(0)]) + pl.coalesce([pl.col("XDCURBAL"), pl.lit(0)]),
        pl.coalesce([pl.col("DBALANCE"), pl.lit(0)]) + pl.coalesce([pl.col("XBALANCE"), pl.lit(0)])
    ]).rename({
        "literal": "drop_me"  # placeholder for coalesce; polars names them automatically
    }).with_columns([
        pl.col("ICURBAL") + pl.coalesce([pl.col("XICURBAL"), pl.lit(0)]).alias("ICURBAL"),
        pl.col("DCURBAL") + pl.coalesce([pl.col("XDCURBAL"), pl.lit(0)]).alias("DCURBAL"),
        pl.col("DBALANCE") + pl.coalesce([pl.col("XBALANCE"), pl.lit(0)]).alias("DBALANCE")
    ]).drop(["SUBACCT_F1","XICURBAL","XDCURBAL","XBALANCE"])
)

if SAVE_INTERMEDIATE:
    save(MAST_ext, f"_step_mast_ext_{REPTMON}{NOWK}")

# ======================================
# 8. FACFINAL creation (rollup by ACCTNO,BTREL -> _TYPE_==2 => SUBACCT='OV')
#    In SAS this is achieved by a summary then mapping _TYPE_ == 2
# ======================================
FAC_roll = (
    MAST_ext.groupby(["ACCTNO","BTREL"])
            .agg([
                pl.col("ICURBAL").sum(),
                pl.col("DCURBAL").sum(),
                pl.col("DBALANCE").sum(),
                pl.col("INTRECV").sum()
            ])
            .rename({"BTREL":"SUBACCT"})
)
FACFINAL = FAC_roll.with_columns(pl.lit("OV").alias("SUBACCT")).with_columns(
    pl.col("SUBACCT")  # already 'OV'
)

# Merge FACFINAL back into MAST_ext by (ACCTNO,SUBACCT)
MAST_full = (
    MAST_ext.join(
        FACFINAL,
        on=["ACCTNO","SUBACCT"],
        how="outer",
        suffix="_FAC"
    )
    .with_columns([
        pl.coalesce([pl.col("ICURBAL"), pl.col("ICURBAL_FAC")]).alias("ICURBAL"),
        pl.coalesce([pl.col("DCURBAL"), pl.col("DCURBAL_FAC")]).alias("DCURBAL"),
        pl.coalesce([pl.col("DBALANCE"), pl.col("DBALANCE_FAC")]).alias("DBALANCE"),
        pl.coalesce([pl.col("INTRECV"), pl.col("INTRECV_FAC")]).alias("INTRECV"),
    ])
    .drop(["ICURBAL_FAC","DCURBAL_FAC","DBALANCE_FAC","INTRECV_FAC"])
)

# ======================================
# 9. Enrich with BTRSA.MASTddmm (cust info etc.)
# ======================================
MAST_src = read_parquet_or_empty(ds(f"BTRSA_MAST{REPTDAY}{REPTMON}"))
# In SAS: (DROP=ACCTNO RENAME=(ACCTNOX=ACCTNO CUSTCODE=CUSTCOD1)) then CUSTCODE=CUSTCOD1
# We’ll assume Parquet already has ACCTNO and CUSTCODE clean; if you have ACCTNOX, adjust:

# Final join by ACCTNO (kept minimal; extend columns if needed)
MAST_joined = (
    MAST_full.join(MAST_src, on="ACCTNO", how="left", suffix="_SRC")
)

if SAVE_INTERMEDIATE:
    save(MAST_joined, f"_step_mast_joined_{REPTMON}{NOWK}")

# ======================================
# 10. COLLATER (BTCOLL.COLLATER) — restrict ACCTNO range & attach CCLASSC
# ======================================
collater = read_parquet_or_empty(ds("BTCOLL_COLLATER"))
if not collater.is_empty():
    collater = (
        collater.rename({"ACCTNO": "ACCTNO2"})
                .with_columns(pl.col("ACCTNO2").cast(pl.Int64))
                .filter((pl.col("ACCTNO2") > 2500000000) & (pl.col("ACCTNO2") < 2600000000))
                .with_columns(pl.col("ACCTNO2").alias("ACCTNO"))
                .select(["ACCTNO","CCLASSC"])
                .rename({"CCLASSC":"COLLATER"})
                .unique(subset=["ACCTNO"])
    )
    MAST_joined = MAST_joined.join(collater, on="ACCTNO", how="left")

# ======================================
# 11. SUBA warehouse prep (BTRSA.SUBA ddmm)
# ======================================
SUBA_raw = read_parquet_or_empty(ds(f"BTRSA_SUBA{REPTDAY}{REPTMON}"))

# SUBA1: sort by ACCTNO,SUBACCT,SINDICAT, DESC CREATDS; then dedup
SUBA1 = (
    SUBA_raw.sort(by=["ACCTNO","SUBACCT","SINDICAT","CREATDS"], descending=[False, False, False, True])
            .unique(subset=["ACCTNO","SUBACCT"], keep="first")
)

# SUBA2: sort by ACCTNO,SUBACCT,SINDICAT, CREATDS; then dedup (keep earliest)
SUBA2 = (
    SUBA_raw.sort(by=["ACCTNO","SUBACCT","SINDICAT","CREATDS"])
            .select(["ACCTNO","SUBACCT","CREATDS","EXPIRDS"])
            .unique(subset=["ACCTNO","SUBACCT"], keep="first")
)

SUBA = SUBA1.join(SUBA2, on=["ACCTNO","SUBACCT"], how="left")

# ======================================
# 12. Build hierarchy MAST (FACIL/PARENT/LEVELS/LEVEL1..LEVEL5)
#     Mirrors the long SAS DATA step logic
# ======================================
def derive_hierarchy(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns([
        pl.lit("E").alias("IND"),
        pl.when((pl.col("BTREL").str.strip() == "") & (pl.col("SUBACCT").str.strip() == "OV"))
          .then(pl.struct(facil=pl.lit("OV"), parent=pl.lit("NIL"), ind=pl.lit("A")))
          .otherwise(pl.struct(
              facil=pl.when(pl.col("SUBACCT").str.lengths() >= 5)
                         .then(pl.col("SUBACCT").str.slice(0,5))
                         .otherwise(pl.col("SUBACCT")),
              parent=pl.col("BTREL"),
              ind=pl.lit("E")
          ))
          .alias("H")
    ]).with_columns([
        pl.col("H").struct.field("facil").alias("FACIL"),
        pl.col("H").struct.field("parent").alias("PARENT"),
        pl.col("H").struct.field("ind").alias("IND")
    ]).drop(["H"])

    # Overrides from SAS:
    df = df.with_columns([
        pl.when(pl.col("PARENT").str.strip() == "")
          .then(pl.lit("OV"))
          .otherwise(pl.col("PARENT")).alias("LEVEL1"),
        pl.col("FACIL").alias("KEYFACI")
    ])

    # Build levels iteratively (LEVEL2..LEVEL5): simplified
    # In SAS %LEVEL macro propagates parent/child paths.
    # Here we approximate: LEVEL2..LEVEL5 default to PARENT or FACIL as needed.
    df = df.with_columns([
        pl.when(pl.col("SUBACCT").str.slice(1,3) == "SGL")
          .then(pl.lit("SGL"))
          .otherwise(pl.lit(None)).alias("LEVEL2"),
        pl.lit(None).alias("LEVEL3"),
        pl.lit(None).alias("LEVEL4"),
        pl.lit(None).alias("LEVEL5"),
        pl.lit(2).alias("LEVELS")  # OV->SGL case forces 2; else we keep 2 as baseline
    ])

    # Normalization like SAS removing leading group digits on LEVEL2..5
    def normalize_level(col):
        return pl.when(pl.col(col).is_not_null() & pl.col(col).str.lengths() > 0)
                 .then(
                     pl.when(pl.col(col).str.slice(0,1).is_in([str(i) for i in range(1,10)]))
                       .then(pl.col(col).str.slice(1,3))
                       .otherwise(pl.col(col))
                 ).otherwise(pl.col(col))

    for c in ["LEVEL2","LEVEL3","LEVEL4","LEVEL5"]:
        df = df.with_columns(normalize_level(c).alias(c))

    return df

MAST_h = derive_hierarchy(SUBA)

# Merge hierarchy into MAST_joined by ACCTNO,SUBACCT
MAST_merged = (
    MAST_joined.join(
        MAST_h.select(["ACCTNO","SUBACCT","LEVELS","LEVEL1","LEVEL2","LEVEL3","LEVEL4","LEVEL5","KEYFACI","IND"]),
        on=["ACCTNO","SUBACCT"], how="left"
    )
)

# ======================================
# 13. DIRECT flags (D/I) by SUBA (DIRCTIND across rows)
# ======================================
# From BTRSA.SUBA… (ACCTNOX to ACCTNO already assumed aligned)
DIRECTX = SUBA_raw.select(["ACCTNO","SUBACCT","DIRCTIND"])
# collapse to one row per ACCTNO,SUBACCT with flags D/I
DIRECT_flags = (
    DIRECTX.groupby(["ACCTNO","SUBACCT"])
           .agg([
               (pl.col("DIRCTIND") == "D").any().cast(pl.Int8).alias("D"),
               (pl.col("DIRCTIND") == "I").any().cast(pl.Int8).alias("I"),
           ])
)

# ======================================
# 14. BTMAST assemble (final)
#     MIRRORS the long SAS enrichment block (simplified where external formats used)
# ======================================
BTMAST = (
    MAST_merged.join(DIRECT_flags, on=["ACCTNO","SUBACCT"], how="left")
               .with_columns([
                   # DIRCTIND derived from SUBACCT(2..4) against $FACX
                   pl.when(pl.col("SUBACCT").str.slice(1,3).is_in(list(FACX_D)))
                     .then(pl.lit("D"))
                     .when(pl.col("SUBACCT").str.slice(1,3).is_in(list(FACX_I)))
                     .then(pl.lit("I"))
                     .otherwise(pl.lit(""))
                     .alias("DIRCTIND"),
               ])
)

# Derive DCURBALX/ICURBALX with negatives to zero + null out by flags (D/I)
BTMAST = BTMAST.with_columns([
    pl.when(pl.col("DCURBAL").is_not_null() & (pl.col("DCURBAL") < 0)).then(pl.lit(0)).otherwise(pl.col("DCURBAL")).alias("DCURBALX"),
    pl.when(pl.col("ICURBAL").is_not_null() & (pl.col("ICURBAL") < 0)).then(pl.lit(0)).otherwise(pl.col("ICURBAL")).alias("ICURBALX"),
]).with_columns([
    pl.when(pl.col("I").is_null()).then(pl.col("ICURBALX"))
      .when(pl.col("I") == 1).then(pl.col("ICURBALX"))
      .otherwise(pl.lit(None)).alias("ICURBALX"),
    pl.when(pl.col("D").is_null()).then(pl.col("DCURBALX"))
      .when(pl.col("D") == 1).then(pl.col("DCURBALX"))
      .otherwise(pl.lit(None)).alias("DCURBALX"),
])

# DUNDRAWN / IUNDRAWN
BTMAST = BTMAST.with_columns([
    pl.when(pl.col("DCURBALX").is_not_null() & pl.col("ICURBALX").is_not_null())
      .then(pl.col("APPRLIMT") - pl.col("DCURBALX") - pl.col("ICURBALX"))
      .when(pl.col("DCURBALX").is_not_null())
      .then(pl.col("APPRLIMT") - pl.col("DCURBALX"))
      .otherwise(pl.lit(None))
      .alias("DUNDRAWN"),
    (pl.col("APPRLIMT") - pl.col("ICURBALX")).alias("IUNDRAWN"),
])

# ORIGMT by tenor
BTMAST = BTMAST.with_columns(
    pl.when((pl.col("EXPRDATE").is_not_null()) & (pl.col("ISSDTE").is_not_null()) & ((pl.col("EXPRDATE") - pl.col("ISSDTE")).dt.days() < 366))
      .then(pl.lit("10"))
      .otherwise(pl.lit("20"))
      .alias("ORIGMT")
)

# Sector code logic (requires external formats in SAS; here we keep what’s available)
# If you have lookup tables for $SECTCD, $RVRSE, $INDSECT translate them here with joins.

# SINDICAT handling (zero-out balances if 'S')
BTMAST = BTMAST.with_columns(
    pl.when(pl.col("SINDICAT") == "S")
      .then(None)
      .otherwise(pl.col("DBALANCE")).alias("DBALANCE")
).with_columns([
    pl.when(pl.col("SINDICAT") == "S").then(None).otherwise(pl.col("DCURBAL")).alias("DCURBAL"),
    pl.when(pl.col("SINDICAT") == "S").then(None).otherwise(pl.col("DCURBALX")).alias("DCURBALX"),
    pl.when(pl.col("SINDICAT") == "S").then(None).otherwise(pl.col("INTRECV")).alias("INTRECV"),
    pl.when(pl.col("SINDICAT") == "S").then(None).otherwise(pl.col("ICURBAL")).alias("ICURBAL"),
    pl.when(pl.col("SINDICAT") == "S").then(None).otherwise(pl.col("ICURBALX")).alias("ICURBALX"),
    pl.when(pl.col("SINDICAT") == "S").then(None).otherwise(pl.col("APPRLIMT")).alias("APPRLIMT"),
    pl.when(pl.col("SINDICAT") == "S").then(None).otherwise(pl.col("DUNDRAWN")).alias("DUNDRAWN"),
    pl.when(pl.col("SINDICAT") == "S").then(None).otherwise(pl.col("IUNDRAWN")).alias("IUNDRAWN"),
]).with_columns(
    pl.lit("D").alias("AMTIND")  # as SAS sets AMTIND='D'
)

# Deduplicate by ACCTNO,SUBACCT (like PROC SORT NODUPKEYS + DUPOUT handling)
BTMAST = BTMAST.unique(subset=["ACCTNO","SUBACCT"], keep="first")

# ======================================
# 15. SAVE OUTPUTS
# ======================================
save(BTMAST, f"BTRWH1_BTMAST{REPTMON}{NOWK}")
save(MAST_joined, f"BTRWH1_MAST{REPTMON}{NOWK}")  # useful intermediate
save(BTDTL, f"BTRWH1_BTDTL{REPTMON}{NOWK}_validated")  # optional snapshot

print(f" Completed BTMAST build for report date {RDATE} (REPTMON={REPTMON}, NOWK={NOWK})")
