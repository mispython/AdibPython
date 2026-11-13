#!/usr/bin/env python3
"""
EIBDAWSA - Average Savings Account Analysis
Exact SAS parity (except REPTDATE = now() - 1 day, per operator instruction).
Output dataset and file are lowercase: awsa<MM>.sas7bdat
"""

from __future__ import annotations

import argparse
import gc
import logging
import os
import sys
import time
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd

warnings.filterwarnings('ignore')

import PBMISFMT
import PBBDPFMT


# =============================================================================
# CONFIG
# =============================================================================
@dataclass
class Config:
    input_file: Path = field(default_factory=lambda: Path(os.getenv(
        'EIBDAWSA_INPUT',
        '/host_pq/dp/parquet/year={reptyear}/month={reptmon}/day={reptday}/DPTRBLGS.parquet'
    )))
    output_dir: Path = field(default_factory=lambda: Path(os.getenv(
        'EIBDAWSA_OUTPUT',
        '/host_pq/mis/input/AWSA'
    )))
    log_file: Path = field(default_factory=lambda: Path(os.getenv(
        'EIBDAWSA_LOG',
        '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA/eibdawsa.log'
    )))

    sas_cfgname: str = os.getenv('EIBDAWSA_SAS_CFG', 'default')
    sas_libref: str = 'MISOUT'

    duckdb_memory_limit: str = os.getenv('EIBDAWSA_MEM', '12GB')
    duckdb_threads: int = int(os.getenv('EIBDAWSA_THREADS', '8'))
    duckdb_tmp_dir: Path = Path('/tmp/duckdb_tmp')

    # SAS macro values
    agelimit: int = 12
    maxage: int = 18
    agebelow: int = 11

    # SAS filter constants
    bankno: int = 33
    reptno: int = 1001
    fmtcode: int = 1
    exclude_openind: tuple = ('B', 'C', 'P')
    exclude_products: tuple = (297, 298)

    # Products handled by ALWSA/ALWSS (savings family)
    sa_products: tuple = (204, 207, 215)
    # Product handled by ALWBC (Wadiah Current)
    ca_products: tuple = (93,)

    min_input_rows: int = 1_000


# =============================================================================
# LOGGING
# =============================================================================
def setup_logging(log_file: Path, level: int = logging.INFO) -> logging.Logger:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger('EIBDAWSA')
    logger.setLevel(level)
    logger.handlers.clear()
    logger.propagate = False

    fmt = logging.Formatter('%(asctime)s | %(levelname)-7s | %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

    fh = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger


# =============================================================================
# FORMAT HELPER
# =============================================================================
def _fmt(func, value, default='0'):
    """Call a SAS format function; None/NaN → default."""
    if value is None or (isinstance(value, float) and value != value):
        try:
            return func(None)
        except Exception:
            return default
    return func(value)


# =============================================================================
# AGE + ACCYTD (SAS parity)
# =============================================================================
def calculate_age(bdate_int, reptdate: datetime, cfg: Config) -> int:
    if bdate_int is None or bdate_int == 0 or pd.isna(bdate_int):
        return 0
    try:
        s = str(int(bdate_int)).zfill(6)
        bday, bmonth, byear = int(s[4:6]), int(s[2:4]), int(s[0:2])
        byear = 1900 + byear if byear > 30 else 2000 + byear

        age = reptdate.year - byear
        if (bmonth == reptdate.month and bday > reptdate.day) or bmonth > reptdate.month:
            age -= 1

        if age < cfg.agelimit:
            return cfg.agebelow
        if age >= cfg.maxage:
            return cfg.maxage
        return cfg.agelimit
    except Exception:
        return 0


def accytd_flag(opendt, closedt, reptyear: int) -> int:
    try:
        opened = int(opendt) if opendt is not None and not pd.isna(opendt) else 0
        closed = int(closedt) if closedt is not None and not pd.isna(closedt) else 0
        if opened != 0 and closed == 0:
            yy = int(str(opened).zfill(6)[:2])
            yy = 1900 + yy if yy > 30 else 2000 + yy
            return 1 if yy == reptyear else 0
    except Exception:
        pass
    return 0


# =============================================================================
# EXTRACT
# =============================================================================
def extract(cfg: Config, reptdate: datetime, logger: logging.Logger) -> pd.DataFrame:
    logger.info("Initialising DuckDB (mem=%s, threads=%d)",
                cfg.duckdb_memory_limit, cfg.duckdb_threads)
    cfg.duckdb_tmp_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(':memory:')
    con.execute(f"PRAGMA memory_limit='{cfg.duckdb_memory_limit}'")
    con.execute(f"PRAGMA threads={cfg.duckdb_threads}")
    con.execute(f"PRAGMA temp_directory='{cfg.duckdb_tmp_dir}'")

    try:
        con.execute(
            f"CREATE OR REPLACE VIEW dptrblgs AS "
            f"SELECT * FROM parquet_scan('{cfg.input_file}')"
        )

        cols = con.execute("DESCRIBE dptrblgs").fetchdf()
        available = {c.upper() for c in cols['column_name'].tolist()}
        required = {'BANKNO', 'REPTNO', 'FMTCODE', 'BRANCH', 'ACCTNO', 'CLOSEDT',
                    'OPENDT', 'CUSTCODE', 'PURPOSE', 'OPENIND', 'AVGAMT',
                    'PRODUCT', 'RACE', 'CURBAL', 'BDATE'}
        missing = required - available
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")

        col_map = {c.upper(): c for c in cols['column_name'].tolist()}

        def c(name: str) -> str:
            return f'"{col_map[name.upper()]}"'

        excl_openind = ','.join(f"'{x}'" for x in cfg.exclude_openind)
        excl_prod    = ','.join(str(x) for x in cfg.exclude_products)
        all_prod     = ','.join(str(x) for x in (*cfg.sa_products, *cfg.ca_products))

        sql = f"""
        SELECT
            {c('BRANCH')}   AS BRANCH,
            {c('ACCTNO')}   AS ACCTNO,
            {c('CLOSEDT')}  AS CLOSEDT,
            {c('OPENDT')}   AS OPENDT,
            {c('CUSTCODE')} AS CUSTCODE,
            {c('PURPOSE')}  AS PURPOSE,
            {c('OPENIND')}  AS OPENIND,
            {c('AVGAMT')}   AS AVGAMT,
            {c('PRODUCT')}  AS PRODUCT,
            {c('RACE')}     AS RACE,
            {c('CURBAL')}   AS CURBAL,
            {c('BDATE')}    AS BDATE
        FROM dptrblgs
        WHERE {c('BANKNO')}  = {cfg.bankno}
          AND {c('REPTNO')}  = {cfg.reptno}
          AND {c('FMTCODE')} = {cfg.fmtcode}
          AND UPPER(TRIM(CAST({c('OPENIND')} AS VARCHAR))) NOT IN ({excl_openind})
          AND {c('PRODUCT')} NOT IN ({excl_prod})
          AND {c('PRODUCT')} IN ({all_prod})
        """
        df = con.execute(sql).fetchdf()
    finally:
        con.close()
        gc.collect()

    logger.info("Extracted %d rows (products %s)",
                len(df), sorted((*cfg.sa_products, *cfg.ca_products)))

    if len(df) < cfg.min_input_rows:
        raise ValueError(
            f"Extracted {len(df)} rows, below threshold {cfg.min_input_rows}"
        )

    if df.empty:
        return df

    # Type normalisation
    df['PRODUCT']  = df['PRODUCT'].astype('int64')
    df['CUSTCODE'] = pd.to_numeric(df['CUSTCODE'], errors='coerce').fillna(0).astype('int64')
    df['BRANCH']   = pd.to_numeric(df['BRANCH'],   errors='coerce').fillna(0).astype('int64')
    df['AVGAMT']   = pd.to_numeric(df['AVGAMT'],   errors='coerce').fillna(0.0).astype('float64')
    df['CURBAL']   = pd.to_numeric(df['CURBAL'],   errors='coerce').fillna(0.0).astype('float64')
    return df


# =============================================================================
# ALWSA / ALWSS (products 204, 207, 215)  →  ALWSA1
# =============================================================================
def build_alwsa(df: pd.DataFrame, reptdate: datetime, cfg: Config,
                logger: logging.Logger) -> pd.DataFrame:
    sa = df[df['PRODUCT'].isin(cfg.sa_products)].copy()
    if sa.empty:
        logger.info("ALWSA/ALWSS pass: 0 rows")
        return sa

    logger.info("ALWSA/ALWSS pass: %d rows", len(sa))

    sa['CUSTCD']  = sa['CUSTCODE'].map(lambda v: _fmt(PBBDPFMT.sacustcd_format, v, default='78'))
    sa['PRODCD']  = sa['PRODUCT'].map(lambda v: _fmt(PBBDPFMT.saprod_format, v, default='42120'))
    sa['DENOM']   = sa['PRODUCT'].map(lambda v: _fmt(PBBDPFMT.sadenom_format, v, default='D'))
    sa['STATECD'] = sa['BRANCH'].map(lambda v: _fmt(PBBDPFMT.statecd_format, v, default='B'))
    sa['AVGRNGE'] = sa['AVGAMT'].map(lambda v: _fmt(PBBDPFMT.ddrange_format, v, default='5000001'))
    sa['AGE']     = sa['BDATE'].map(lambda v: calculate_age(v, reptdate, cfg))
    sa['ACCYTD']  = sa.apply(
        lambda r: accytd_flag(r['OPENDT'], r['CLOSEDT'], reptdate.year), axis=1
    )

    # DATA ALWSA1: DEPRANGE = PUT(CURBAL, IWSRNGE.)  — one format, all SA rows
    sa['DEPRANGE'] = sa['CURBAL'].map(lambda v: _fmt(PBMISFMT.IWSRNGE, v, default=' '))

    # DATA ALWSA1: IF PURPOSE=' ' THEN PURPOSE='0'; IF RACE=' ' THEN RACE='0';
    sa['PURPOSE'] = (sa['PURPOSE'].astype(str).str.strip()
                       .replace({'': '0', 'nan': '0', 'None': '0'}))
    sa['RACE']    = (sa['RACE'].astype(str).str.strip()
                       .replace({'': '0', 'nan': '0', 'None': '0'}))
    return sa


# =============================================================================
# ALWBC (product 93)  →  ALWBC1
# =============================================================================
def build_alwbc(df: pd.DataFrame, reptdate: datetime, cfg: Config,
                logger: logging.Logger) -> pd.DataFrame:
    ca = df[df['PRODUCT'].isin(cfg.ca_products)].copy()
    if ca.empty:
        logger.info("ALWBC pass: 0 rows")
        return ca

    logger.info("ALWBC pass: %d rows", len(ca))

    ca['CUSTCD']  = ca['CUSTCODE'].map(lambda v: _fmt(PBBDPFMT.ddcustcd_format, v, default='79'))
    ca['PRODCD']  = ca['PRODUCT'].map(lambda v: _fmt(PBBDPFMT.caprod_format, v, default='42110'))
    ca['DENOM']   = ca['PRODUCT'].map(lambda v: _fmt(PBBDPFMT.cadenom_format, v, default='D'))
    ca['STATECD'] = ca['BRANCH'].map(lambda v: _fmt(PBBDPFMT.statecd_format, v, default='B'))
    ca['AVGRNGE'] = ca['AVGAMT'].map(lambda v: _fmt(PBBDPFMT.ddrange_format, v, default='5000001'))
    ca['AGE']     = ca['BDATE'].map(lambda v: calculate_age(v, reptdate, cfg))
    ca['ACCYTD']  = ca.apply(
        lambda r: accytd_flag(r['OPENDT'], r['CLOSEDT'], reptdate.year), axis=1
    )

    # DATA ALWBC1: DEPRANGE = PUT(CURBAL, IBWSRNGD.)
    ca['DEPRANGE'] = ca['CURBAL'].map(lambda v: _fmt(PBMISFMT.IBWSRNGD, v, default=' '))

    # DATA ALWBC1: IF RACE=' ' THEN RACE='0';   (PURPOSE not defaulted here)
    ca['PURPOSE'] = ca['PURPOSE'].astype(str).str.strip().replace(
        {'': ' ', 'nan': ' ', 'None': ' '}
    )
    ca['RACE'] = ca['RACE'].astype(str).str.strip().replace(
        {'': '0', 'nan': '0', 'None': '0'}
    )
    return ca


# =============================================================================
# AGGREGATE (PROC SUMMARY NWAY)
# =============================================================================
def aggregate(combined: pd.DataFrame, reptdate: datetime,
              logger: logging.Logger) -> pd.DataFrame:
    logger.info("Aggregating %d rows …", len(combined))

    agg = (
        combined.groupby(
            ['PURPOSE', 'RACE', 'CUSTCD', 'AVGRNGE', 'DEPRANGE', 'PRODUCT'],
            dropna=False
        )
        .agg(
            NOACCT  = ('ACCTNO', 'count'),
            CURBAL  = ('CURBAL', 'sum'),
            AVGACCT = ('AVGAMT', 'count'),
            AVGAMT  = ('AVGAMT', 'sum'),
            ACCYTD  = ('ACCYTD', 'sum'),
        )
        .reset_index()
    )

    agg['REPTDATE'] = int(reptdate.strftime('%Y%m%d'))

    denom_lookup  = combined.drop_duplicates('PRODUCT').set_index('PRODUCT')['DENOM'].to_dict()
    prodcd_lookup = combined.drop_duplicates('PRODUCT').set_index('PRODUCT')['PRODCD'].to_dict()
    agg['DENOM']  = agg['PRODUCT'].map(denom_lookup)
    agg['PRODCD'] = agg['PRODUCT'].map(prodcd_lookup)

    for col in ('NOACCT', 'AVGACCT', 'ACCYTD', 'REPTDATE'):
        agg[col] = agg[col].astype('int64')

    logger.info("Aggregated to %d rows", len(agg))
    return agg


# =============================================================================
# RECONCILE
# =============================================================================
def reconcile(raw: pd.DataFrame, agg: pd.DataFrame, logger: logging.Logger) -> None:
    raw_n, raw_b = len(raw), float(raw['CURBAL'].sum())
    agg_n, agg_b = int(agg['NOACCT'].sum()), float(agg['CURBAL'].sum())
    if raw_n != agg_n:
        raise ValueError(f"Row reconciliation: raw={raw_n} agg={agg_n}")
    if abs(raw_b - agg_b) > 0.01:
        raise ValueError(f"Balance reconciliation: raw={raw_b:,.2f} agg={agg_b:,.2f}")
    logger.info("Reconciliation OK: %d accounts, RM %s", agg_n, f"{agg_b:,.2f}")


# =============================================================================
# SAS WRITE
# =============================================================================
def write_sas(df: pd.DataFrame, table_name: str, cfg: Config,
              logger: logging.Logger, append: bool = False) -> Path:
    import saspy

    # All-lowercase member name and filename
    sas_table = table_name.lower()

    logger.info("Connecting to SAS (cfg=%s) …", cfg.sas_cfgname)
    sas = saspy.SASsession(cfgname=cfg.sas_cfgname)

    try:
        sas.saslib(cfg.sas_libref, path=str(cfg.output_dir))
        assigned = [x.upper() for x in sas.assigned_librefs()]
        if cfg.sas_libref.upper() not in assigned:
            raise RuntimeError(
                f"Libref {cfg.sas_libref} not assigned. Present: {assigned}"
            )
        logger.info("Libref %s → %s", cfg.sas_libref, cfg.output_dir)

        if not append:
            logger.info("Ensuring prior %s.%s is removed (if present)",
                        cfg.sas_libref, sas_table)
            del_res = sas.submit(f"""
                proc datasets lib={cfg.sas_libref} nolist;
                    delete {sas_table};
                quit;
            """)
            del_log = del_res.get('LOG', '') if isinstance(del_res, dict) else ''
            if 'ERROR' in del_log.upper():
                logger.warning("PROC DATASETS delete log tail:\n%s", del_log[-1000:])

        logger.info("df2sd → %s.%s (%d rows)",
                    cfg.sas_libref, sas_table, len(df))
        write_res = sas.df2sd(df, table=sas_table, libref=cfg.sas_libref)
        write_log = write_res.get('LOG', '') if isinstance(write_res, dict) else ''
        if 'ERROR' in write_log.upper():
            logger.error("df2sd LOG tail:\n%s", write_log[-2000:])
            raise RuntimeError(
                f"df2sd reported ERROR for {cfg.sas_libref}.{sas_table}"
            )

        if append:
            logger.info("PROC APPEND into %s.%s", cfg.sas_libref, sas_table)
            app_res = sas.submit(f"""
                proc append base={cfg.sas_libref}.{sas_table}
                             data=WORK.{sas_table} FORCE;
                run;
                proc datasets lib=WORK nolist;
                    delete {sas_table};
                quit;
            """)
            app_log = app_res.get('LOG', '') if isinstance(app_res, dict) else ''
            if 'ERROR' in app_log.upper():
                logger.error("PROC APPEND LOG tail:\n%s", app_log[-2000:])
                raise RuntimeError(
                    f"PROC APPEND failed for {cfg.sas_libref}.{sas_table}"
                )

        # SAS-side existence check
        verify = sas.submit(f"""
            data _null_;
                if exist("{cfg.sas_libref}.{sas_table}") then
                    put "EIBDAWSA_EXIST=1";
                else
                    put "EIBDAWSA_EXIST=0";
            run;
        """)
        verify_log = verify.get('LOG', '') if isinstance(verify, dict) else ''
        if 'EIBDAWSA_EXIST=1' not in verify_log:
            logger.error("SAS says %s.%s does NOT exist. LOG tail:\n%s",
                         cfg.sas_libref, sas_table, verify_log[-2000:])
            raise RuntimeError(
                f"SAS reports {cfg.sas_libref}.{sas_table} does NOT exist"
            )
        logger.info("SAS confirms %s.%s exists", cfg.sas_libref, sas_table)

    finally:
        try:
            sas.endsas()
        except Exception:
            pass

    # Filesystem confirmation (Linux lowercase)
    try:
        os.sync()
    except Exception:
        pass
    time.sleep(1.0)

    target = cfg.output_dir / f"{sas_table}.sas7bdat"
    if not target.exists():
        matches = [p for p in cfg.output_dir.iterdir()
                   if p.name.lower() == f"{sas_table}.sas7bdat"]
        if matches:
            target = matches[0]
            logger.info("Found (case-insensitive): %s", target)
        else:
            raise RuntimeError(f"Expected file not on disk: {target}")

    size = target.stat().st_size
    if size == 0:
        raise RuntimeError(f"File on disk is 0 bytes: {target}")

    logger.info("SAS dataset written: %s (%d bytes)", target, size)
    return target


# =============================================================================
# MAIN
# =============================================================================
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='EIBDAWSA — SAS-parity pipeline')
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--append', action='store_true',
                   help='Append instead of replace (SAS PROC APPEND parity)')
    p.add_argument('--verbose', action='store_true')
    return p.parse_args()


def main() -> int:
    args = parse_args()
    cfg = Config()

    # REPTDATE = now() - 1 day (per operator instruction; SAS reads TBDATE,
    # but this run is overridden to use the previous calendar day)
    reptdate = (datetime.now() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    table_name = f"awsa{reptdate.month:02d}"     # lowercase

    logger = setup_logging(cfg.log_file,
                           logging.DEBUG if args.verbose else logging.INFO)
    logger.info("=" * 78)
    logger.info("EIBDAWSA START")
    logger.info("=" * 78)
    logger.info("Report date : %s", reptdate.strftime('%d/%m/%Y'))
    logger.info("Input       : %s", cfg.input_file)
    logger.info("Output dir  : %s", cfg.output_dir)
    logger.info("Target      : %s.%s", cfg.sas_libref, table_name)
    logger.info("Mode        : %s",
                'dry-run' if args.dry_run else 'append' if args.append else 'replace')

    if not cfg.input_file.exists():
        logger.error("Input not found: %s", cfg.input_file)
        return 1

    try:
        raw = extract(cfg, reptdate, logger)
        if raw.empty:
            logger.warning("No rows after filtering")
            return 0

        alwsa = build_alwsa(raw, reptdate, cfg, logger)
        alwbc = build_alwbc(raw, reptdate, cfg, logger)

        combined = pd.concat([alwsa, alwbc], ignore_index=True, sort=False)
        logger.info("Combined ALWSA+ALWBC: %d rows", len(combined))

        agg = aggregate(combined, reptdate, logger)
        reconcile(combined, agg, logger)

        logger.info("-" * 78)
        logger.info("Total accounts     : %d", int(agg['NOACCT'].sum()))
        logger.info("Total balance      : RM %s", f"{agg['CURBAL'].sum():,.2f}")
        logger.info("Total average amt  : RM %s", f"{agg['AVGAMT'].sum():,.2f}")
        logger.info("New accts this year: %d", int(agg['ACCYTD'].sum()))
        for prod, cnt in agg.groupby('PRODUCT')['NOACCT'].sum().items():
            logger.info("  Product %3d      : %d", int(prod), int(cnt))
        logger.info("-" * 78)

        if args.dry_run:
            logger.info("Dry-run — skipping SAS write")
            return 0

        target = write_sas(agg, table_name, cfg, logger, append=args.append)
        logger.info("EIBDAWSA END — success. Output: %s", target)
        return 0

    except MemoryError as e:
        logger.exception("OOM: %s", e)
        return 1
    except Exception as e:
        logger.exception("EIBDAWSA failed: %s", e)
        return 1


if __name__ == '__main__':
    sys.exit(main())
