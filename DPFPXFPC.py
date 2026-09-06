from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
import polars as pl
import pyreadstat
import saspy
import os
import gc
import logging
import sys
from typing import Optional, Set
import time


# =========================
# Configuration
# =========================
class Config:
    """Configuration settings for the ETL process."""
    
    # Paths
    BASE_INPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS")
    BASE_OUTPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRCGCS")
    
    # File names
    MNITB_CURRENT_PATTERN = "intg_dp_acct_current_m{reptmon}.sas7bdat"
    MNILN_LNNOTE_PATTERN = "enrh_ln_note_m{reptmon}.sas7bdat"
    CRFTABL_NAME = "crftabl.txt"
    MAST_PATTERN = "btmast{reptmon}{nowk}{reptyear2}.sas7bdat"
    COLL_PATTERN = "LCCRISEX_{year}{month}{day}"
    DESC_PATTERN = "LCCRISEX_DESC_{year}{month}{day}"
    
    # Record lengths
    COLL_RECORD_LENGTHS = [151, 152, 160, 200, 256, 320, 400, 512, 1024]
    DESC_RECORD_LENGTH = 220
    
    # Processing
    CHUNK_SIZE = 100000
    DESC_CENSUS_MIN = 51000000
    DESC_CENSUS_MAX = 1099999999
    
    # SAS
    SAS_CONFIG = 'default'
    SAS_OUTPUT_LIB = 'outlib'
    SAS_OUTPUT_DATASET = 'npgsexcp'
    
    # Logging
    LOG_LEVEL = logging.INFO
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


# =========================
# Logging Setup
# =========================
def setup_logging():
    """Configure logging for the ETL process."""
    logging.basicConfig(
        level=Config.LOG_LEVEL,
        format=Config.LOG_FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('eibrcgcs_etl.log')
        ]
    )
    return logging.getLogger(__name__)


logger = setup_logging()


# =========================
# EBCDIC Translation
# =========================
EBCDIC_TO_ASCII = {
    0xF0: '0', 0xF1: '1', 0xF2: '2', 0xF3: '3', 0xF4: '4',
    0xF5: '5', 0xF6: '6', 0xF7: '7', 0xF8: '8', 0xF9: '9',
    0xC1: 'A', 0xC2: 'B', 0xC3: 'C', 0xC4: 'D', 0xC5: 'E',
    0xC6: 'F', 0xC7: 'G', 0xC8: 'H', 0xC9: 'I', 0xD1: 'J',
    0xD2: 'K', 0xD3: 'L', 0xD4: 'M', 0xD5: 'N', 0xD6: 'O',
    0xD7: 'P', 0xD8: 'Q', 0xD9: 'R', 0xE2: 'S', 0xE3: 'T',
    0xE4: 'U', 0xE5: 'V', 0xE6: 'W', 0xE7: 'X', 0xE8: 'Y',
    0xE9: 'Z', 0x40: ' ', 0x4B: '.', 0x6B: ',', 0x5A: '!',
    0x7A: ':', 0x7B: '#', 0x7C: '@', 0x6D: '_', 0x4E: '+',
    0x60: '-', 0x61: '/', 0x6C: '%', 0x5C: '*', 0x7D: "'",
    0x7E: '=', 0x4A: '[', 0x5B: ']', 0x6A: '|', 0x7F: '"',
}


# =========================
# Helper Functions
# =========================
def calculate_week_of_month(date_obj: datetime) -> int:
    """
    Calculate week of month:
    Week 1: days 1-8, Week 2: days 9-15, Week 3: days 16-22, Week 4: days 23-end
    """
    day = date_obj.day
    if day <= 8:
        return 1
    elif day <= 15:
        return 2
    elif day <= 22:
        return 3
    else:
        return 4


def ebcdic_to_ascii(byte_data: bytes) -> str:
    """Convert EBCDIC bytes to ASCII string."""
    result = []
    for byte in byte_data:
        result.append(EBCDIC_TO_ASCII.get(byte, ' '))
    return ''.join(result).strip()


def ebcdic_bytes_to_int(byte_data: bytes) -> int:
    """Convert EBCDIC numeric bytes to integer."""
    ascii_str = ebcdic_to_ascii(byte_data)
    try:
        return int(ascii_str) if ascii_str else 0
    except ValueError:
        return 0


def unpack_packed_decimal(data: bytes, scale: int = 0) -> int:
    """Unpack a packed decimal (COMP-3) field."""
    if not data:
        return 0
    
    hex_str = data.hex().upper()
    nibbles = list(hex_str)
    sign_nibble = nibbles[-1] if nibbles else 'F'
    digit_nibbles = nibbles[:-1]
    
    if digit_nibbles:
        try:
            value = int(''.join(digit_nibbles))
        except ValueError:
            value = 0
    else:
        value = 0
    
    if sign_nibble in ['D', 'B']:
        value = -value
    
    if scale > 0:
        value = value / (10 ** scale)
    
    return value


# =========================
# File Readers
# =========================
def read_sas7bdat(file_path: Path) -> pl.DataFrame:
    """Read SAS7BDAT file with error handling."""
    try:
        logger.info(f"Reading {file_path.name}...")
        start_time = time.time()
        
        df, meta = pyreadstat.read_sas7bdat(str(file_path))
        
        pl_df = pl.from_pandas(df)
        pl_df = pl_df.rename({col: col.lower() for col in pl_df.columns})
        
        # Convert acctno to Int64
        if 'acctno' in pl_df.columns:
            pl_df = pl_df.with_columns([
                pl.col('acctno').cast(pl.Int64, strict=False).alias('acctno')
            ])
        
        elapsed = time.time() - start_time
        logger.info(f"Read {pl_df.height} rows from {file_path.name} in {elapsed:.2f}s")
        
        return pl_df
    
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        raise


def read_sas7bdat_filtered(file_path: Path, acctno_filter: Set[int]) -> pl.DataFrame:
    """Read SAS7BDAT file in chunks and filter for specific account numbers."""
    try:
        logger.info(f"Reading {file_path.name} with filter ({len(acctno_filter)} accounts)...")
        start_time = time.time()
        
        # Get metadata
        _, meta = pyreadstat.read_sas7bdat(str(file_path), metadataonly=True)
        total_rows = meta.number_rows
        logger.info(f"Total rows in {file_path.name}: {total_rows}")
        
        chunks = []
        offset = 0
        chunk_size = Config.CHUNK_SIZE
        
        while offset < total_rows:
            df_chunk, _ = pyreadstat.read_sas7bdat(
                str(file_path),
                row_offset=offset,
                row_limit=min(chunk_size, total_rows - offset)
            )
            
            pl_chunk = pl.from_pandas(df_chunk)
            pl_chunk = pl_chunk.rename({col: col.lower() for col in pl_chunk.columns})
            
            if 'acctno' in pl_chunk.columns:
                pl_chunk = pl_chunk.with_columns([
                    pl.col('acctno').cast(pl.Int64, strict=False).alias('acctno')
                ])
                pl_chunk = pl_chunk.filter(pl.col('acctno').is_in(acctno_filter))
                
                if pl_chunk.height > 0:
                    chunks.append(pl_chunk)
            
            offset += chunk_size
            
            if offset % 1000000 == 0:
                logger.info(f"Processed {offset}/{total_rows} rows...")
        
        if chunks:
            result = pl.concat(chunks, how="vertical")
        else:
            result = pl.DataFrame()
        
        elapsed = time.time() - start_time
        logger.info(f"Read {result.height} matching rows from {file_path.name} in {elapsed:.2f}s")
        
        return result
    
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        raise


def read_crftabl(file_path: Path) -> pl.DataFrame:
    """Read CRFTABL fixed-width text file."""
    try:
        logger.info(f"Reading {file_path.name}...")
        start_time = time.time()
        
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        parsed_data = []
        for line in lines:
            if not line.strip():
                continue
            if len(line) < 386:
                line = line.rstrip('\n').ljust(386)
            
            rectyp1 = line[0:1].strip()
            if rectyp1 == '1':
                continue
            
            parsed_data.append({
                'tfid': line[3:11].strip(),
                'subacct': line[11:16].strip(),
                'preind': line[364:365].strip(),
                'censust': int(line[367:368].strip() or 0),
                'acctno': int(line[376:386].strip() or 0)
            })
        
        df = pl.DataFrame(parsed_data)
        
        elapsed = time.time() - start_time
        logger.info(f"Read {df.height} rows from {file_path.name} in {elapsed:.2f}s")
        
        return df
    
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        raise


def read_coll_binary(file_path: Path) -> pl.DataFrame:
    """Read COLL binary file with packed decimal fields."""
    try:
        logger.info(f"Reading {file_path.name}...")
        start_time = time.time()
        
        file_size = os.path.getsize(file_path)
        logger.info(f"File size: {file_size / (1024**3):.2f} GB")
        
        # Detect record length
        record_length = None
        for length in Config.COLL_RECORD_LENGTHS:
            if file_size % length == 0:
                record_length = length
                break
        
        if record_length is None:
            record_length = 151
            logger.warning(f"Using minimum record length: {record_length}")
        
        total_records = file_size // record_length
        logger.info(f"Record length: {record_length}, Total records: {total_records}")
        
        all_data = []
        
        with open(file_path, 'rb') as f:
            for chunk_start in range(0, total_records, Config.CHUNK_SIZE):
                chunk_end = min(chunk_start + Config.CHUNK_SIZE, total_records)
                records_to_read = chunk_end - chunk_start
                bytes_to_read = records_to_read * record_length
                chunk_data = f.read(bytes_to_read)
                
                for i in range(records_to_read):
                    record_start = i * record_length
                    record = chunk_data[record_start:record_start + record_length]
                    
                    if len(record) < 151:
                        continue
                    
                    ccollno = unpack_packed_decimal(record[3:9])
                    acctno = unpack_packed_decimal(record[145:151])
                    
                    if ccollno > 0 and acctno > 0:
                        all_data.append({'ccollno': ccollno, 'acctno': acctno})
                
                if chunk_start % 1000000 == 0 and chunk_start > 0:
                    logger.info(f"Processed {chunk_start} records...")
        
        df = pl.DataFrame(all_data) if all_data else pl.DataFrame({
            'ccollno': pl.Series([], dtype=pl.Int64),
            'acctno': pl.Series([], dtype=pl.Int64)
        })
        
        elapsed = time.time() - start_time
        logger.info(f"Read {df.height} valid records from COLL in {elapsed:.2f}s")
        
        return df
    
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        raise


def read_desc_ebcdic(file_path: Path) -> pl.DataFrame:
    """Read DESC file as EBCDIC fixed-width format."""
    try:
        logger.info(f"Reading {file_path.name}...")
        start_time = time.time()
        
        file_size = os.path.getsize(file_path)
        logger.info(f"File size: {file_size / (1024**3):.2f} GB")
        
        record_length = Config.DESC_RECORD_LENGTH
        total_records = file_size // record_length
        logger.info(f"Record length: {record_length}, Total records: {total_records}")
        
        all_data = []
        processed = 0
        
        with open(file_path, 'rb') as f:
            for chunk_start in range(0, total_records, Config.CHUNK_SIZE):
                chunk_end = min(chunk_start + Config.CHUNK_SIZE, total_records)
                records_to_read = chunk_end - chunk_start
                bytes_to_read = records_to_read * record_length
                chunk_data = f.read(bytes_to_read)
                
                for i in range(records_to_read):
                    record_start = i * record_length
                    record = chunk_data[record_start:record_start + record_length]
                    
                    if len(record) < 220:
                        continue
                    
                    ccollno = ebcdic_bytes_to_int(record[0:11])
                    census = ebcdic_bytes_to_int(record[210:220])
                    
                    if ccollno > 0 and Config.DESC_CENSUS_MIN <= census <= Config.DESC_CENSUS_MAX:
                        all_data.append({
                            'ccollno': ccollno,
                            'cinstcl': ebcdic_to_ascii(record[50:52]),
                            'natguar': ebcdic_to_ascii(record[54:56]),
                            'census': census
                        })
                    
                    processed += 1
                
                if processed % 1000000 == 0:
                    logger.info(f"Processed {processed} records, found {len(all_data)} valid...")
        
        df = pl.DataFrame(all_data) if all_data else pl.DataFrame({
            'ccollno': pl.Series([], dtype=pl.Int64),
            'cinstcl': pl.Series([], dtype=pl.Utf8),
            'natguar': pl.Series([], dtype=pl.Utf8),
            'census': pl.Series([], dtype=pl.Int64)
        })
        
        elapsed = time.time() - start_time
        logger.info(f"Read {df.height} valid records from DESC in {elapsed:.2f}s")
        
        return df
    
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        raise


# =========================
# Main ETL Process
# =========================
def main():
    """Main ETL process."""
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("Starting EIBRCGCS ETL Process")
    logger.info("=" * 60)
    
    try:
        # Calculate dates
        reptdate = datetime.now() - timedelta(days=1)
        reptmon = f"{reptdate.month:02d}"
        reptyear2 = f"{reptdate.year % 100:02d}"
        reptday = f"{reptdate.day:02d}"
        nowk = calculate_week_of_month(reptdate)
        
        logger.info(f"REPTDATE: {reptdate}")
        logger.info(f"REPTMON: {reptmon}")
        logger.info(f"REPTYEAR2: {reptyear2}")
        logger.info(f"REPTDAY: {reptday}")
        logger.info(f"NOWK: {nowk}")
        
        # Build file paths
        mnitb_current = Config.BASE_INPUT / Config.MNITB_CURRENT_PATTERN.format(reptmon=reptmon)
        mniln_lnnote = Config.BASE_INPUT / Config.MNILN_LNNOTE_PATTERN.format(reptmon=reptmon)
        crftabl = Config.BASE_INPUT / Config.CRFTABL_NAME
        mast_file = Config.BASE_INPUT / Config.MAST_PATTERN.format(
            reptmon=reptmon, nowk=nowk, reptyear2=reptyear2
        )
        coll_file = Config.BASE_INPUT / Config.COLL_PATTERN.format(
            year=reptdate.year, month=reptmon, day=reptday
        )
        desc_file = Config.BASE_INPUT / Config.DESC_PATTERN.format(
            year=reptdate.year, month=reptmon, day=reptday
        )
        
        # Validate files exist
        for file_path, name in [
            (mnitb_current, "MNITB.CURRENT"),
            (mniln_lnnote, "MNILN.LNNOTE"),
            (crftabl, "CRFTABL"),
            (mast_file, "MAST"),
            (coll_file, "COLL"),
            (desc_file, "DESC")
        ]:
            if not file_path.exists():
                raise FileNotFoundError(f"{name} file not found: {file_path}")
        
        # Step 1: Get target accounts from COLL/DESC
        logger.info("Step 1: Getting target accounts from COLL/DESC...")
        coll = read_coll_binary(coll_file)
        desc = read_desc_ebcdic(desc_file)
        
        if coll.height > 0 and desc.height > 0:
            coll_filtered = coll.join(desc, on="ccollno", how="inner")
            target_acctnos = coll_filtered.select(["acctno"]).unique()
            target_set = set(target_acctnos['acctno'].to_list())
            logger.info(f"Target accounts: {len(target_set)}")
        else:
            target_set = set()
            logger.warning("No target accounts found from COLL/DESC")
        
        del coll, desc
        gc.collect()
        
        if not target_set:
            logger.warning("No target accounts. Exiting.")
            return
        
        # Step 2: Process CRFTABL
        logger.info("Step 2: Processing CRFTABL...")
        crft = read_crftabl(crftabl)
        crft = crft.with_columns([
            pl.when(pl.col("censust") == 3).then(pl.lit("P51"))
             .when(pl.col("censust") == 4).then(pl.lit("P72"))
             .when(pl.col("censust") == 5).then(pl.lit("P65"))
             .otherwise(pl.lit("   "))
             .alias("sch")
        ])
        crft = crft.filter(pl.col("sch") == "   ")
        crft = crft.unique(subset=["acctno", "censust", "subacct"], keep="first")
        
        # Merge with MAST
        mast = read_sas7bdat(mast_file)
        mast = mast.select(["acctno"]).unique()
        crft = crft.join(mast, on="acctno", how="inner")
        crft = crft.filter(pl.col("acctno") > 0)
        crft = crft.with_columns([
            pl.lit(0).cast(pl.Int64).alias("noteno"),
            pl.lit(0).cast(pl.Int64).alias("product"),
        ])
        crft = crft.unique(subset=["acctno", "subacct"], keep="first")
        crft = crft.select(["acctno", "censust", "product", "noteno"])
        crft = crft.filter(pl.col("acctno").is_in(target_set))
        logger.info(f"CRFT matching records: {crft.height}")
        
        del mast
        gc.collect()
        
        # Step 3: Process MNITB.CURRENT
        logger.info("Step 3: Processing MNITB.CURRENT...")
        ca = read_sas7bdat_filtered(mnitb_current, target_set)
        
        if ca.height > 0:
            ca = ca.select(["acctno", "censust", "product"]).with_columns([
                pl.col('acctno').cast(pl.Int64),
                pl.col('censust').cast(pl.Int64),
                pl.col('product').cast(pl.Int64),
                pl.lit(0).cast(pl.Int64).alias("noteno"),
                pl.lit("   ").alias("sch")
            ])
            ca = ca.with_columns([
                pl.when((pl.col("product") == 112) & (pl.col("censust") == 301)).then(pl.lit("P70"))
                 .when((pl.col("product") == 112) & (pl.col("censust") == 300)).then(pl.lit("P51"))
                 .when((pl.col("product") == 112) & (pl.col("censust") == 302)).then(pl.lit("P72"))
                 .when((pl.col("product") == 114) & (pl.col("censust") == 303)).then(pl.lit("P72"))
                 .when((pl.col("product") == 108) & (pl.col("censust") == 304)).then(pl.lit("P75"))
                 .otherwise(pl.col("sch"))
                 .alias("sch")
            ])
            ca = ca.filter(pl.col("sch") == "   ")
            ca = ca.select(["acctno", "censust", "product", "noteno"])
        else:
            ca = pl.DataFrame({
                'acctno': pl.Series([], dtype=pl.Int64),
                'censust': pl.Series([], dtype=pl.Int64),
                'product': pl.Series([], dtype=pl.Int64),
                'noteno': pl.Series([], dtype=pl.Int64)
            })
        
        logger.info(f"CA matching records: {ca.height}")
        
        # Step 4: Process MNILN.LNNOTE
        logger.info("Step 4: Processing MNILN.LNNOTE...")
        ln = read_sas7bdat_filtered(mniln_lnnote, target_set)
        
        if ln.height > 0:
            ln = ln.with_columns([
                pl.col('loantype').alias('product'),
                pl.col('census').alias('censust'),
                pl.lit("   ").alias("sch"),
            ])
            ln = ln.with_columns([
                pl.when((pl.col("loantype") == 510) & (pl.col("census").is_in([5.12, 5.13]))).then(pl.lit("P70"))
                 .when((pl.col("loantype") == 532) & (pl.col("census") == 3.00)).then(pl.lit("P51"))
                 .when((pl.col("loantype") == 524) & (pl.col("census") == 5.16)).then(pl.lit("P72"))
                 .when((pl.col("loantype") == 527) & (pl.col("census") == 5.17)).then(pl.lit("P72"))
                 .when((pl.col("loantype") == 531) & (pl.col("census") == 5.00)).then(pl.lit("P63"))
                 .when((pl.col("loantype") == 533) & (pl.col("census") == 533.01)).then(pl.lit("P64"))
                 .when((pl.col("loantype") == 533) & (pl.col("census") == 533.00)).then(pl.lit("P65"))
                 .otherwise(pl.col("sch"))
                 .alias("sch")
            ])
            ln = ln.filter(pl.col("sch") == "   ")
            ln = ln.select(["acctno", "noteno", "product", "censust"])
            ln = ln.with_columns([
                pl.col('product').cast(pl.Int64),
                pl.col('censust').cast(pl.Int64)
            ])
        else:
            ln = pl.DataFrame({
                'acctno': pl.Series([], dtype=pl.Int64),
                'noteno': pl.Series([], dtype=pl.Int64),
                'product': pl.Series([], dtype=pl.Int64),
                'censust': pl.Series([], dtype=pl.Int64)
            })
        
        logger.info(f"LN matching records: {ln.height}")
        
        # Step 5: Combine all data
        logger.info("Step 5: Combining all data...")
        
        # Ensure consistent schemas
        ca_final = ca.select(["acctno", "censust", "product", "noteno"]).with_columns([
            pl.col('acctno').cast(pl.Int64),
            pl.col('censust').cast(pl.Int64),
            pl.col('product').cast(pl.Int64),
            pl.col('noteno').cast(pl.Int64)
        ])
        
        ln_final = ln.select(["acctno", "censust", "product", "noteno"]).with_columns([
            pl.col('acctno').cast(pl.Int64),
            pl.col('censust').cast(pl.Int64),
            pl.col('product').cast(pl.Int64),
            pl.col('noteno').cast(pl.Int64)
        ])
        
        crft_final = crft.select(["acctno", "censust", "product", "noteno"]).with_columns([
            pl.col('acctno').cast(pl.Int64),
            pl.col('censust').cast(pl.Int64),
            pl.col('product').cast(pl.Int64),
            pl.col('noteno').cast(pl.Int64)
        ])
        
        excp = pl.concat([ca_final, ln_final, crft_final], how="vertical").sort(by=["acctno"])
        logger.info(f"Final EXCP records: {excp.height}")
        
        # Step 6: Write output
        if excp.height > 0:
            logger.info("Step 6: Writing output...")
            
            # Create output directory
            out_dir = Config.BASE_OUTPUT / "excp"
            out_dir.mkdir(parents=True, exist_ok=True)
            
            # Write using SASpy
            sas = saspy.SASsession(cfgname=Config.SAS_CONFIG)
            excp_pandas = excp.to_pandas()
            sas.df2sd(excp_pandas, 'work_excp')
            
            sas_code = f"""
            libname {Config.SAS_OUTPUT_LIB} "{out_dir}";
            data {Config.SAS_OUTPUT_LIB}.{Config.SAS_OUTPUT_DATASET};
                set work_excp;
            run;
            """
            
            sas.submit(sas_code)
            sas.endsas()
            
            logger.info(f"Output written to {out_dir / (Config.SAS_OUTPUT_DATASET + '.sas7bdat')}")
        else:
            logger.warning("No records to write.")
        
        elapsed = time.time() - start_time
        logger.info(f"ETL process completed in {elapsed:.2f}s")
        
    except Exception as e:
        logger.error(f"ETL process failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
