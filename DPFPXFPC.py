from __future__ import annotations

from pathlib import Path
from datetime import date, datetime, timedelta
import polars as pl
import pyreadstat
import saspy
import numpy as np
import gc
import sys
import os


# =========================
# Paths
# =========================
BASE_OUTPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBLTRRF")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

LOAN_LNNOTE   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m08.sas7bdat")
LOAN_LNCOMM   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/enrh_ln_comm_m08.sas7bdat")
CISLN_LOAN    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMHPTOP/loan.sas7bdat")
COLL_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831")
DESC_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831")
MICR_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLTRRF/BOPESS.txt")
NPGS_TRRF_IN  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLTRRF/trrf.sas7bdat")

OUT_DIR  = BASE_OUTPUT
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = None

CHUNK_SIZE = 50000
COLL_RECORD_LENGTH = 380
DESC_RECORD_LENGTH = 3050


# =========================
# Helpers (minimal for debugging)
# =========================
def read_sas7bdat_in_chunks(file_path, chunk_size=CHUNK_SIZE, filter_func=None, keep_columns=None):
    chunks = []
    row_offset = 0
    file_path_str = str(file_path)
    schema_columns = None
    
    while True:
        try:
            df_chunk, meta_chunk = pyreadstat.read_sas7bdat(
                file_path_str, 
                row_offset=row_offset,
                row_limit=chunk_size
            )
            
            if df_chunk is None or len(df_chunk) == 0:
                break
            
            pl_chunk = pl.from_pandas(df_chunk)
            
            if schema_columns is None:
                schema_columns = pl_chunk.columns
            
            if keep_columns:
                existing_cols = [c for c in keep_columns if c in pl_chunk.columns]
                if existing_cols:
                    pl_chunk = pl_chunk.select(existing_cols)
            
            if filter_func:
                pl_chunk = filter_func(pl_chunk)
                
            if pl_chunk.height > 0:
                chunks.append(pl_chunk)
            
            row_offset += len(df_chunk)
            del df_chunk
            gc.collect()
            
            if len(pl_chunk) < chunk_size:
                break
                
        except Exception as e:
            print(f"Error: {e}")
            break
    
    if chunks:
        result = pl.concat(chunks, how="vertical", rechunk=True)
        del chunks
        gc.collect()
        return result
    else:
        return pl.DataFrame()


def decode_ebcdic_bytes(data_bytes):
    try:
        return data_bytes.decode('cp037')
    except:
        try:
            return data_bytes.decode('cp500')
        except:
            return data_bytes.decode('latin-1')


def unpack_packed_decimal(b):
    if len(b) == 0:
        return 0
    
    digits = []
    for i in range(len(b) - 1):
        digits.append((b[i] >> 4) & 0x0F)
        digits.append(b[i] & 0x0F)
    
    digits.append((b[-1] >> 4) & 0x0F)
    sign = b[-1] & 0x0F
    
    value = 0
    for digit in digits:
        if digit > 9:
            return 0
        value = value * 10 + digit
    
    if sign == 0x0D:
        value = -value
    
    return value


def read_fixed_length_ebcdic(file_path, record_length, record_parser, chunk_size=CHUNK_SIZE):
    all_records = []
    chunk_records = []
    
    with open(file_path, 'rb') as f:
        while True:
            record_bytes = f.read(record_length)
            if not record_bytes or len(record_bytes) < record_length:
                break
            
            record = record_parser(record_bytes)
            if record is not None:
                chunk_records.append(record)
                
                if len(chunk_records) >= chunk_size:
                    all_records.extend(chunk_records)
                    chunk_records = []
                    gc.collect()
        
        if chunk_records:
            all_records.extend(chunk_records)
    
    return pl.DataFrame(all_records) if all_records else pl.DataFrame()


def parse_coll_record(record_bytes):
    try:
        if len(record_bytes) < 158:
            return None
        
        ccollno = unpack_packed_decimal(record_bytes[3:9])
        acctno = unpack_packed_decimal(record_bytes[145:151])
        noteno = unpack_packed_decimal(record_bytes[152:158])
        
        return {
            'CCOLLNO': ccollno,
            'ACCTNO': acctno,
            'NOTENO': noteno
        }
    except:
        return None


def parse_desc_record(record_bytes):
    try:
        if len(record_bytes) < 298:
            return None
        
        ccollno_str = decode_ebcdic_bytes(record_bytes[0:11]).strip()
        cinstcl = decode_ebcdic_bytes(record_bytes[50:52]).strip()
        natguar = decode_ebcdic_bytes(record_bytes[54:56]).strip()
        cgcgur = decode_ebcdic_bytes(record_bytes[127:130]).strip()
        census_str = decode_ebcdic_bytes(record_bytes[210:220]).strip()
        tranche = decode_ebcdic_bytes(record_bytes[290:298]).strip()
        
        ccollno = int(float(ccollno_str)) if ccollno_str else 0
        census = float(census_str) if census_str else 0.0
        
        if cgcgur in ('080', '090'):
            sch = '7Q' if cgcgur == '080' else '8Q'
            
            return {
                'CCOLLNO': ccollno,
                'CINSTCL': cinstcl,
                'NATGUAR': natguar,
                'CGCGUR': cgcgur,
                'CENSUS': census,
                'TRANCHE': tranche,
                'SCH': sch
            }
        return None
    except:
        return None


# =========================
# DEBUGGING: Compare LOAN1 and COLL ACCTNO values
# =========================
print("="*80)
print("DEBUGGING: Comparing LOAN1 and COLL ACCTNO values")
print("="*80)

# Read LOAN1 (LOANTYPE=570, COMMNO > 0)
def filter_loan1(chunk):
    if chunk.height == 0:
        return chunk
    if "LOANTYPE" in chunk.columns:
        chunk = chunk.filter(pl.col("LOANTYPE").cast(pl.Float64) == 570.0)
    if "COMMNO" in chunk.columns:
        chunk = chunk.filter(pl.col("COMMNO") > 0)
    return chunk

loan1 = read_sas7bdat_in_chunks(
    LOAN_LNNOTE,
    chunk_size=CHUNK_SIZE,
    filter_func=filter_loan1,
    keep_columns=['ACCTNO', 'NOTENO', 'LOANTYPE', 'COMMNO', 'NAME']
)

print(f"\nLOAN1 rows: {loan1.height}")
if loan1.height > 0:
    print("LOAN1 ACCTNO and NOTENO values:")
    print(loan1.select(['ACCTNO', 'NOTENO', 'NAME']))

# Read COLL (just first few records with matching criteria)
print("\nReading COLL file...")
coll = read_fixed_length_ebcdic(
    COLL_FILE,
    COLL_RECORD_LENGTH,
    parse_coll_record,
    chunk_size=10000
)

# Read DESC
print("Reading DESC file...")
desc = read_fixed_length_ebcdic(
    DESC_FILE,
    DESC_RECORD_LENGTH,
    parse_desc_record,
    chunk_size=10000
)

# Merge COLL and DESC
if coll.height > 0 and desc.height > 0:
    coll = coll.with_columns(pl.col("CCOLLNO").cast(pl.Int64))
    desc = desc.with_columns(pl.col("CCOLLNO").cast(pl.Int64))
    coll = coll.join(desc, on="CCOLLNO", how="inner")
    coll = coll.filter((pl.col("CINSTCL") == "18") & (pl.col("NATGUAR") == "06"))
    print(f"\nCOLL after merge with DESC: {coll.height} rows")

# Show COLL ACCTNO values
if coll.height > 0:
    print("\nCOLL ACCTNO and NOTENO values (first 20):")
    print(coll.select(['ACCTNO', 'NOTENO', 'CCOLLNO']).head(20))
    
    # Show unique ACCTNO in COLL
    unique_coll_acct = coll['ACCTNO'].unique().to_list()
    print(f"\nUnique ACCTNO in COLL: {len(unique_coll_acct)}")
    print(f"First 20: {unique_coll_acct[:20]}")

# Compare LOAN1 ACCTNO with COLL ACCTNO
if loan1.height > 0 and coll.height > 0:
    loan1_acctnos = set(loan1['ACCTNO'].cast(pl.Int64).to_list())
    coll_acctnos = set(coll['ACCTNO'].cast(pl.Int64).to_list())
    
    print(f"\nLOAN1 ACCTNOs: {loan1_acctnos}")
    print(f"COLL ACCTNOs (first 10): {list(coll_acctnos)[:10]}")
    
    # Find intersection
    intersection = loan1_acctnos.intersection(coll_acctnos)
    print(f"\nMatching ACCTNOs: {len(intersection)}")
    if intersection:
        print(f"Matching values: {intersection}")
    
    # Also check NOTENO
    loan1_notenos = set(loan1['NOTENO'].cast(pl.Int64).to_list())
    coll_notenos = set(coll['NOTENO'].cast(pl.Int64).to_list())
    
    print(f"\nLOAN1 NOTENOs: {loan1_notenos}")
    print(f"COLL NOTENOs (first 20): {list(coll_notenos)[:20]}")
    
    noteno_intersection = loan1_notenos.intersection(coll_notenos)
    print(f"\nMatching NOTENOs: {len(noteno_intersection)}")
    if noteno_intersection:
        print(f"Matching NOTENO values: {noteno_intersection}")

# Try join with just ACCTNO
if loan1.height > 0 and coll.height > 0:
    print("\n" + "="*80)
    print("TESTING JOINS")
    print("="*80)
    
    # Cast to same type
    loan1_test = loan1.with_columns([
        pl.col("ACCTNO").cast(pl.Int64),
        pl.col("NOTENO").cast(pl.Int64),
    ])
    coll_test = coll.with_columns([
        pl.col("ACCTNO").cast(pl.Int64),
        pl.col("NOTENO").cast(pl.Int64),
    ])
    
    # Join on ACCTNO only
    join_acct = loan1_test.join(coll_test.select(['ACCTNO']).unique(), on="ACCTNO", how="inner")
    print(f"Join on ACCTNO only: {join_acct.height} matches")
    
    # Join on NOTENO only
    join_note = loan1_test.join(coll_test.select(['NOTENO']).unique(), on="NOTENO", how="inner")
    print(f"Join on NOTENO only: {join_note.height} matches")
    
    # Join on both
    join_both = loan1_test.join(coll_test, on=["ACCTNO", "NOTENO"], how="inner")
    print(f"Join on ACCTNO+NOTENO: {join_both.height} matches")
    
    # Show what's in COLL that might match
    print("\nCOLL records with ACCTNO starting with 20:")
    coll_20 = coll.filter(pl.col("ACCTNO").cast(pl.Int64) >= 2000000000)
    coll_20 = coll_20.filter(pl.col("ACCTNO").cast(pl.Int64) <= 2999999999)
    print(f"COLL records with ACCTNO in 2xxxxxxx range: {coll_20.height}")
    if coll_20.height > 0:
        print(coll_20.select(['ACCTNO', 'NOTENO', 'CCOLLNO']).head(20))

print("\n" + "="*80)
print("DEBUGGING COMPLETE")
print("="*80)
