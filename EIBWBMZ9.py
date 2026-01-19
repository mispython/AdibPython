import polars as pl
from pathlib import Path
from datetime import datetime, date
import sys
import re

def eibwbmz9():
    base = Path.cwd()
    eldsrv_path = base / "ELDSRV"
    eldsrv_path.mkdir(exist_ok=True)
    
    # Step 1: Create REPTDATE (SAS SELECT/WHEN logic)
    today = date.today()
    day = today.day
    
    if 8 <= day <= 14:
        reptdate = date(today.year, today.month, 8)
        wk = '1'
    elif 15 <= day <= 21:
        reptdate = date(today.year, today.month, 15)
        wk = '2'
    elif 22 <= day <= 27:
        reptdate = date(today.year, today.month, 22)
        wk = '3'
    else:
        # First day of current month minus 1 day = last day of previous month
        reptdate = date(today.year, today.month, 1)
        from datetime import timedelta
        reptdate = reptdate - timedelta(days=1)
        wk = '4'
    
    # SAS CALL SYMPUT equivalents
    nowk = wk
    rdate = reptdate.strftime("%d%m%y")  # DDMMYY8 format
    reptmon = f"{reptdate.month:02d}"  # Z2 format
    reptyear = str(reptdate.year)[-2:]  # YEAR2 format
    
    print(f"NOWK: {nowk}, RDATE: {rdate}, REPTMON: {reptmon}, REPTYEAR: {reptyear}")
    
    # Step 2: Check input file date (SAS INFILE with column positions)
    try:
        with open(base / "ELDSRV9", 'r') as f:
            first_line = f.readline().strip()
            
        # Extract DD, MM, YY from fixed positions (cols 54-69)
        # SAS: @054 DD 2. @057 MM 2. @060 YY 4.
        dd = int(first_line[53:55])  # 0-indexed: 53-55 = cols 54-55
        mm = int(first_line[56:58])  # 0-indexed: 56-58 = cols 57-58  
        yy = int(first_line[59:63])  # 0-indexed: 59-63 = cols 60-63
        
        eldsdt1 = date(yy, mm, dd)
        eldsdt1_str = eldsdt1.strftime("%d%m%y")
        print(f"Input file date: {eldsdt1_str}")
        
    except Exception as e:
        print(f"Error reading input file: {e}")
        sys.exit(1)
    
    # Step 3: Check dates match (SAS macro condition)
    if eldsdt1_str == rdate:
        # Process the file (SAS INFILE with fixed columns)
        try:
            # Read data from line 2 onwards with fixed widths
            # Each field is 13 chars: @01 MAANO, @17 AANO01, etc.
            widths = [13] * 16  # MAANO + 15 AANO fields
            
            df = pl.read_csv(
                base / "ELDSRV9",
                skip_rows=1,
                has_header=False,
                new_columns=["MAANO", "AANO01", "AANO02", "AANO03", "AANO04",
                           "AANO05", "AANO06", "AANO07", "AANO08", "AANO09",
                           "AANO10", "AANO11", "AANO12", "AANO13", "AANO14", "AANO15"],
                encoding="utf8"
            )
            
            # Clean each AANO field (SAS COMPRESS logic)
            for col in [f"AANO{i:02d}" for i in range(1, 16)]:
                # SAS: COMP = COMPRESS(AANO01,'/0123456789')
                # SAS: IF AANO01 EQ COMP THEN AANO01 = ' '
                comp = df[col].str.replace_all(r'[/0-9]', '')
                df = df.with_columns(
                    pl.when(df[col] == comp)
                    .then(pl.lit(''))
                    .otherwise(df[col])
                    .alias(col)
                )
                
                # SAS: AANO01 = COMPRESS(AANO01,'@*(#)-')
                df = df.with_columns(
                    df[col].str.replace_all(r'[@*\(\)#-]', '').alias(col)
                )
                
                # SAS: AANO01 = COMPRESS(AANO01) - remove all whitespace
                df = df.with_columns(
                    df[col].str.replace_all(r'\s+', '').alias(col)
                )
            
            # Convert MAANO to uppercase
            df = df.with_columns(df["MAANO"].str.to_uppercase().alias("MAANO"))
            
            # Save ELDSRV.ELN9
            df.write_parquet(eldsrv_path / "ELN9.parquet")
            
            # SAS: Append ELDSRVO.ELN9 if exists
            old_path = base / "ELDSRVO" / "ELN9.parquet"
            if old_path.exists():
                old_df = pl.read_parquet(old_path)
                df = pl.concat([old_df, df])
                df.write_parquet(eldsrv_path / "ELN9.parquet")
            
            # Sort by MAANO
            df = df.sort("MAANO")
            
            # Create TEST dataset with GRP counter (SAS RETAIN)
            test_df = df.with_columns(
                pl.int_range(1, len(df) + 1).alias("GRP")
            )
            test_df = test_df.with_columns(pl.col("GRP").alias("MAAGRP"))
            
            # Save ELN9 dataset
            df.write_parquet(eldsrv_path / "ELN9_simple.parquet")
            
            # Read LOAN.LNNOTE datasets
            loan_df1 = pl.read_parquet(base / "LOAN" / "LNNOTE.parquet")
            loan_df2 = pl.read_parquet(base / "ILOAN" / "LNNOTE.parquet")
            loanx_df = pl.concat([loan_df1, loan_df2])
            
            # Save LOANX
            loanx_df.write_parquet(base / "LOANX.parquet")
            
            print("File processed successfully")
            
            # Execute external program (SAS %INC)
            # Note: You'll need to implement eibwbmt9.py separately
            try:
                from eibwbmt9 import process as eibwbmt9_process
                final_df = eibwbmt9_process()
                final_df.write_parquet(eldsrv_path / "EGRP.parquet")
                
                # Print where MAAGRP is null (SAS PROC PRINT)
                null_rows = final_df.filter(pl.col("MAAGRP").is_null())
                if len(null_rows) > 0:
                    print("Rows where MAAGRP is null:")
                    print(null_rows)
                    
            except ImportError:
                print("Note: PGM(EIBWBMT9) not implemented yet")
                
        except Exception as e:
            print(f"Error processing file: {e}")
            sys.exit(1)
            
    else:
        # SAS %PUT statements
        print(f"THE SAP.PBB.ELDS.NEWBNM1.TEXT(0) NOT DATED {rdate}")
        print("THE JOB IS NOT DONE !!")
        
        # SAS ABORT 77
        sys.exit(77)

if __name__ == "__main__":
    eibwbmz9()
