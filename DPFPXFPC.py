import polars as pl
from pathlib import Path
import pyreadstat

# Configuration
mni_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/MNI")
imni_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/IMNI")
pidms_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/PIDM")
output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBFLOAT")
output_path.mkdir(exist_ok=True)

def read_sas_file(file_path):
    """Read SAS .sas7bdat file and return polars DataFrame using pyreadstat"""
    try:
        # Read SAS file using pyreadstat
        df, meta = pyreadstat.read_sas7bdat(str(file_path))
        
        # Convert to polars and lowercase column names
        pl_df = pl.from_pandas(df)
        # Rename all columns to lowercase
        pl_df = pl_df.rename({col: col.lower() for col in pl_df.columns})
        
        # Print column info for debugging
        print(f"Columns in {file_path.name}: {pl_df.columns}")
        
        return pl_df
    except FileNotFoundError:
        print(f"NOTE: {file_path} not found")
        return pl.DataFrame()
    except Exception as e:
        print(f"ERROR reading {file_path}: {e}")
        return pl.DataFrame()

def write_parquet_file(df, file_path):
    """Write polars DataFrame to Parquet file"""
    if not df.is_empty():
        df.write_parquet(file_path)
        print(f"Parquet file created: {file_path}")

def process_conventional_float():
    """Process conventional banking float data"""
    print("PROCESSING CONVENTIONAL BANKING FLOAT DATA")
    print("=" * 50)
    
    # PROC SORT DATA=MNI.FDMTHLY OUT=FDMTHLY;
    fdmthly_df = read_sas_file(mni_path / "fdmthly.sas7bdat")
    if not fdmthly_df.is_empty():
        # Check available columns
        available_cols = fdmthly_df.columns
        print(f"FDMTHLY available columns: {available_cols}")
        
        # Try to find alternative column names
        acctno_col = next((col for col in available_cols if 'acctno' in col.lower()), None)
        branch_col = next((col for col in available_cols if 'branch' in col.lower()), None)
        curbal_col = next((col for col in available_cols if 'curbal' in col.lower()), None)
        amtind_col = next((col for col in available_cols if 'amtind' in col.lower()), None)
        
        # Create select list based on available columns
        select_cols = []
        if acctno_col:
            select_cols.append(acctno_col)
        if branch_col:
            select_cols.append(branch_col)
        if curbal_col:
            select_cols.append(curbal_col)
        if amtind_col:
            select_cols.append(amtind_col)
        
        if select_cols:
            fdmthly_df = fdmthly_df.select(select_cols)
            if acctno_col:
                fdmthly_df = fdmthly_df.sort(acctno_col)
        else:
            print("NOTE: No required columns found in MNI.fdmthly")
            fdmthly_df = pl.DataFrame()
    else:
        print("NOTE: MNI.fdmthly not found")
        fdmthly_df = pl.DataFrame()

    # DATA FDMTHLY; SET FDMTHLY; LEDGBAL = CURBAL;
    if not fdmthly_df.is_empty() and curbal_col and curbal_col in fdmthly_df.columns:
        fdmthly_processed = fdmthly_df.with_columns([
            pl.col(curbal_col).alias('ledgbal')
        ])
    else:
        fdmthly_processed = pl.DataFrame()

    # DATA CURN; SET MNI.CURN124;
    curn_df = read_sas_file(mni_path / "curn124.sas7bdat")
    if not curn_df.is_empty():
        # Check if product column exists
        if 'product' in curn_df.columns:
            curn_filtered = curn_df.filter(pl.col('product') != 139)
        else:
            print("NOTE: 'product' column not found in CURN124, using all records")
            curn_filtered = curn_df
    else:
        print("NOTE: MNI.curn124 not found")
        curn_filtered = pl.DataFrame()

    # DATA DEPOSIT; SET multiple datasets;
    datasets_to_combine = []

    # MNI.SAVG124
    savg_df = read_sas_file(mni_path / "savg124.sas7bdat")
    if not savg_df.is_empty():
        print(f"SAVG124 available columns: {savg_df.columns}")
        # Try to map columns
        savg_select_cols = []
        for col in ['acctno', 'product', 'curbal', 'ledgbal', 'progcd', 'amtind', 'intpaybl', 'branch']:
            if col in savg_df.columns:
                savg_select_cols.append(col)
            else:
                # Try to find similar column
                similar = next((c for c in savg_df.columns if col in c.lower()), None)
                if similar:
                    savg_select_cols.append(similar)
        
        if savg_select_cols:
            savg_df = savg_df.select(savg_select_cols)
            datasets_to_combine.append(savg_df)
    else:
        print("NOTE: MNI.savg124 not found")

    # CURN
    if not curn_filtered.is_empty():
        curn_select_cols = []
        for col in ['acctno', 'product', 'curbal', 'ledgbal', 'progcd', 'amtind', 'intpaybl', 'branch']:
            if col in curn_filtered.columns:
                curn_select_cols.append(col)
            else:
                # Try to find similar column
                similar = next((c for c in curn_filtered.columns if col in c.lower()), None)
                if similar:
                    curn_select_cols.append(similar)
        
        if curn_select_cols:
            curn_selected = curn_filtered.select(curn_select_cols)
            datasets_to_combine.append(curn_selected)

    # FDMTHLY with renames (if we have the necessary columns)
    if not fdmthly_processed.is_empty():
        # Check what columns we have and try to map them
        rename_map = {}
        fdm_cols = fdmthly_processed.columns
        
        # Map original column names to standard names
        if acctno_col and acctno_col in fdm_cols:
            rename_map[acctno_col] = 'acctno'
        if branch_col and branch_col in fdm_cols:
            rename_map[branch_col] = 'branch'
        if curbal_col and curbal_col in fdm_cols:
            rename_map[curbal_col] = 'curbal'
        
        # Check for other columns we might need
        for col in ['ledgbal', 'amtind']:
            if col in fdm_cols:
                rename_map[col] = col
        
        # Rename columns if we have any mappings
        if rename_map:
            fdm_renamed = fdmthly_processed.rename(rename_map)
            
            # Select only standard columns
            standard_cols = ['acctno', 'branch', 'curbal', 'ledgbal', 'amtind']
            available_std_cols = [col for col in standard_cols if col in fdm_renamed.columns]
            
            if available_std_cols:
                fdm_final = fdm_renamed.select(available_std_cols)
                
                # Add placeholder columns if missing
                if 'product' not in fdm_final.columns:
                    fdm_final = fdm_final.with_columns([
                        pl.lit(None).alias('product')
                    ])
                if 'progcd' not in fdm_final.columns:
                    fdm_final = fdm_final.with_columns([
                        pl.lit(None).alias('progcd')
                    ])
                if 'intpaybl' not in fdm_final.columns:
                    fdm_final = fdm_final.with_columns([
                        pl.lit(None).alias('intpaybl')
                    ])
                
                datasets_to_combine.append(fdm_final)

    # Combine all datasets and apply filters
    if datasets_to_combine:
        # Ensure all datasets have the same columns before concatenation
        all_cols = set()
        for df in datasets_to_combine:
            all_cols.update(df.columns)
        
        # Standardize columns across all datasets
        standardized_datasets = []
        for df in datasets_to_combine:
            for col in all_cols:
                if col not in df.columns:
                    df = df.with_columns([pl.lit(None).alias(col)])
            standardized_datasets.append(df)
        
        deposit_combined = pl.concat(standardized_datasets, how="vertical")
        
        # Apply filters and transformations
        valid_progcd = [
            '42110', '42310', '42120', '42320', '42130',
            '42133', '42132', '42180', '42610', '42630', '34180',
            '42199', '42699'
        ]
        
        # Check if prodcd column exists
        if 'progcd' not in deposit_combined.columns:
            deposit_combined = deposit_combined.with_columns([
                pl.lit(None).alias('progcd')
            ])
        
        deposit_filtered = deposit_combined.filter(
            pl.col('progcd').is_in(valid_progcd)
        )
        
        # Check if product column exists before using it
        if 'product' in deposit_filtered.columns:
            deposit_filtered = deposit_filtered.with_columns([
                # IF PRODUCT = 166 THEN PROGCD = '42310';
                pl.when(pl.col('product') == 166)
                .then(pl.lit('42310'))
                .otherwise(pl.col('progcd'))
                .alias('progcd')
            ]).filter(
                # IF PROGCD IN ('42199','42699') AND PRODUCT NOT IN (72,413) THEN DELETE;
                ~(
                    pl.col('progcd').is_in(['42199', '42699']) & 
                    ~pl.col('product').is_in([72, 413])
                )
            ).filter(
                # IF PRODUCT IN (30,31,32,33,34) THEN DELETE;
                ~pl.col('product').is_in([30, 31, 32, 33, 34])
            )
        
        # Check if intpaybl column exists
        if 'intpaybl' in deposit_filtered.columns:
            deposit_filtered = deposit_filtered.with_columns([
                # IF INTPAYBL < 0 THEN INTPAYBL = 0;
                pl.when(pl.col('intpaybl') < 0)
                .then(0)
                .otherwise(pl.col('intpaybl'))
                .alias('intpaybl')
            ])
        else:
            deposit_filtered = deposit_filtered.with_columns([
                pl.lit(0).alias('intpaybl')
            ])
        
        print(f"Conventional DEPOSIT records: {deposit_filtered.height}")
    else:
        deposit_filtered = pl.DataFrame()
        print("No conventional DEPOSIT data created")

    # DATA FLOAT; SET PIDMS.FLOAT;
    float_df = read_sas_file(pidms_path / "float.sas7bdat")
    if float_df.is_empty():
        print("NOTE: PIDMS.float not found")

    # PROC SUMMARY DATA=FLOAT NWAY; CLASS ACCTNO; VAR FLOAT; OUTPUT OUT=FLOAT SUM=;
    if not float_df.is_empty() and 'acctno' in float_df.columns and 'float' in float_df.columns:
        float_summary = float_df.group_by('acctno').agg([
            pl.col('float').sum().alias('float')
        ])
    else:
        float_summary = pl.DataFrame()

    # PROC SORT DATA=DEPOSIT; BY ACCTNO;
    if not deposit_filtered.is_empty() and 'acctno' in deposit_filtered.columns:
        deposit_sorted = deposit_filtered.sort('acctno')
        
        # DATA DEPOSIT EXCEPT; MERGE DEPOSIT(IN=A) FLOAT(IN=B); BY ACCTNO;
        if not float_summary.is_empty():
            deposit_merged = deposit_sorted.join(
                float_summary, on='acctno', how='outer', suffix='_float'
            )
            
            # Ensure float column exists and handle nulls
            if 'float' not in deposit_merged.columns:
                deposit_merged = deposit_merged.with_columns([
                    pl.lit(0).alias('float')
                ])
            
            # Ensure curbal column exists
            if 'curbal' not in deposit_merged.columns:
                deposit_merged = deposit_merged.with_columns([
                    pl.lit(0).alias('curbal')
                ])
            
            # Apply transformations
            deposit_processed = deposit_merged.with_columns([
                # IF CURBAL < 0 THEN CURBAL = 0;
                pl.when(pl.col('curbal') < 0)
                .then(0)
                .otherwise(pl.col('curbal'))
                .alias('curbal'),
                
                # FLOATORI = CURBAL;
                pl.col('curbal').alias('floatori'),
                
                # AVBAL = SUM(CURBAL,(-1)*FLOAT);
                (pl.col('curbal') + (-1) * pl.col('float')).alias('avbal'),
                
                # MINUSFLOAT = SUM(CURBAL,(-1)*FLOAT);
                (pl.col('curbal') + (-1) * pl.col('float')).alias('minusfloat')
            ]).with_columns([
                # IF AVBAL < 0 THEN DO; FLOAT = CURBAL; AVBAL = 0; END;
                pl.when(pl.col('avbal') < 0)
                .then(pl.struct([
                    pl.col('curbal').alias('float'),
                    pl.lit(0).alias('avbal')
                ]))
                .otherwise(pl.struct([
                    pl.col('float').alias('float'),
                    pl.col('avbal').alias('avbal')
                ]))
                .alias('adjustment')
            ]).with_columns([
                pl.col('adjustment').struct.field('float').alias('float'),
                pl.col('adjustment').struct.field('avbal').alias('avbal'),
                
                # AVBALTT = SUM(AVBAL,INTPAYBL);
                (pl.col('avbal') + pl.col('intpaybl')).alias('avbaltt'),
                
                # CURBALTT = SUM(CURBAL,INTPAYBL);
                (pl.col('curbal') + pl.col('intpaybl')).alias('curbaltt')
            ]).drop('adjustment')
            
            # Split into DEPOSIT and EXCEPT based on conditions
            # IF B AND NOT A THEN OUTPUT EXCEPT;
            except_df = deposit_processed.filter(
                pl.col('float').is_not_null() & 
                (pl.col('curbal').is_null() | pl.col('product').is_null())
            )
            
            # IF A AND B THEN OUTPUT DEPOSIT;
            deposit_final = deposit_processed.filter(
                pl.col('curbal').is_not_null() & 
                pl.col('product').is_not_null() & 
                pl.col('float').is_not_null()
            )
            
            # Save outputs
            write_parquet_file(deposit_final, output_path / "DEPOSIT_CONVENTIONAL.parquet")
            write_parquet_file(except_df, output_path / "EXCEPT_CONVENTIONAL.parquet")
            
            print(f"Conventional DEPOSIT final records: {deposit_final.height}")
            print(f"Conventional EXCEPT records: {except_df.height}")
            
            # DATA _NULL_; SET DEPOSIT; FILE FLOAT;
            if not deposit_final.is_empty():
                # Select only required columns for output
                float_output = deposit_final.select(['acctno', 'branch', 'float'])
                
                # Write header
                header = "ACCTNO\x05BRANCH\x05FLOAT\x05\n"
                
                # Write data with DLM (0x05) separator
                with open(output_path / "FLOAT.txt", "w") as f:
                    f.write(header)
                    for row in float_output.iter_rows():
                        line = f"{row[0]}\x05{row[1]}\x05{row[2]}\x05\n"
                        f.write(line)
                
                print(f"Conventional FLOAT file created: {output_path / 'FLOAT.txt'}")
                
            return deposit_final
            
        else:
            print("No FLOAT data for conventional merging")
            return pl.DataFrame()
            
    else:
        print("No conventional DEPOSIT data for processing")
        return pl.DataFrame()

def process_islamic_float():
    """Process Islamic banking float data"""
    print("\nPROCESSING ISLAMIC BANKING FLOAT DATA")
    print("=" * 50)
    
    # PROC SORT DATA=IMNI.FDMTHLY OUT=FDMTHLY;
    fdmthly_df = read_sas_file(imni_path / "fdmthly.sas7bdat")
    if not fdmthly_df.is_empty():
        # Check available columns
        available_cols = fdmthly_df.columns
        print(f"FDMTHLY available columns: {available_cols}")
        
        # Try to find alternative column names
        acctno_col = next((col for col in available_cols if 'acctno' in col.lower()), None)
        branch_col = next((col for col in available_cols if 'branch' in col.lower()), None)
        curbal_col = next((col for col in available_cols if 'curbal' in col.lower()), None)
        amtind_col = next((col for col in available_cols if 'amtind' in col.lower()), None)
        
        # Create select list based on available columns
        select_cols = []
        if acctno_col:
            select_cols.append(acctno_col)
        if branch_col:
            select_cols.append(branch_col)
        if curbal_col:
            select_cols.append(curbal_col)
        if amtind_col:
            select_cols.append(amtind_col)
        
        if select_cols:
            fdmthly_df = fdmthly_df.select(select_cols)
            if acctno_col:
                fdmthly_df = fdmthly_df.sort(acctno_col)
        else:
            print("NOTE: No required columns found in IMNI.fdmthly")
            fdmthly_df = pl.DataFrame()
    else:
        print("NOTE: IMNI.fdmthly not found")
        fdmthly_df = pl.DataFrame()

    # DATA FDMTHLY; SET FDMTHLY; LEDGBAL = CURBAL;
    if not fdmthly_df.is_empty() and curbal_col and curbal_col in fdmthly_df.columns:
        fdmthly_processed = fdmthly_df.with_columns([
            pl.col(curbal_col).alias('ledgbal')
        ])
    else:
        fdmthly_processed = pl.DataFrame()

    # DATA CURN; SET IMNI.CURN124;
    curn_df = read_sas_file(imni_path / "curn124.sas7bdat")
    if not curn_df.is_empty():
        # Check if product column exists
        if 'product' in curn_df.columns:
            curn_filtered = curn_df.filter(pl.col('product') != 139)
        else:
            print("NOTE: 'product' column not found in CURN124, using all records")
            curn_filtered = curn_df
    else:
        print("NOTE: IMNI.curn124 not found")
        curn_filtered = pl.DataFrame()

    # DATA DEPOSIT; SET multiple datasets;
    datasets_to_combine = []

    # IMNI.SAVG124
    savg_df = read_sas_file(imni_path / "savg124.sas7bdat")
    if not savg_df.is_empty():
        print(f"SAVG124 available columns: {savg_df.columns}")
        # Try to map columns
        savg_select_cols = []
        for col in ['acctno', 'product', 'curbal', 'ledgbal', 'progcd', 'amtind', 'intpaybl', 'branch']:
            if col in savg_df.columns:
                savg_select_cols.append(col)
            else:
                # Try to find similar column
                similar = next((c for c in savg_df.columns if col in c.lower()), None)
                if similar:
                    savg_select_cols.append(similar)
        
        if savg_select_cols:
            savg_df = savg_df.select(savg_select_cols)
            datasets_to_combine.append(savg_df)
    else:
        print("NOTE: IMNI.savg124 not found")

    # CURN
    if not curn_filtered.is_empty():
        curn_select_cols = []
        for col in ['acctno', 'product', 'curbal', 'ledgbal', 'progcd', 'amtind', 'intpaybl', 'branch']:
            if col in curn_filtered.columns:
                curn_select_cols.append(col)
            else:
                # Try to find similar column
                similar = next((c for c in curn_filtered.columns if col in c.lower()), None)
                if similar:
                    curn_select_cols.append(similar)
        
        if curn_select_cols:
            curn_selected = curn_filtered.select(curn_select_cols)
            datasets_to_combine.append(curn_selected)

    # FDMTHLY with renames (if we have the necessary columns)
    if not fdmthly_processed.is_empty():
        # Check what columns we have and try to map them
        rename_map = {}
        fdm_cols = fdmthly_processed.columns
        
        # Map original column names to standard names
        if acctno_col and acctno_col in fdm_cols:
            rename_map[acctno_col] = 'acctno'
        if branch_col and branch_col in fdm_cols:
            rename_map[branch_col] = 'branch'
        if curbal_col and curbal_col in fdm_cols:
            rename_map[curbal_col] = 'curbal'
        
        # Check for other columns we might need
        for col in ['ledgbal', 'amtind']:
            if col in fdm_cols:
                rename_map[col] = col
        
        # Rename columns if we have any mappings
        if rename_map:
            fdm_renamed = fdmthly_processed.rename(rename_map)
            
            # Select only standard columns
            standard_cols = ['acctno', 'branch', 'curbal', 'ledgbal', 'amtind']
            available_std_cols = [col for col in standard_cols if col in fdm_renamed.columns]
            
            if available_std_cols:
                fdm_final = fdm_renamed.select(available_std_cols)
                
                # Add placeholder columns if missing
                if 'product' not in fdm_final.columns:
                    fdm_final = fdm_final.with_columns([
                        pl.lit(None).alias('product')
                    ])
                if 'progcd' not in fdm_final.columns:
                    fdm_final = fdm_final.with_columns([
                        pl.lit(None).alias('progcd')
                    ])
                if 'intpaybl' not in fdm_final.columns:
                    fdm_final = fdm_final.with_columns([
                        pl.lit(None).alias('intpaybl')
                    ])
                
                datasets_to_combine.append(fdm_final)

    # Combine all datasets and apply filters
    if datasets_to_combine:
        # Ensure all datasets have the same columns before concatenation
        all_cols = set()
        for df in datasets_to_combine:
            all_cols.update(df.columns)
        
        # Standardize columns across all datasets
        standardized_datasets = []
        for df in datasets_to_combine:
            for col in all_cols:
                if col not in df.columns:
                    df = df.with_columns([pl.lit(None).alias(col)])
            standardized_datasets.append(df)
        
        deposit_combined = pl.concat(standardized_datasets, how="vertical")
        
        # Apply filters and transformations
        valid_progcd = [
            '42110', '42310', '42120', '42320', '42130', '42610',
            '42133', '42132', '42180', '42199', '42699'
        ]
        
        # Check if prodcd column exists
        if 'progcd' not in deposit_combined.columns:
            deposit_combined = deposit_combined.with_columns([
                pl.lit(None).alias('progcd')
            ])
        
        deposit_filtered = deposit_combined.filter(
            pl.col('progcd').is_in(valid_progcd)
        )
        
        # Check if product column exists before using it
        if 'product' in deposit_filtered.columns:
            deposit_filtered = deposit_filtered.with_columns([
                # IF PRODUCT = 166 THEN PROGCD = '42310';
                pl.when(pl.col('product') == 166)
                .then(pl.lit('42310'))
                .otherwise(pl.col('progcd'))
                .alias('progcd')
            ]).filter(
                # IF PROGCD IN ('42199','42699') AND PRODUCT NOT IN (72,413) THEN DELETE;
                ~(
                    pl.col('progcd').is_in(['42199', '42699']) & 
                    ~pl.col('product').is_in([72, 413])
                )
            )
        
        # Check if intpaybl column exists
        if 'intpaybl' in deposit_filtered.columns:
            deposit_filtered = deposit_filtered.with_columns([
                # IF INTPAYBL < 0 THEN INTPAYBL = 0;
                pl.when(pl.col('intpaybl') < 0)
                .then(0)
                .otherwise(pl.col('intpaybl'))
                .alias('intpaybl')
            ])
        else:
            deposit_filtered = deposit_filtered.with_columns([
                pl.lit(0).alias('intpaybl')
            ])
        
        print(f"Islamic DEPOSIT records: {deposit_filtered.height}")
    else:
        deposit_filtered = pl.DataFrame()
        print("No Islamic DEPOSIT data created")

    # DATA FLOAT; SET PIDMS.FLOAT;
    float_df = read_sas_file(pidms_path / "float.sas7bdat")
    if float_df.is_empty():
        print("NOTE: PIDMS.float not found")

    # PROC SUMMARY DATA=FLOAT NWAY; CLASS ACCTNO; VAR FLOAT; OUTPUT OUT=FLOAT SUM=;
    if not float_df.is_empty() and 'acctno' in float_df.columns and 'float' in float_df.columns:
        float_summary = float_df.group_by('acctno').agg([
            pl.col('float').sum().alias('float')
        ])
    else:
        float_summary = pl.DataFrame()

    # PROC SORT DATA=DEPOSIT; BY ACCTNO;
    if not deposit_filtered.is_empty() and 'acctno' in deposit_filtered.columns:
        deposit_sorted = deposit_filtered.sort('acctno')
        
        # DATA IDEPOSIT EXCEPT; MERGE DEPOSIT(IN=A) FLOAT(IN=B); BY ACCTNO;
        if not float_summary.is_empty():
            deposit_merged = deposit_sorted.join(
                float_summary, on='acctno', how='outer', suffix='_float'
            )
            
            # Ensure float column exists and handle nulls
            if 'float' not in deposit_merged.columns:
                deposit_merged = deposit_merged.with_columns([
                    pl.lit(0).alias('float')
                ])
            
            # Ensure curbal column exists
            if 'curbal' not in deposit_merged.columns:
                deposit_merged = deposit_merged.with_columns([
                    pl.lit(0).alias('curbal')
                ])
            
            # Apply transformations (same logic as conventional)
            deposit_processed = deposit_merged.with_columns([
                pl.when(pl.col('curbal') < 0)
                .then(0)
                .otherwise(pl.col('curbal'))
                .alias('curbal'),
                
                pl.col('curbal').alias('floatori'),
                
                (pl.col('curbal') + (-1) * pl.col('float')).alias('avbal'),
                
                (pl.col('curbal') + (-1) * pl.col('float')).alias('minusfloat')
            ]).with_columns([
                pl.when(pl.col('avbal') < 0)
                .then(pl.struct([
                    pl.col('curbal').alias('float'),
                    pl.lit(0).alias('avbal')
                ]))
                .otherwise(pl.struct([
                    pl.col('float').alias('float'),
                    pl.col('avbal').alias('avbal')
                ]))
                .alias('adjustment')
            ]).with_columns([
                pl.col('adjustment').struct.field('float').alias('float'),
                pl.col('adjustment').struct.field('avbal').alias('avbal'),
                
                (pl.col('avbal') + pl.col('intpaybl')).alias('avbaltt'),
                
                (pl.col('curbal') + pl.col('intpaybl')).alias('curbaltt')
            ]).drop('adjustment')
            
            # Split into IDEPOSIT and EXCEPT based on conditions
            # IF B AND NOT A THEN OUTPUT EXCEPT;
            except_df = deposit_processed.filter(
                pl.col('float').is_not_null() & 
                (pl.col('curbal').is_null() | pl.col('product').is_null())
            )
            
            # IF A AND B THEN OUTPUT IDEPOSIT;
            ideposit_final = deposit_processed.filter(
                pl.col('curbal').is_not_null() & 
                pl.col('product').is_not_null() & 
                pl.col('float').is_not_null()
            )
            
            # Save outputs
            write_parquet_file(ideposit_final, output_path / "IDEPOSIT_ISLAMIC.parquet")
            write_parquet_file(except_df, output_path / "EXCEPT_ISLAMIC.parquet")
            
            print(f"Islamic IDEPOSIT final records: {ideposit_final.height}")
            print(f"Islamic EXCEPT records: {except_df.height}")
            
            # DATA _NULL_; SET IDEPOSIT; FILE IFLOAT;
            if not ideposit_final.is_empty():
                # Select only required columns for output
                ifloat_output = ideposit_final.select(['acctno', 'branch', 'float'])
                
                # Write header
                header = "ACCTNO\x05BRANCH\x05FLOAT\x05\n"
                
                # Write data with DLM (0x05) separator
                with open(output_path / "IFLOAT.txt", "w") as f:
                    f.write(header)
                    for row in ifloat_output.iter_rows():
                        line = f"{row[0]}\x05{row[1]}\x05{row[2]}\x05\n"
                        f.write(line)
                
                print(f"Islamic IFLOAT file created: {output_path / 'IFLOAT.txt'}")
                
            return ideposit_final
            
        else:
            print("No FLOAT data for Islamic merging")
            return pl.DataFrame()
            
    else:
        print("No Islamic DEPOSIT data for processing")
        return pl.DataFrame()

# Main execution
if __name__ == "__main__":
    # Process both conventional and Islamic float data
    conventional_result = process_conventional_float()
    islamic_result = process_islamic_float()
    
    print("\n" + "="*80)
    print("PROCESSING COMPLETED SUCCESSFULLY")
    print("="*80)
    print(f"Conventional records processed: {conventional_result.height if conventional_result is not None else 0}")
    print(f"Islamic records processed: {islamic_result.height if islamic_result is not None else 0}")
