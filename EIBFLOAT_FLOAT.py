import polars as pl
import duckdb
from pathlib import Path

# Configuration
mni_path = Path("MNI")
imni_path = Path("IMNI")
pidms_path = Path("PIDMS")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

def process_conventional_float():
    """Process conventional banking float data"""
    print("PROCESSING CONVENTIONAL BANKING FLOAT DATA")
    print("=" * 50)
    
    # PROC SORT DATA=MNI.FDMTHLY OUT=FDMTHLY;
    try:
        fdmthly_df = pl.read_parquet(mni_path / "FDMTHLY.parquet").select([
            'ACCTNO', 'BRANCH', 'INTPLAN', 'CURBAL', 'BIC', 'AMTIND', 'INTPAY'
        ]).sort('ACCTNO')
    except FileNotFoundError:
        print("NOTE: MNI.FDMTHLY not found")
        fdmthly_df = pl.DataFrame()

    # DATA FDMTHLY; SET FDMTHLY; LEDGBAL = CURBAL;
    if not fdmthly_df.is_empty():
        fdmthly_processed = fdmthly_df.with_columns([
            pl.col('CURBAL').alias('LEDGBAL')
        ])
    else:
        fdmthly_processed = pl.DataFrame()

    # DATA CURN; SET MNI.CURN124;
    try:
        curn_df = pl.read_parquet(mni_path / "CURN124.parquet")
        # IF PRODUCT = 139 THEN DELETE;
        curn_filtered = curn_df.filter(pl.col('PRODUCT') != 139)
    except FileNotFoundError:
        print("NOTE: MNI.CURN124 not found")
        curn_filtered = pl.DataFrame()

    # DATA DEPOSIT; SET multiple datasets;
    datasets_to_combine = []

    # MNI.SAVG124
    try:
        savg_df = pl.read_parquet(mni_path / "SAVG124.parquet").select([
            'ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'
        ])
        datasets_to_combine.append(savg_df)
    except FileNotFoundError:
        print("NOTE: MNI.SAVG124 not found")

    # CURN
    if not curn_filtered.is_empty():
        curn_selected = curn_filtered.select([
            'ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'
        ])
        datasets_to_combine.append(curn_selected)

    # FDMTHLY with renames
    if not fdmthly_processed.is_empty():
        fdmthly_renamed = fdmthly_processed.select([
            'ACCTNO', 'BRANCH', 'CURBAL', 'LEDGBAL', 'AMTIND'
        ]).with_columns([
            pl.col('INTPLAN').alias('PRODUCT'),
            pl.col('BIC').alias('PRODCD'),
            pl.col('INTPAY').alias('INTPAYBL')
        ])
        datasets_to_combine.append(fdmthly_renamed)

    # Combine all datasets and apply filters
    if datasets_to_combine:
        deposit_combined = pl.concat(datasets_to_combine, how="diagonal")
        
        # Apply filters and transformations
        valid_prodcd = [
            '42110', '42310', '42120', '42320', '42130',
            '42133', '42132', '42180', '42610', '42630', '34180',
            '42199', '42699'
        ]
        
        deposit_filtered = deposit_combined.filter(
            pl.col('PRODCD').is_in(valid_prodcd)
        ).with_columns([
            # IF PRODUCT = 166 THEN PRODCD = '42310';
            pl.when(pl.col('PRODUCT') == 166)
            .then(pl.lit('42310'))
            .otherwise(pl.col('PRODCD'))
            .alias('PRODCD')
        ]).filter(
            # IF PRODCD IN ('42199','42699') AND PRODUCT NOT IN (72,413) THEN DELETE;
            ~(
                pl.col('PRODCD').is_in(['42199', '42699']) & 
                ~pl.col('PRODUCT').is_in([72, 413])
            )
        ).filter(
            # IF PRODUCT IN (30,31,32,33,34) THEN DELETE;
            ~pl.col('PRODUCT').is_in([30, 31, 32, 33, 34])
        ).with_columns([
            # IF INTPAYBL < 0 THEN INTPAYBL = 0;
            pl.when(pl.col('INTPAYBL') < 0)
            .then(0)
            .otherwise(pl.col('INTPAYBL'))
            .alias('INTPAYBL')
        ])
        
        print(f"Conventional DEPOSIT records: {deposit_filtered.height}")
    else:
        deposit_filtered = pl.DataFrame()
        print("No conventional DEPOSIT data created")

    # DATA FLOAT; SET PIDMS.FLOAT;
    try:
        float_df = pl.read_parquet(pidms_path / "FLOAT.parquet")
    except FileNotFoundError:
        print("NOTE: PIDMS.FLOAT not found")
        float_df = pl.DataFrame()

    # PROC SUMMARY DATA=FLOAT NWAY; CLASS ACCTNO; VAR FLOAT; OUTPUT OUT=FLOAT SUM=;
    if not float_df.is_empty():
        float_summary = float_df.group_by('ACCTNO').agg([
            pl.col('FLOAT').sum().alias('FLOAT')
        ])
    else:
        float_summary = pl.DataFrame()

    # PROC SORT DATA=DEPOSIT; BY ACCTNO;
    if not deposit_filtered.is_empty():
        deposit_sorted = deposit_filtered.sort('ACCTNO')
        
        # DATA DEPOSIT EXCEPT; MERGE DEPOSIT(IN=A) FLOAT(IN=B); BY ACCTNO;
        if not float_summary.is_empty():
            deposit_merged = deposit_sorted.join(
                float_summary, on='ACCTNO', how='outer', suffix='_float'
            )
            
            # Apply transformations
            deposit_processed = deposit_merged.with_columns([
                # IF CURBAL < 0 THEN CURBAL = 0;
                pl.when(pl.col('CURBAL') < 0)
                .then(0)
                .otherwise(pl.col('CURBAL'))
                .alias('CURBAL'),
                
                # FLOATORI = CURBAL;
                pl.col('CURBAL').alias('FLOATORI'),
                
                # AVBAL = SUM(CURBAL,(-1)*FLOAT);
                (pl.col('CURBAL') + (-1) * pl.col('FLOAT')).alias('AVBAL'),
                
                # MINUSFLOAT = SUM(CURBAL,(-1)*FLOAT);
                (pl.col('CURBAL') + (-1) * pl.col('FLOAT')).alias('MINUSFLOAT')
            ]).with_columns([
                # IF AVBAL < 0 THEN DO; FLOAT = CURBAL; AVBAL = 0; END;
                pl.when(pl.col('AVBAL') < 0)
                .then(pl.struct([
                    pl.col('CURBAL').alias('FLOAT'),
                    pl.lit(0).alias('AVBAL')
                ]))
                .otherwise(pl.struct([
                    pl.col('FLOAT').alias('FLOAT'),
                    pl.col('AVBAL').alias('AVBAL')
                ]))
                .alias('adjustment')
            ]).with_columns([
                pl.col('adjustment').struct.field('FLOAT').alias('FLOAT'),
                pl.col('adjustment').struct.field('AVBAL').alias('AVBAL'),
                
                # AVBALTT = SUM(AVBAL,INTPAYBL);
                (pl.col('AVBAL') + pl.col('INTPAYBL')).alias('AVBALTT'),
                
                # CURBALTT = SUM(CURBAL,INTPAYBL);
                (pl.col('CURBAL') + pl.col('INTPAYBL')).alias('CURBALTT')
            ]).drop('adjustment')
            
            # Split into DEPOSIT and EXCEPT based on conditions
            # IF B AND NOT A THEN OUTPUT EXCEPT;
            except_df = deposit_processed.filter(
                pl.col('FLOAT').is_not_null() & 
                (pl.col('CURBAL').is_null() | pl.col('PRODUCT').is_null())
            )
            
            # IF A AND B THEN OUTPUT DEPOSIT;
            deposit_final = deposit_processed.filter(
                pl.col('CURBAL').is_not_null() & 
                pl.col('PRODUCT').is_not_null() & 
                pl.col('FLOAT').is_not_null()
            )
            
            # Save outputs
            deposit_final.write_parquet(output_path / "DEPOSIT_CONVENTIONAL.parquet")
            except_df.write_parquet(output_path / "EXCEPT_CONVENTIONAL.parquet")
            
            print(f"Conventional DEPOSIT final records: {deposit_final.height}")
            print(f"Conventional EXCEPT records: {except_df.height}")
            
            # DATA _NULL_; SET DEPOSIT; FILE FLOAT;
            if not deposit_final.is_empty():
                # Select only required columns for output
                float_output = deposit_final.select(['ACCTNO', 'BRANCH', 'FLOAT'])
                
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
    try:
        fdmthly_df = pl.read_parquet(imni_path / "FDMTHLY.parquet").select([
            'ACCTNO', 'BRANCH', 'INTPLAN', 'CURBAL', 'BIC', 'AMTIND', 'INTPAY'
        ]).sort('ACCTNO')
    except FileNotFoundError:
        print("NOTE: IMNI.FDMTHLY not found")
        fdmthly_df = pl.DataFrame()

    # DATA FDMTHLY; SET FDMTHLY; LEDGBAL = CURBAL;
    if not fdmthly_df.is_empty():
        fdmthly_processed = fdmthly_df.with_columns([
            pl.col('CURBAL').alias('LEDGBAL')
        ])
    else:
        fdmthly_processed = pl.DataFrame()

    # DATA CURN; SET IMNI.CURN124;
    try:
        curn_df = pl.read_parquet(imni_path / "CURN124.parquet")
        # IF PRODUCT = 139 THEN DELETE;
        curn_filtered = curn_df.filter(pl.col('PRODUCT') != 139)
    except FileNotFoundError:
        print("NOTE: IMNI.CURN124 not found")
        curn_filtered = pl.DataFrame()

    # DATA DEPOSIT; SET multiple datasets;
    datasets_to_combine = []

    # IMNI.SAVG124
    try:
        savg_df = pl.read_parquet(imni_path / "SAVG124.parquet").select([
            'ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'
        ])
        datasets_to_combine.append(savg_df)
    except FileNotFoundError:
        print("NOTE: IMNI.SAVG124 not found")

    # CURN
    if not curn_filtered.is_empty():
        curn_selected = curn_filtered.select([
            'ACCTNO', 'PRODUCT', 'CURBAL', 'LEDGBAL', 'PRODCD', 'AMTIND', 'INTPAYBL', 'BRANCH'
        ])
        datasets_to_combine.append(curn_selected)

    # FDMTHLY with renames
    if not fdmthly_processed.is_empty():
        fdmthly_renamed = fdmthly_processed.select([
            'ACCTNO', 'BRANCH', 'CURBAL', 'LEDGBAL', 'AMTIND'
        ]).with_columns([
            pl.col('INTPLAN').alias('PRODUCT'),
            pl.col('BIC').alias('PRODCD'),
            pl.col('INTPAY').alias('INTPAYBL')
        ])
        datasets_to_combine.append(fdmthly_renamed)

    # Combine all datasets and apply filters
    if datasets_to_combine:
        deposit_combined = pl.concat(datasets_to_combine, how="diagonal")
        
        # Apply filters and transformations
        valid_prodcd = [
            '42110', '42310', '42120', '42320', '42130', '42610',
            '42133', '42132', '42180', '42199', '42699'
        ]
        
        deposit_filtered = deposit_combined.filter(
            pl.col('PRODCD').is_in(valid_prodcd)
        ).with_columns([
            # IF PRODUCT = 166 THEN PRODCD = '42310';
            pl.when(pl.col('PRODUCT') == 166)
            .then(pl.lit('42310'))
            .otherwise(pl.col('PRODCD'))
            .alias('PRODCD')
        ]).filter(
            # IF PRODCD IN ('42199','42699') AND PRODUCT NOT IN (72,413) THEN DELETE;
            ~(
                pl.col('PRODCD').is_in(['42199', '42699']) & 
                ~pl.col('PRODUCT').is_in([72, 413])
            )
        ).with_columns([
            # IF INTPAYBL < 0 THEN INTPAYBL = 0;
            pl.when(pl.col('INTPAYBL') < 0)
            .then(0)
            .otherwise(pl.col('INTPAYBL'))
            .alias('INTPAYBL')
        ])
        
        print(f"Islamic DEPOSIT records: {deposit_filtered.height}")
    else:
        deposit_filtered = pl.DataFrame()
        print("No Islamic DEPOSIT data created")

    # DATA FLOAT; SET PIDMS.FLOAT;
    try:
        float_df = pl.read_parquet(pidms_path / "FLOAT.parquet")
    except FileNotFoundError:
        print("NOTE: PIDMS.FLOAT not found")
        float_df = pl.DataFrame()

    # PROC SUMMARY DATA=FLOAT NWAY; CLASS ACCTNO; VAR FLOAT; OUTPUT OUT=FLOAT SUM=;
    if not float_df.is_empty():
        float_summary = float_df.group_by('ACCTNO').agg([
            pl.col('FLOAT').sum().alias('FLOAT')
        ])
    else:
        float_summary = pl.DataFrame()

    # PROC SORT DATA=DEPOSIT; BY ACCTNO;
    if not deposit_filtered.is_empty():
        deposit_sorted = deposit_filtered.sort('ACCTNO')
        
        # DATA IDEPOSIT EXCEPT; MERGE DEPOSIT(IN=A) FLOAT(IN=B); BY ACCTNO;
        if not float_summary.is_empty():
            deposit_merged = deposit_sorted.join(
                float_summary, on='ACCTNO', how='outer', suffix='_float'
            )
            
            # Apply transformations (same logic as conventional)
            deposit_processed = deposit_merged.with_columns([
                pl.when(pl.col('CURBAL') < 0)
                .then(0)
                .otherwise(pl.col('CURBAL'))
                .alias('CURBAL'),
                
                pl.col('CURBAL').alias('FLOATORI'),
                
                (pl.col('CURBAL') + (-1) * pl.col('FLOAT')).alias('AVBAL'),
                
                (pl.col('CURBAL') + (-1) * pl.col('FLOAT')).alias('MINUSFLOAT')
            ]).with_columns([
                pl.when(pl.col('AVBAL') < 0)
                .then(pl.struct([
                    pl.col('CURBAL').alias('FLOAT'),
                    pl.lit(0).alias('AVBAL')
                ]))
                .otherwise(pl.struct([
                    pl.col('FLOAT').alias('FLOAT'),
                    pl.col('AVBAL').alias('AVBAL')
                ]))
                .alias('adjustment')
            ]).with_columns([
                pl.col('adjustment').struct.field('FLOAT').alias('FLOAT'),
                pl.col('adjustment').struct.field('AVBAL').alias('AVBAL'),
                
                (pl.col('AVBAL') + pl.col('INTPAYBL')).alias('AVBALTT'),
                
                (pl.col('CURBAL') + pl.col('INTPAYBL')).alias('CURBALTT')
            ]).drop('adjustment')
            
            # Split into IDEPOSIT and EXCEPT based on conditions
            # IF B AND NOT A THEN OUTPUT EXCEPT;
            except_df = deposit_processed.filter(
                pl.col('FLOAT').is_not_null() & 
                (pl.col('CURBAL').is_null() | pl.col('PRODUCT').is_null())
            )
            
            # IF A AND B THEN OUTPUT IDEPOSIT;
            ideposit_final = deposit_processed.filter(
                pl.col('CURBAL').is_not_null() & 
                pl.col('PRODUCT').is_not_null() & 
                pl.col('FLOAT').is_not_null()
            )
            
            # Save outputs
            ideposit_final.write_parquet(output_path / "IDEPOSIT_ISLAMIC.parquet")
            except_df.write_parquet(output_path / "EXCEPT_ISLAMIC.parquet")
            
            print(f"Islamic IDEPOSIT final records: {ideposit_final.height}")
            print(f"Islamic EXCEPT records: {except_df.height}")
            
            # DATA _NULL_; SET IDEPOSIT; FILE IFLOAT;
            if not ideposit_final.is_empty():
                # Select only required columns for output
                ifloat_output = ideposit_final.select(['ACCTNO', 'BRANCH', 'FLOAT'])
                
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
