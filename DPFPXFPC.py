import polars as pl
import pandas as pd
import pyreadstat
from pathlib import Path
from datetime import datetime, timedelta

def eibrsrgf():
    # Paths
    input_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRSRGF")
    output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRSRGF")
    
    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Get previous day's date (replacing REPTDATE)
    current_date = datetime.now() - timedelta(days=1)
    
    mm = current_date.month
    mm1 = mm - 1 if mm > 1 else 12
    
    reptmon = f"{mm:02d}"
    reptmon1 = f"{mm1:02d}"
    reptyear = str(current_date.year)
    reptday = f"{current_date.day:02d}"
    rdate = current_date.strftime("%d%m%y")
    ndate = f"{current_date.day:02d}{current_date.month:02d}"
    
    print(f"REPTMON: {reptmon}, RDATE: {rdate}")
    print(f"Processing date: {current_date.strftime('%Y-%m-%d')}")
    
    # Read CGCS data from SAS file using pyreadstat
    sas_file = input_path / f"lnnpgs{reptmon}.sas7bdat"
    
    try:
        # Read SAS file using pyreadstat
        df, meta = pyreadstat.read_sas7bdat(
            str(sas_file),
            encoding='latin1'
        )
        
        # Convert to polars DataFrame with lowercase column names
        cgcs_df = pl.from_pandas(df)
        cgcs_df = cgcs_df.rename({col: col.lower() for col in cgcs_df.columns})
        
        print(f"Successfully read {len(cgcs_df)} rows from {sas_file}")
        
    except FileNotFoundError:
        print(f"File not found: {sas_file}")
        return
    except Exception as e:
        print(f"Error reading SAS file: {e}")
        return
    
    # Filter for CVAR02 IN ('10','63')
    cgcs_df = cgcs_df.filter(pl.col("cvar02").is_in(["10", "63"]))
    cgcs_df = cgcs_df.with_columns(pl.lit(" " * 10).alias("cvarxx"))
    cgcs_df = cgcs_df.sort(["cvar01", "cvar06"])
    
    print(f"Filtered data: {len(cgcs_df)} rows after CVAR02 filter")
    
    # Write CGCSF text file (matching SAS PUT statement format)
    text_output_file = output_path / f"cgcsf_{reptmon}.txt"
    
    with open(text_output_file, 'w') as f:
        for row in cgcs_df.iter_rows(named=True):
            # Format each field according to SAS PUT statement
            cvar01 = f"{row.get('cvar01', 0):10.0f}"  # CVAR01 10.
            cvar02 = f"{str(row.get('cvar02', '')).ljust(2):2s}"  # CVAR02 $2.
            cvar03 = f"{str(row.get('cvar03', '')).ljust(15):15s}"  # CVAR03 $15.
            cvar04 = f"{str(row.get('cvar04', '')).ljust(50):50s}"  # CVAR04 $50.
            
            # CVAR05 date handling (DDMMYY10.)
            cvar05 = " " * 10
            if 'cvar05' in row and row['cvar05']:
                try:
                    if hasattr(row['cvar05'], 'strftime'):
                        cvar05 = row['cvar05'].strftime("%d/%m/%Y")
                    elif isinstance(row['cvar05'], (int, float)) and not pd.isna(row['cvar05']):
                        # Handle SAS date format (days since 1960-01-01)
                        sas_epoch = datetime(1960, 1, 1)
                        actual_date = sas_epoch + timedelta(days=int(row['cvar05']))
                        cvar05 = actual_date.strftime("%d/%m/%Y")
                    else:
                        cvar05 = str(row['cvar05']).rjust(10)
                except:
                    cvar05 = " " * 10
            
            cvarxx = " " * 10  # CVARXX $10.
            cvar06 = f"{row.get('cvar06', 0):10.0f}"  # CVAR06 10.
            cvar07 = f"{str(row.get('cvar07', '')).ljust(2):2s}"  # CVAR07 $2.
            cvar08 = f"{row.get('cvar08', 0):10.2f}"  # CVAR08 10.2
            cvar09 = f"{row.get('cvar09', 0):10.2f}"  # CVAR09 10.2
            cvar10 = f"{row.get('cvar10', 0):10.2f}"  # CVAR10 10.2
            cvar11 = f"{row.get('cvar11', 0):5.0f}"  # CVAR11 5.
            cvar12 = f"{str(row.get('cvar12', '')).ljust(3):3s}"  # CVAR12 $3.
            cvar13 = f"{str(row.get('cvar13', '')).ljust(10):10s}"  # CVAR13 $10.
            cvar14 = f"{str(row.get('cvar14', '')).ljust(4):4s}"  # CVAR14 $4.
            cvar15 = f"{str(row.get('cvar15', '')).ljust(5):5s}"  # CVAR15 $5.
            
            # Construct line with semicolons at exact positions
            line = f"{cvar01};{cvar02};{cvar03};{cvar04};{cvar05};{cvarxx};" \
                   f"{cvar06};{cvar07};{cvar08};{cvar09};{cvar10};{cvar11};" \
                   f"{cvar12};{cvar13};{cvar14};{cvar15};"
            f.write(line + "\n")
    
    print(f"Text file written: {text_output_file}")
    
    # Optionally write Parquet for archival
    parquet_output_file = output_path / f"cgcs_{reptmon}.parquet"
    cgcs_df.write_parquet(parquet_output_file)
    print(f"Parquet file written: {parquet_output_file}")
    
    print(f"Processing complete.")
    
    return {
        'text_file': str(text_output_file),
        'parquet_file': str(parquet_output_file),
        'rows_processed': len(cgcs_df)
    }

if __name__ == "__main__":
    result = eibrsrgf()
    if result:
        print("\nProcessing Summary:")
        print(f"Rows processed: {result['rows_processed']}")
        print(f"Output files:")
        print(f"  - Text: {result['text_file']}")
        print(f"  - Parquet: {result['parquet_file']}")
