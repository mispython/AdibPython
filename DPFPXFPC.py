import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
import pyreadstat
import saspy
from NPGSRPT import npgs_report

def eibrsmee():
    npgs_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRSMEE")
    output = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRSMEE")
    
    # Create output directory if it doesn't exist
    output.mkdir(parents=True, exist_ok=True)
    
    # Initialize SAS session
    sas = saspy.SASsession()
    
    # Get previous day's date (instead of reading REPTDATE)
    reptdate = datetime.now() - timedelta(days=1)
    
    mm = reptdate.month
    mm1 = mm - 1 if mm > 1 else 12
    
    reptmon = f"{mm:02d}"
    reptmon1 = f"{mm1:02d}"
    reptyear = str(reptdate.year)
    reptday = f"{reptdate.day:02d}"
    rdate = reptdate.strftime("%d%m%y")
    
    print(f"REPTMON: {reptmon}, RDATE: {rdate}")
    
    # Read LNSMEE SAS dataset
    try:
        npgs_df, meta = pyreadstat.read_sas7bdat(
            npgs_path / f"lnsmee{reptmon}.sas7bdat"
        )
        # Convert to Polars DataFrame
        npgs_df = pl.from_pandas(npgs_df)
        # Convert column names to lowercase
        npgs_df = npgs_df.rename({col: col.lower() for col in npgs_df.columns})
    except Exception as e:
        print(f"File not found or error reading: NPGS/lnsmee{reptmon}.sas7bdat - {e}")
        return
    
    # Create SMEE dataset
    smee_df = npgs_df.with_columns(
        pl.col("cvar13").alias("ndate"),
        pl.col("cvar12").alias("status")
    ).select(["cvar01", "cvar06", "status", "ndate"])
    
    # Write SMEE dataset in multiple formats
    smee_df.write_parquet(output / "smee.parquet")
    smee_df.write_csv(output / "smee.csv")
    
    # Write SMEE as SAS dataset using saspy
    smee_pd = smee_df.to_pandas()
    sas.df2sd(smee_pd, table='smee', libref='work')
    sas.submit(f"""
        PROC EXPORT DATA=work.smee
            OUTFILE="{output / 'smee.sas7bdat'}"
            DBMS=SAS7BDAT REPLACE;
        RUN;
    """)
    
    # Process NPGS data
    npgs_df = npgs_df.with_columns(
        pl.lit(" " * 10).alias("cvarx1"),
        pl.lit(" " * 10).alias("cvarx2"),
        pl.lit(" " * 4).alias("cvarx3"),
        pl.when(pl.col("cvar12") == "NPL").then("NP").otherwise("AP").alias("cvar12a")
    )
    
    # Filter for SCH=E5
    npgs_df = npgs_df.filter(
        (pl.col("cvar02") == "E5") &
        (pl.col("natguar") == "06") &
        (pl.col("cinstcl") == "18")
    )
    
    npgs_df = npgs_df.with_columns(pl.lit(" " * 10).alias("cvarxx"))
    npgs_df = npgs_df.sort(["cvar01", "cvar06"])
    
    # Ensure branch column exists for the report (if not in source data)
    if "branch" not in npgs_df.columns:
        npgs_df = npgs_df.with_columns(pl.lit(0).alias("branch"))
    
    # Write SC5T file as text (CSV format with semicolons)
    with open(output / "sc5t.txt", 'w') as f:
        for row in npgs_df.iter_rows(named=True):
            cvar01 = f"{row.get('cvar01', 0):10.0f}"
            cvar02 = f"{row.get('cvar02', ''):2s}"
            cvar03 = f"{row.get('cvar03', ''):15s}"
            cvar04 = f"{row.get('cvar04', ''):50s}"
            
            # cvar05 date
            cvar05 = " " * 10
            if 'cvar05' in row and row['cvar05']:
                try:
                    if hasattr(row['cvar05'], 'strftime'):
                        cvar05 = row['cvar05'].strftime("%d/%m/%Y")
                    else:
                        cvar05 = str(row['cvar05']).rjust(10)
                except:
                    cvar05 = " " * 10
            
            cvarxx = " " * 10
            cvar06 = f"{row.get('cvar06', 0):10.0f}"
            cvar07 = f"{row.get('cvar07', ''):2s}"
            cvar08 = f"{row.get('cvar08', 0):13.2f}"
            cvar09 = f"{row.get('cvar09', 0):13.2f}"
            cvar10 = f"{row.get('cvar10', 0):13.2f}"
            cvar11 = f"{row.get('cvar11', 0):5.0f}"
            cvar12 = f"{row.get('cvar12', ''):3s}"
            cvar13 = f"{row.get('cvar13', ''):10s}"
            cvar14 = f"{row.get('cvar14', ''):4s}"
            cvar15 = f"{row.get('cvar15', ''):5s}"
            
            line = f"{cvar01};{cvar02};{cvar03};{cvar04};{cvar05};{cvarxx};" \
                   f"{cvar06};{cvar07};{cvar08};{cvar09};{cvar10};{cvar11};" \
                   f"{cvar12};{cvar13};{cvar14};{cvar15};"
            f.write(line + "\n")
    
    # Write NPGS as Parquet
    npgs_df.write_parquet(output / "npgs_filtered.parquet")
    
    # Write NPGS as SAS dataset using saspy
    npgs_pd = npgs_df.to_pandas()
    sas.df2sd(npgs_pd, table='npgs_filtered', libref='work')
    sas.submit(f"""
        PROC EXPORT DATA=work.npgs_filtered
            OUTFILE="{output / 'npgs_filtered.sas7bdat'}"
            DBMS=SAS7BDAT REPLACE;
        RUN;
    """)
    
    # Generate report using NPGSRPT module
    print("=" * 60)
    print("PUBLIC BANK BERHAD")
    print(f"DETAIL OF ACCTS (SCH=E5) FOR SUBMISSION TO CGC @ {rdate}")
    print("=" * 60)
    
    # Prepare titles for the report
    title1 = "PUBLIC BANK BERHAD"
    title2 = f"DETAIL OF ACCTS (SCH=E5) FOR SUBMISSION TO CGC @ {rdate}"
    
    # Generate the report using npgs_report function
    report_path = output / "sc5r.txt"
    npgs_report(
        df=npgs_df,
        report_path=str(report_path),
        title1=title1,
        title2=title2
    )
    
    print(f"Processing complete. Files created in {output}:")
    print("- sc5t.txt (text format)")
    print("- sc5r.txt (report)")
    print("- smee.parquet, smee.sas7bdat, smee.csv")
    print("- npgs_filtered.parquet, npgs_filtered.sas7bdat")
    
    # Close SAS session
    sas.endsas()

if __name__ == "__main__":
    eibrsmee()
