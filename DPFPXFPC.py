def load_ibgpidm():
    """DATA IBGPIDM; INFILE IBGPIDM FIRSTOBS=1; 
       INPUT @01 ACCTNO 10. @12 IBGAMT 16.2; RUN;
       PROC SORT; BY ACCTNO;
       PROC SUMMARY DATA=IBGPIDM NWAY; BY ACCTNO; VAR IBGAMT; OUTPUT OUT=DEPOSIT.IBGPIDM SUM=; RUN;"""
    try:
        # Following SAS INFILE statement - read from text file
        # INFILE IBGPIDM FIRSTOBS=1;
        # INPUT @01 ACCTNO 10. @12 IBGAMT 16.2;
        
        ibgpidm_file = f"{PATHS['DEPOSIT']}IBGPIDM.txt"
        
        # Check if file exists
        if not Path(ibgpidm_file).exists():
            # Try alternative case
            ibgpidm_file = f"{PATHS['DEPOSIT']}ibgpidm.txt"
        
        if not Path(ibgpidm_file).exists():
            print(f"Warning: IBGPIDM text file not found at {ibgpidm_file}")
            # Fallback to SAS dataset if text file not found
            sas_file = f"{PATHS['DEPOSIT']}ibgpidm.sas7bdat"
            if Path(sas_file).exists():
                print(f"  Using SAS dataset instead: {sas_file}")
                df, meta = pyreadstat.read_sas7bdat(sas_file)
                df = pl.DataFrame(df)
                df.columns = [col.lower() for col in df.columns]
            else:
                print("Error: No IBGPIDM file found")
                return pl.DataFrame()
        else:
            # Read text file with fixed width format
            # SAS INPUT @01 ACCTNO 10. @12 IBGAMT 16.2;
            # This means:
            # - ACCTNO starts at position 1, length 10
            # - IBGAMT starts at position 12, length 16
            
            # Read the text file
            data = []
            with open(ibgpidm_file, 'r') as f:
                for line in f:
                    if line.strip():  # Skip empty lines
                        # Extract fields based on SAS INPUT positions
                        acctno = line[0:10].strip()  # @01 ACCTNO 10.
                        ibgamt = line[11:27].strip()  # @12 IBGAMT 16.2
                        
                        if acctno and ibgamt:
                            try:
                                data.append({
                                    'acctno': acctno,
                                    'ibgamt': float(ibgamt)
                                })
                            except ValueError:
                                # Skip lines with invalid numeric data
                                continue
            
            if not data:
                print(f"Warning: No valid data found in {ibgpidm_file}")
                return pl.DataFrame()
            
            df = pl.DataFrame(data)
        
        # Standardize acctno
        if 'acctno' in df.columns:
            df = df.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
        
        # PROC SUMMARY DATA=IBGPIDM NWAY; BY ACCTNO; VAR IBGAMT; OUTPUT OUT=DEPOSIT.IBGPIDM SUM=;
        result = df.group_by('acctno').agg([
            pl.col('ibgamt').cast(pl.Float64, strict=False).fill_null(0).sum().alias('ibgamt')
        ])
        return result
    except Exception as e:
        print(f"Error loading IBGPIDM: {e}")
        import traceback
        traceback.print_exc()
        return pl.DataFrame()
