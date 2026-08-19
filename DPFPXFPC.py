def load_client():
    """
    Load CLIENT file from text file
    DATA DEPOSIT.CLIENT;
      INFILE CLIENT;
      INPUT @002 ACCTNO  10. @;
      IF COMPRESS(ACCTNO, "1234567890") = ' ' THEN DO;
         INPUT @021 NAME    $40.;
         OUTPUT;
      END;
      KEY = SUBSTR(NAME,1,10);
    RUN;
    """
    try:
        # Try different possible file extensions and locations
        file_paths = [
            f"{PATHS['DEPOSIT']}client.txt",
            f"{PATHS['DEPOSIT']}CLIENT.txt",
            f"{PATHS['DEPOSIT']}client.dat",
            f"{PATHS['DEPOSIT']}CLIENT.dat",
            f"{PATHS['DEPOSIT']}client.sas7bdat",
            f"{PATHS['DEPOSIT']}CLIENT.sas7bdat",
            f"{PATHS['DEPOSIT']}client",
            f"{PATHS['DEPOSIT']}CLIENT",
            # Also check in SACA directory
            f"{PATHS['SACA']}client.txt",
            f"{PATHS['SACA']}CLIENT.txt",
            f"{PATHS['SACA']}client.sas7bdat",
            f"{PATHS['SACA']}CLIENT.sas7bdat",
        ]
        
        filepath = None
        for fp in file_paths:
            if Path(fp).exists():
                filepath = fp
                print(f"  Found CLIENT file: {filepath}")
                break
        
        if filepath is None:
            print(f"  Warning: CLIENT file not found in any expected location")
            # List files in DEPOSIT directory to help debug
            deposit_path = Path(PATHS['DEPOSIT'])
            if deposit_path.exists():
                print(f"  Files in {PATHS['DEPOSIT']}:")
                for f in deposit_path.iterdir():
                    if 'client' in f.name.lower() or 'CLIENT' in f.name:
                        print(f"    - {f.name}")
            return pd.DataFrame()
        
        # Check if it's a SAS file or text file
        if filepath.endswith('.sas7bdat'):
            df = read_sas_file(filepath)
            if not df.empty and 'acctno' in df.columns:
                df = standardize_acctno(df)
                if 'name' in df.columns:
                    df['key'] = df['name'].str[:10]
                return df
            return pd.DataFrame()
        
        # Read as text file
        data = []
        with open(filepath, 'r', errors='ignore') as f:
            lines = f.readlines()
        
        print(f"  Read {len(lines)} lines from CLIENT file")
        
        for i, line in enumerate(lines):
            if len(line) >= 60:  # Need positions up to 60 for NAME
                # INPUT @002 ACCTNO 10.
                acct_str = line[1:11].strip()
                
                # Check if ACCTNO contains only digits
                if acct_str and acct_str.replace(' ', '').isdigit():
                    try:
                        acctno = str(int(float(acct_str)))  # Handle possible float format
                        # INPUT @021 NAME $40.
                        name = line[20:60].strip()
                        if name:
                            data.append({
                                'acctno': acctno,
                                'name': name,
                                'key': name[:10]  # KEY = SUBSTR(NAME,1,10)
                            })
                    except ValueError:
                        # Try without float conversion
                        acct_str_clean = acct_str.replace(' ', '')
                        if acct_str_clean.isdigit():
                            acctno = acct_str_clean
                            name = line[20:60].strip()
                            if name:
                                data.append({
                                    'acctno': acctno,
                                    'name': name,
                                    'key': name[:10]
                                })
        
        df = pd.DataFrame(data)
        print(f"  Parsed {len(df)} records from CLIENT file")
        
        # PROC SORT DATA=DEPOSIT.CLIENT NODUPKEYS; BY ACCTNO;
        if not df.empty and 'acctno' in df.columns:
            df = standardize_acctno(df)
            df = df.drop_duplicates(subset=['acctno'])
        
        return df
    except Exception as e:
        print(f"  Error reading CLIENT file: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
