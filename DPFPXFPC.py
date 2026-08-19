def load_client():
    """
    Load CLIENT file - either from SAS dataset (already processed) or from text file
    """
    try:
        # First try to read as SAS dataset (already processed)
        client_sas = f"{PATHS['DEPOSIT']}client.sas7bdat"
        if Path(client_sas).exists():
            df = read_sas_file(client_sas)
            if not df.empty:
                print(f"  Found CLIENT as SAS dataset: {client_sas}")
                df = standardize_acctno(df)
                
                # Check if it already has the processed columns
                if 'avbal' in df.columns and 'avbaltt' in df.columns:
                    print(f"  CLIENT SAS file appears to be pre-processed")
                    print(f"  CLIENT columns: {df.columns.tolist()}")
                    
                    # The file already has all the data we need
                    # Just need to ensure we have the right columns for output
                    return df
                else:
                    # It's a raw client file, need to process
                    if 'name' in df.columns:
                        df['key'] = df['name'].str[:10]
                    return df
        
        # If no SAS file, try text file
        file_paths = [
            f"{PATHS['DEPOSIT']}client.txt",
            f"{PATHS['DEPOSIT']}CLIENT.txt",
            f"{PATHS['DEPOSIT']}client.dat",
            f"{PATHS['DEPOSIT']}CLIENT.dat",
            f"{PATHS['DEPOSIT']}client",
            f"{PATHS['DEPOSIT']}CLIENT",
        ]
        
        filepath = None
        for fp in file_paths:
            if Path(fp).exists():
                filepath = fp
                print(f"  Found CLIENT file: {filepath}")
                break
        
        if filepath is None:
            print(f"  Warning: CLIENT file not found")
            return pd.DataFrame()
        
        # Read as text file
        data = []
        with open(filepath, 'r', errors='ignore') as f:
            lines = f.readlines()
        
        print(f"  Read {len(lines)} lines from CLIENT file")
        
        for line in lines:
            if len(line) >= 60:
                acct_str = line[1:11].strip()
                if acct_str and acct_str.replace(' ', '').isdigit():
                    try:
                        acctno = str(int(float(acct_str)))
                        name = line[20:60].strip()
                        if name:
                            data.append({
                                'acctno': acctno,
                                'name': name,
                                'key': name[:10]
                            })
                    except ValueError:
                        continue
        
        df = pd.DataFrame(data)
        
        if not df.empty and 'acctno' in df.columns:
            df = standardize_acctno(df)
            df = df.drop_duplicates(subset=['acctno'])
        
        return df
    except Exception as e:
        print(f"  Error reading CLIENT file: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()





    # ========== PART 2: CLIENT ACCOUNTS ==========
    print("\nProcessing Client Accounts...")
    
    client_df = load_client()
    print(f"  CLIENT master: {len(client_df)} rows")
    
    # Check if client_df is already processed (has avbal and avbaltt columns)
    is_preprocessed = 'avbal' in client_df.columns and 'avbaltt' in client_df.columns
    
    if is_preprocessed:
        print(f"  Client data appears to be pre-processed")
        print(f"  Using pre-processed client data directly")
        
        # The client data already has all the necessary columns
        # Just need to split by threshold and output
        client = client_df.copy()
        
        # Ensure we have all necessary columns for output
        for col in ['si', 'ibgamt', 'plusbal', 'unclaim', 'amtind', 'purpose']:
            if col not in client.columns:
                client[col] = 0 if col in ['si', 'ibgamt', 'plusbal', 'unclaim'] else ''
        
        # Split by threshold
        if 'avbaltt' in client.columns:
            client_high = client[client['avbaltt'] > 60000]
            client_low = client[client['avbaltt'] <= 60000]
        else:
            client_high = pd.DataFrame()
            client_low = pd.DataFrame()
        
        print(f"  Client >60k: {len(client_high)} accounts")
        print(f"  Client <=60k: {len(client_low)} accounts")
        
        # Write TEXT outputs
        def write_client_txt(df, title, filename):
            if df.empty:
                return
            lines = []
            lines.append(" ")
            lines.append(title)
            lines.append(" ")
            header = "BRANCH;ACCTNO;NAME;PURPOSE;AVBAL;INTPAYBL;PRODUCT;AMTIND;PLUSBAL;UNCLAIM;SI;IBGAMT;AVBALTT;"
            lines.append(header)
            
            for _, r in df.iterrows():
                line = (
                    f"{r.get('branch', '')};{r.get('acctno', '')};{r.get('name', '')};{r.get('purpose', '')};"
                    f"{r.get('avbal', 0):.2f};{r.get('intpaybl', 0):.2f};{r.get('product', '')};{r.get('amtind', '')};"
                    f"{r.get('plusbal', 0):.2f};{r.get('unclaim', 0):.2f};{r.get('si', 0):.2f};"
                    f"{r.get('ibgamt', 0):.2f};{r.get('avbaltt', 0):.2f};"
                )
                lines.append(line)
            
            output_path = Path(f"{PATHS['OUTPUT']}{filename}")
            output_path.write_text('\n'.join(lines))
            print(f"  Output written to: {output_path}")
        
        write_client_txt(client_high, "CLIENT >60000", "islamic_client_high.txt")
        write_client_txt(client_low, "CLIENT <=60000", "islamic_client_low.txt")
        
        # Print reports
        if not client_high.empty and 'branch' in client_high.columns:
            print("\nCLIENT >60000 by Branch:")
            branch_summary = client_high.groupby('branch')['avbaltt'].sum().sort_index()
            for branch, total in branch_summary.items():
                print(f"  Branch {branch}: RM {total:,.2f}")
        
        if not client_low.empty and 'branch' in client_low.columns:
            print("\nCLIENT <=60000 by Branch:")
            branch_summary = client_low.groupby('branch')['avbaltt'].sum().sort_index()
            for branch, total in branch_summary.items():
                print(f"  Branch {branch}: RM {total:,.2f}")
    else:
        # Process client data from scratch (original logic)
        print(f"  Processing client data from raw source")
        # ... rest of the original processing logic ...
