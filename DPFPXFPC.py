    # ========== PART 2: CLIENT ACCOUNTS ==========
    print("\nProcessing Client Accounts...")
    
    client_df = load_client()
    print(f"  CLIENT master: {len(client_df)} rows")
    
    # Load SASA (DATA SASA; SET SACA.SAVING DEP.UMA)
    uma = load_uma()
    sasa_list = [sa] if not sa.empty else []
    if not uma.empty:
        # Ensure UMA has same columns as SA
        if 'purpose' in uma.columns:
            uma = uma[uma['purpose'].astype(str).isin(['5', '6'])]
        sasa_list.append(uma)
    sasa = pd.concat(sasa_list, ignore_index=True) if sasa_list else pd.DataFrame()
    if not sasa.empty:
        sasa = sasa.sort_values('acctno')
        print(f"  SASA: {len(sasa)} rows")
    
    if not client_df.empty and not saca.empty:
        # DATA DEPOSIT; SET SA CA FD;
        # Create deposit from saca (SA/CA/FD combined)
        deposit = saca.copy()
        
        # PROC SORT DATA=DEPOSIT NODUPKEYS; BY ACCTNO;
        deposit = deposit.drop_duplicates(subset=['acctno'])
        
        # PROC SORT DATA=DEPOSIT; BY ACCTNO;
        deposit = deposit.sort_values('acctno')
        
        # DATA DEPOSIT; MERGE DEPOSIT(IN=A) FLOAT(IN=B); BY ACCTNO;
        if not float_df.empty:
            deposit = deposit.merge(float_df, on='acctno', how='left')
        else:
            deposit['float'] = 0
        
        # AVBAL = SUM(CURBAL,(-1)*FLOAT)
        deposit['float'] = deposit['float'].fillna(0) if 'float' in deposit.columns else 0
        deposit['curbal'] = deposit['curbal'].fillna(0) if 'curbal' in deposit.columns else 0
        deposit['intpaybl'] = deposit['intpaybl'].fillna(0) if 'intpaybl' in deposit.columns else 0
        
        deposit['avbal'] = deposit['curbal'] - deposit['float']
        # AVBALTT = SUM(AVBAL,INTPAYBL)
        deposit['avbaltt'] = deposit['avbal'] + deposit['intpaybl']
        
        # DATA DEPOSIT.CLIENT; MERGE DEPOSIT.CLIENT(IN=A) DEPOSIT(IN=B); BY ACCTNO; IF A & B;
        client = client_df.merge(deposit, on='acctno', how='inner')
        
        # Keep only needed columns for now, but preserve all deposit columns
        # The SAS code keeps: BRANCH ACCTNO NAME PRODUCT AVBAL INTPAYBL AVBALTT CURBAL FLOAT
        cols_to_keep = ['branch', 'acctno', 'name', 'product', 'avbal', 'intpaybl', 'avbaltt', 'curbal', 'float']
        cols_to_keep = [c for c in cols_to_keep if c in client.columns]
        client = client[cols_to_keep].copy()
        
        # AVBALTT = SUM(AVBAL,INTPAYBL) - recalculate
        client['avbaltt'] = client['avbal'] + client['intpaybl']
        
        # DATA DEPOSIT.CLIENT; MERGE DEPOSIT.CLIENT(IN=A) DEP (IN=B); BY ACCTNO; IF A & B;
        if not dep.empty:
            # Before merging, save the columns we need from client
            client_cols_before = client.columns.tolist()
            
            # Ensure both have same acctno type
            client = standardize_acctno(client)
            dep = standardize_acctno(dep)
            
            # Merge with dep to get AMTIND
            client = client.merge(dep[['acctno', 'amtind']], on='acctno', how='inner')
            
            # Ensure we still have all original columns
            for col in client_cols_before:
                if col not in client.columns:
                    print(f"  Warning: Column {col} was lost during merge")
        
        # Now check if we have the columns we need
        required_cols = ['acctno', 'avbal', 'intpaybl']
        missing_cols = [c for c in required_cols if c not in client.columns]
        if missing_cols:
            print(f"  Warning: Missing required columns: {missing_cols}")
            print(f"  Available columns: {client.columns.tolist()}")
        
        # DATA DEPOSIT.CLIENT; MERGE DEPOSIT.CLIENT(IN=A) REMIT(DROP=NAME); BY ACCTNO;
        if not remit_df.empty:
            remit_cols = ['acctno', 'plusbal', 'unclaim']
            remit_cols = [c for c in remit_cols if c in remit_df.columns]
            if remit_cols and 'acctno' in client.columns:
                client = standardize_acctno(client)
                remit_df = standardize_acctno(remit_df)
                client = client.merge(remit_df[remit_cols], on='acctno', how='left')
                client['plusbal'] = client['plusbal'].fillna(0) if 'plusbal' in client.columns else 0
                client['unclaim'] = client['unclaim'].fillna(0) if 'unclaim' in client.columns else 0
            else:
                client['plusbal'] = 0
                client['unclaim'] = 0
        else:
            client['plusbal'] = 0
            client['unclaim'] = 0
        
        # AVBALTT = SUM(AVBAL,PLUSBAL,UNCLAIM,INTPAYBL)
        if all(col in client.columns for col in ['avbal', 'plusbal', 'unclaim', 'intpaybl']):
            client['avbaltt'] = (
                client['avbal'] + 
                client['plusbal'] + 
                client['unclaim'] + 
                client['intpaybl']
            )
        else:
            print(f"  Warning: Cannot calculate avbaltt, missing columns")
            print(f"  Available columns: {client.columns.tolist()}")
        
        # SI = 0; AVBALTT = SUM(AVBALTT,SI)
        client['si'] = 0
        if 'avbaltt' in client.columns:
            client['avbaltt'] = client['avbaltt'] + client['si']
        else:
            client['avbaltt'] = client['si']
        
        # DATA DEPOSIT.CLIENT; MERGE DEPOSIT.CLIENT(IN=A) DEPOSIT.IBGPIDM(IN=B RENAME=(IBGAMT=IBGAMTX));
        if not ibg_df.empty and 'acctno' in client.columns:
            client = standardize_acctno(client)
            ibg_df = standardize_acctno(ibg_df)
            client = client.merge(ibg_df, on='acctno', how='left')
            client['ibgamt'] = client['ibgamt'].fillna(0) if 'ibgamt' in client.columns else 0
        else:
            client['ibgamt'] = 0
        
        # AVBALTT = SUM(AVBALTT,IBGAMT)
        if 'avbaltt' in client.columns:
            client['avbaltt'] = client['avbaltt'] + client['ibgamt']
        
        # Split by threshold
        # WHERE AVBALTT > 60000
        client_high = client[client['avbaltt'] > 60000] if 'avbaltt' in client.columns else pd.DataFrame()
        # WHERE AVBALTT <= 60000
        client_low = client[client['avbaltt'] <= 60000] if 'avbaltt' in client.columns else pd.DataFrame()
        
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
