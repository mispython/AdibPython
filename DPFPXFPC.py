    # ========== PART 2: CLIENT ACCOUNTS ==========
    print("\nProcessing Client Accounts...")
    
    client_df = load_client()
    print(f"  CLIENT master: {len(client_df)} rows")
    print(f"  CLIENT columns: {client_df.columns.tolist()}")
    
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
        print(f"  Deposit (from saca) columns: {deposit.columns.tolist()}")
        
        # PROC SORT DATA=DEPOSIT NODUPKEYS; BY ACCTNO;
        deposit = deposit.drop_duplicates(subset=['acctno'])
        
        # PROC SORT DATA=DEPOSIT; BY ACCTNO;
        deposit = deposit.sort_values('acctno')
        
        # DATA DEPOSIT; MERGE DEPOSIT(IN=A) FLOAT(IN=B); BY ACCTNO;
        if not float_df.empty:
            deposit = deposit.merge(float_df, on='acctno', how='left')
            print(f"  Deposit after FLOAT merge columns: {deposit.columns.tolist()}")
        else:
            deposit['float'] = 0
        
        # AVBAL = SUM(CURBAL,(-1)*FLOAT)
        deposit['float'] = deposit['float'].fillna(0) if 'float' in deposit.columns else 0
        deposit['curbal'] = deposit['curbal'].fillna(0) if 'curbal' in deposit.columns else 0
        deposit['intpaybl'] = deposit['intpaybl'].fillna(0) if 'intpaybl' in deposit.columns else 0
        
        # Check if curbal exists
        if 'curbal' not in deposit.columns:
            print(f"  WARNING: 'curbal' column not found in deposit!")
            print(f"  Available columns: {deposit.columns.tolist()}")
        else:
            deposit['avbal'] = deposit['curbal'] - deposit['float']
            # AVBALTT = SUM(AVBAL,INTPAYBL)
            deposit['avbaltt'] = deposit['avbal'] + deposit['intpaybl']
            print(f"  Deposit after AVBAL calculation columns: {deposit.columns.tolist()}")
        
        # DATA DEPOSIT.CLIENT; MERGE DEPOSIT.CLIENT(IN=A) DEPOSIT(IN=B); BY ACCTNO; IF A & B;
        client = client_df.merge(deposit, on='acctno', how='inner', suffixes=('', '_deposit'))
        print(f"  Client after merge with deposit: {len(client)} rows")
        print(f"  Client columns after merge: {client.columns.tolist()}")
        
        # Check for duplicate column names
        if 'name_deposit' in client.columns:
            client = client.drop(columns=['name_deposit'])
        if 'key_deposit' in client.columns:
            client = client.drop(columns=['key_deposit'])
        
        # Ensure we have 'avbal' column
        if 'avbal' not in client.columns:
            print(f"  WARNING: 'avbal' column missing after merge!")
            # Try to find it with different name
            for col in client.columns:
                if 'avbal' in col.lower():
                    print(f"    Found similar column: {col}")
                    client = client.rename(columns={col: 'avbal'})
                    break
        
        if 'avbal' in client.columns and 'intpaybl' in client.columns:
            # AVBALTT = SUM(AVBAL,INTPAYBL) - recalculate
            client['avbaltt'] = client['avbal'] + client['intpaybl']
        else:
            print(f"  ERROR: Cannot calculate avbaltt. Missing columns.")
            print(f"  Available columns: {client.columns.tolist()}")
            return
        
        # DATA DEPOSIT.CLIENT; MERGE DEPOSIT.CLIENT(IN=A) DEP (IN=B); BY ACCTNO; IF A & B;
        if not dep.empty:
            # Ensure both have same acctno type
            client = standardize_acctno(client)
            dep = standardize_acctno(dep)
            
            # Merge only with amtind from dep
            if 'amtind' in dep.columns:
                client = client.merge(dep[['acctno', 'amtind']], on='acctno', how='inner')
            else:
                print(f"  WARNING: 'amtind' not found in dep. Dep columns: {dep.columns.tolist()}")
        
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
        
        # SI = 0; AVBALTT = SUM(AVBALTT,SI)
        client['si'] = 0
        client['avbaltt'] = client['avbaltt'] + client['si']
        
        # DATA DEPOSIT.CLIENT; MERGE DEPOSIT.CLIENT(IN=A) DEPOSIT.IBGPIDM(IN=B RENAME=(IBGAMT=IBGAMTX));
        if not ibg_df.empty and 'acctno' in client.columns:
            client = standardize_acctno(client)
            ibg_df = standardize_acctno(ibg_df)
            client = client.merge(ibg_df, on='acctno', how='left')
            client['ibgamt'] = client['ibgamt'].fillna(0) if 'ibgamt' in client.columns else 0
        else:
            client['ibgamt'] = 0
        
        # AVBALTT = SUM(AVBALTT,IBGAMT)
        client['avbaltt'] = client['avbaltt'] + client['ibgamt']
        
        # Print final columns for debugging
        print(f"  Final client columns: {client.columns.tolist()}")
        
        # Split by threshold
        client_high = client[client['avbaltt'] > 60000]
        client_low = client[client['avbaltt'] <= 60000]
        
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
