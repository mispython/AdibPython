import polars as pl
from pathlib import Path
from datetime import datetime, timedelta

def eibmpbfi():
    base = Path.cwd()
    loan_path = base / "LOAN"
    
    # Execute external programs
    try:
        from pbblnfmt import process as pbblnfmt_process
        pbblnfmt_process()
    except ImportError:
        print("Note: PGM(PBBLNFMT) not executed")
    
    try:
        from pbbelf import process as pbbelf_process
        pbbelf_process()
    except ImportError:
        print("Note: PGM(PBBELF) not executed")
    
    # 1. REPTDATE processing with week logic
    reptdate_df = pl.read_parquet(loan_path / "REPTDATE.parquet")
    reptdate = reptdate_df["REPTDATE"][0]
    
    # Week determination
    reptday = reptdate.day
    if reptday == 8:
        nowk = '1'
    elif reptday == 15:
        nowk = '2'
    elif reptday == 22:
        nowk = '3'
    else:
        nowk = '4'
    
    reptyear = str(reptdate.year)[-2:]
    reptmon = f"{reptdate.month:02d}"
    reptday_str = f"{reptdate.day:02d}"
    rdatx = f"{reptdate.day:02d}{reptdate.month:02d}"
    mdate = rdatx  # Same format
    
    print(f"NOWK: {nowk}, REPTYEAR: {reptyear}, REPTMON: {reptmon}, RDATX: {rdatx}")
    
    # 2. REMFMT format mapping (Python equivalent of SAS PROC FORMAT)
    def remfmt(value):
        """Convert remaing months to format code"""
        if value <= 0.255:
            return '01'   # UP TO 1 WEEK
        elif value <= 1:
            return '02'   # >1 WEEK - 1 MONTH
        elif value <= 3:
            return '03'   # >1 MONTH - 3 MONTHS
        elif value <= 6:
            return '04'   # >3 - 6 MONTHS
        elif value <= 12:
            return '05'   # >6 MONTHS - 1 YEAR
        else:
            return '06'   # > 1 YEAR
    
    # 3. Execute RDLMPBIF program
    try:
        from rdlmpbif import process as rdlmpbif_process
        pbif_df = rdlmpbif_process()
    except ImportError:
        # Create sample PBIF data if module doesn't exist
        print("Note: PGM(RDLMPBIF) not executed, using sample data")
        pbif_df = create_sample_pbif()
    
    if pbif_df.is_empty():
        print("No PBIF data available")
        return
    
    # 4. Process PBIF data (SAS macros converted to functions)
    def calculate_remmth(row, rpyear, rpmth, rpday):
        """Calculate remaining months (REMMTH macro)"""
        matdte = row.get('MATDTE')
        if not matdte:
            return 0.0
        
        # Convert to datetime if needed
        if isinstance(matdte, (int, float)):
            # SAS date conversion
            sas_base = datetime(1960, 1, 1)
            matdte = sas_base + timedelta(days=int(matdte))
        
        mdyr = matdte.year
        mdmth = matdte.month
        mdday = matdte.day
        
        # Days in months
        rpdays = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        mddays = [31, 29 if mdyr % 4 == 0 else 28, 31, 30, 31, 30, 
                  31, 31, 30, 31, 30, 31]
        
        # Adjust day if > days in month
        if mdday > rpdays[rpmth - 1]:
            mdday = rpdays[rpmth - 1]
        
        # Calculate differences
        remy = mdyr - int(rpyear)
        remm = mdmth - rpmth
        remd = mdday - rpday
        
        # Convert to months
        return remy * 12 + remm + remd / rpdays[rpmth - 1]
    
    # Process each row
    processed_rows = []
    for row in pbif_df.iter_rows(named=True):
        # Get base values
        clientno = row.get('CLIENTNO', '')
        custcx = row.get('CUSTCX', '')
        balance = row.get('BALANCE', 0)
        inlimit = row.get('INLIMIT', 0)
        matdte = row.get('MATDTE', None)
        
        # Calculate CUST
        cust = '08' if str(custcx) in ['77', '78', '95', '96'] else '09'
        
        # Calculate days difference
        days = 0
        if matdte:
            # Convert rdatx to date (DDMM)
            try:
                mat_date = matdte if isinstance(matdte, datetime) else \
                          datetime.strptime(str(matdte), "%d%m%y")
                days = (mat_date - datetime.strptime(rdatx + reptyear, "%d%m%y")).days
            except:
                days = 0
        
        # Calculate REMMTH
        if days < 8:
            remmth = 0.1
        else:
            remmth = calculate_remmth(row, reptyear, int(reptmon), int(reptday_str))
        
        # Create 4 output records per input record
        # Record 1: ITEM='219', BNMCODE='93' + ITEM + CUST + REMFMT + '0000Y'
        bnmcode1 = f"93219{cust}{remfmt(remmth)}0000Y"
        processed_rows.append({
            'BNMCODE': bnmcode1,
            'AMOUNT': balance,
            'ITEM': '219'
        })
        
        # Record 2: ITEM='219', BNMCODE='95' + ITEM + CUST + REMFMT + '0000Y'
        bnmcode2 = f"95219{cust}{remfmt(remmth)}0000Y"
        processed_rows.append({
            'BNMCODE': bnmcode2,
            'AMOUNT': balance,
            'ITEM': '219'
        })
        
        # Record 3: ITEM='429', BNMCODE='93' + ITEM + '00010000Y'
        bnmcode3 = "9342900010000Y"
        amount3 = (inlimit - balance) * 0.20 if inlimit > balance else 0
        processed_rows.append({
            'BNMCODE': bnmcode3,
            'AMOUNT': amount3,
            'ITEM': '429'
        })
        
        # Record 4: ITEM='429', BNMCODE='95' + ITEM + '00' + REMFMT + '0000Y'
        bnmcode4 = f"9542900{remfmt(remmth)}0000Y"
        amount4 = (inlimit - balance) if inlimit > balance else 0
        processed_rows.append({
            'BNMCODE': bnmcode4,
            'AMOUNT': amount4,
            'ITEM': '429'
        })
    
    # Create processed dataframe
    pbif_processed = pl.DataFrame(processed_rows)
    
    # 5. Summarize by BNMCODE
    pbif_summary = pbif_processed.group_by("BNMCODE").agg(
        pl.sum("AMOUNT").alias("AMOUNT"),
        pl.first("ITEM").alias("ITEM")  # Keep ITEM for reference
    )
    
    # 6. Write FISS file
    generate_fiss_file(pbif_summary, reptday_str, reptmon, reptyear, base / "FISS.txt")
    
    print(f"Processing complete. Output: FISS.txt")
    print(f"Input records: {len(pbif_df)}, Output records: {len(pbif_summary)}")

def generate_fiss_file(df, reptday, reptmon, reptyear, output_path):
    """Generate FISS file with header and data"""
    if df.is_empty():
        return
    
    with open(output_path, 'w') as f:
        # Write header
        f.write(f"RLFM{reptday}{reptmon}{reptyear}\n")
        
        # Write data rows
        for row in df.iter_rows(named=True):
            bnmcode = row.get('BNMCODE', '').ljust(14)
            amount = f"{row.get('AMOUNT', 0):.2f}"
            # Fixed values from SAS
            amtusd = "0"
            amtsgd = "0"
            
            line = f"{bnmcode};{amount};{amtusd};{amtsgd}\n"
            f.write(line)
    
    print(f"FISS file created with {len(df)} records")

def create_sample_pbif():
    """Create sample PBIF data for testing"""
    data = {
        'CLIENTNO': ['DMH01D0KL', 'HTD21D5KL', 'ABC123DEF'],
        'CUSTCX': ['77', '78', '09'],
        'BALANCE': [1000000.00, 500000.00, 750000.00],
        'INLIMIT': [1200000.00, 600000.00, 800000.00],
        'MATDTE': [
            datetime(2024, 4, 3),   # 03APR24
            datetime(2024, 5, 1),   # 01MAY24
            datetime(2024, 12, 15)  # 15DEC24
        ]
    }
    return pl.DataFrame(data)

# Simplified RDLMPBIF module example
def rdlmpbif_process():
    """Example RDLMPBIF program implementation"""
    # This would normally read loan data
    return create_sample_pbif()

# Main execution with simplified logic
def eibmpbfi_simple():
    """Simplified version"""
    base = Path.cwd()
    
    # Get report date
    reptdate = pl.read_parquet(base / "LOAN/REPTDATE.parquet")["REPTDATE"][0]
    reptyear = str(reptdate.year)[-2:]
    reptmon = f"{reptdate.month:02d}"
    reptday = f"{reptdate.day:02d}"
    rdatx = f"{reptdate.day:02d}{reptdate.month:02d}"
    
    print(f"Processing BPFI report for {reptday}/{reptmon}/{reptyear}")
    
    # REMFMT function
    def remfmt(value):
        if value <= 0.255: return '01'
        elif value <= 1: return '02'
        elif value <= 3: return '03'
        elif value <= 6: return '04'
        elif value <= 12: return '05'
        else: return '06'
    
    # Create sample data if RDLMPBIF not available
    try:
        from rdlmpbif import process
        pbif_df = process()
    except:
        print("Using sample data")
        pbif_df = pl.DataFrame({
            'CLIENTNO': ['TEST001', 'TEST002'],
            'CUSTCX': ['77', '09'],
            'BALANCE': [1000000, 500000],
            'INLIMIT': [1200000, 600000],
            'MATDTE': [datetime(2024, 6, 30), datetime(2024, 9, 30)]
        })
    
    # Process data
    records = []
    for row in pbif_df.iter_rows(named=True):
        cust = '08' if str(row.get('CUSTCX', '')).strip() in ['77', '78', '95', '96'] else '09'
        balance = row.get('BALANCE', 0)
        inlimit = row.get('INLIMIT', 0)
        
        # Calculate remaining months (simplified)
        matdte = row.get('MATDTE')
        if isinstance(matdte, datetime):
            # Simple month difference
            today = datetime.strptime(f"{rdatx}{reptyear}", "%d%m%y")
            months_diff = (matdte.year - today.year) * 12 + (matdte.month - today.month)
            remmth = max(0.1, months_diff + (matdte.day - today.day) / 30)
        else:
            remmth = 0.1
        
        # Create 4 records per input
        remfmt_code = remfmt(remmth)
        
        # Record 1 & 2
        records.extend([
            {'BNMCODE': f"93219{cust}{remfmt_code}0000Y", 'AMOUNT': balance},
            {'BNMCODE': f"95219{cust}{remfmt_code}0000Y", 'AMOUNT': balance}
        ])
        
        # Record 3 & 4
        avail = max(0, inlimit - balance)
        records.extend([
            {'BNMCODE': "9342900010000Y", 'AMOUNT': avail * 0.20},
            {'BNMCODE': f"9542900{remfmt_code}0000Y", 'AMOUNT': avail}
        ])
    
    # Summarize
    result_df = pl.DataFrame(records)
    summary = result_df.group_by("BNMCODE").agg(pl.sum("AMOUNT").alias("AMOUNT"))
    
    # Generate output
    with open(base / "FISS.txt", 'w') as f:
        f.write(f"RLFM{reptday}{reptmon}{reptyear}\n")
        for row in summary.iter_rows(named=True):
            f.write(f"{row['BNMCODE']:14s};{row['AMOUNT']:.2f};0;0\n")
    
    print(f"Generated {len(summary)} BNM codes")

if __name__ == "__main__":
    eibmpbfi_simple()
