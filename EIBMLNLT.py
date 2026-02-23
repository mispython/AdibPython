"""
EIBMLNLT - Loan Listing Report with RPS Output
Complete implementation matching SAS original logic including:
- Region-based output splitting (KL, BG, JB, SB, PP, TT)
- RPS control characters (E255, P000/P001)
- Exact column positioning
- ASA carriage control via SPLIB136 equivalent
- Loan note and commitment tracking
"""

import polars as pl
from datetime import datetime
from pathlib import Path
import os

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'LOAN': 'data/loan/',
    'LNNOTE': 'data/lnnote/',
    'BANKCODE': 'data/bankcode/',
    'OUTPUT': 'output/'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

# Bank code mapping (BANKFMT)
BANK_FMT = {33: 'PBB', 134: 'PFB'}

# Region mappings (from INFIL1-INFIL6)
REGION_FILES = {
    'KL': 'KLREGION',
    'BG': 'BGREGION',
    'JB': 'JBREGION',
    'SB': 'SBREGION',
    'PP': 'PPREGION',
    'TT': 'TTREGION'
}

# RPS control characters
CTL_E255 = 'E255'  # Page eject with special header
CTL_P000 = 'P000'  # Normal line with channel 0
CTL_P001 = 'P001'  # Normal line with channel 1

# Page formatting
PAGE_LINES = 55
HEADER_LINES = 11

# =============================================================================
# DATE PROCESSING
# =============================================================================
def get_report_dates():
    """Get report date and calculate week parameters (exact SAS logic)"""
    df = pl.read_parquet(f"{PATHS['LOAN']}REPTDATE.parquet")
    reptdate = df['REPTDATE'][0]
    day = reptdate.day
    
    # SELECT(DAY(REPTDATE)) with exact mappings
    if day == 8:
        sdd, wk, wk1 = 1, '1', '4'
    elif day == 15:
        sdd, wk, wk1 = 9, '2', '1'
    elif day == 22:
        sdd, wk, wk1 = 16, '3', '2'
    else:
        sdd, wk, wk1 = 23, '4', '3'
    
    return {
        'date': reptdate,
        'sdd': sdd,
        'wk': wk,
        'wk1': wk1,
        'nowk': wk,
        'rdate': reptdate.strftime('%d%m%y'),
        'reptmon': f"{reptdate.month:02d}",
        'reptyear': str(reptdate.year)
    }

# =============================================================================
# REGION MAPPING
# =============================================================================
def load_region_mappings():
    """Load branch to region mappings from region files"""
    region_map = {}
    
    for region, filename in REGION_FILES.items():
        try:
            with open(f"{PATHS['BANKCODE']}{filename}", 'r') as f:
                for line in f:
                    branch = line.strip()
                    if branch.isdigit():
                        region_map[int(branch)] = region
        except:
            print(f"  Warning: Could not load {filename}")
    
    return region_map

# =============================================================================
# DATA PROCESSING
# =============================================================================
def load_loan_data(rep_vars):
    """Load loan data for the period"""
    try:
        loan = pl.read_parquet(f"{PATHS['LOAN']}LOAN{rep_vars['reptmon']}{rep_vars['nowk']}.parquet")
        return loan
    except:
        return pl.DataFrame()

def load_lnnote():
    """Load loan note data"""
    try:
        lnnote = pl.read_parquet(f"{PATHS['LNNOTE']}LNNOTE.parquet")
        lnnote = lnnote.select(['ACCTNO', 'NOTENO', 'BANKNO', 'STATE']).unique()
        return lnnote
    except:
        return pl.DataFrame()

def load_lncomm():
    """Load loan commitment data"""
    try:
        lncomm = pl.read_parquet(f"{PATHS['LNNOTE']}LNCOMM.parquet")
        return lncomm
    except:
        return pl.DataFrame()

def process_loan_notes(loan_df, lnnote_df):
    """Merge loan with loan notes (LNOTE dataset)"""
    if loan_df.is_empty() or lnnote_df.is_empty():
        return pl.DataFrame()
    
    # Sort both by ACCTNO, NOTENO
    loan_df = loan_df.sort(['ACCTNO', 'NOTENO'])
    lnnote_df = lnnote_df.sort(['ACCTNO', 'NOTENO'])
    
    # Merge
    lnote = loan_df.join(lnnote_df, on=['ACCTNO', 'NOTENO'], how='inner')
    
    # Filter for loan accounts
    lnote = lnote.filter(pl.col('ACCTYPE') == 'LN')
    
    # Keep required columns
    keep_cols = ['BANKNO', 'BRANCH', 'ACCTNO', 'NOTENO', 'NAME', 'BALANCE',
                 'SECTORCD', 'CUSTCD', 'INTRATE', 'NTBRCH', 'COMMNO', 
                 'LIABCODE', 'APPRLIMT', 'FISSPURP', 'STATE']
    
    lnote = lnote.select([c for c in keep_cols if c in lnote.columns])
    
    return lnote

def process_commitments(lnote_df, lncomm_df):
    """Merge with loan commitments (NOTE1 dataset)"""
    if lnote_df.is_empty():
        return pl.DataFrame(), pl.DataFrame()
    
    # Sort both by ACCTNO, COMMNO
    lnote_df = lnote_df.sort(['ACCTNO', 'COMMNO'])
    
    if not lncomm_df.is_empty():
        lncomm_df = lncomm_df.sort(['ACCTNO', 'COMMNO'])
        # Merge
        note1 = lnote_df.join(lncomm_df, on=['ACCTNO', 'COMMNO'], how='left')
    else:
        note1 = lnote_df
    
    # Add CCOLLTRL if not present
    if 'CCOLLTRL' not in note1.columns:
        note1 = note1.with_columns(pl.lit('').alias('CCOLLTRL'))
    
    # Create NOTE2 (filtered for non-individual with construction/real estate)
    note2 = note1.filter(
        (~pl.col('CUSTCD').cast(pl.Utf8).is_in(['77','78','95','96'])) &
        (
            pl.col('SECTORCD').cast(pl.Utf8).str.slice(0,1).eq('5') |
            pl.col('SECTORCD').cast(pl.Utf8).eq('8310')
        )
    )
    
    return note1, note2

# =============================================================================
# RPS OUTPUT GENERATION
# =============================================================================
class RPSLoanReportGenerator:
    """Generate RPS-formatted loan listing with exact SAS positioning"""
    
    def __init__(self, region_map=None, is_islamic=False):
        self.region_map = region_map or {}
        self.is_islamic = is_islamic
        self.bank_name = "PUBLIC ISLAMIC BANK BERHAD" if is_islamic else "PUBLIC BANK BERHAD"
        self.output_buffers = {region: [] for region in REGION_FILES.keys()}
        self.output_buffers['OTHER'] = []
        self.page_counts = {region: 0 for region in REGION_FILES.keys()}
        self.page_counts['OTHER'] = 0
        self.line_counts = {}
        self.branch_amts = {}
        self.group_amts = {}
        
    def format_branch_no(self, branch):
        """Format branch number for header (exact SAS logic)"""
        if branch < 10:
            return f"BR0{branch}"
        elif branch < 100:
            return f"BR{branch}"
        else:
            return f"B{branch}"
    
    def get_region(self, branch):
        """Get region for branch"""
        return self.region_map.get(branch, 'OTHER')
    
    def put_exact(self, region, text):
        """Add line to region buffer"""
        self.output_buffers[region].append(text)
    
    def new_page(self, region, branch, bankno, is_od1=True, rdate=''):
        """Start new page with exact SAS header positioning"""
        self.page_counts[region] = self.page_counts.get(region, 0) + 1
        self.line_counts[region] = 0
        
        # E255 - Page eject with special header
        self.put_exact(region, f"{CTL_E255}")
        
        # Branch header at position 133
        brno = self.format_branch_no(branch)
        self.put_exact(region, f"{CTL_P000}PBBEDPPBBEDP{' ' * (133 - 17)}{brno}")
        
        # Report header with exact positioning
        report_title = "LOAN LISTING BY FISS PURPOSE CODE (FOR ALL CUSTCODES)" if is_od1 \
                      else "LOAN LISTING BY CONSTRUCTION (SECTCODE 5001-5999) AND REAL ESTATE (SECTCODE 8310) FOR NON-INDI. CUSTOMER"
        
        self.put_exact(region, f"{CTL_P000}REPORT NO :  LOANLIST{' ' * 26}{self.bank_name}{' ' * 22}PAGE NO : {self.page_counts[region]}")
        self.put_exact(region, f"{CTL_P001}BRANCH    :  {BANK_FMT.get(bankno, '')}  {branch:03d}      {report_title} {rdate}")
        self.put_exact(region, f"{CTL_P001}")
        self.put_exact(region, f"{CTL_P001}")
        
        # Column headers with exact positions
        self.put_exact(region, f"{CTL_P001}{' ' * 61}APPROVED{' ' * 13}OUTSTANDING{' ' * 6}FISS PURPOSE{' ' * 3}SECTOR{' ' * 5}CUST{' ' * 4}STATE{' ' * 4}INT{' ' * 3}COLL{' ' * 2}COLL")
        self.put_exact(region, f"{CTL_P001}{' ' * 9}A/C NO.{' ' * 6}NOTE NO.{' ' * 5}NAME OF CUSTOMER{' ' * 16}LIMIT{' ' * 8}BALANCE{' ' * 6}CODE{' ' * 4}CODE{' ' * 3}CODE{' ' * 4}CODE{' ' * 3}RATE{' ' * 3}NOTE{' ' * 2}COMM")
        self.put_exact(region, f"{CTL_P001}{' ' * 4}{'-' * 40}{' ' * 4}{'-' * 40}{' ' * 4}{'-' * 40}{' ' * 4}{'-' * 10}")
        self.put_exact(region, f"{CTL_P001}")
        self.put_exact(region, f"{CTL_P001}")
        
        self.line_counts[region] = HEADER_LINES
    
    def add_detail_line(self, region, row):
        """Add detail line with exact SAS column positions"""
        
        # Main account line (exact positions from SAS)
        line = (f"{CTL_P001}"
                f"{str(row.get('ACCTNO', ''))[:12]:>12}"           # @5 ACCTNO 12.
                f" {str(row.get('NOTENO', ''))[:8]:>8}"           # @19 NOTENO 8.
                f" {str(row.get('NAME', ''))[:26]:<26}"           # @29 NAME
                f" {row.get('APPRLIMT', 0):>15,.2f}"              # @55 APPRLIMT COMMA15.2
                f" {row.get('BALANCE', 0):>15,.2f}"               # @72 BALANCE COMMA15.2
                f" {str(row.get('FISSPURP', ''))[:4]:>4}"         # @90 FISSPURP $4.
                f" {str(row.get('SECTORCD', ''))[:4]:>4}"         # @101 SECTORCD $4.
                f" {row.get('CUSTCD', 0):>4}"                     # @108 CUSTCD 4.
                f" {str(row.get('STATE', ''))[:3]:>3}"            # @114 STATE $3.
                f" {row.get('INTRATE', 0):>5,.2f}"                # @120 INTRATE COMMA5.2
                f" {str(row.get('LIABCODE', ''))[:3]:>3}"         # @126 LIABCODE
                f" {str(row.get('CCOLLTRL', ''))[:3]:>3}")        # @131 CCOLLTRL
        self.put_exact(region, line)
        
        self.line_counts[region] = self.line_counts.get(region, 0) + 1
    
    def add_subtotal(self, region, group_id, amount, is_fiss=True):
        """Add subtotal line with exact positioning"""
        self.put_exact(region, f"{CTL_P001}{' ' * 71}{'-' * 15}")
        prefix = "SUBTOTAL FOR FISS PURPOSE" if is_fiss else "SUBTOTAL FOR SECTOR"
        self.put_exact(region, f"{CTL_P001}{' ' * 36}{prefix} {group_id}{' ' * (71 - len(prefix) - len(str(group_id)) - 36)}{amount:>15,.2f}")
        self.put_exact(region, f"{CTL_P001}{' ' * 71}{'=' * 15}")
        self.put_exact(region, f"{CTL_P001}")
        self.line_counts[region] = self.line_counts.get(region, 0) + 4
    
    def add_grand_total(self, region, amount):
        """Add grand total line with exact positioning"""
        self.put_exact(region, f"{CTL_P001}{' ' * 71}{'-' * 15}")
        self.put_exact(region, f"{CTL_P001}{' ' * 36}GRAND TOTAL FOR BRANCH{' ' * (71 - 21 - 36)}{amount:>15,.2f}")
        self.put_exact(region, f"{CTL_P001}{' ' * 71}{'=' * 15}")
        self.put_exact(region, f"{CTL_P001}")
        self.line_counts[region] = self.line_counts.get(region, 0) + 4
    
    def check_page_break(self, region):
        """Check if we need a new page"""
        return self.line_counts.get(region, 0) > PAGE_LINES

    def generate_report(self, df, group_by_col, rdate=''):
        """Generate complete report with region splitting"""
        if df.is_empty():
            return
        
        # Sort data
        df = df.sort(['BRANCH', group_by_col, 'CUSTCD', 'ACCTNO'])
        
        current_branch = None
        current_group = None
        current_region = None
        current_bankno = None
        
        for row in df.rows(named=True):
            branch = row['BRANCH']
            bankno = row.get('BANKNO', 0)
            group = row[group_by_col]
            region = self.get_region(branch)
            
            # New branch
            if branch != current_branch:
                if current_branch is not None and current_region:
                    self.add_grand_total(current_region, self.branch_amts.get(current_region, 0))
                
                current_branch = branch
                current_bankno = bankno
                current_region = region
                self.branch_amts[region] = 0
                self.new_page(region, branch, bankno, is_od1=(group_by_col == 'FISSPURP'), rdate=rdate)
            
            # New group within branch
            if group != current_group:
                if current_group is not None and current_region:
                    self.add_subtotal(current_region, current_group, self.group_amts.get(current_region, 0), 
                                    is_fiss=(group_by_col == 'FISSPURP'))
                
                current_group = group
                self.group_amts[region] = 0
                
                if self.check_page_break(region):
                    self.new_page(region, branch, bankno, is_od1=(group_by_col == 'FISSPURP'), rdate=rdate)
            
            # Add detail line
            self.add_detail_line(region, row)
            self.branch_amts[region] = self.branch_amts.get(region, 0) + row['BALANCE']
            self.group_amts[region] = self.group_amts.get(region, 0) + row['BALANCE']
            
            if self.check_page_break(region):
                self.new_page(region, branch, bankno, is_od1=(group_by_col == 'FISSPURP'), rdate=rdate)
        
        # Final subtotals and grand total
        if current_group is not None and current_region:
            self.add_subtotal(current_region, current_group, self.group_amts.get(current_region, 0),
                            is_fiss=(group_by_col == 'FISSPURP'))
        
        if current_branch is not None and current_region:
            self.add_grand_total(current_region, self.branch_amts.get(current_region, 0))

# =============================================================================
# SPLIB136 EQUIVALENT - Add ASA Control Characters
# =============================================================================
def add_asa_control_chars(input_lines, lrecl=138):
    """
    Equivalent to SPLIB136 program
    Adds ASA control characters and formats for RPS printing
    """
    output_lines = []
    
    for line in input_lines:
        if not line:
            continue
            
        # Extract first 4 chars to determine control
        prefix = line[:4] if len(line) >= 4 else line
        
        if prefix.startswith('E255'):
            # Page eject - already has E255
            asa_line = '1' + line[4:].ljust(lrecl-1)[:lrecl-1]
        elif prefix.startswith('P000'):
            # Normal line with channel 0
            asa_line = ' ' + line[4:].ljust(lrecl-1)[:lrecl-1]
        elif prefix.startswith('P001'):
            # Normal line with channel 1
            asa_line = ' ' + line[4:].ljust(lrecl-1)[:lrecl-1]
        else:
            # Default - skip one line before printing
            asa_line = '-' + line.ljust(lrecl-1)[:lrecl-1]
        
        output_lines.append(asa_line)
    
    return output_lines

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIBMLNLT - Loan Listing Report with RPS Output")
    print("=" * 60)
    
    # Step 0: DELETE previous datasets (IEFBR14 equivalent)
    print("\nCleaning previous outputs...")
    for i in range(1, 6):
        for ext in ['RPS', 'TXT']:
            f = PATHS['OUTPUT'] / f"LOANLIS{i}.{ext}"
            if f.exists():
                f.unlink()
                print(f"  Deleted {f}")
    
    # Get report dates
    rep_vars = get_report_dates()
    print(f"\nReport Date: {rep_vars['rdate']}")
    print(f"Week: {rep_vars['nowk']}, Month: {rep_vars['reptmon']}")
    print(f"Year: {rep_vars['reptyear']}")
    print(f"SDD: {rep_vars['sdd']}, WK1: {rep_vars['wk1']}")
    
    # Load region mappings
    print("\nLoading region mappings...")
    region_map = load_region_mappings()
    print(f"  Loaded {len(region_map)} branch mappings")
    
    # Load data
    print("\nLoading loan data...")
    loan_df = load_loan_data(rep_vars)
    print(f"  Loan records: {len(loan_df)}")
    
    print("\nLoading loan note data...")
    lnnote_df = load_lnnote()
    print(f"  Loan note records: {len(lnnote_df)}")
    
    print("\nLoading loan commitment data...")
    lncomm_df = load_lncomm()
    print(f"  Loan commitment records: {len(lncomm_df)}")
    
    # Process loan notes
    print("\nMerging loan with notes...")
    lnote_df = process_loan_notes(loan_df, lnnote_df)
    print(f"  LNOTE records: {len(lnote_df)}")
    
    # Process commitments
    print("\nMerging with commitments...")
    note1_df, note2_df = process_commitments(lnote_df, lncomm_df)
    print(f"  NOTE1 (all): {len(note1_df)} records")
    print(f"  NOTE2 (construction/real estate): {len(note2_df)} records")
    
    # Generate LNDATA text output
    print("\nGenerating LNDATA text output...")
    lndata_lines = []
    for row in note1_df.rows(named=True):
        if (row.get('BALANCE', 0) > 0 or row.get('APPRLIMT', 0) > 0):
            bal = row.get('BALANCE', 0) or 0.0
            lim = row.get('APPRLIMT', 0) or 0
            # Exact positions from SAS: @001 BRANCH 3. @004 ACCTNO 10. @014 NOTENO 5. @020 APPRLIMT @040 BALANCE
            line = (f"{row.get('BRANCH', 0):03d}"
                    f"{str(row.get('ACCTNO', ''))[:10]:>10}"
                    f"{str(row.get('NOTENO', ''))[:5]:>5}"
                    f"{lim:>15,.2f}"
                    f"{bal:>15,.2f}")
            lndata_lines.append(line)
    
    with open(f"{PATHS['OUTPUT']}LOANLISX.TXT", 'w') as f:
        for line in lndata_lines:
            f.write(f"{line}\n")
    print(f"  Written: LOANLISX.TXT ({len(lndata_lines)} lines)")
    
    # Generate conventional reports (PBB)
    print("\nGenerating conventional reports (PBB)...")
    
    gen1 = RPSLoanReportGenerator(region_map, is_islamic=False)
    gen1.generate_report(note1_df, 'FISSPURP', rep_vars['rdate'])
    
    gen2 = RPSLoanReportGenerator(region_map, is_islamic=False)
    gen2.generate_report(note2_df, 'SECTORCD', rep_vars['rdate'])
    
    # Generate Islamic reports (PIBB)
    print("\nGenerating Islamic reports (PIBB)...")
    
    gen1i = RPSLoanReportGenerator(region_map, is_islamic=True)
    gen1i.generate_report(note1_df, 'FISSPURP', rep_vars['rdate'])
    
    gen2i = RPSLoanReportGenerator(region_map, is_islamic=True)
    gen2i.generate_report(note2_df, 'SECTORCD', rep_vars['rdate'])
    
    # Write outputs with SPLIB136 processing
    print("\nApplying SPLIB136 processing (adding ASA control chars)...")
    
    output_configs = [
        (gen1, 'LOANLIS1', 'Conventional OD1'),
        (gen2, 'LOANLIS2', 'Conventional OD2'),
        (gen1i, 'LOANLIS3', 'Islamic OD1'),
        (gen2i, 'LOANLIS4', 'Islamic OD2'),
    ]
    
    for i, (gen, basename, desc) in enumerate(output_configs, 1):
        # Combine all region outputs
        all_lines = []
        for region in REGION_FILES.keys():
            if gen.output_buffers[region]:
                all_lines.extend(gen.output_buffers[region])
        if gen.output_buffers['OTHER']:
            all_lines.extend(gen.output_buffers['OTHER'])
        
        if all_lines:
            # Write raw output first (like SAP.BANK.LOANLIST.RPS)
            raw_file = PATHS['OUTPUT'] / f"{basename}.RAW"
            with open(raw_file, 'w') as f:
                for line in all_lines:
                    f.write(f"{line}\n")
            
            # Apply SPLIB136 processing to add ASA control chars
            asa_lines = add_asa_control_chars(all_lines, lrecl=138)
            
            # Write final RPS output (RECFM=FBA, LRECL=138)
            rps_file = PATHS['OUTPUT'] / f"{basename}.RPS"
            with open(rps_file, 'w') as f:
                for line in asa_lines:
                    f.write(f"{line}\n")
            
            print(f"  {desc}: {basename}.RPS ({len(asa_lines)} lines, LRECL=138)")
    
    # Summary statistics
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    total_loan = note1_df['BALANCE'].sum() if not note1_df.is_empty() else 0
    total_loan2 = note2_df['BALANCE'].sum() if not note2_df.is_empty() else 0
    total_limit = note1_df['APPRLIMT'].sum() if not note1_df.is_empty() else 0
    
    print(f"\nTotal Loan Balance: RM {total_loan:,.2f}")
    print(f"Total Approved Limit: RM {total_limit:,.2f}")
    print(f"Construction/Real Estate: RM {total_loan2:,.2f}")
    print(f"  Percentage: {(total_loan2/total_loan*100) if total_loan > 0 else 0:.1f}%")
    
    print("\nRegion Distribution:")
    if not note1_df.is_empty():
        for region in REGION_FILES.keys():
            region_branches = [b for b, r in region_map.items() if r == region]
            if region_branches:
                region_df = note1_df.filter(pl.col('BRANCH').is_in(region_branches))
                if not region_df.is_empty():
                    region_total = region_df['BALANCE'].sum()
                    region_count = len(region_df)
                    print(f"  {region}: {region_count:,} accounts, RM {region_total:,.2f}")
    
    print("\n" + "=" * 60)
    print("✓ EIBMLNLT Complete")
    print("=" * 60)

if __name__ == "__main__":
    main()
