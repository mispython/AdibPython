#!/usr/bin/env python3
"""
File Name: EIBDLIBT
BNM Liquidity Report for Trade Finance
Processes BA (Banker's Acceptance) and TR (Trade) transactions
Based on SAS original code
"""

import pyreadstat
import polars as pl
from datetime import datetime, timedelta, date
from pathlib import Path
import warnings

# Import BNM modules
from pbblnfmt import get_remfmt, get_prdformat, get_days_in_month
from pbbelf import calculate_next_bldate, calculate_remmth, get_days_in_report_month

warnings.filterwarnings('ignore')

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path(__file__).resolve().parent

INPUT_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

# Input SAS files (only 2 main inputs as per SAS code)
BNM1_BASE = INPUT_DIR / "bt_reptdate.sas7bdat"  # Report date file
BTDTL_BASE = INPUT_DIR / "bt_btdtl"  # Base name for BTDTL (without date suffix)
PBA01_BASE = INPUT_DIR / "pba01"     # Base name for PBA01 (without date suffix)

# Output files
OUTPUT_PARQUET = OUTPUT_DIR / "bt.parquet"
OUTPUT_CSV = OUTPUT_DIR / "bt.csv"
OUTPUT_NOTE_PARQUET = OUTPUT_DIR / "bt_note.parquet"  # Without missing remmth

# Create output directory
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# HELPER FUNCTIONS FOR READING SAS FILES
# ============================================================================

def read_sas_file(filepath: Path) -> pl.DataFrame:
    """Read SAS .sas7bdat file and return as Polars DataFrame"""
    print(f"  Reading: {filepath.name}")
    df, meta = pyreadstat.read_sas7bdat(str(filepath))
    
    # Convert SAS dates to Python dates
    for col_name, col_format in zip(meta.column_names, meta.column_formats):
        if col_format and 'DATE' in str(col_format).upper():
            if col_name in df.columns:
                # Convert numeric SAS dates to datetime
                df[col_name] = df[col_name].apply(
                    lambda x: date(1960, 1, 1) + timedelta(days=int(x)) if pd.notna(x) and x > 0 else None
                )
    
    return pl.from_pandas(df)


def read_sas_with_date_suffix(base_name: Path, reptyear: str, reptmon: str, reptday: str) -> pl.DataFrame:
    """Read SAS file with date suffix (YYYYMMDD)"""
    # Try different naming patterns
    patterns = [
        base_name.parent / f"{base_name.stem}{reptyear}{reptmon}{reptday}.sas7bdat",
        base_name.parent / f"{base_name.stem}_{reptyear}{reptmon}{reptday}.sas7bdat",
        Path(str(base_name).replace('.sas7bdat', f'{reptyear}{reptmon}{reptday}.sas7bdat')),
        Path(str(base_name) + f"{reptyear}{reptmon}{reptday}.sas7bdat"),
    ]
    
    for filepath in patterns:
        if filepath.exists():
            return read_sas_file(filepath)
    
    raise FileNotFoundError(f"SAS file not found with date {reptyear}{reptmon}{reptday}")


# ============================================================================
# MAIN PROCESSING CLASS
# ============================================================================

class BNMLiquidityReport:
    """BNM Liquidity Report Processor"""
    
    def __init__(self):
        self.reptdate = None
        self.reptyear = None
        self.reptmon = None
        self.reptday = None
        self.nowk = None
        self.runoff_dt = None
        self.base_date = date(1960, 1, 1)
        
    def get_week_number(self, day: int) -> str:
        """Determine week number based on report day"""
        if day == 8:
            return '1'
        elif day == 15:
            return '2'
        elif day == 22:
            return '3'
        else:
            return '4'
    
    def step1_read_reptdate(self):
        """Step 1: Read report date and set variables"""
        print("\n" + "=" * 80)
        print("STEP 1: Reading Report Date")
        print("=" * 80)
        
        reptdate_df = read_sas_file(BNM1_BASE)
        self.reptdate = reptdate_df['REPTDATE'][0]
        
        # Convert if needed
        if isinstance(self.reptdate, (int, float)):
            self.reptdate = self.base_date + timedelta(days=int(self.reptdate))
        
        self.reptyear = self.reptdate.year
        self.reptmon = self.reptdate.month
        self.reptday = self.reptdate.day
        self.nowk = self.get_week_number(self.reptdate.day)
        
        print(f"  Report Date: {self.reptdate.strftime('%d/%m/%Y')}")
        print(f"  Year: {self.reptyear:02d}, Month: {self.reptmon:02d}, Day: {self.reptday:02d}")
        print(f"  Week Number: {self.nowk}")
        
    def step2_calculate_runoff_date(self):
        """Step 2: Calculate runoff date (last day of report month)"""
        print("\n" + "=" * 80)
        print("STEP 2: Calculating Runoff Date")
        print("=" * 80)
        
        last_day = get_days_in_month(self.reptyear, self.reptmon)
        self.runoff_dt = date(self.reptyear, self.reptmon, last_day)
        
        print(f"  Runoff Date: {self.runoff_dt.strftime('%d/%m/%Y')}")
        print(f"  SAS Value: {(self.runoff_dt - self.base_date).days}")
        
    def step3_process_btdtl(self):
        """Step 3: Process BTDTL data"""
        print("\n" + "=" * 80)
        print("STEP 3: Processing BTDTL Data")
        print("=" * 80)
        
        # Read BTDTL with date suffix
        btdtl_df = read_sas_with_date_suffix(
            BTDTL_BASE, 
            f"{self.reptyear%100:02d}", 
            f"{self.reptmon:02d}", 
            f"{self.reptday:02d}"
        )
        
        # Filter and keep necessary columns
        self.btdtl_data = btdtl_df.filter(
            (pl.col('ISSDTE') > 0) | (pl.col('EXPRDATE') > 0)
        ).select(['TRANSREF', 'ISSDTE', 'EXPRDATE', 'PAYAMT'])
        
        # Sort and keep first per TRANSREF (descending ISSDTE) - like SAS NODUPKEY
        self.btdtl_data = self.btdtl_data.sort(
            ['TRANSREF', 'ISSDTE'], descending=[False, True]
        ).unique(subset=['TRANSREF'], keep='first')
        
        print(f"  BTDTL records: {len(self.btdtl_data)}")
        
    def step4_process_pba(self):
        """Step 4: Process PBA data (Banker's Acceptance)"""
        print("\n" + "=" * 80)
        print("STEP 4: Processing PBA Data")
        print("=" * 80)
        
        # Read PBA01 with date suffix
        pba_df = read_sas_with_date_suffix(
            PBA01_BASE,
            f"{self.reptyear%100:02d}", 
            f"{self.reptmon:02d}", 
            f"{self.reptday:02d}"
        )
        
        # Extract TRANSREF (skip first character) - like SUBSTR(TRANSREF,2,8)
        pba_data = pba_df.with_columns([
            pl.col('TRANSREF').cast(pl.Utf8).str.slice(1, 8).alias('TRANSREF')
        ])
        
        # Merge PBA with BTDTL
        self.ba_data = pba_data.join(self.btdtl_data, on='TRANSREF', how='left')
        
        print(f"  BA records after merge: {len(self.ba_data)}")
        
    def step5_process_ba(self):
        """Step 5: Process BA transactions"""
        print("\n" + "=" * 80)
        print("STEP 5: Processing BA Transactions")
        print("=" * 80)
        
        ba_records = []
        reptdate_sas = (self.reptdate - self.base_date).days
        runoff_sas = (self.runoff_dt - self.base_date).days
        
        for row in self.ba_data.iter_rows(named=True):
            # Calculate balance
            fcv = row.get('FCVALUE', 0) or 0
            unearned = row.get('UNEARNED', 0) or 0
            balance = fcv - unearned
            
            if balance == 0:
                continue
                
            custcd = str(row.get('CUSTCD', '')) if row.get('CUSTCD') else ''
            product = row.get('PRODUCT', 0) or 0
            bldate = row.get('BLDATE', 0) or 0
            issdte = row.get('ISSDTE', 0) or 0
            exprdate = row.get('EXPRDATE', 0) or 0
            payamt = row.get('PAYAMT', 0) or 0
            
            # Determine customer type
            cust = '08' if custcd in ['77', '78', '95', '96'] else '09'
            
            # Determine item code (based on SAS logic)
            if custcd in ['77', '78', '95', '96']:
                item = '219'  # Default for BT
            else:
                item = '219'  # Default for others
            
            # Hardcode for product 100
            if product == 100:
                item = '212'
            
            # Calculate days
            days = 0
            if bldate and bldate > 0:
                days = reptdate_sas - bldate
            
            # Process maturity profile
            remmth = None
            current_balance = balance
            current_bldate = bldate
            current_issdte = issdte
            
            # Convert SAS dates to Python dates if needed
            if exprdate and exprdate > 0:
                expr_date = self.base_date + timedelta(days=int(exprdate))
                expr_sas = exprdate
            else:
                expr_date = None
                expr_sas = None
            
            if expr_sas and expr_sas <= runoff_sas:
                remmth = None
            elif expr_sas and (expr_sas - runoff_sas) < 8:
                remmth = 0.1
            else:
                # Payment frequency (hardcoded to '3' = 6 months)
                payfreq = '3'
                freq = 6  # For '3'
                
                # Initialize bldate if needed
                if current_bldate <= 0:
                    current_bldate = current_issdte
                    while current_bldate > 0 and current_bldate <= reptdate_sas:
                        bl_date = self.base_date + timedelta(days=int(current_bldate))
                        iss_date = self.base_date + timedelta(days=int(current_issdte)) if current_issdte > 0 else bl_date
                        next_date = calculate_next_bldate(bl_date, iss_date, payfreq, freq)
                        current_bldate = (next_date - self.base_date).days if next_date else 0
                
                if payamt < 0:
                    payamt = 0
                
                if current_bldate > 0 and expr_sas and (current_bldate > expr_sas or current_balance <= payamt):
                    current_bldate = expr_sas
                
                # Process payment schedule
                while current_bldate > 0 and expr_sas and current_bldate <= expr_sas:
                    if current_bldate <= runoff_sas:
                        remmth = None
                    elif (current_bldate - runoff_sas) < 8:
                        remmth = 0.1
                    else:
                        mat_date = self.base_date + timedelta(days=int(current_bldate))
                        remmth = calculate_remmth(mat_date, self.runoff_dt, 
                                                   self.runoff_dt.year, self.runoff_dt.month, self.runoff_dt.day)
                    
                    if remmth and remmth > 1:
                        break
                    if current_bldate == expr_sas:
                        break
                    
                    if payamt > 0 and remmth is not None:
                        amount = payamt
                        current_balance -= payamt
                        
                        # Part 2-RM record (95)
                        bnmcode = f"95{item}{cust}{get_remfmt(remmth)}0000Y"
                        ba_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})
                        
                        # Part 1-RM record (93) - NPL if days > 89
                        remmth_npl = 13 if days > 89 else remmth
                        bnmcode = f"93{item}{cust}{get_remfmt(remmth_npl)}0000Y"
                        ba_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})
                    
                    # Calculate next bldate
                    bl_date = self.base_date + timedelta(days=int(current_bldate))
                    iss_date = self.base_date + timedelta(days=int(current_issdte)) if current_issdte > 0 else bl_date
                    next_date = calculate_next_bldate(bl_date, iss_date, payfreq, freq)
                    current_bldate = (next_date - self.base_date).days if next_date else 0
                    
                    if current_bldate > 0 and expr_sas and (current_bldate > expr_sas or current_balance <= payamt):
                        current_bldate = expr_sas
                
                # Calculate final remmth for remaining balance
                if current_bldate > 0 and current_bldate <= runoff_sas:
                    remmth = None
                elif current_bldate > 0 and (current_bldate - runoff_sas) < 8:
                    remmth = 0.1
                elif current_bldate > 0:
                    mat_date = self.base_date + timedelta(days=int(current_bldate))
                    remmth = calculate_remmth(mat_date, self.runoff_dt,
                                               self.runoff_dt.year, self.runoff_dt.month, self.runoff_dt.day)
            
            # Output remaining balance
            if current_balance != 0:
                # Part 2-RM record
                bnmcode = f"95{item}{cust}{get_remfmt(remmth)}0000Y"
                ba_records.append({'BNMCODE': bnmcode, 'AMOUNT': current_balance})
                
                # Part 1-RM record
                remmth_npl = 13 if days > 89 else remmth
                bnmcode = f"93{item}{cust}{get_remfmt(remmth_npl)}0000Y"
                ba_records.append({'BNMCODE': bnmcode, 'AMOUNT': current_balance})
        
        self.ba_df = pl.DataFrame(ba_records) if ba_records else pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})
        print(f"  BA records created: {len(ba_records)}")
        
    def step6_process_tr(self):
        """Step 6: Process TR transactions"""
        print("\n" + "=" * 80)
        print("STEP 6: Processing TR Transactions")
        print("=" * 80)
        
        # Re-read BTDTL for TR (same as step 3 but with different filters)
        btdtl_full = read_sas_with_date_suffix(
            BTDTL_BASE,
            f"{self.reptyear%100:02d}", 
            f"{self.reptmon:02d}", 
            f"{self.reptday:02d}"
        )
        
        # Filter for TR: LIABCODE not in BAI/BAP/BAS/BAE and DIRCTIND='D'
        tr_data = btdtl_full.filter(
            (~pl.col('LIABCODE').cast(pl.Utf8).is_in(['BAI', 'BAP', 'BAS', 'BAE'])) &
            (pl.col('DIRCTIND').cast(pl.Utf8) == 'D')
        )
        
        print(f"  TR records before processing: {len(tr_data)}")
        
        tr_records = []
        reptdate_sas = (self.reptdate - self.base_date).days
        runoff_sas = (self.runoff_dt - self.base_date).days
        
        for row in tr_data.iter_rows(named=True):
            outstand = row.get('OUTSTAND', 0) or 0
            
            if outstand == 0:
                continue
                
            custcd = str(row.get('CUSTCD', '')) if row.get('CUSTCD') else ''
            product = row.get('PRODUCT', 0) or 0
            bldate = row.get('BLDATE', 0) or 0
            issdte = row.get('ISSDTE', 0) or 0
            exprdate = row.get('EXPRDATE', 0) or 0
            payamt = row.get('PAYAMT', 0) or 0
            
            # Determine customer type
            cust = '08' if custcd in ['77', '78', '95', '96'] else '09'
            
            # Determine item code
            if custcd in ['77', '78', '95', '96']:
                item = '219'
            else:
                item = '219'
            
            if product == 100:
                item = '212'
            
            # Calculate days
            days = 0
            if bldate and bldate > 0:
                days = reptdate_sas - bldate
            
            # Process maturity profile
            remmth = None
            current_outstand = outstand
            current_bldate = bldate
            current_issdte = issdte
            
            if exprdate and exprdate > 0:
                expr_sas = exprdate
            else:
                expr_sas = None
            
            if expr_sas and expr_sas <= runoff_sas:
                remmth = None
            elif expr_sas and (expr_sas - runoff_sas) < 8:
                remmth = 0.1
            else:
                payfreq = '3'
                freq = 6
                
                if current_bldate <= 0:
                    current_bldate = current_issdte
                    while current_bldate > 0 and current_bldate <= reptdate_sas:
                        bl_date = self.base_date + timedelta(days=int(current_bldate))
                        iss_date = self.base_date + timedelta(days=int(current_issdte)) if current_issdte > 0 else bl_date
                        next_date = calculate_next_bldate(bl_date, iss_date, payfreq, freq)
                        current_bldate = (next_date - self.base_date).days if next_date else 0
                
                if payamt < 0:
                    payamt = 0
                
                if current_bldate > 0 and expr_sas and (current_bldate > expr_sas or current_outstand <= payamt):
                    current_bldate = expr_sas
                
                while current_bldate > 0 and expr_sas and current_bldate <= expr_sas:
                    if current_bldate <= runoff_sas:
                        remmth = None
                    elif (current_bldate - runoff_sas) < 8:
                        remmth = 0.1
                    else:
                        mat_date = self.base_date + timedelta(days=int(current_bldate))
                        remmth = calculate_remmth(mat_date, self.runoff_dt,
                                                   self.runoff_dt.year, self.runoff_dt.month, self.runoff_dt.day)
                    
                    if remmth and remmth > 1:
                        break
                    if current_bldate == expr_sas:
                        break
                    
                    if payamt > 0 and remmth is not None:
                        amount = payamt
                        current_outstand -= payamt
                        
                        bnmcode = f"95{item}{cust}{get_remfmt(remmth)}0000Y"
                        tr_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})
                        
                        remmth_npl = 0.1 if days > 89 else remmth
                        bnmcode = f"93{item}{cust}{get_remfmt(remmth_npl)}0000Y"
                        tr_records.append({'BNMCODE': bnmcode, 'AMOUNT': amount})
                    
                    bl_date = self.base_date + timedelta(days=int(current_bldate))
                    iss_date = self.base_date + timedelta(days=int(current_issdte)) if current_issdte > 0 else bl_date
                    next_date = calculate_next_bldate(bl_date, iss_date, payfreq, freq)
                    current_bldate = (next_date - self.base_date).days if next_date else 0
                    
                    if current_bldate > 0 and expr_sas and (current_bldate > expr_sas or current_outstand <= payamt):
                        current_bldate = expr_sas
                
                if current_bldate > 0 and current_bldate <= runoff_sas:
                    remmth = None
                elif current_bldate > 0 and (current_bldate - runoff_sas) < 8:
                    remmth = 0.1
                elif current_bldate > 0:
                    mat_date = self.base_date + timedelta(days=int(current_bldate))
                    remmth = calculate_remmth(mat_date, self.runoff_dt,
                                               self.runoff_dt.year, self.runoff_dt.month, self.runoff_dt.day)
            
            if current_outstand != 0:
                bnmcode = f"95{item}{cust}{get_remfmt(remmth)}0000Y"
                tr_records.append({'BNMCODE': bnmcode, 'AMOUNT': current_outstand})
                
                remmth_npl = 0.1 if days > 89 else remmth
                bnmcode = f"93{item}{cust}{get_remfmt(remmth_npl)}0000Y"
                tr_records.append({'BNMCODE': bnmcode, 'AMOUNT': current_outstand})
        
        self.tr_df = pl.DataFrame(tr_records) if tr_records else pl.DataFrame({'BNMCODE': [], 'AMOUNT': []})
        print(f"  TR records created: {len(tr_records)}")
        
    def step7_combine_and_filter(self):
        """Step 7: Combine BA and TR, filter missing remmth"""
        print("\n" + "=" * 80)
        print("STEP 7: Combining and Filtering Data")
        print("=" * 80)
        
        combine_df = pl.concat([self.ba_df, self.tr_df])
        
        # Filter out records with missing remmth (code '07')
        missing_df = combine_df.filter(pl.col('BNMCODE').str.slice(7, 2) == '07')
        if len(missing_df) > 0:
            print(f"  Records with MISSING remmth (code '07'): {len(missing_df)}")
            print(f"  Missing amount sum: {missing_df['AMOUNT'].sum():,.2f}")
        else:
            print("  Records with MISSING remmth (code '07'): 0")
        
        self.note_df = combine_df.filter(pl.col('BNMCODE').str.slice(7, 2) != '07')
        print(f"  Valid records: {len(self.note_df)}")
        
    def step8_summarize_and_output(self):
        """Step 8: Summarize and output"""
        print("\n" + "=" * 80)
        print("STEP 8: Summarizing and Writing Output")
        print("=" * 80)
        
        # Summarize by BNMCODE
        bnm_df = self.note_df.group_by('BNMCODE').agg([
            pl.col('AMOUNT').sum().alias('AMOUNT')
        ]).sort('BNMCODE')
        
        # Write outputs
        print(f"\n  Writing Parquet: {OUTPUT_PARQUET}")
        bnm_df.write_parquet(OUTPUT_PARQUET)
        
        print(f"  Writing CSV: {OUTPUT_CSV}")
        bnm_df.write_csv(OUTPUT_CSV)
        
        # Also output NOTE file (without missing)
        print(f"  Writing NOTE Parquet: {OUTPUT_NOTE_PARQUET}")
        self.note_df.write_parquet(OUTPUT_NOTE_PARQUET)
        
        # Summary
        total_amount = bnm_df['AMOUNT'].sum() if len(bnm_df) > 0 else 0
        
        print("\n" + "=" * 80)
        print("PROCESSING COMPLETE")
        print("=" * 80)
        print(f"\nOutput files:")
        print(f"  Parquet: {OUTPUT_PARQUET}")
        print(f"  CSV:     {OUTPUT_CSV}")
        print(f"  NOTE:    {OUTPUT_NOTE_PARQUET}")
        print(f"\nSummary:")
        print(f"  Total BNM Codes: {len(bnm_df)}")
        print(f"  Total Amount:    {total_amount:,.2f}")
        
        if len(bnm_df) > 0 and len(bnm_df) <= 20:
            print(f"\nBreakdown by BNMCODE:")
            print("-" * 50)
            for row in bnm_df.iter_rows(named=True):
                print(f"  {row['BNMCODE']}: {row['AMOUNT']:>15,.2f}")
    
    def run(self):
        """Run all processing steps"""
        import pandas as pd  # Import here for date conversion
        
        self.step1_read_reptdate()
        self.step2_calculate_runoff_date()
        self.step3_process_btdtl()
        self.step4_process_pba()
        self.step5_process_ba()
        self.step6_process_tr()
        self.step7_combine_and_filter()
        self.step8_summarize_and_output()


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    processor = BNMLiquidityReport()
    processor.run()
