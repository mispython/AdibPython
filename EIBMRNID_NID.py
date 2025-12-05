import polars as pl
import duckdb
from pathlib import Path
import datetime
from datetime import date
import numpy as np
from typing import Dict, List, Tuple

class NIDProcessor:
    def __init__(self, nid_data_path: str, trnch_data_path: str, report_date: date = None):
        """
        Initialize NID processor
        
        Args:
            nid_data_path: Path to RNIDM data
            trnch_data_path: Path to TRNCHM data
            report_date: Reporting date (defaults to last day of previous month)
        """
        self.nid_path = Path(nid_data_path)
        self.trnch_path = Path(trnch_data_path)
        
        # Set report date (last day of previous month)
        if report_date is None:
            today = date.today()
            first_of_month = date(today.year, today.month, 1)
            self.reptdate = first_of_month - datetime.timedelta(days=1)
        else:
            self.reptdate = report_date
            
        self.startdte = date(self.reptdate.year, self.reptdate.month, 1)
        
        # Format strings for output
        self.reptmon = f"{self.reptdate.month:02d}"
        self.reptday = f"{self.reptdate.day:02d}"
        self.reptdt = self.reptdate.strftime("%d/%m/%Y")
        self.rpdate_str = self.reptdate.strftime("%Y-%m-%d")
        
        # Define format dictionaries
        self.remfmta = self._create_format_dict_a()
        self.remfmtb = self._create_format_dict_b()
        
        # Days in month arrays
        self.rpdays = self._create_days_in_month_array(self.reptdate.year)
        self.mddays = self._create_days_in_month_array(self.reptdate.year)
        
    def _create_format_dict_a(self) -> Dict[Tuple[float, float], str]:
        """Create format dictionary for REMFMTA"""
        return {
            (-float('inf'), 6): '1. LE  6      ',
            (6, 12): '2. GT  6 TO 12',
            (12, 24): '3. GT 12 TO 24',
            (24, 36): '4. GT 24 TO 36',
            (36, 60): '5. GT 36 TO 60',
            'other': '              '
        }
    
    def _create_format_dict_b(self) -> Dict[Tuple[float, float], str]:
        """Create format dictionary for REMFMTB"""
        return {
            (-float('inf'), 1): '1. LE  1      ',
            (1, 3): '2. GT  1 TO  3',
            (3, 6): '3. GT  3 TO  6',
            (6, 9): '4. GT  6 TO  9',
            (9, 12): '5. GT  9 TO 12',
            (12, 24): '6. GT 12 TO 24',
            (24, 36): '7. GT 24 TO 36',
            (36, 60): '8. GT 36 TO 60',
            'other': '             '
        }
    
    def _create_days_in_month_array(self, year: int) -> List[int]:
        """Create array of days in each month for a given year"""
        days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        # Adjust for leap year
        if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
            days_in_month[1] = 29
        return days_in_month
    
    def _apply_format(self, value: float, fmt_dict: Dict) -> str:
        """Apply format based on value ranges"""
        if pd.isna(value):
            return fmt_dict['other']
        
        for (low, high), label in fmt_dict.items():
            if low == 'other':
                continue
            if low <= value < high:
                return label
        return fmt_dict['other']
    
    def _calculate_remmth(self, row: dict) -> float:
        """Calculate remaining months"""
        matdt = row['matdt']
        curbal = row['curbal']
        
        # Skip calculation for certain conditions
        if matdt <= self.reptdate:
            return None
        
        matdt_diff = (matdt - self.reptdate).days
        if matdt_diff < 8:
            return 0.1
        
        # Calculate remaining months
        mdyr = matdt.year
        mdmth = matdt.month
        mdday = min(matdt.day, self.rpdays[mdmth - 1])
        
        remy = mdyr - self.reptdate.year
        remm = mdmth - self.reptdate.month
        remd = mdday - self.reptdate.day
        
        # Adjust for month/year rollovers
        if remd < 0:
            remm -= 1
            if remm < 0:
                remy -= 1
                remm += 12
            remd += self.rpdays[(mdmth - 2) % 12]  # Previous month's days
        
        remmth = remy * 12 + remm + remd / self.rpdays[mdmth - 1]
        
        # Also calculate rem30d
        rem30d = (matdt - self.reptdate).days / 30
        
        return remmth
    
    def load_and_process_data(self) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """Load and merge NID and TRNCH data"""
        # Load data using Polars
        rnid_df = pl.read_csv(self.nid_path)
        trnch_df = pl.read_csv(self.trnch_path)
        
        # Merge data
        merged_df = rnid_df.join(
            trnch_df, 
            on="trancheno", 
            how="inner"
        )
        
        # Convert date columns
        date_columns = ['matdt', 'startdt', 'early_wddt']
        for col in date_columns:
            if col in merged_df.columns:
                merged_df = merged_df.with_columns(
                    pl.col(col).str.strptime(pl.Date, format="%Y-%m-%d")
                )
        
        # Filter for positive current balance
        merged_df = merged_df.filter(pl.col("curbal") > 0)
        
        # Add calculation columns
        merged_df = merged_df.with_columns([
            pl.lit(self.reptdate).alias("reptdate"),
            pl.lit(self.startdte).alias("startdte"),
        ])
        
        return merged_df
    
    def create_table1_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create data for Table 1 - Outstanding Retail NID"""
        # Filter conditions
        table1_df = df.filter(
            (pl.col("matdt") > pl.col("reptdate")) &
            (pl.col("startdt") <= pl.col("reptdate"))
        )
        
        # Calculate remaining months
        table1_df = table1_df.with_columns([
            pl.struct(["matdt", "curbal"]).map_elements(
                lambda x: self._calculate_remmth(x),
                return_dtype=pl.Float64
            ).alias("remmth")
        ])
        
        # Apply filters for different output tables
        table1_condition = (
            (pl.col("nidstat") == "N") & 
            (pl.col("cdstat") == "A")
        )
        
        table1_output = table1_df.filter(table1_condition).with_columns([
            pl.lit(0).alias("heldmkt"),
            (pl.col("curbal") - pl.col("heldmkt")).alias("outstanding"),
            pl.col("remmth").map_elements(
                lambda x: self._apply_format(x, self.remfmta),
                return_dtype=pl.Utf8
            ).alias("remmfmt")
        ])
        
        return table1_output
    
    def create_table3_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create data for Table 3 - Indicative Mid Yield"""
        # Filter conditions
        table3_condition = (
            (pl.col("nidstat") == "N") & 
            (pl.col("cdstat") == "A")
        )
        
        table3_df = df.filter(table3_condition).with_columns([
            pl.col("remmth").map_elements(
                lambda x: self._apply_format(x, self.remfmtb),
                return_dtype=pl.Utf8
            ).alias("remmfmt"),
            ((pl.col("intplrate_bid") + pl.col("intplrate_offer")) / 2).alias("yield")
        ])
        
        # Remove duplicates
        table3_df = table3_df.unique(subset=["remmfmt", "trancheno"])
        
        return table3_df
    
    def create_table2_data(self, df: pl.DataFrame) -> Dict:
        """Create data for Table 2 - Monthly Trading Volume"""
        # Filter for early withdrawals
        table2_condition = (
            (pl.col("nidstat") == "E") &
            (pl.col("early_wddt") >= self.startdte) &
            (pl.col("early_wddt") <= self.reptdate)
        )
        
        table2_df = df.filter(table2_condition)
        
        # Calculate aggregates
        nidcnt = table2_df.height
        nidvol = table2_df["curbal"].sum()
        
        return {
            "nidcnt": nidcnt,
            "nidvol": nidvol
        }
    
    def generate_summary_tables(self):
        """Generate all summary tables"""
        # Load and process data
        df = self.load_and_process_data()
        
        # Create table data
        table1_data = self.create_table1_data(df)
        table3_data = self.create_table3_data(df)
        table2_stats = self.create_table2_data(df)
        
        # Generate Table 1 summary
        table1_summary = table1_data.group_by("remmfmt").agg([
            pl.sum("curbal").alias("curbal_sum"),
            pl.sum("heldmkt").alias("heldmkt_sum"),
            pl.sum("outstanding").alias("outstanding_sum")
        ]).sort("remmfmt")
        
        # Generate Table 3 summary with yield calculations
        table3_summary = table3_data.group_by("remmfmt").agg([
            pl.sum("yield").alias("pctyield_sum"),
            pl.col("yield").filter(pl.col("yield") > 0).count().alias("cntyield"),
            pl.col("yield").mean().alias("midyield")
        ]).sort("remmfmt")
        
        # Calculate overall average yield
        overall_yield = table3_data.filter(pl.col("yield") > 0)["yield"].mean()
        
        return {
            "table1": table1_summary,
            "table2": table2_stats,
            "table3": table3_summary,
            "overall_yield": overall_yield
        }
    
    def generate_output_report(self, output_path: Path):
        """Generate the final output report"""
        summaries = self.generate_summary_tables()
        
        # Convert to pandas for easier formatting (optional, could use Polars directly)
        table1_df = summaries["table1"].to_pandas()
        table3_df = summaries["table3"].to_pandas()
        
        with open(output_path, 'w', encoding='utf-8') as f:
            # Header
            f.write("PUBLIC BANK BERHAD\n\n")
            f.write("REPORT ON RETAIL RINGGIT-DENOMINATED NEGOTIABLE ")
            f.write("INSTRUMENT OF DEPOSIT (NID)\n")
            f.write(f"REPORTING DATE : {self.reptdt}\n\n")
            
            # Table 1
            f.write("\x05\n")
            f.write("Table 1 - Outstanding Retail NID\n\n")
            f.write("\x05\n")
            f.write("REMAINING MATURITY (IN MONTHS)\x05")
            f.write("GROSS ISSUANCE\x05")
            f.write("HELD FOR MARKET MARKING\x05")
            f.write("NET OUTSTANDING\x05\n")
            f.write("\x05\x05(A)\x05(B)\x05(A-B)\x05\n")
            
            total_curbal = 0
            total_heldmkt = 0
            total_outstanding = 0
            
            for _, row in table1_df.iterrows():
                remmfmt = row['remmfmt']
                remmgrp = remmfmt.split('.')[1].strip() if '.' in remmfmt else ''
                
                curbal = float(row['curbal_sum'])
                heldmkt = float(row['heldmkt_sum'])
                outstanding = float(row['outstanding_sum'])
                
                total_curbal += curbal
                total_heldmkt += heldmkt
                total_outstanding += outstanding
                
                f.write(f"\x05{remmgrp}\x05")
                f.write(f"{curbal:,.2f}\x05")
                f.write(f"{heldmkt:,.2f}\x05")
                f.write(f"{outstanding:,.2f}\x05\n")
            
            # Table 1 totals
            f.write(f"\x05TOTAL\x05")
            f.write(f"{total_curbal:,.2f}\x05")
            f.write(f"{total_heldmkt:,.2f}\x05")
            f.write(f"{total_outstanding:,.2f}\x05\n")
            
            # Table 2
            f.write("\x05\n")
            f.write("Table 2 - Monthly Trading Volume\n\n")
            f.write("\x05\n")
            f.write("GROSS MONTHLY PURCHASE OF RETAIL NID BY THE BANK\x05")
            f.write("A) NUMBER OF NID\x05")
            
            nidcnt = summaries["table2"]["nidcnt"]
            nidvol = summaries["table2"]["nidvol"]
            
            if nidcnt == 0:
                f.write("0.00\x05\n")
                f.write("\x05\x05B) VOLUME OF NID\x050.00\x05\n")
            else:
                f.write(f"{nidcnt}\x05\n")
                f.write(f"\x05\x05B) VOLUME OF NID\x05{nidvol:,.2f}\x05\n")
            
            # Table 3
            f.write("\x05\n")
            f.write("Table 3 - Indicative Mid Yield\n\n")
            f.write("\x05\n")
            f.write("REMAINING MATURITY (IN MONTHS)\x05(%)\x05\n")
            
            for _, row in table3_df.iterrows():
                remmfmt = row['remmfmt']
                remmgrp = remmfmt.split('.')[1].strip() if '.' in remmfmt else ''
                midyield = float(row['midyield'])
                
                f.write(f"\x05{remmgrp}\x05{midyield:.4f}\x05\n")
            
            # Overall average
            overall_yield = summaries["overall_yield"]
            f.write("\x05AVERAGE BID-ASK SPREAD ACROSS MATURITIES\x05")
            f.write(f"{overall_yield:.4f}\x05\n")


# Alternative implementation using DuckDB for SQL-like operations
class NIDProcessorDuckDB(NIDProcessor):
    def load_and_process_data_duckdb(self) -> pl.DataFrame:
        """Load and process data using DuckDB"""
        conn = duckdb.connect()
        
        # Register date as parameter
        conn.execute(f"""
            CREATE TEMP TABLE date_params AS 
            SELECT 
                '{self.rpdate_str}'::DATE as reptdate,
                '{self.startdte}'::DATE as startdte
        """)
        
        # Load and merge data
        query = f"""
        WITH rnid AS (
            SELECT * FROM read_csv_auto('{self.nid_path}')
        ),
        trnch AS (
            SELECT * FROM read_csv_auto('{self.trnch_path}')
        ),
        merged AS (
            SELECT r.*, t.* EXCLUDE(trancheno)
            FROM rnid r
            INNER JOIN trnch t ON r.trancheno = t.trancheno
        ),
        with_dates AS (
            SELECT 
                m.*,
                d.reptdate,
                d.startdte,
                CAST(m.matdt AS DATE) as matdt_date,
                CAST(m.startdt AS DATE) as startdt_date,
                CAST(m.early_wddt AS DATE) as early_wddt_date
            FROM merged m
            CROSS JOIN date_params d
            WHERE m.curbal > 0
        )
        SELECT * FROM with_dates
        """
        
        result = conn.execute(query).fetch_arrow_table()
        return pl.from_arrow(result)


# Usage example
def main():
    # Initialize processor
    processor = NIDProcessor(
        nid_data_path="path/to/NID/RNIDM01.csv",
        trnch_data_path="path/to/NID/TRNCHM01.csv"
    )
    
    # Generate report
    output_path = Path("NID_REPORT.txt")
    processor.generate_output_report(output_path)
    
    print(f"Report generated: {output_path}")
    
    # Or use DuckDB version
    processor_db = NIDProcessorDuckDB(
        nid_data_path="path/to/NID/RNIDM01.csv",
        trnch_data_path="path/to/NID/TRNCHM01.csv"
    )
    
    # Process with DuckDB
    df = processor_db.load_and_process_data_duckdb()
    print(f"Processed {len(df)} records")


if __name__ == "__main__":
    main()
