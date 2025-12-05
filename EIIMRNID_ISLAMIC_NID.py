import polars as pl
import duckdb
from pathlib import Path
import datetime
from datetime import date
import numpy as np
from typing import Dict, List, Tuple
import re

class IslamicNIDProcessor:
    def __init__(self, inid_data_path: str, trnch_data_path: str, 
                 sharia_approval_path: str, report_date: date = None):
        """
        Initialize Islamic NID processor
        
        Args:
            inid_data_path: Path to Islamic NID data
            trnch_data_path: Path to tranche data
            sharia_approval_path: Path to Sharia approval records
            report_date: Reporting date (defaults to last day of previous month)
        """
        self.inid_path = Path(inid_data_path)
        self.trnch_path = Path(trnch_path)
        self.sharia_path = Path(sharia_approval_path)
        
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
        
        # Islamic-specific attributes
        self.sharia_compliance_status = self._load_sharia_status()
        
        # Define format dictionaries for Islamic instruments
        self.remfmta = self._create_islamic_format_dict_a()
        self.remfmtb = self._create_islamic_format_dict_b()
        
        # Islamic profit rate buckets (different from interest rates)
        self.profit_rate_buckets = self._create_profit_rate_buckets()
        
        # Days in month arrays
        self.rpdays = self._create_days_in_month_array(self.reptdate.year)
        self.mddays = self._create_days_in_month_array(self.reptdate.year)
        
    def _load_sharia_status(self) -> Dict:
        """Load Sharia compliance approval status"""
        # This would typically come from a Sharia board approval database
        return {
            'approved_contracts': ['Murabahah', 'Wakalah', 'Salam', 'Istisna'],
            'prohibited_elements': ['Riba', 'Gharar', 'Maysir'],
            'approved_counterparties': self._load_approved_counterparties()
        }
    
    def _load_approved_counterparties(self) -> List[str]:
        """Load list of Sharia-approved counterparties"""
        try:
            df = pl.read_csv(self.sharia_path)
            return df['counterparty_id'].to_list()
        except:
            return []
    
    def _create_islamic_format_dict_a(self) -> Dict[Tuple[float, float], str]:
        """Create format dictionary for Islamic REMFMTA"""
        return {
            (-float('inf'), 6): '1. LE  6      ',
            (6, 12): '2. GT  6 TO 12',
            (12, 24): '3. GT 12 TO 24',
            (24, 36): '4. GT 24 TO 36',
            (36, 60): '5. GT 36 TO 60',
            (60, 120): '6. GT 60 TO 120',  # Extended for longer-term Islamic contracts
            'other': '              '
        }
    
    def _create_islamic_format_dict_b(self) -> Dict[Tuple[float, float], str]:
        """Create format dictionary for Islamic REMFMTB"""
        return {
            (-float('inf'), 1): '1. LE  1      ',
            (1, 3): '2. GT  1 TO  3',
            (3, 6): '3. GT  3 TO  6',
            (6, 9): '4. GT  6 TO  9',
            (9, 12): '5. GT  9 TO 12',
            (12, 24): '6. GT 12 TO 24',
            (24, 36): '7. GT 24 TO 36',
            (36, 60): '8. GT 36 TO 60',
            (60, 84): '9. GT 60 TO 84',  # Extended for Islamic instruments
            (84, 120): '10. GT 84 TO 120',
            'other': '             '
        }
    
    def _create_profit_rate_buckets(self) -> Dict:
        """Create profit rate buckets for Islamic instruments"""
        return {
            'tier_1': (0.0, 2.0),    # Very low risk/low return
            'tier_2': (2.0, 4.0),    # Standard Murabahah rates
            'tier_3': (4.0, 6.0),    # Higher risk projects
            'tier_4': (6.0, 8.0),    # Special projects
            'tier_5': (8.0, float('inf'))  # High risk/return
        }
    
    def _create_days_in_month_array(self, year: int) -> List[int]:
        """Create array of days in each month for a given year"""
        days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        # Adjust for leap year
        if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
            days_in_month[1] = 29
        return days_in_month
    
    def _apply_islamic_format(self, value: float, fmt_dict: Dict) -> str:
        """Apply format based on value ranges for Islamic instruments"""
        if value is None or np.isnan(value):
            return fmt_dict['other']
        
        for (low, high), label in fmt_dict.items():
            if low == 'other':
                continue
            if low <= value < high:
                return label
        return fmt_dict['other']
    
    def _validate_sharia_compliance(self, row: dict) -> Tuple[bool, str]:
        """Validate if transaction complies with Sharia principles"""
        issues = []
        
        # Check contract type
        contract_type = row.get('contract_type', '').upper()
        if contract_type not in [ct.upper() for ct in self.sharia_compliance_status['approved_contracts']]:
            issues.append(f"Unapproved contract type: {contract_type}")
        
        # Check for prohibited elements in description
        description = row.get('instrument_desc', '').upper()
        for prohibited in self.sharia_compliance_status['prohibited_elements']:
            if prohibited.upper() in description:
                issues.append(f"Contains prohibited element: {prohibited}")
        
        # Check counterparty approval
        counterparty = row.get('counterparty_id', '')
        if counterparty not in self.sharia_compliance_status['approved_counterparties']:
            issues.append(f"Counterparty not Sharia-approved: {counterparty}")
        
        # Check profit rate structure
        profit_rate = row.get('profit_rate', 0)
        upfront_fee = row.get('upfront_fee', 0)
        
        # Islamic principle: Profit should not be guaranteed
        if row.get('guaranteed_profit', 'N').upper() == 'Y':
            issues.append("Profit appears to be guaranteed (not allowed)")
        
        # Check for hidden interest (riba)
        if profit_rate > 0 and upfront_fee > 0 and profit_rate + upfront_fee > 15:  # Example threshold
            issues.append("Total return exceeds reasonable profit margin")
        
        is_compliant = len(issues) == 0
        compliance_note = "; ".join(issues) if issues else "Sharia Compliant"
        
        return is_compliant, compliance_note
    
    def _calculate_remaining_period(self, row: dict) -> float:
        """
        Calculate remaining period for Islamic instruments
        Different calculation for Murabahah vs Salam vs Istisna
        """
        matdt = row['matdt']
        contract_type = row.get('contract_type', 'MURABAHAH').upper()
        
        if matdt <= self.reptdate:
            return None
        
        matdt_diff = (matdt - self.reptdate).days
        
        # Special handling for different Islamic contracts
        if contract_type == 'SALAM':
            # Salam contracts have delivery date separate from payment date
            delivery_date = row.get('delivery_date', matdt)
            if isinstance(delivery_date, str):
                delivery_date = datetime.datetime.strptime(delivery_date, '%Y-%m-%d').date()
            
            # For Salam, remaining period based on delivery date
            if delivery_date > self.reptdate:
                effective_date = delivery_date
            else:
                effective_date = matdt
            
            effective_diff = (effective_date - self.reptdate).days
            if effective_diff < 15:  # 15 days for Salam
                return 0.1
        
        elif contract_type == 'ISTISNA':
            # Istisna (manufacturing contract) may have milestone dates
            milestone_date = row.get('next_milestone_date', matdt)
            if isinstance(milestone_date, str):
                milestone_date = datetime.datetime.strptime(milestone_date, '%Y-%m-%d').date()
            
            if milestone_date > self.reptdate:
                effective_date = milestone_date
            else:
                effective_date = matdt
            
            effective_diff = (effective_date - self.reptdate).days
            if effective_diff < 30:  # 30 days for Istisna milestones
                return 0.1
        
        else:  # MURABAHAH, WAKALAH, etc.
            if matdt_diff < 8:
                return 0.1
        
        # Standard remaining months calculation
        mdyr = matdt.year
        mdmth = matdt.month
        mdday = min(matdt.day, self.rpdays[mdmth - 1])
        
        remy = mdyr - self.reptdate.year
        remm = mdmth - self.reptdate.month
        remd = mdday - self.reptdate.day
        
        if remd < 0:
            remm -= 1
            if remm < 0:
                remy -= 1
                remm += 12
            remd += self.rpdays[(mdmth - 2) % 12]
        
        remmth = remy * 12 + remm + remd / self.rpdays[mdmth - 1]
        
        return remmth
    
    def _calculate_effective_profit_rate(self, row: dict) -> float:
        """
        Calculate effective profit rate for Islamic instruments
        Considering profit margin, fees, and cost of underlying asset
        """
        profit_rate = row.get('profit_rate', 0)
        upfront_fee = row.get('upfront_fee', 0)
        cost_price = row.get('asset_cost_price', row.get('face_value', 1))
        
        if cost_price <= 0:
            return profit_rate
        
        # Effective rate includes upfront fees amortized
        tenure_days = (row['matdt'] - row['issuance_date']).days if 'matdt' in row and 'issuance_date' in row else 365
        if tenure_days <= 0:
            tenure_days = 365
        
        # Annualized fee component
        annualized_fee = (upfront_fee / cost_price) * (365 / tenure_days) * 100
        
        # Total effective rate
        effective_rate = profit_rate + annualized_fee
        
        return effective_rate
    
    def load_and_process_islamic_data(self) -> pl.DataFrame:
        """Load and merge Islamic NID data with Sharia compliance checks"""
        # Load data using Polars
        inid_df = pl.read_csv(self.inid_path)
        trnch_df = pl.read_csv(self.trnch_path)
        
        # Merge data
        merged_df = inid_df.join(
            trnch_df, 
            on="trancheno", 
            how="inner"
        )
        
        # Convert date columns
        date_columns = ['matdt', 'issuance_date', 'startdt', 'early_wddt', 
                       'delivery_date', 'next_milestone_date']
        for col in date_columns:
            if col in merged_df.columns:
                merged_df = merged_df.with_columns(
                    pl.col(col).str.strptime(pl.Date, format="%Y-%m-%d")
                )
        
        # Filter for positive current balance
        merged_df = merged_df.filter(pl.col("curbal") > 0)
        
        # Add Sharia compliance validation
        merged_df = merged_df.with_columns([
            pl.struct(merged_df.columns).map_elements(
                lambda x: self._validate_sharia_compliance(dict(zip(merged_df.columns, x)))[0],
                return_dtype=pl.Boolean
            ).alias("sharia_compliant"),
            
            pl.struct(merged_df.columns).map_elements(
                lambda x: self._validate_sharia_compliance(dict(zip(merged_df.columns, x)))[1],
                return_dtype=pl.Utf8
            ).alias("compliance_note"),
        ])
        
        # Filter for Sharia compliant instruments only
        merged_df = merged_df.filter(pl.col("sharia_compliant") == True)
        
        # Add calculation columns
        merged_df = merged_df.with_columns([
            pl.lit(self.reptdate).alias("reptdate"),
            pl.lit(self.startdte).alias("startdte"),
        ])
        
        return merged_df
    
    def create_islamic_table1_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create data for Table 1 - Outstanding Islamic NID"""
        # Filter conditions
        table1_df = df.filter(
            (pl.col("matdt") > pl.col("reptdate")) &
            (pl.col("startdt") <= pl.col("reptdate"))
        )
        
        # Calculate remaining period based on contract type
        table1_df = table1_df.with_columns([
            pl.struct(["matdt", "contract_type", "delivery_date", 
                      "next_milestone_date"]).map_elements(
                lambda x: self._calculate_remaining_period(x),
                return_dtype=pl.Float64
            ).alias("remmth"),
            
            pl.struct(["profit_rate", "upfront_fee", "asset_cost_price", 
                      "matdt", "issuance_date"]).map_elements(
                lambda x: self._calculate_effective_profit_rate(x),
                return_dtype=pl.Float64
            ).alias("effective_profit_rate")
        ])
        
        # Apply filters for Islamic instruments
        table1_condition = (
            (pl.col("nidstat") == "N") & 
            (pl.col("cdstat") == "A") &
            (pl.col("sharia_compliant") == True)
        )
        
        table1_output = table1_df.filter(table1_condition).with_columns([
            pl.lit(0).alias("heldmkt"),
            (pl.col("curbal") - pl.col("heldmkt")).alias("outstanding"),
            pl.col("remmth").map_elements(
                lambda x: self._apply_islamic_format(x, self.remfmta),
                return_dtype=pl.Utf8
            ).alias("remmfmt"),
            
            # Islamic-specific columns
            pl.col("effective_profit_rate").alias("profit_rate"),
            pl.col("contract_type").alias("islamic_contract"),
            pl.col("underlying_asset").alias("asset_type")
        ])
        
        return table1_output
    
    def create_islamic_table3_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create data for Table 3 - Indicative Profit Rate"""
        # Filter conditions for Islamic instruments
        table3_condition = (
            (pl.col("nidstat") == "N") & 
            (pl.col("cdstat") == "A") &
            (pl.col("sharia_compliant") == True)
        )
        
        table3_df = df.filter(table3_condition).with_columns([
            pl.col("remmth").map_elements(
                lambda x: self._apply_islamic_format(x, self.remfmtb),
                return_dtype=pl.Utf8
            ).alias("remmfmt"),
            pl.col("effective_profit_rate").alias("profit_rate"),
            
            # Calculate bid-ask spread for Islamic instruments
            ((pl.col("buying_rate") + pl.col("selling_rate")) / 2).alias("mid_rate"),
            
            # Risk weighting for Islamic instruments
            pl.when(pl.col("contract_type") == "MURABAHAH").then(1.0)
            .when(pl.col("contract_type") == "SALAM").then(1.2)
            .when(pl.col("contract_type") == "ISTISNA").then(1.5)
            .otherwise(1.0).alias("risk_weight")
        ])
        
        # Remove duplicates
        table3_df = table3_df.unique(subset=["remmfmt", "trancheno"])
        
        return table3_df
    
    def create_islamic_table2_data(self, df: pl.DataFrame) -> Dict:
        """Create data for Table 2 - Monthly Trading Volume for Islamic NIDs"""
        # Filter for early withdrawals in Islamic instruments
        table2_condition = (
            (pl.col("nidstat") == "E") &
            (pl.col("early_wddt") >= self.startdte) &
            (pl.col("early_wddt") <= self.reptdate) &
            (pl.col("sharia_compliant") == True)
        )
        
        table2_df = df.filter(table2_condition)
        
        # Calculate aggregates with Islamic categorization
        nidcnt = table2_df.height
        nidvol = table2_df["curbal"].sum()
        
        # Additional Islamic metrics
        by_contract = table2_df.group_by("contract_type").agg([
            pl.count().alias("count"),
            pl.sum("curbal").alias("volume")
        ])
        
        by_asset = table2_df.group_by("underlying_asset").agg([
            pl.count().alias("count"),
            pl.sum("curbal").alias("volume")
        ])
        
        return {
            "nidcnt": nidcnt,
            "nidvol": nidvol,
            "by_contract": by_contract,
            "by_asset": by_asset
        }
    
    def create_sharia_compliance_report(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create Sharia compliance summary report"""
        compliance_summary = df.group_by("contract_type").agg([
            pl.count().alias("total_count"),
            pl.sum("curbal").alias("total_volume"),
            pl.col("compliance_note").filter(
                pl.col("compliance_note") != "Sharia Compliant"
            ).count().alias("compliance_issues"),
            pl.col("effective_profit_rate").mean().alias("avg_profit_rate")
        ])
        
        # Calculate overall compliance percentage
        total = df.height
        compliant = df.filter(pl.col("sharia_compliant") == True).height
        compliance_pct = (compliant / total * 100) if total > 0 else 100
        
        return compliance_summary, compliance_pct
    
    def generate_islamic_summary_tables(self):
        """Generate all summary tables for Islamic NIDs"""
        # Load and process data
        df = self.load_and_process_islamic_data()
        
        # Create table data
        table1_data = self.create_islamic_table1_data(df)
        table3_data = self.create_islamic_table3_data(df)
        table2_stats = self.create_islamic_table2_data(df)
        compliance_summary, compliance_pct = self.create_sharia_compliance_report(df)
        
        # Generate Table 1 summary
        table1_summary = table1_data.group_by(["remmfmt", "islamic_contract"]).agg([
            pl.sum("curbal").alias("curbal_sum"),
            pl.sum("heldmkt").alias("heldmkt_sum"),
            pl.sum("outstanding").alias("outstanding_sum"),
            pl.mean("profit_rate").alias("avg_profit_rate")
        ]).sort(["remmfmt", "islamic_contract"])
        
        # Generate Table 3 summary with profit rate calculations
        table3_summary = table3_data.group_by("remmfmt").agg([
            pl.mean("mid_rate").alias("mid_rate"),
            pl.mean("profit_rate").alias("avg_profit_rate"),
            pl.col("risk_weight").mean().alias("avg_risk_weight"),
            pl.count().alias("instrument_count")
        ]).sort("remmfmt")
        
        # Calculate risk-adjusted profit rate
        overall_risk_adj_rate = (table3_data["mid_rate"] * table3_data["risk_weight"]).mean()
        
        return {
            "table1": table1_summary,
            "table2": table2_stats,
            "table3": table3_summary,
            "compliance_summary": compliance_summary,
            "compliance_pct": compliance_pct,
            "overall_risk_adj_rate": overall_risk_adj_rate
        }
    
    def generate_islamic_output_report(self, output_path: Path):
        """Generate the final Islamic NID output report"""
        summaries = self.generate_islamic_summary_tables()
        
        # Convert to pandas for easier formatting
        table1_df = summaries["table1"].to_pandas()
        table3_df = summaries["table3"].to_pandas()
        compliance_df = summaries["compliance_summary"].to_pandas()
        
        with open(output_path, 'w', encoding='utf-8') as f:
            # Islamic Banking Header
            f.write("PUBLIC ISLAMIC BANK BERHAD\n\n")
            f.write("REPORT ON RETAIL RINGGIT-DENOMINATED ISLAMIC ")
            f.write("NEGOTIABLE INSTRUMENT OF DEPOSIT (INID)\n")
            f.write(f"REPORTING DATE : {self.reptdt}\n")
            f.write(f"SHARIA COMPLIANCE: {summaries['compliance_pct']:.1f}%\n\n")
            
            # Sharia Compliance Summary
            f.write("\x05\n")
            f.write("Sharia Compliance Summary\n\n")
            f.write("\x05\n")
            f.write("CONTRACT TYPE\x05TOTAL COUNT\x05TOTAL VOLUME\x05")
            f.write("COMPLIANCE ISSUES\x05AVG PROFIT RATE\x05\n")
            
            for _, row in compliance_df.iterrows():
                f.write(f"\x05{row['contract_type']}\x05")
                f.write(f"{row['total_count']}\x05")
                f.write(f"{row['total_volume']:,.2f}\x05")
                f.write(f"{row['compliance_issues']}\x05")
                f.write(f"{row['avg_profit_rate']:.2f}%\x05\n")
            
            # Table 1 - Outstanding Islamic NID
            f.write("\x05\n")
            f.write("Table 1 - Outstanding Islamic NID (Sharia Compliant)\n\n")
            f.write("\x05\n")
            f.write("REMAINING MATURITY\x05CONTRACT TYPE\x05")
            f.write("GROSS ISSUANCE\x05HELD FOR MARKET\x05")
            f.write("NET OUTSTANDING\x05AVG PROFIT RATE\x05\n")
            f.write("\x05\x05\x05(A)\x05(B)\x05(A-B)\x05(%)\x05\n")
            
            total_curbal = 0
            total_heldmkt = 0
            total_outstanding = 0
            
            for _, row in table1_df.iterrows():
                remmfmt = row['remmfmt']
                remmgrp = remmfmt.split('.')[1].strip() if '.' in remmfmt else ''
                
                curbal = float(row['curbal_sum'])
                heldmkt = float(row['heldmkt_sum'])
                outstanding = float(row['outstanding_sum'])
                profit_rate = float(row['avg_profit_rate'])
                
                total_curbal += curbal
                total_heldmkt += heldmkt
                total_outstanding += outstanding
                
                f.write(f"\x05{remmgrp}\x05{row['islamic_contract']}\x05")
                f.write(f"{curbal:,.2f}\x05")
                f.write(f"{heldmkt:,.2f}\x05")
                f.write(f"{outstanding:,.2f}\x05")
                f.write(f"{profit_rate:.2f}\x05\n")
            
            # Table 1 totals
            f.write(f"\x05TOTAL\x05ALL CONTRACTS\x05")
            f.write(f"{total_curbal:,.2f}\x05")
            f.write(f"{total_heldmkt:,.2f}\x05")
            f.write(f"{total_outstanding:,.2f}\x05\x05\n")
            
            # Table 2 - Monthly Trading Volume
            f.write("\x05\n")
            f.write("Table 2 - Monthly Trading Volume (Islamic NID)\n\n")
            
            table2_stats = summaries["table2"]
            nidcnt = table2_stats["nidcnt"]
            nidvol = table2_stats["nidvol"]
            
            f.write("\x05\n")
            f.write("GROSS MONTHLY PURCHASE OF ISLAMIC NID BY THE BANK\x05")
            f.write("A) NUMBER OF NID\x05")
            
            if nidcnt == 0:
                f.write("0\x05\n")
                f.write("\x05\x05B) VOLUME OF NID\x050.00\x05\n")
            else:
                f.write(f"{nidcnt}\x05\n")
                f.write(f"\x05\x05B) VOLUME OF NID\x05{nidvol:,.2f}\x05\n")
            
            # Breakdown by contract type
            f.write("\x05\n")
            f.write("Breakdown by Contract Type:\n")
            f.write("\x05\n")
            f.write("CONTRACT TYPE\x05COUNT\x05VOLUME (RM)\x05\n")
            
            if hasattr(table2_stats['by_contract'], 'to_pandas'):
                by_contract_df = table2_stats['by_contract'].to_pandas()
                for _, row in by_contract_df.iterrows():
                    f.write(f"\x05{row['contract_type']}\x05")
                    f.write(f"{row['count']}\x05")
                    f.write(f"{row['volume']:,.2f}\x05\n")
            
            # Table 3 - Indicative Profit Rate
            f.write("\x05\n")
            f.write("Table 3 - Indicative Profit Rate & Risk Weight\n\n")
            f.write("\x05\n")
            f.write("REMAINING MATURITY\x05MID RATE (%)\x05")
            f.write("AVG PROFIT RATE (%)\x05RISK WEIGHT\x05\n")
            
            for _, row in table3_df.iterrows():
                remmfmt = row['remmfmt']
                remmgrp = remmfmt.split('.')[1].strip() if '.' in remmfmt else ''
                mid_rate = float(row['mid_rate'])
                avg_rate = float(row['avg_profit_rate'])
                risk_weight = float(row['avg_risk_weight'])
                
                f.write(f"\x05{remmgrp}\x05")
                f.write(f"{mid_rate:.4f}\x05")
                f.write(f"{avg_rate:.4f}\x05")
                f.write(f"{risk_weight:.2f}\x05\n")
            
            # Overall risk-adjusted rate
            overall_rate = summaries['overall_risk_adj_rate']
            f.write("\x05OVERALL RISK-ADJUSTED PROFIT RATE\x05")
            f.write(f"{overall_rate:.4f}\x05\x05\x05\n")
            
            # Sharia Compliance Statement
            f.write("\n\nSHARIA COMPLIANCE STATEMENT:\n")
            f.write("All instruments reported herein have been reviewed and approved\n")
            f.write("by the Sharia Supervisory Board of Public Islamic Bank Berhad.\n")
            f.write(f"Report generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")


# DuckDB implementation for Islamic NID
class IslamicNIDProcessorDuckDB(IslamicNIDProcessor):
    def load_islamic_data_duckdb(self) -> pl.DataFrame:
        """Load Islamic data using DuckDB with Sharia compliance checks"""
        conn = duckdb.connect()
        
        # Register parameters
        conn.execute(f"""
            CREATE TEMP TABLE date_params AS 
            SELECT 
                '{self.rpdate_str}'::DATE as reptdate,
                '{self.startdte}'::DATE as startdte
        """)
        
        # Load Sharia approved counterparties
        if self.sharia_path.exists():
            conn.execute(f"""
                CREATE TEMP TABLE sharia_approved AS
                SELECT DISTINCT counterparty_id 
                FROM read_csv_auto('{self.sharia_path}')
            """)
        else:
            conn.execute("CREATE TEMP TABLE sharia_approved (counterparty_id VARCHAR)")
        
        # Main query with Islamic-specific logic
        query = f"""
        WITH inid AS (
            SELECT * FROM read_csv_auto('{self.inid_path}')
        ),
        trnch AS (
            SELECT * FROM read_csv_auto('{self.trnch_path}')
        ),
        merged AS (
            SELECT 
                i.*, 
                t.* EXCLUDE(trancheno),
                CASE 
                    WHEN i.contract_type IN ('MURABAHAH', 'WAKALAH', 'SALAM', 'ISTISNA') 
                    THEN TRUE ELSE FALSE 
                END as contract_approved,
                CASE 
                    WHEN i.counterparty_id IN (SELECT counterparty_id FROM sharia_approved)
                    THEN TRUE ELSE FALSE 
                END as counterparty_approved
            FROM inid i
            INNER JOIN trnch t ON i.trancheno = t.trancheno
            WHERE i.curbal > 0
        ),
        with_dates AS (
            SELECT 
                m.*,
                d.reptdate,
                d.startdte,
                CAST(m.matdt AS DATE) as matdt_date,
                CAST(m.issuance_date AS DATE) as issuance_date,
                CAST(m.startdt AS DATE) as startdt_date,
                CAST(m.early_wddt AS DATE) as early_wddt_date,
                -- Calculate effective profit rate
                (m.profit_rate + 
                 (m.upfront_fee / NULLIF(m.asset_cost_price, 0)) * 
                 365 / NULLIF(DATEDIFF('day', m.issuance_date, m.matdt), 0) * 100
                ) as effective_profit_rate,
                -- Sharia compliance flag
                (contract_approved AND counterparty_approved AND 
                 m.guaranteed_profit = 'N') as sharia_compliant
            FROM merged m
            CROSS JOIN date_params d
        )
        SELECT * FROM with_dates WHERE sharia_compliant = TRUE
        """
        
        result = conn.execute(query).fetch_arrow_table()
        return pl.from_arrow(result)


# Usage example for Islamic NID
def main_islamic():
    # Initialize Islamic NID processor
    processor = IslamicNIDProcessor(
        inid_data_path="path/to/Islamic/INIDM01.csv",
        trnch_data_path="path/to/Islamic/TRNCHM01.csv",
        sharia_approval_path="path/to/Sharia/approved_counterparties.csv"
    )
    
    # Generate Islamic NID report
    output_path = Path("ISLAMIC_NID_REPORT.txt")
    processor.generate_islamic_output_report(output_path)
    
    print(f"Islamic NID Report generated: {output_path}")
    
    # Use DuckDB version for better performance
    processor_db = IslamicNIDProcessorDuckDB(
        inid_data_path="path/to/Islamic/INIDM01.csv",
        trnch_data_path="path/to/Islamic/TRNCHM01.csv",
        sharia_approval_path="path/to/Sharia/approved_counterparties.csv"
    )
    
    # Process with DuckDB
    df = processor_db.load_islamic_data_duckdb()
    print(f"Processed {len(df)} Sharia-compliant instruments")


if __name__ == "__main__":
    main_islamic()
