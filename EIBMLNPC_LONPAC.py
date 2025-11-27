import polars as pl
import duckdb
from pathlib import Path
from datetime import datetime, timedelta
import calendar

class LonpacProcessor:
    def __init__(self, input_dir: str, output_dir: str):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Calculate reporting dates
        self._calculate_report_dates()
        
    def _calculate_report_dates(self):
        """Calculate report dates based on current date and week logic"""
        today = datetime.now()
        day = today.day
        
        # Determine week and report date
        if 8 <= day <= 14:
            self.reptdate = datetime(today.year, today.month, 8)
            self.wk = '1'
            # Previous month
            if today.month == 1:
                self.mm1 = 12
                self.yy1 = today.year - 1
            else:
                self.mm1 = today.month - 1
                self.yy1 = today.year
        elif 15 <= day <= 21:
            self.reptdate = datetime(today.year, today.month, 15)
            self.wk = '2'
            self.mm1 = today.month
            self.yy1 = today.year
        elif 22 <= day <= 27:
            self.reptdate = datetime(today.year, today.month, 22)
            self.wk = '3'
            self.mm1 = today.month
            self.yy1 = today.year
        else:
            # First day of current month minus 1 day
            self.reptdate = datetime(today.year, today.month, 1) - timedelta(days=1)
            self.wk = '4'
            self.mm1 = self.reptdate.month
            self.yy1 = self.reptdate.year
            
        self.prevdate = datetime(self.yy1, self.mm1, 1)
        self.reptyear4 = self.reptdate.year
        self.reptmon = str(self.reptdate.month).zfill(2)
        
    def parse_date(self, date_str: str, format_type: str = 'ddmmyyyy') -> datetime:
        """Parse date string in DD/MM/YYYY format"""
        if not date_str or date_str.strip() == '':
            return None
        try:
            if format_type == 'ddmmyyyy':
                parts = date_str.split('/')
                if len(parts) == 3:
                    return datetime(int(parts[2]), int(parts[1]), int(parts[0]))
        except:
            return None
        return None
    
    def process_pa_data(self):
        """Process Personal Accident (PA) product data"""
        # Read PA parquet file
        df = pl.read_parquet(self.input_dir / "pa.parquet")
        
        # Parse dates
        df = df.with_columns([
            pl.col("ISSUEDTX").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("ISSUEDT"),
            pl.col("EXPDT").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("EXPIRYDT"),
            pl.col("SUBMITDX").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("SUBMITDT"),
            pl.col("PROPOSALDT").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("PROPOSAL_DT"),
            pl.col("DOBX").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("DOB"),
        ])
        
        # Filter and transform
        df = df.filter(
            (pl.col("AGENTNO").is_not_null()) & 
            (pl.col("AGENTNO") != "") &
            (pl.col("POLICYNO").is_not_null()) &
            (pl.col("POLICYNO") != "")
        )
        
        # Add product identifier
        df = df.with_columns([
            pl.lit("LONPAC_PA").alias("PRODUCT"),
            pl.col("ACCTNOX").str.slice(13, 5).alias("NOTENO"),
            pl.col("ACCTNOX").str.slice(0, 12).str.replace_all("-", "").alias("ACCTNO")
        ])
        
        # Split into PROD and CUST datasets
        prod_cols = ["AGENTNO", "POLICYNO", "BRANCH", "ACCTNO", "NOTENO", "INSURED", 
                     "ISSUEDT", "EXPIRYDT", "PREMIUM", "PRODUCT", "PROD_CODE", "PROD_DESC",
                     "PROCESS_MTH", "SUBMITDT", "PROPOSAL_DT", "POLICYNO_OLD", "AUTO_RENEWAL_IND"]
        
        cust_cols = ["NAME", "POLICYNO", "NEWIC", "OLDIC", "REGNO", "GENDER", "RACE", 
                     "MARITAL", "DOB", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "REGNO_NEW"]
        
        df_prod = df.select(prod_cols)
        df_cust = df.select(cust_cols).filter(
            (pl.col("NAME").is_not_null()) & (pl.col("NAME") != "")
        )
        
        # Calculate AGE and IC for customers
        df_cust = df_cust.with_columns([
            (pl.lit(self.reptyear4) - pl.col("DOB").dt.year()).alias("AGE"),
            pl.col("NEWIC").str.replace_all("-", "").alias("IC")
        ])
        
        # Save to parquet
        df_prod.write_parquet(self.output_dir / "lonpac_paprod.parquet")
        df_cust.write_parquet(self.output_dir / "lonpac_pacust.parquet")
        
        return df_prod, df_cust
    
    def process_motor_data(self):
        """Process Motor product data"""
        df = pl.read_parquet(self.input_dir / "motor.parquet")
        
        # Parse dates
        df = df.with_columns([
            pl.col("ISSUEDTX").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("ISSUEDT"),
            pl.col("EXPDT").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("EXPIRYDT"),
            pl.col("SUBMITDX").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("SUBMITDT"),
            pl.col("PROPOSALDT").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("PROPOSAL_DT"),
            pl.col("DOBX").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("DOB"),
        ])
        
        # Filter and transform
        df = df.filter(
            (pl.col("AGENTNO").is_not_null()) & (pl.col("AGENTNO") != "") &
            (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "")
        )
        
        df = df.with_columns([
            pl.lit("LONPAC_MOTOR").alias("PRODUCT"),
            pl.col("CREGNO").str.replace_all(" ", "").alias("CREGNO")
        ])
        
        # Split datasets
        prod_cols = ["AGENTNO", "POLICYNO", "CREGNO", "BRANCH", "INSURED",
                     "ISSUEDT", "EXPIRYDT", "PREMIUM", "PRODUCT", "PROD_CODE", "PROD_DESC",
                     "PROCESS_MTH", "SUBMITDT", "PROPOSAL_DT", "POLICYNO_OLD"]
        
        cust_cols = ["POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "GENDER", "RACE",
                     "MARITAL", "DOB", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "REGNO_NEW"]
        
        df_prod = df.select(prod_cols)
        df_cust = df.select(cust_cols).filter(
            (pl.col("NAME").is_not_null()) & (pl.col("NAME") != "")
        )
        
        df_cust = df_cust.with_columns([
            (pl.lit(self.reptyear4) - pl.col("DOB").dt.year()).alias("AGE"),
            pl.col("NEWIC").str.replace_all("-", "").alias("IC")
        ])
        
        df_prod.write_parquet(self.output_dir / "lonpac_motorprod.parquet")
        df_cust.write_parquet(self.output_dir / "lonpac_motorcust.parquet")
        
        return df_prod, df_cust
    
    def process_misc_data(self):
        """Process Miscellaneous product data"""
        df = pl.read_parquet(self.input_dir / "misc.parquet")
        
        # Parse dates
        df = df.with_columns([
            pl.col("ISSUEDTX").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("ISSUEDT"),
            pl.col("EXPDT").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("EXPIRYDT"),
            pl.col("SUBMITDX").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("SUBMITDT"),
            pl.col("PROPOSALDT").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("PROPOSAL_DT"),
            pl.col("DOBX").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("DOB"),
        ])
        
        df = df.filter(
            (pl.col("AGENTNO").is_not_null()) & (pl.col("AGENTNO") != "") &
            (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "")
        )
        
        df = df.with_columns(pl.lit("LONPAC_MOTOR").alias("PRODUCT"))
        
        prod_cols = ["AGENTNO", "POLICYNO", "BRANCH", "INSURED",
                     "ISSUEDT", "EXPIRYDT", "PREMIUM", "PRODUCT", "PROD_CODE", "PROD_DESC",
                     "PROCESS_MTH", "SUBMITDT", "PROPOSAL_DT", "POLICYNO_OLD"]
        
        cust_cols = ["POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "GENDER", "RACE",
                     "DOB", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "REGNO_NEW"]
        
        df_prod = df.select(prod_cols)
        df_cust = df.select(cust_cols).filter(
            (pl.col("NAME").is_not_null()) & (pl.col("NAME") != "")
        )
        
        df_cust = df_cust.with_columns([
            (pl.lit(self.reptyear4) - pl.col("DOB").dt.year()).alias("AGE"),
            pl.col("NEWIC").str.replace_all("-", "").alias("IC")
        ])
        
        return df_prod, df_cust
    
    def process_fire_data(self):
        """Process Fire product data"""
        df = pl.read_parquet(self.input_dir / "fire.parquet")
        
        # Parse dates
        df = df.with_columns([
            pl.col("ISSUEDTX").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("ISSUEDT"),
            pl.col("EXPDT").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("EXPIRYDT"),
            pl.col("SUBMITDX").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("SUBMITDT"),
            pl.col("PROPOSALDT").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("PROPOSAL_DT"),
            pl.col("DOBX").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("DOB"),
        ])
        
        df = df.filter(
            (pl.col("POLICYNO") != "F.ENDT.MAS......") &
            (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "") &
            (pl.col("AGENTNO").is_not_null()) & (pl.col("AGENTNO") != "")
        )
        
        df = df.with_columns([
            pl.lit("LONPAC_FIRE").alias("PRODUCT"),
            pl.col("ACCTNOX").str.slice(13, 5).alias("NOTENO"),
            pl.col("ACCTNOX").str.slice(0, 12).str.replace_all("-", "").alias("ACCTNO")
        ])
        
        prod_cols = ["AGENTNO", "POLICYNO", "BRANCH", "ACCTNO", "NOTENO", "INSURED",
                     "ISSUEDT", "EXPIRYDT", "PRODUCT", "PREMIUM", "PROD_CODE", "PROD_DESC",
                     "PROCESS_MTH", "SUBMITDT", "PROPOSAL_DT", "POLICYNO_OLD",
                     "CCOLLNO", "AUTO_DEBIT_IND", "AUTO_RENEWAL_IND", "PROP_INS_ADDRESS", "TOT_STOREY"]
        
        cust_cols = ["POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "GENDER", "RACE",
                     "MARITAL", "DOB", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "REGNO_NEW"]
        
        df_prod = df.select(prod_cols)
        df_cust = df.select(cust_cols).filter(
            (pl.col("NAME").is_not_null()) & (pl.col("NAME") != "")
        )
        
        df_cust = df_cust.with_columns([
            (pl.lit(self.reptyear4) - pl.col("DOB").dt.year()).alias("AGE"),
            pl.col("NEWIC").str.replace_all("-", "").alias("IC")
        ])
        
        df_prod.write_parquet(self.output_dir / "lonpac_fireprod.parquet")
        df_cust.write_parquet(self.output_dir / "lonpac_firecust.parquet")
        
        return df_prod, df_cust
    
    def process_hire_data(self, file_name: str, output_prefix: str):
        """Process Hire Purchase data (both HIRE and NHIRE)"""
        df = pl.read_parquet(self.input_dir / file_name)
        
        # Parse dates
        df = df.with_columns([
            pl.col("ISSUEDTX").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("ISSUEDT"),
            pl.col("EXPDT").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("EXPIRYDT"),
            pl.col("SUBMITDX").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("SUBMITDT"),
            pl.col("PROPOSALDT").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("PROPOSAL_DT"),
            pl.col("DOBX").map_elements(lambda x: self.parse_date(x), return_dtype=pl.Datetime).alias("DOB"),
        ])
        
        df = df.filter(
            (pl.col("AGENTNO").is_not_null()) & (pl.col("AGENTNO") != "") &
            (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "")
        )
        
        df = df.with_columns([
            pl.lit("LONPAC_HP").alias("PRODUCT"),
            pl.col("CREGNO").str.replace_all(" ", "").alias("CARREG"),
            pl.col("CREGNO").str.replace_all(" ", "").alias("CREGNO")
        ])
        
        prod_cols = ["AGENTNO", "POLICYNO", "CREGNO", "INSURED", "ISSUEDT", "EXPIRYDT",
                     "PREMIUM", "AGENTNO", "CARREG", "CREGNO", "PRODUCT", "PROD_CODE",
                     "PROD_DESC", "PROCESS_MTH", "SUBMITDT", "PROPOSAL_DT", "POLICYNO_OLD"]
        
        cust_cols = ["NAME", "POLICYNO", "NEWIC", "OLDIC", "REGNO", "GENDER", "RACE",
                     "MARITAL", "DOB", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "REGNO_NEW"]
        
        df_prod = df.select(prod_cols)
        df_cust = df.select(cust_cols).filter(
            (pl.col("POLICYNO") != "F.ENDT.MAS......") &
            (pl.col("NAME").is_not_null()) & (pl.col("NAME") != "")
        )
        
        df_cust = df_cust.with_columns([
            (pl.lit(self.reptyear4) - pl.col("DOB").dt.year()).alias("AGE"),
            pl.col("NEWIC").str.replace_all("-", "").alias("IC")
        ])
        
        df_prod.write_parquet(self.output_dir / f"lonpac_{output_prefix}prod.parquet")
        df_cust.write_parquet(self.output_dir / f"lonpac_{output_prefix}cust.parquet")
        
        return df_prod, df_cust
    
    def merge_all_data(self):
        """Merge all customer and product data"""
        # Read all customer datasets
        pacust = pl.read_parquet(self.output_dir / "lonpac_pacust.parquet")
        
        # Combine motor and misc customer data
        motorcust = pl.read_parquet(self.output_dir / "lonpac_motorcust.parquet")
        if (self.output_dir / "lonpac_misccust.parquet").exists():
            misccust = pl.read_parquet(self.output_dir / "lonpac_misccust.parquet")
            motorcust = pl.concat([motorcust, misccust])
        
        firecust = pl.read_parquet(self.output_dir / "lonpac_firecust.parquet")
        hirecust = pl.read_parquet(self.output_dir / "lonpac_hirecust.parquet")
        nonhirecust = pl.read_parquet(self.output_dir / "lonpac_nonhirecust.parquet")
        
        # Merge all customers
        all_cust = pl.concat([pacust, motorcust, firecust, hirecust, nonhirecust], how="diagonal")
        all_cust = all_cust.filter(
            (pl.col("POLICYNO").is_not_null()) & 
            (pl.col("POLICYNO") != "") &
            (pl.col("POLICYNO") != "Policy issued by")
        ).unique(subset=["POLICYNO"])
        
        all_cust.write_parquet(self.output_dir / "lonpac_cust.parquet")
        
        # Read all product datasets
        paprod = pl.read_parquet(self.output_dir / "lonpac_paprod.parquet")
        
        # Combine motor and misc product data
        motorprod = pl.read_parquet(self.output_dir / "lonpac_motorprod.parquet")
        if (self.output_dir / "lonpac_miscprod.parquet").exists():
            miscprod = pl.read_parquet(self.output_dir / "lonpac_miscprod.parquet")
            motorprod = pl.concat([motorprod, miscprod])
        
        fireprod = pl.read_parquet(self.output_dir / "lonpac_fireprod.parquet")
        
        # Merge all products
        all_prod = pl.concat([paprod, motorprod, fireprod], how="diagonal")
        all_prod = all_prod.filter(
            (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "")
        ).unique(subset=["POLICYNO"])
        
        all_prod.write_parquet(self.output_dir / "lonpac_prod.parquet")
        
        return all_cust, all_prod
    
    def run_full_process(self):
        """Execute complete data processing pipeline"""
        print("Starting LONPAC data processing...")
        
        print("Processing PA data...")
        self.process_pa_data()
        
        print("Processing Motor data...")
        self.process_motor_data()
        
        if (self.input_dir / "misc.parquet").exists():
            print("Processing Misc data...")
            misc_prod, misc_cust = self.process_misc_data()
            misc_prod.write_parquet(self.output_dir / "lonpac_miscprod.parquet")
            misc_cust.write_parquet(self.output_dir / "lonpac_misccust.parquet")
        
        print("Processing Fire data...")
        self.process_fire_data()
        
        print("Processing Hire Purchase data...")
        self.process_hire_data("hire.parquet", "hire")
        
        print("Processing Non-Hire Purchase data...")
        self.process_hire_data("nhire.parquet", "nonhire")
        
        print("Merging all datasets...")
        self.merge_all_data()
        
        print("Processing complete!")

# Usage example
if __name__ == "__main__":
    processor = LonpacProcessor(
        input_dir="./input_data",
        output_dir="./output_data"
    )
    processor.run_full_process()
