import polars as pl
from datetime import datetime, timedelta
from pathlib import Path

BASE_INPUT_PATH = Path("/host/cis/parquet/") # Folder for source files
BASE_OUTPUT_PATH = Path("/pythonITD/mis_dev/sas_migration/CIGR/output") # Folder for output files
BASE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# File paths
CIS_FILE = BASE_INPUT_PATH / "year=2025/month=12/day=08/CUST_GROUPING_OUT.parquet"
RCIS_FILE = BASE_INPUT_PATH / "year=2025/month=12/day=08/CUST_GROUPING_RPTCC.parquet"

# Helper Functions
def get_report_date_info():
    today = datetime.today()
    first_day_this_month = today.replace(day=1)
    last_day_prev_month = first_day_this_month - timedelta(days=1)

    reptmon = f"{last_day_prev_month:02d}"
    reptyear = str(last_day_prev_month.year)[-2:]

    return last_day_prev_month, reptmon, reptyear

def read_fixed_width_file(file_path, columns, widths, dtypes):
    with open(file_path, "r", encoding="utf-8") as f:
        lines = [line.rstrip("\n") for line in f]

    data = []
    for line in lines:
        row = []
        start = 0
        for w in widths:
            row.append(line[start:start+w].strip())
            start += w
        data.append(row)

    df = pl.DataFrame(data, schema=columns)

    # Apply dtypes conversion if needed
    for col, dtype in dtypes.items():
        df = df.with_columns(pl.col(col).cast(dtype))
    return df

def save_outputs(df, name):
    df.write_parquet(BASE_OUTPUT_PATH / f"{name}.parquet")
    df.write_csv(BASE_OUTPUT_PATH / f"{name}.csv")

# MAIN SCRIPT
if __name__ == "__main__":
    reptdate, reptmon, reptyear, = get_report_date_info()

    #CISBASEL
    cisbasel_columns = [
        "GROUPNO", "CUSTNO", "FULLNAME", "ACCTNO", "NOTENO", 
        "PRODUCT", "AMTINDC", "BALAMT", "TOTAMT", "RLENCODE", "PRIMSEC"
    ]
    cisbasel_widths = [11, 11, 11, 40, 10, 5, 3, 3, 24, 24, 3, 1]
    cisbasel_dtypes = {
        "ACCTNO" : pl.Int64,
        "NOTENO" : pl.Int64,
        "BALAMT" : pl.Float64,
        "TOTAMT" : pl.Float64,
    }

    cisbasel_df = read_fixed_width_file(CIS_FILE, cisbasel_columns, cisbasel_widths, cisbasel_dtypes)
    save_outputs(cisbasel_df, f"LOAN_CISBASEL{reptmon}{reptyear}")

    #CISRPTCC
    cisrptcc_columns = [
        "GRPING", "C1CUST", "C1TYPE", "C1CODE", "C1DESC",
        "C2CUST", "C2TYPE", "C2CODE", "C2DESC"
    ]
    cisrptcc_wdiths = [11, 11, 1, 3, 15, 11, 1, 3, 15]
    cisrptcc_dtypes = {
        "GRPING": pl.Int64
    }

    cisrptcc_df = read_fixed_width_file(RCIS_FILE, cisrptcc_columns, cisrptcc_wdiths, cisrptcc_dtypes)
    save_outputs(cisrptcc_df, f"LOAN_CISRPTCC{reptmon}{reptyear}")

    print(cisbasel_df.head(100))




Traceback (most recent call last):
  File "/pythonITD/mis_dev/sas_migration/CIGR/output/EIBWCIGR.py", line 65, in <module>
    cisbasel_df = read_fixed_width_file(CIS_FILE, cisbasel_columns, cisbasel_widths, cisbasel_dtypes)
  File "/pythonITD/mis_dev/sas_migration/CIGR/output/EIBWCIGR.py", line 26, in read_fixed_width_file
    lines = [line.rstrip("\n") for line in f]
  File "/pythonITD/mis_dev/sas_migration/CIGR/output/EIBWCIGR.py", line 26, in <listcomp>
    lines = [line.rstrip("\n") for line in f]
  File "/usr/lib64/python3.9/codecs.py", line 322, in decode
    (result, consumed) = self._buffer_decode(data, self.errors, final)
UnicodeDecodeError: 'utf-8' codec can't decode byte 0xbe in position 7: invalid start byte







DATA REPTDATE;
  REPTDATE = MDY(MONTH(TODAY()),1,YEAR(TODAY()))-1;
  MM = MONTH(REPTDATE);
  CALL SYMPUT('REPTMON',PUT(MM,Z2.));
  CALL SYMPUT('REPTYEAR',PUT(REPTDATE,YEAR2.));
RUN;


OPTIONS NOCENTER YEARCUTOFF=1950 LS=132;

DATA LOAN.CISBASEL&REPTMON&REPTYEAR;
   INFILE CIS;
   INPUT    @01   GRPING        $11.
            @13   GROUPNO       $11.
            @24   CUSTNO        $11.
            @35   FULLNAME      $40.
            @75   ACCTNO         10.
            @85   NOTENO          5.
            @90   PRODUCT        $3.
            @93   AMTINDC        $3.
            @96   BALAMT         24.2
            @123  TOTAMT         24.2
            @150  RLENCODE       $3.
            @155  PRIMSEC        $1.;
RUN;

DATA LOAN.CISRPTCC&REPTMON&REPTYEAR;
   INFILE RCIS;
   INPUT    @01   GRPING         11.
            @15   C1CUST        $11.
            @30   C1TYPE         $1.
            @35   C1CODE         $3.
            @40   C1DESC        $15.
            @60   C2CUST        $11.
            @75   C2TYPE         $1.
            @80   C2CODE         $3.
            @85   C2DESC        $15.;
RUN;

