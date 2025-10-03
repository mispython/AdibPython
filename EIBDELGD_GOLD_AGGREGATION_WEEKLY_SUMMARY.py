# --------------------------------------------------------
# Job: EIBDELGD - GOLD WEEKLY SUMMARY REPORT
# Converted from SAS to Python (DuckDB + PyArrow)
# --------------------------------------------------------

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import datetime

# --------------------------------------------------------
# Step 1: REPTDATE logic
# --------------------------------------------------------
today = datetime.date.today()
sxdate = today - datetime.timedelta(days=1)  # SXDATE = TODAY()-1

MM = sxdate.month
DD = sxdate.day
MM1 = MM + 1 if MM < 12 else 1

# WK calculation (weekly bucket by day ranges)
if 1 <= DD <= 8:
    WK = "1"
elif 9 <= DD <= 15:
    WK = "2"
elif 16 <= DD <= 22:
    WK = "3"
else:
    WK = "4"

INDATES   = int(sxdate.strftime("%Y%m%d"))   # like Z5. SAS format (yyyymmdd)
REPTDAY   = str(DD).zfill(2)
REPTMON   = str(MM).zfill(2)
REPTMON1  = str(MM1).zfill(2)
REPTYEA2  = str(sxdate.year)[-2:]
REPTYEA4  = str(sxdate.year)

# --------------------------------------------------------
# Step 2: Load GOLD input parquet
# Example filename pattern: GOLD241001.parquet (YYMMDD)
# --------------------------------------------------------
input_file = f"GOLD{REPTYEA2}{REPTMON}{REPTDAY}.parquet"

con = duckdb.connect()
con.execute(f"""
    CREATE OR REPLACE TABLE gold AS 
    SELECT * FROM read_parquet('{input_file}')
""")

# --------------------------------------------------------
# Step 3: Data Transformation
# --------------------------------------------------------
# CUSTCODE >= 80, fix BNMCODE, assign REPTDATS
con.execute(f"""
    CREATE OR REPLACE TABLE gold_filtered AS
    SELECT *,
           4929995000000 AS BNMCODE,
           {INDATES} AS REPTDATS,
           CURBALRM AS AMOUNT
    FROM gold
    WHERE CUSTCODE >= 80
""")

# Compute ELDAY mapping in Python (like SAS IF cascade)
df = con.execute("SELECT * FROM gold_filtered").arrow()

def map_eldays(row_date: datetime.date) -> str:
    d = row_date.day
    m = row_date.month
    y = row_date.year

    if d in (1, 9, 16, 23): return "DAYA"
    if d in (2, 10, 17, 24): return "DAYB"
    if d in (3, 11, 18, 25): return "DAYC"
    if d in (4, 12, 19, 26): return "DAYD"
    if d in (5, 13, 20, 27): return "DAYE"
    if d in (6, 14, 21, 28): return "DAYF"
    if d in (7, 29): return "DAYG"
    if d == 30: return "DAYH"
    if d in (8, 15, 22, 31): return "DAYI"

    # Adjust for month end
    if m in (4, 6, 9, 11) and d == 30: return "DAYI"
    if m == 2:
        if d == 28: return "DAYI"
        if y % 4 == 0:   # leap year
            if d == 28: return "DAYF"
            if d == 29: return "DAYI"
    return "DAYI"

eldays = []
for row in df.to_pylist():
    d_str = str(row["REPTDATS"])
    row_date = datetime.datetime.strptime(d_str, "%Y%m%d").date()
    eldays.append(map_eldays(row_date))

df = df.append_column("ELDAY", pa.array(eldays))

# --------------------------------------------------------
# Step 4: Aggregation (PROC SUMMARY → GROUP BY)
# --------------------------------------------------------
con.register("gold_df", df)

agg_tbl = con.execute("""
    SELECT BNMCODE, ELDAY, SUM(AMOUNT) AS AMOUNT
    FROM gold_df
    GROUP BY BNMCODE, ELDAY
""").arrow()

# --------------------------------------------------------
# Step 5: Save Output
# --------------------------------------------------------
output_parquet = f"GOLD{REPTMON}{WK}.parquet"
output_csv     = f"GOLD{REPTMON}{WK}.csv"

pq.write_table(agg_tbl, output_parquet)
con.register("agg_tbl", agg_tbl)
con.execute(f"COPY agg_tbl TO '{output_csv}' (HEADER, DELIMITER ',')")

print(f"EIBDELGD job executed successfully. Outputs: {output_parquet}, {output_csv}")
