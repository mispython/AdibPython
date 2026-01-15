import duckdb
import polars as pl
from datetime import datetime, timedelta
import common.parquet_splitter as parquet_splitter
import common.data_cleaner as data_cleaner
import common.fixed_width_reader as fixed_width_reader
import ast
import os

# Obtain date for file paths
batch_date = datetime.now()-timedelta(days=1)
file_date = batch_date.strftime("%d%m%y")
folder_day = batch_date.strftime("%d")
folder_month = batch_date.strftime("%m")
folder_year = batch_date.strftime("%Y")

# Define global file paths
table_layout_folder = "/host/dp/input/table_layout/lookup"
control1_f = f"/host/dp/input/lookup/DPCOLTPM.txt"
control2_f = f"/host/dp/input/SRSCTRL1_{folder_year}{folder_month}{folder_day}.txt"
inputf1_f = f"/host/dp/parquet/year={folder_year}/month={folder_month}/day={folder_day}/DPFEED03_CSTPM001.parquet"
inputf2_f = f"/host/dp/parquet/year={folder_year}/month={folder_month}/day={folder_day}/DP_ALLFILE.parquet"
output1_f = f"/host/dp/output/DP_SASTPMF3_PYMT_SUMM_{folder_year}{folder_month}{folder_day}.txt"
excpo_f = f"/host/dp/output/DP_SASTPMF3_PYMT_EXCP_{folder_year}{folder_month}{folder_day}.txt"

# Step 1: Read INPUTF1.parquet using DuckDB and convert to Polars
# Construct table layout file path
program_name = os.path.basename(__file__).rsplit('.', maxsplit=1)[0]
inputf1_dsn = inputf1_f.rsplit('/', maxsplit=1)[1].rsplit('.', maxsplit=1)[0].split('_', maxsplit=1)[1]
inputf1_layout_path = f"{table_layout_folder}/{program_name}_{inputf1_dsn}.txt"

select_clause = parquet_splitter.split(inputf1_layout_path)

con = duckdb.connect()
aaa = pl.from_arrow(con.execute(f"""
    SELECT {select_clause} FROM '{inputf1_f}'
    WHERE TTRANCODE != '874' AND SRCECDE = '313'
""").fetch_arrow_table())

# Step 2: Load CONTROL1 fixed-width
# control1_schema = [("ACCTNO", 10, str), ("SHORTNAME", 3, str), ("LONGNAME", 20, str)]
# control1 = pl.read_csv(control1_f, separator=None, has_header=False,
#                        new_columns=[x[0] for x in control1_schema],
#                        encoding="utf8", infer_schema_length=0, skip_rows=0,
#                        width=[x[1] for x in control1_schema])
control1_schema = [
    (0, 10, "ACCTNO"),
    (11, 3, "SHORTNAME"),
    (15, 20, "LONGNAME")
]
df = fixed_width_reader.read(control1_f, control1_schema)

# Step 3: Load CONTROL2 fixed-width
# control2 = pl.read_csv(control2_f, separator=None, has_header=False,
#                        new_columns=["ACTDATE"], width=[8], encoding="utf8")
control2_schema = [
    (0, 8, "ACTDATE")
]
control2 = fixed_width_reader.read(control2_f, control2_schema)

rptday = str(control2[0, "ACTDATE"])

rpt_dd, rpt_mm, rpt_yy = rptday[6:8], rptday[4:6], rptday[2:4]

# Step 4: Load INPUTF2 fixed-width
# inputf2_schema = [
#     ("FTRANSID", 16, str), ("FSERIALN", 3, str), ("FMSTOKEN", 2, str), ("FTOKEN", 2, str),
#     ("FEXCHGID", 10, str), ("FBATCH", 40, str), ("FNOOFORD", 2, str), ("FPROCDT", 8, str),
#     ("FTXNDATE", 14, str), ("FFPXDATE", 14, str), ("FENCRYVA", 48, str), ("FSELORDN", 40, str),
#     ("FSELERID", 10, str), ("FSELERDE", 30, str), ("FSELBKID", 15, str), ("FSELBRCH", 10, str),
#     ("FSELACNO", 20, str), ("FSELLERI", 35, str), ("FBUYBKID", 15, str), ("FBUYBRCH", 10, str),
#     ("FBUYACNO", 20, str), ("FBUYNAME", 40, str), ("FBUYERID", 20, str), ("FMAKERNM", 40, str),
#     ("FBUYERIB", 35, str), ("FCHRGTYP", 4, str), ("FTRXCNCY", 3, str), ("FTRXAMNT", 8, str),
#     ("FSETCNCY", 3, str), ("FSETAMNT", 8, str), ("FDRAUCDE", 2, str), ("FDRAUTNO", 10, str),
#     ("FCRAUCDE", 2, str), ("FCRAUTNO", 10, str), ("FTIMESTA", 26, str),
# ]
# inputf2 = pl.read_csv(inputf2_f, separator=None, has_header=False,
#                       new_columns=[x[0] for x in inputf2_schema],
#                       encoding="utf8", infer_schema_length=0, skip_rows=0,
#                       width=[x[1] for x in inputf2_schema])
# bbb = inputf2.filter(pl.col("FSELERID") == "SE00027722")

bbb_query = f"""
    SELECT *
    FROM read_parquet('{inputf2_f}')
    WHERE FSELERID = 'SE00027722'
"""

bbb = duckdb.sql(bbb_query).pl()
bbb = bbb.with_columns(
    pl.col("FTRANSID").cast(pl.String)
)

# Step 5: Process AAA
aaa = aaa.with_columns([
    pl.when((pl.col("FRMTCDE").cast(pl.Int32) == 2) & (pl.col("TRAITY2") == "L"))
      .then(pl.col("TRAILDT1").str.slice(17, 18).str.strip_chars("* "))
      .otherwise(None).alias("REFNO1"),
    pl.when(pl.col("TRANCDE") == "795")
      .then(pl.col("TRAILDT1").str.slice(11, 3))
      .otherwise(pl.col("TRANCDE")).alias("TRANCDE"),
    pl.when(pl.col("TRANCDE") == "796")
      .then(pl.col("TRANAMT"))
      .otherwise(None).alias("TRXNFEE")
])
aaa = aaa.with_columns([
    pl.col("REFNO1").fill_null("").alias("FTRANSID")
])

# Step 6: Split CR and DR
createf1 = aaa.filter(pl.col("DATATYP") == "C")
createf2 = aaa.filter(pl.col("DATATYP") == "D")

# Step 7: Extract TRXNFEE from first EBKCHQ in DR
trxnfee = createf2.unique(subset=["EBKCHQ"]).select(["EBKCHQ", "RECNO", "TRANAMT"]).rename({"TRANAMT": "TRXNFEE"})

# Step 8: Merge CR with TRXNFEE
outf3 = createf1.join(trxnfee, on="EBKCHQ", how="left")

# Step 9: Merge with BBB
# outff = outf3.join(bbb, on="FTRANSID", how="inner")
# excpo = outf3.join(bbb, on="FTRANSID", how="anti")
merged = outf3.join(bbb, on="FTRANSID", how="full", coalesce=True).with_columns(
    pl.col("TRANAMT").fill_null(0)
)
outff = merged.filter(pl.col("BANKNO").is_not_null()).fill_nan("").fill_null("")
excpo = merged.filter(pl.col("FSELORDN").is_null() & pl.col("BANKNO").is_not_null())

# Step 10: Write OUTPUT1.txt
with open(output1_f, "w") as f:
    for row in outff.iter_rows(named=True):
        if row.get("COMMIND", "").strip() and int(row["TRANAMT"]) != 150:
            # nettamt = row["TRANAMT"] - row["TRXNFEE"]
            trxnfee = ""
            nettamt = ""
            if row["TRXNFEE"] != "":
                trxnfee = int(row["TRXNFEE"])
                nettamt = int(row["TRANAMT"]) - trxnfee

            f.write(f"{rpt_dd:>2}{rpt_mm:>2}{rpt_yy:>2}{row['FTRANSID']:>16}{row['FSELORDN']:>40}"
                    f"{int(row['TRANAMT']):>13}{trxnfee:>13}{nettamt:>13}"
                    f"{row['RECNO']:>6}{row['TRANCDE']:>3}{row['FSELERID']:>10}"
                    f"{row['ACCTNO']:>10}{row['FBUYBKID']:>15}{row['FSELERDE']:>30}"
                    f"{row['FBUYACNO']:>20}\n")

# Step 11: Write EXCPO.txt
with open(excpo_f, "w") as f:
    if excpo.is_empty():
        f.write("------ NO EXCEPTIONS FOR TODAY -------\n")
    else:
        totamt = 0
        totcnt = 0
        f.write(" " * 19 + "==========================================\n")
        f.write(" " * 19 + "   !!! ITEMS WITH MISSING INFO !!!\n")
        f.write(" " * 19 + " RECORD AVAILABLE IN DEPOSIT RAW FILE\n")
        f.write(" " * 19 + "   BUT NOT FOUND IN MA FPX EXT FILE\n")
        f.write(" " * 19 + "  PLEASE NOTIFY SKK, RCD, NPH, LFK\n")
        f.write(" " * 19 + "     AND DEPOSIT TEAM VIA EMAIL!!\n")
        f.write(" " * 19 + "==========================================\n")
        f.write(" " * 54 + "PAGE NO. :     1\n")
        f.write(" " * 19 + "       DAILY EXCEPTION REPORT        \n")
        f.write(" " * 19 + "   FOR TICKETPRO MALAYSIA SDN BHD      \n")
        f.write(" " * 19 + " ---------------------------------\n")
        f.write("COUNT    TRAN DATE   FPX TRANS ID      TRAN AMOUNT     TRN  SRC\n")
        f.write("-" * 120 + "\n")

        for row in excpo.iter_rows(named=True):
            tranamt = float(row["TRANAMT"]) / 100
            totamt += tranamt
            totcnt += 1
            f.write(f"{totcnt:>7}  {rpt_dd:>2}{rpt_mm:>2}{rpt_yy:>2}      {row['FTRANSID']:>16}  "
                    f"{tranamt:>13.2f}   {row['TRANCDE']:>3}  {row['SRCECDE']:>3}\n")

        f.write(" " * 4 + f"TOTAL EXCEPTION AMOUNT  : {totamt:13.2f}\n")
        f.write(" " * 4 + f"TOTAL EXCEPTION COUNT   : {totcnt:7}\n")
        f.write(f"{' ':15}*** THIS REPORT IS GENERATED ON {datetime.today().strftime('%d/%m/%Y')} ***")
