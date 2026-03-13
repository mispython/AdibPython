import duckdb
import polars as pl
from datetime import datetime, timedelta

BASE_PATH = "/data"
INPUT_PATH = f"{BASE_PATH}/input"
OUTPUT_PATH = f"{BASE_PATH}/output"

ECAIRATE_FILE = f"{INPUT_PATH}/ecairate.parquet"

COUNTERKAP_FILE = f"{OUTPUT_PATH}/counterkap.parquet"
BEST_FILE = f"{OUTPUT_PATH}/best.parquet"
MS_FILE = f"{OUTPUT_PATH}/ms.parquet"

con = duckdb.connect()

reptdate = datetime.today() - timedelta(days=1)
day = reptdate.day

if 1 <= day <= 8:
    wk, wk1, wk2 = "1","4","1"
elif 9 <= day <= 15:
    wk, wk1, wk2 = "2","1","2"
elif 16 <= day <= 22:
    wk, wk1, wk2 = "3","2","3"
else:
    wk, wk1, wk2 = "4","3","4"

rept_mon = f"{reptdate.month:02d}"
rept_yr = reptdate.strftime("%y")

ratingb = {
'AAA':1,'AA+':1,'AA':1,'AA-':1,'Aaa':1,'Aa1':1,'Aa2':1,'Aa3':1,'AA1':1,'AA2':1,'AA3':1,
'A+':2,'A':2,'A-':2,'A1':2,'A2':2,'A3':2,
'BBB+':3,'BBB':3,'BBB-':3,'Baa1':3,'Baa2':3,'Baa3':3,'BBB1':3,'BBB2':3,'BBB3':3,
'BB+':4,'BB':4,'BB-':4,'Ba1':4,'Ba2':4,'Ba3':4,'BB1':4,'BB2':4,'BB3':4,
'B+':5,'B':5,'B-':5,'B1':5,'B2':5,'B3':5,
'CCC+':6,'CCC':6,'CCC-':6,'CC':6,'C':6,'D':6,
'Caa1':6,'Caa2':6,'Caa3':6,'Ca':6,'DDD':6,'DD':6,'C1':6,'C2':6,'C3':6
}

ratingc = {
'A-1+':1,'A-1':1,'P-1':1,'F1+':1,'F1':1,'P1':1,'MARC-1':1,
'A-2':2,'P-2':2,'F2':2,'P2':2,'MARC-2':2,
'A-3':3,'P-3':3,'F3':3,'P3':3,'MARC-3':3,
'B':4,'B-1':4,'B-2':4,'B-3':4,'C':4,'D':4,'NP':4,'MARC-4':4
}

df = pl.from_arrow(con.execute(f"SELECT * FROM '{ECAIRATE_FILE}'").arrow())

ms = df.filter(pl.col("TABLETYP") == "a")

counter = df.filter(pl.col("TABLETYP") == "b")

counterkap = (
    counter.select([
        "CUSTID","AGENCYCD","AGNAME",
        "CUSTNAME","CNAME","LSTMODDT",
        "CSTLC","CLTLC"
    ])
    .with_columns([
        pl.col("CSTLC").map_elements(lambda x: ratingc.get(x,99)).alias("DRCSTLC"),
        pl.col("CLTLC").map_elements(lambda x: ratingb.get(x,99)).alias("DRCLTLC")
    ])
)

drcstlc = (
    counterkap
    .filter(pl.col("DRCSTLC") != 99)
    .sort(["CUSTID","DRCSTLC"], descending=[False,True])
    .group_by("CUSTID")
    .first()
)

drcltlc = (
    counterkap
    .filter(pl.col("DRCLTLC") != 99)
    .sort(["CUSTID","DRCLTLC"], descending=[False,True])
    .group_by("CUSTID")
    .first()
)

ram = (
    counter
    .filter(
        (pl.col("AGENCYCD") == "RAM") &
        ((pl.col("CSTLC") != "") | (pl.col("CLTLC") != ""))
    )
    .select(["CUSTID","AGENCYCD","CSTLC","CLTLC"])
)

counterkap2 = (
    drcstlc.join(drcltlc, on="CUSTID", how="outer")
    .filter(pl.col("CUSTID") != "")
    .unique(subset=["CUSTID"])
)

best = counterkap2.join(ram, on="CUSTID", how="left")

counterkap2.write_parquet(COUNTERKAP_FILE)
best.write_parquet(BEST_FILE)

ms = (
    ms.with_columns([
        pl.when(pl.col("TYPERATE").is_in([1,2]))
        .then(pl.col("RATING").map_elements(lambda x: ratingc.get(x,99)))
        .when(pl.col("TYPERATE").is_in([3,4]))
        .then(pl.col("RATING").map_elements(lambda x: ratingb.get(x,99)))
        .otherwise(99)
        .alias("RRATING")
    ])
)

ms_valid = (
    ms
    .filter(pl.col("RRATING") != 99)
    .sort(["SECID","RRATING"])
)

rrating1 = (
    ms_valid
    .group_by("SECID")
    .first()
)

rrating2 = (
    ms_valid
    .group_by("SECID")
    .last()
)

ms_final = (
    rrating1.join(rrating2, on="SECID", how="left", suffix="_2")
    .with_columns([
        pl.when(pl.col("TYPERATE") == 1).then(pl.col("RATING")).alias("CSTLC"),
        pl.when(pl.col("TYPERATE") == 2).then(pl.col("RATING")).alias("CSTFC"),
        pl.when(pl.col("TYPERATE") == 3).then(pl.col("RATING")).alias("CLTLC"),
        pl.when(pl.col("TYPERATE") == 4).then(pl.col("RATING")).alias("CLTFC")
    ])
)

ms_final.write_parquet(MS_FILE)
