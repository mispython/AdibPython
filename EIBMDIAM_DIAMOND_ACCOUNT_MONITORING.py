import duckdb
import pyarrow as pa
import pyarrow.compute as pc

# Load REPTDATE
reptdate_tbl = duckdb.sql("SELECT * FROM deposit_reptdate").to_arrow()
reptdate = reptdate_tbl.column("reptdate")[0].as_py()

# Derive reporting values
import datetime
reptdate_dt = datetime.date.fromisoformat(str(reptdate))

dayone = datetime.date(reptdate_dt.year, reptdate_dt.month, 1)
nowk = (
    "1" if 1 <= reptdate_dt.day <= 8 else
    "2" if 9 <= reptdate_dt.day <= 15 else
    "3" if 16 <= reptdate_dt.day <= 22 else
    "4"
)

# ------------------------------
# Process Saving + Current
# ------------------------------
saca_tbl = duckdb.sql("""
    SELECT acctno, 
           CAST(SUBSTR(CAST(opendt AS VARCHAR),1,8) AS DATE) AS opendate
    FROM deposit_saving
    UNION
    SELECT acctno,
           CAST(SUBSTR(CAST(opendt AS VARCHAR),1,8) AS DATE) AS opendate
    FROM deposit_current
""").to_arrow()

saca_tbl = saca_tbl.filter(
    (saca_tbl["opendate"] >= pa.scalar(dayone)) &
    (saca_tbl["opendate"] <= pa.scalar(reptdate_dt))
).drop_duplicates(["acctno"])

# Add CDNO, Branch, Product, balances (stub from external FD.SACA flatfile)
fd_saca_raw = duckdb.sql("SELECT * FROM flatfile_saca").to_arrow()

fd_saca = fd_saca_raw.join(saca_tbl, keys="acctno", join_type="inner")

# Compute nodays
fd_saca = fd_saca.append_column(
    "nodays",
    pc.if_else(
        pc.is_valid(fd_saca["opendate"]),
        pc.add(
            pc.subtract(
                pa.scalar(reptdate_dt),
                fd_saca["opendate"]
            ).cast(pa.int64()),
            pa.scalar(1)
        ),
        pa.scalar(reptdate_dt.day)
    )
)

# ------------------------------
# Process FD + PLUSFD + FCYFD
# ------------------------------
def process_fd(source_table, flatfile_name):
    fd_tbl = duckdb.sql(f"""
        SELECT acctno,
               CAST(SUBSTR(CAST(opendt AS VARCHAR),1,8) AS DATE) AS opendate
        FROM {source_table}
    """).to_arrow()

    fd_tbl = fd_tbl.filter(
        (fd_tbl["opendate"] >= pa.scalar(dayone)) &
        (fd_tbl["opendate"] <= pa.scalar(reptdate_dt))
    ).drop_duplicates(["acctno"])

    flatfile = duckdb.sql(f"SELECT * FROM {flatfile_name}").to_arrow()
    merged = flatfile.join(fd_tbl, keys="acctno", join_type="inner")

    merged = merged.append_column(
        "nodays",
        pc.if_else(
            pc.is_valid(merged["opendate"]),
            pc.add(
                pc.subtract(
                    pa.scalar(reptdate_dt),
                    merged["opendate"]
                ).cast(pa.int64()),
                pa.scalar(1)
            ),
            pa.scalar(reptdate_dt.day)
        )
    )
    return merged

fd_main   = process_fd("deposit_fd", "flatfile_fd")
plus_fd   = process_fd("plusfd", "flatfile_plusfd")
fcy_fd    = process_fd("fcyfd", "flatfile_fcyfd")

# ------------------------------
# Final consolidated dictionary
# ------------------------------
final_result = {
    "FD_SACA": fd_saca,
    "FD_MAIN": fd_main,
    "PLUSFD": plus_fd,
    "FCYFD": fcy_fd,
    "NOWK": nowk,
    "RDATE": reptdate_dt.strftime("%d/%m/%Y")
}
