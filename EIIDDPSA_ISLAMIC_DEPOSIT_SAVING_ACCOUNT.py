import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from datetime import datetime

# Connect to DuckDB (in-memory or file-based if needed)
con = duckdb.connect()

# Load REPTDATE from DPTRBL.REPTDATE
reptdate_tbl = con.execute("SELECT * FROM DPTRBL.REPTDATE").arrow()

# Derive WEEK, MONTH, YEAR, DAY
def process_reptdate(tbl: pa.Table) -> pa.Table:
    reptdate = tbl.column("REPTDATE")
    day = pc.day(reptdate)
    month = pc.month(reptdate)
    year = pc.year(reptdate)

    wk = []
    mm = []
    for d, m in zip(day.to_pylist(), month.to_pylist()):
        if 1 <= d <= 8:
            w = "1"
        elif 9 <= d <= 15:
            w = "2"
        elif 16 <= d <= 22:
            w = "3"
        else:
            w = "4"
        wk.append(w)
        mm.append(m - 1 if w != "4" else m if m > 0 else 12)

    tbl = tbl.append_column("WK", pa.array(wk))
    tbl = tbl.append_column("MM", pa.array(mm))
    tbl = tbl.append_column("YEAR", year)
    tbl = tbl.append_column("MONTH", month)
    tbl = tbl.append_column("DAY", day)
    return tbl

reptdate_tbl = process_reptdate(reptdate_tbl)
con.register("FIXED_REPTDATE", reptdate_tbl)

# --- CIS transformation ---
cis_tbl = con.execute("SELECT * FROM CIS.DEPOSIT").arrow()

def transform_cis(tbl: pa.Table) -> pa.Table:
    acctno = tbl["ACCTNO"]
    country = tbl["CITIZEN"]
    race = tbl["RACE"]

    # Convert BIRTHDAT -> DOBCIS
    dob = []
    for b in tbl["BIRTHDAT"].to_pylist():
        if b and len(str(b)) >= 8:
            dd = int(str(b)[:2])
            mm = int(str(b)[2:4])
            yy = int(str(b)[4:8])
            dob.append(datetime(yy, mm, dd))
        else:
            dob.append(None)

    return pa.table({
        "ACCTNO": acctno,
        "COUNTRY": country,
        "DOBCIS": pa.array(dob),
        "RACE": race
    })

cis_tbl = transform_cis(cis_tbl)
con.register("CIS", cis_tbl)

# --- FD extraction ---
fd_tbl = con.execute("SELECT * FROM DPTRBL.FD").arrow()

# Transform FD (apply filtering + column adjustments)
def transform_fd(tbl: pa.Table) -> pa.Table:
    acctno = tbl["ACCTNO"]
    product = tbl["PRODUCT"]
    openind = tbl["OPENIND"]

    # Filter OPENIND not in (B, C, P) and PRODUCT range
    mask = []
    for o, p in zip(openind.to_pylist(), product.to_pylist()):
        if o not in ("B", "C", "P") and not (350 <= p <= 380):
            mask.append(True)
        else:
            mask.append(False)
    filtered = tbl.filter(pa.array(mask))

    # Date conversions (BDATE, OPENDT, REOPENDT, CLOSEDT, LASTTRAN, DTLSTCUST)
    def convert_date(col, fmt="MMDDYY"):
        vals = []
        for v in col.to_pylist():
            if v and v not in (0, "."):
                s = str(v).zfill(8)
                try:
                    dt = datetime.strptime(s[:8], "%m%d%Y")
                except:
                    dt = None
                vals.append(dt)
            else:
                vals.append(None)
        return pa.array(vals)

    fd_tbl = filtered.append_column("DOBMNI", convert_date(filtered["BDATE"]))
    fd_tbl = fd_tbl.set_column(fd_tbl.schema.get_field_index("OPENDT"), "OPENDT",
                               convert_date(filtered["OPENDT"]))
    fd_tbl = fd_tbl.set_column(fd_tbl.schema.get_field_index("REOPENDT"), "REOPENDT",
                               convert_date(filtered["REOPENDT"]))
    fd_tbl = fd_tbl.set_column(fd_tbl.schema.get_field_index("CLOSEDT"), "CLOSEDT",
                               convert_date(filtered["CLOSEDT"]))
    fd_tbl = fd_tbl.set_column(fd_tbl.schema.get_field_index("LASTTRAN"), "LASTTRAN",
                               convert_date(filtered["LASTTRAN"]))
    fd_tbl = fd_tbl.set_column(fd_tbl.schema.get_field_index("DTLSTCUST"), "DTLSTCUST",
                               convert_date(filtered["DTLSTCUST"]))

    # Normalize PURPOSE
    purpose = []
    for p in fd_tbl["PURPOSE"].to_pylist():
        purpose.append("1" if p == "4" else p)
    fd_tbl = fd_tbl.set_column(fd_tbl.schema.get_field_index("PURPOSE"), "PURPOSE",
                               pa.array(purpose))

    return fd_tbl

fd_transformed = transform_fd(fd_tbl)
con.register("FDFD", fd_transformed)

# Merge CIS, FD, SMSACC, MISMTD.FAVG
con.execute("""
CREATE OR REPLACE TABLE FIXED_FD_FINAL AS
SELECT 
    CIS.ACCTNO,
    CIS.COUNTRY,
    CIS.DOBCIS,
    CIS.RACE,
    FDFD.*,
    COALESCE(SAACC.ESIGNATURE, 'N') AS ESIGNATURE,
    MIS.*
FROM FDFD
LEFT JOIN CIS USING (ACCTNO)
LEFT JOIN SIGNA.SMSACC SAACC USING (ACCTNO)
LEFT JOIN MISMTD.FAVG MIS USING (ACCTNO)
""")

result = con.execute("SELECT * FROM FIXED_FD_FINAL").arrow()
print(result.schema)
