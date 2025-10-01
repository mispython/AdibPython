import duckdb
import pyarrow as pa
import pyarrow.compute as pc

# --- Step 1: Load REPTDATE ---
reptdate = duckdb.sql("SELECT * FROM dptrbl_reptdate").to_arrow()

# Compute WK (week number in month)
reptdate = reptdate.append_column(
    "WK",
    pc.case_when(
        (pc.day(reptdate["REPTDATE"]) <= 8, pa.scalar("1")),
        ((pc.day(reptdate["REPTDATE"]) <= 15, pa.scalar("2"))),
        ((pc.day(reptdate["REPTDATE"]) <= 22, pa.scalar("3"))),
        (True, pa.scalar("4"))
    )
)

# Compute MM
reptdate = reptdate.append_column(
    "MM",
    pc.if_else(
        pc.not_equal(reptdate["WK"], pa.scalar("4")),
        pc.subtract(pc.month(reptdate["REPTDATE"]), pa.scalar(1)),
        pc.month(reptdate["REPTDATE"])
    )
)

# Ensure rollover
reptdate = reptdate.set_column(
    reptdate.schema.get_field_index("MM"),
    "MM",
    pc.if_else(
        pc.equal(reptdate["MM"], pa.scalar(0)),
        pa.scalar(12),
        reptdate["MM"]
    )
)

# Extract fields as Python scalars for later
rept_year = duckdb.sql("SELECT strftime(REPTDATE, '%Y') AS y FROM reptdate").fetchone()[0]
rept_mon  = duckdb.sql("SELECT strftime(REPTDATE, '%m') AS m FROM reptdate").fetchone()[0]
rept_day  = duckdb.sql("SELECT strftime(REPTDATE, '%d') AS d FROM reptdate").fetchone()[0]
wk        = duckdb.sql("SELECT WK FROM reptdate").fetchone()[0]

# --- Step 2: CIS extraction ---
cis = duckdb.sql("""
    SELECT 
        ACCTNO,
        CITIZEN AS COUNTRY,
        SUBSTR(BIRTHDAT,1,2) AS BDD,
        SUBSTR(BIRTHDAT,3,2) AS BMM,
        SUBSTR(BIRTHDAT,5,4) AS BYY,
        CUSTNAM1 AS NAME,
        RACE
    FROM cis_deposit
    WHERE SECCUST='901'
""").to_arrow()

cis = cis.append_column(
    "DOBCIS",
    pc.strptime(
        pc.binary_join_element_wise([cis["BMM"], cis["BDD"], cis["BYY"]], "-"),
        format="%m-%d-%Y", unit="s"
    )
)

cis = duckdb.sql("SELECT DISTINCT * FROM cis").to_arrow()

# --- Step 3: Monthly Logic for SA + UMA ---
if wk != "4" and rept_mon == "01":
    sa  = duckdb.sql("SELECT *, 0 AS FEEYTD FROM dptrbl_saving").to_arrow()
    uma = duckdb.sql("SELECT *, 0 AS FEEYTD FROM dptrbl_uma").to_arrow()
else:
    crmsa = duckdb.sql(f"SELECT DISTINCT * FROM mnicrm_sa{int(rept_mon)-1:02}").to_arrow()
    crmuma = duckdb.sql(f"SELECT DISTINCT * FROM mnicrm_uma{int(rept_mon)-1:02}").to_arrow()

    sa = duckdb.sql("""
        SELECT a.*, COALESCE(b.ACCTNO, a.ACCTNO) AS ACCTNO
        FROM dptrbl_saving a
        LEFT JOIN crmsa b USING(ACCTNO)
    """).to_arrow()

    uma = duckdb.sql("""
        SELECT a.*, COALESCE(b.ACCTNO, a.ACCTNO) AS ACCTNO
        FROM dptrbl_uma a
        LEFT JOIN crmuma b USING(ACCTNO)
    """).to_arrow()

# --- Step 4: Combine SA + UMA into SAVG ---
savg = duckdb.sql("""
    SELECT * FROM sa
    UNION ALL
    SELECT * FROM uma
""").to_arrow()

# Status parsing
for i in range(4, 11):
    savg = savg.append_column(f"ACSTATUS{i}", pc.utf8_slice_codeunits(savg["STATCD"], i-1, 1))

# Date conversions
def safe_date(col, fmt, length):
    return pc.if_else(
        pc.and_(
            pc.is_valid(col),
            pc.not_equal(col, pa.scalar(0))
        ),
        pc.strptime(pc.utf8_slice_codeunits(pc.cast(col, pa.string()), 0, length), fmt, unit="s"),
        pa.scalar(None, type=pa.timestamp("s"))
    )

savg = savg.set_column(savg.schema.get_field_index("OPENDT"), "OPENDT", safe_date(savg["OPENDT"], "%m%d%Y", 8))
savg = savg.set_column(savg.schema.get_field_index("REOPENDT"), "REOPENDT", safe_date(savg["REOPENDT"], "%m%d%Y", 8))
savg = savg.set_column(savg.schema.get_field_index("CLOSEDT"), "CLOSEDT", safe_date(savg["CLOSEDT"], "%m%d%Y", 8))
savg = savg.set_column(savg.schema.get_field_index("LASTTRAN"), "LASTTRAN", safe_date(savg["LASTTRAN"], "%m%d%y", 6))
savg = savg.set_column(savg.schema.get_field_index("DTLSTCUST"), "DTLSTCUST", safe_date(savg["DTLSTCUST"], "%m%d%y", 6))

# --- Step 5: Join with SAACC + MISMTD ---
saacc = duckdb.sql("SELECT * FROM signa_smsacc").to_arrow()
mismtd = duckdb.sql(f"SELECT * FROM mismtd_savg{rept_mon}").to_arrow()

isa = duckdb.sql("""
    SELECT cis.*, s.*, saacc.*, m.*
    FROM savg s
    LEFT JOIN cis USING(ACCTNO)
    LEFT JOIN saacc USING(ACCTNO)
    LEFT JOIN mismtd m USING(ACCTNO)
""").to_arrow()

# Handle ESIGNATURE
isa = isa.set_column(
    isa.schema.get_field_index("ESIGNATURE"),
    "ESIGNATURE",
    pc.if_else(pc.equal(isa["ESIGNATURE"], pa.scalar("")), pa.scalar("N"), isa["ESIGNATURE"])
)

# --- Step 6: Final output table ---
final_table_name = f"SAVING_ISA{rept_year}{rept_mon}{rept_day}"
duckdb.register(final_table_name, isa)

print(f"Final table created: {final_table_name}")
