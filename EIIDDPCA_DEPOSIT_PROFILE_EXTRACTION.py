import duckdb
import pyarrow as pa
import pyarrow.compute as pc

# --- Step 1: Load CIS Data ---
# Example: replace with actual CIS.DEPOSIT table source
cis_deposit = duckdb.sql("SELECT * FROM cis_deposit").to_arrow()

# Derive CIS
cis = (
    cis_deposit
    .filter(pc.equal(cis_deposit['SECCUST'], pa.scalar('901')))
    .select([
        "ACCTNO",
        pc.field("CITIZEN").alias("COUNTRY"),
        pc.binary_slice(pc.field("BIRTHDAT"), 0, 2).cast(pa.int32()).alias("BDD"),
        pc.binary_slice(pc.field("BIRTHDAT"), 2, 2).cast(pa.int32()).alias("BMM"),
        pc.binary_slice(pc.field("BIRTHDAT"), 4, 4).cast(pa.int32()).alias("BYY"),
        "CUSTNAM1"
    ])
)

# Compute DOBCIS (birthdate)
# In SAS: MDY(BMM, BDD, BYY)
# Here: parse into pyarrow date
cis = cis.append_column("DOBCIS", pc.strptime(
    pc.binary_join_element_wise(
        [cis["BMM"], cis["BDD"], cis["BYY"]], 
        "-"), 
    format="%m-%d-%Y", unit="s"
))

cis = duckdb.sql("SELECT DISTINCT * FROM cis").to_arrow()

# --- Step 2: Handle REPTDATE logic (week partitioning, month calc) ---
reptdate = duckdb.sql("SELECT * FROM dptrbl_reptdate").to_arrow()

reptdate = reptdate.append_column(
    "WK",
    pc.case_when(
        (pc.day(reptdate["REPTDATE"]) <= 8, pa.scalar("1")),
        ((pc.day(reptdate["REPTDATE"]) <= 15, pa.scalar("2"))),
        ((pc.day(reptdate["REPTDATE"]) <= 22, pa.scalar("3"))),
        (True, pa.scalar("4"))
    )
)

# Derive MM
reptdate = reptdate.append_column(
    "MM",
    pc.if_else(
        pc.not_equal(reptdate["WK"], pa.scalar("4")),
        pc.subtract(pc.month(reptdate["REPTDATE"]), pa.scalar(1)),
        pc.month(reptdate["REPTDATE"])
    )
)

# Ensure MM rollover
reptdate = reptdate.set_column(
    reptdate.schema.get_field_index("MM"),
    "MM",
    pc.if_else(
        pc.equal(reptdate["MM"], pa.scalar(0)),
        pa.scalar(12),
        reptdate["MM"]
    )
)

# --- Step 3: Monthly logic (simplified) ---
# SAS had a macro MONTHLY with conditional branch
# Here: we prepare CA table
ca = duckdb.sql("SELECT * FROM dptrbl_current").to_arrow()
# If Jan & not 4th week => set FEEYTD=0
# Else join with CRMCA

# --- Step 4: Join Overdraft Data ---
overdft = duckdb.sql("SELECT DISTINCT ACCTNO, CRRINI AS FAACRR, CRRNOW AS CRRCODE, ASCORE_PERM, ASCORE_LTST, ASCORE_COMM FROM od_overdft").to_arrow()

curr = duckdb.sql("""
    SELECT ca.*, od.FAACRR, od.CRRCODE, od.ASCORE_PERM, od.ASCORE_LTST, od.ASCORE_COMM
    FROM ca
    LEFT JOIN overdft od USING(ACCTNO)
""").to_arrow()

# Additional transformations like ACSTATUS, risk mapping, date parsing, etc.
# Similar pc.if_else + pc.strptime to parse SAS dates

# --- Step 5: Merge CIS + CURR + SAACC + MISMTD
saacc = duckdb.sql("SELECT * FROM sig_na_smsacc").to_arrow()
cavg = duckdb.sql("SELECT * FROM mismtd_cavg").to_arrow()

ica = duckdb.sql("""
    SELECT cis.*, curr.*, saacc.*, cavg.*
    FROM curr
    LEFT JOIN cis USING(ACCTNO)
    LEFT JOIN saacc USING(ACCTNO)
    LEFT JOIN cavg USING(ACCTNO)
""").to_arrow()

# --- Step 6: Final merge with ACCUM
accum = duckdb.sql("SELECT * FROM drcrca_accum").to_arrow()

ica_final = duckdb.sql("""
    SELECT i.*, a.*
    FROM ica i
    LEFT JOIN accum a USING(ACCTNO)
""").to_arrow()

# Final dataset: CURRENT.ICA<YYYYMMDD>
