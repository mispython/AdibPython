import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

# ---------------------------
# 1. Load REPTDATE
# ---------------------------
# Assuming DPTRBL.REPTDATE is a table in DuckDB or an external file
reptdate = duckdb.sql("SELECT * FROM DPTRBL.REPTDATE").to_arrow()

# derive week (WK), adjusted month (MM), and macro-like variables
wk = pc.if_else(
    pc.less_equal(pc.day(reptdate['REPTDATE']), pc.scalar(8)),
    pa.array(['1'] * len(reptdate)),
    pc.if_else(
        pc.less_equal(pc.day(reptdate['REPTDATE']), pc.scalar(15)),
        pa.array(['2'] * len(reptdate)),
        pc.if_else(
            pc.less_equal(pc.day(reptdate['REPTDATE']), pc.scalar(22)),
            pa.array(['3'] * len(reptdate)),
            pa.array(['4'] * len(reptdate))
        )
    )
)

mm = pc.month(reptdate['REPTDATE'])
mm = pc.if_else(pc.equal(wk, pc.scalar('4')), mm, mm - 1)
mm = pc.if_else(pc.equal(mm, pc.scalar(0)), pc.scalar(12), mm)

# "macro variables"
reptyear = pc.year(reptdate['REPTDATE'])
reptmon  = pc.month(reptdate['REPTDATE'])
reptday  = pc.day(reptdate['REPTDATE'])
nowk     = wk

# ---------------------------
# 2. CIS Dataset
# ---------------------------
cis = duckdb.sql("SELECT * FROM CIS.DEPOSIT").to_arrow()

# Transform BIRTHDAT → DOBCIS
bdd = pc.utf8_slice_codeunits(cis['BIRTHDAT'], 0, 2)
bmm = pc.utf8_slice_codeunits(cis['BIRTHDAT'], 2, 2)
byy = pc.utf8_slice_codeunits(cis['BIRTHDAT'], 4, 4)

dobcis = duckdb.sql("""
    SELECT acctno,
           CAST(citizen AS VARCHAR) AS country,
           CAST(custnam1 AS VARCHAR) AS name,
           MAKE_DATE(CAST(byy AS INT), CAST(bmm AS INT), CAST(bdd AS INT)) AS dobcis,
           race
    FROM cis
""").to_arrow()

cis = duckdb.sql("SELECT * FROM dobcis").to_arrow()
cis = duckdb.sql("SELECT DISTINCT * FROM cis").to_arrow()

# ---------------------------
# 3. FD Dataset
# ---------------------------
# Simulate macro logic
if (nowk[0].as_py() != '4') and (int(reptmon[0].as_py()) == 1):
    fd = duckdb.sql("SELECT *, 0 as FEEYTD FROM DPTRBL.FD").to_arrow()
else:
    crmfd = duckdb.sql(f"SELECT DISTINCT * FROM MNICRM.FD{int(mm[0].as_py()):02d}").to_arrow()
    fd = duckdb.sql("""
        SELECT a.*, b.*
        FROM DPTRBL.FD a
        LEFT JOIN crmfd b USING(acctno)
    """).to_arrow()

# ---------------------------
# 4. Build FDFD
# ---------------------------
# Create status flags ACSTATUS4–ACSTATUS10
fdfd = duckdb.sql("""
    SELECT acctno, branch, product, deptype, orgtype, orgcode, 
           opendt, openind, sector, custcode, curbal, lasttran,
           ledgbal, state, purpose, user2, mtdavbal,
           riskcode, intplan, intpd, user3, second, inactive,
           bonutype, service, user5, prevbrno, costctr, feeytd,
           intytd, bonusano, closedt, intpaybl, intcycode,
           dobmni, intrstpd, avgamt, dnbfisme, mailcode, dtlstcust,
           acstatus4, acstatus5, acstatus6, acstatus7, acstatus8,
           acstatus9, acstatus10, post_ind, reason, instructions,
           reopendt, stmt_cycle, nxt_stmt_cycle_dt, prin_acct
    FROM fd
""").to_arrow()

# ---------------------------
# 5. Merge with CIS + SAACC + FAVG
# ---------------------------
saacc = duckdb.sql("SELECT * FROM SIGNA.SMSACC").to_arrow()
favg  = duckdb.sql(f"SELECT * FROM MISMTD.FAVG{int(reptmon[0].as_py()):02d}").to_arrow()

fixed_fd = duckdb.sql("""
    SELECT coalesce(c.acctno, f.acctno) as acctno,
           c.*, f.*, s.*, m.*
    FROM fdfd f
    LEFT JOIN cis c USING(acctno)
    LEFT JOIN saacc s USING(acctno)
    LEFT JOIN favg m USING(acctno)
""").to_arrow()

# Handle ESIGNATURE default
if 'ESIGNATURE' in fixed_fd.column_names:
    esig = pc.if_else(
        pc.equal(fixed_fd['ESIGNATURE'], pc.scalar('')),
        pa.array(['N'] * len(fixed_fd)),
        fixed_fd['ESIGNATURE']
    )
    fixed_fd = fixed_fd.set_column(fixed_fd.schema.get_field_index("ESIGNATURE"), "ESIGNATURE", esig)

# ---------------------------------------------------------------
# Save final result (Parquet + CSV)
# ---------------------------------------------------------------
final_name = f"FIXED_FD{int(reptyear[0].as_py())}{int(reptmon[0].as_py()):02d}{int(reptday[0].as_py()):02d}"
out_parquet = f"{final_name}.parquet"
out_csv = f"{final_name}.csv"

# Register Arrow table in DuckDB
duckdb.register("final_tbl", fixed_fd)

# Save to Parquet
duckdb.sql(f"COPY final_tbl TO '{out_parquet}' (FORMAT 'parquet')")

# Save to CSV
duckdb.sql(f"COPY final_tbl TO '{out_csv}' (HEADER, DELIMITER ',')")

print("✅ Job completed successfully.")
print(f"   - Parquet: {out_parquet}")
print(f"   - CSV    : {out_csv}")

