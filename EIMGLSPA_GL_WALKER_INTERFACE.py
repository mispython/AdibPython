# EIMGLSPA_GL_WALKER_INTERFACE.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pathlib import Path
from datetime import datetime

# Configuration
base_path = Path(".")
loan_path = base_path / "LOAN"
npl_path = base_path / "NPL"
output_path = base_path / "OUTPUT"
output_path.mkdir(exist_ok=True)

# Connect to DuckDB
conn = duckdb.connect()

print("EIMGLSPA - GL Walker Interface File Generator")

# Step 1: Process REPTDATE and current datetime
reptdate_query = f"SELECT * FROM read_parquet('{loan_path / \"REPTDATE.parquet\"}')"
reptdate_df = conn.execute(reptdate_query).arrow()

if len(reptdate_df) > 0:
    reptdate = reptdate_df['REPTDATE'][0].as_py()
    mm1 = reptdate.month - 1
    if mm1 == 0:
        mm1 = 12
    
    dt = datetime.now()
    tm = dt.time()
    
    datenow = dt.strftime('%Y%m%d')
    timenow = dt.strftime('%H%M%S')
    
    reptmon = reptdate.strftime('%m')
    prevmon = f"{mm1:02d}"
    rdate = reptdate.strftime('%d/%m/%Y')
    reptdat = reptdate
    nowk = '4'
    rptdte = datenow
    rpttime = timenow
    curday = dt.strftime('%d')
    curmth = dt.strftime('%m')
    curyr = dt.strftime('%Y')
    rptyr = reptdate.strftime('%Y')
    rptdy = reptdate.strftime('%d')

# Step 2: Process NPL data
iis_query = f"SELECT * FROM read_parquet('{npl_path / \"IIS.parquet\"}')"
iis_df = conn.execute(iis_query).arrow()

sp2_query = f"SELECT * FROM read_parquet('{npl_path / \"SP2.parquet\"}')"
sp2_df = conn.execute(sp2_query).arrow()

lnnote_query = f"SELECT ACCTNO, NOTENO, COSTCTR FROM read_parquet('{loan_path / \"LNNOTE.parquet\"}')"
lnnote_df = conn.execute(lnnote_query).arrow()

# Remove duplicates
iis_dedup = conn.execute("SELECT DISTINCT * FROM iis_df ORDER BY ACCTNO, NOTENO").arrow()
sp2_dedup = conn.execute("SELECT DISTINCT * FROM sp2_df ORDER BY ACCTNO, NOTENO").arrow()
lnnote_sorted = conn.execute("SELECT * FROM lnnote_df ORDER BY ACCTNO, NOTENO").arrow()

# Merge data
npliis_query = """
SELECT 
    s.*,
    l.COSTCTR
FROM sp2_dedup s
LEFT JOIN lnnote_sorted l ON s.ACCTNO = l.ACCTNO AND s.NOTENO = l.NOTENO
WHERE s.ACCTNO IS NOT NULL
"""
npliis_df = conn.execute(npliis_query).arrow()

# Adjust COSTCTR
npliis_adj = conn.execute("SELECT *, CASE WHEN COSTCTR = 1146 THEN 146 ELSE COSTCTR END as COSTCTR_ADJ FROM npliis_df").arrow()
npliis_sorted = conn.execute("SELECT * EXCLUDE (COSTCTR), COSTCTR_ADJ as COSTCTR FROM npliis_adj ORDER BY COSTCTR").arrow()

# Step 3: Create GLPROC data
glproc_data = []
for record in npliis_sorted.to_pylist():
    loantyp = record.get('LOANTYP', '')
    sppl = record.get('SPPL', 0)
    recover = record.get('RECOVER', 0)
    costctr = record.get('COSTCTR', 0)
    
    if 'HPD CONV' in str(loantyp):
        if sppl and sppl != 0:
            glproc_data.append({
                'COSTCTR': costctr,
                'ARID': 'SPHPDPJ',
                'JDESC': 'PROVISION FOR SP (HPD)(J)',
                'AVALUE': sppl
            })
        if recover and recover != 0:
            glproc_data.append({
                'COSTCTR': costctr,
                'ARID': 'SPHPDWK', 
                'JDESC': 'SP WRITTEN BACK (HPD)(K)',
                'AVALUE': recover
            })
    else:
        if sppl and sppl != 0:
            glproc_data.append({
                'COSTCTR': costctr,
                'ARID': 'SPAITPJ',
                'JDESC': 'PROVISION OF SP (AITAB) (J)',
                'AVALUE': sppl
            })
        if recover and recover != 0:
            glproc_data.append({
                'COSTCTR': costctr,
                'ARID': 'SPAITWK',
                'JDESC': 'SP WRITTEN BACK (AITAB) (K)', 
                'AVALUE': recover
            })

glproc_df = pa.Table.from_pylist(glproc_data)

# Summarize data
if len(glproc_df) > 0:
    glproc_summary = conn.execute("""
    SELECT COSTCTR, ARID, JDESC, SUM(AVALUE) as AVALUE
    FROM glproc_df
    GROUP BY COSTCTR, ARID, JDESC
    ORDER BY COSTCTR, ARID
    """).arrow()
else:
    glproc_summary = pa.table({
        'COSTCTR': pa.array([], pa.int32()),
        'ARID': pa.array([], pa.string()),
        'JDESC': pa.array([], pa.string()),
        'AVALUE': pa.array([], pa.float64())
    })

# Step 4: Generate GL interface file
glfile_path = output_path / "GLINTERFACE.txt"
with open(glfile_path, 'w') as f:
    # Write header record
    f.write(f"INTFSPVF0{rptyr}{reptmon}{rptdy}{rptdte}{rpttime}\n")
    f.write(f"INTFSPVF9{rptyr}{reptmon}{rptdy}{rptdte}{rpttime}+0000000000000000-0000000000000000+0000000000000000-0000000000000000000000000\n")
    
    # Process records
    if len(glproc_summary) > 0:
        records = glproc_summary.to_pylist()
        cnt = 0
        amttotc = 0
        amttotd = 0
        
        current_arid = None
        brhamt = 0
        
        for i, record in enumerate(records):
            costctr = record['COSTCTR']
            arid = record['ARID']
            jdesc = record['JDESC']
            avalue = round(record['AVALUE'], 2)
            
            # Determine sign
            if avalue < 0:
                avalue = abs(avalue)
                asign = '-'
                amttotc += avalue
            else:
                asign = '+'
                amttotd += avalue
            
            # Check for ARID change
            if arid != current_arid:
                if current_arid is not None:
                    # Write previous ARID summary
                    cnt += 1
                    brhamto = f"{brhamt:016.2f}".replace('.', '').zfill(16)
                    
                    line = f"INTFSPVF1{arid:8}PBBH{costctr:04d}" + " " * 45
                    if 'COR' in arid:
                        line += f"{curyr}{curmth}{curday}"
                    else:
                        line += f"{rptyr}{reptmon}{rptdy}"
                    line += f"{asign}{brhamto}{jdesc:60}{arid:8}SPVF" + " " * 80
                    line += "+0000000000000000+000000000000000"
                    if 'COR' in arid:
                        line += f"{curyr}{curmth}{curday}"
                    else:
                        line += f"{rptyr}{reptmon}{rptdy}"
                    line += "+0000000000000000\n"
                    f.write(line)
                
                current_arid = arid
                brhamt = 0
            
            brhamt += avalue
        
        # Write final ARID summary
        if current_arid is not None:
            cnt += 1
            brhamto = f"{brhamt:016.2f}".replace('.', '').zfill(16)
            
            line = f"INTFSPVF1{current_arid:8}PBBH{costctr:04d}" + " " * 45
            if 'COR' in current_arid:
                line += f"{curyr}{curmth}{curday}"
            else:
                line += f"{rptyr}{reptmon}{rptdy}"
            line += f"{asign}{brhamto}{jdesc:60}{current_arid:8}SPVF" + " " * 80
            line += "+0000000000000000+000000000000000"
            if 'COR' in current_arid:
                line += f"{curyr}{curmth}{curday}"
            else:
                line += f"{rptyr}{reptmon}{rptdy}"
            line += "+0000000000000000\n"
            f.write(line)
        
        # Write footer record
        amttotco = f"{amttotc:016.2f}".replace('.', '').zfill(16)
        amttotdo = f"{amttotd:016.2f}".replace('.', '').zfill(16)
        f.write(f"INTFSPVF9{rptyr}{reptmon}{rptdy}{rptdte}{rpttime}+{amttotdo}-{amttotco}+0000000000000000-0000000000000000{cnt:09d}\n")

print(f"Completed: GL interface file generated with {len(glproc_summary)} records")
conn.close()
