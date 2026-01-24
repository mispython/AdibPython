import duckdb
import polars as pl
from pathlib import Path
import math
import re

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
DLP_DIR = OUTPUT_DIR / 'dlp'
SFTP_DIR = OUTPUT_DIR / 'sftp'

# Input paths
BNM_REPTDATE = INPUT_DIR / 'bnm' / 'reptdate.parquet'
CISR_DEPOSIT = INPUT_DIR / 'cisr' / 'deposit.parquet'
CISD_DEPOSIT = INPUT_DIR / 'cisd' / 'deposit.parquet'
CISL_LOAN = INPUT_DIR / 'cisl' / 'loan.parquet'
CARD_UNICARD = INPUT_DIR / 'card' / 'unicard{reptyear}{reptmon}{nowk}.parquet'
ICOLL_COLLATER = INPUT_DIR / 'icoll' / 'collater.parquet'
COLL_COLLATER = INPUT_DIR / 'coll' / 'collater.parquet'

# Output paths
DLP_CUSTDATA = DLP_DIR / 'custdata.parquet'
SFTP_SCRIPT = SFTP_DIR / 'sftp_commands.txt'

# Create output directories
DLP_DIR.mkdir(parents=True, exist_ok=True)
SFTP_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# Constants
# ============================================================================

STRING_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ=`&-/\\@#*+:().,\"%�!$?_ "
NUMBER_CHARS = "0123456789"
ZERO_CHAR = "0"
FALSIC_LIST = [
    '1234567', '12345678', '123456789', '0123456789',
    'SA9999', 'SA99999', '999999A', '999999W', '999999X'
]

# ============================================================================
# Initialize DuckDB
# ============================================================================

con = duckdb.connect()

# ============================================================================
# Get REPTDATE and calculate variables
# ============================================================================

reptdate_df = pl.read_parquet(BNM_REPTDATE)
reptdate = reptdate_df['reptdate'][0]

day = reptdate.day
mm = reptdate.month
year = reptdate.year

# Determine week (WK)
if day == 8:
    sdd, wk, wk1 = 1, '01', '04'
elif day == 15:
    sdd, wk, wk1 = 9, '02', '01'
elif day == 22:
    sdd, wk, wk1 = 16, '03', '02'
else:
    sdd, wk, wk1 = 23, '04', '03'

# Format variables
nowk = wk
nowk1 = wk1
reptday = f"{day:02d}"
reptmon = f"{mm:02d}"
reptyear = f"{year % 100:02d}"

print(f"Processing date: {reptdate}")
print(f"Week: {nowk}, Month: {reptmon}, Year: {reptyear}")

# Build card file path
card_unicard_path = str(CARD_UNICARD).format(reptyear=reptyear, reptmon=reptmon, nowk=nowk)

# ============================================================================
# Process CIS data from multiple sources
# ============================================================================

# Process CISR.DEPOSIT
con.execute(f"""
    CREATE TEMP TABLE cisrm AS
    SELECT acctno, custno, newic, oldic, priphone, secphone, 
           mobiphon, bussreg, newicind
    FROM read_parquet('{CISR_DEPOSIT}')
    ORDER BY acctno
""")

# Process CISD.DEPOSIT
con.execute(f"""
    CREATE TEMP TABLE cisdp AS
    SELECT acctno, custno, newic, oldic, priphone, secphone, 
           mobiphon, bussreg, newicind
    FROM read_parquet('{CISD_DEPOSIT}')
    ORDER BY acctno
""")

# Process CISL.LOAN
con.execute(f"""
    CREATE TEMP TABLE cisln AS
    SELECT acctno, custno, newic, oldic, priphone, secphone, 
           mobiphon, bussreg, newicind
    FROM read_parquet('{CISL_LOAN}')
    ORDER BY acctno
""")

# ============================================================================
# Process CARD data
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE ccard_base AS
    SELECT cardno, custnbr, newic, oldic, bustelno, homtelno, hphoneno
    FROM read_parquet('{card_unicard_path}')
    WHERE (closecd IN ('', ' ') OR closecd IS NULL)
      AND (reclass IN ('', ' ') OR reclass IS NULL)
    ORDER BY custnbr
""")

# Count cards per customer
con.execute("""
    CREATE TEMP TABLE card_summary AS
    SELECT custnbr, COUNT(*) as freq
    FROM ccard_base
    GROUP BY custnbr
""")

# Get maximum frequency
max_freq = con.execute("SELECT MAX(freq) FROM card_summary").fetchone()[0]
c_value = max_freq + 2 if max_freq else 2
card_name = f"card{c_value}"

print(f"Maximum cards per customer: {max_freq}, Total columns: {c_value}")

# Transpose card numbers (PROC TRANSPOSE equivalent)
con.execute("""
    CREATE TEMP TABLE ccard_numbered AS
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY custnbr ORDER BY cardno) as card_seq
    FROM ccard_base
""")

# Create pivot for card numbers
card_cols = ', '.join([f"MAX(CASE WHEN card_seq = {i} THEN cardno END) as card{i}" 
                       for i in range(1, c_value + 1)])

con.execute(f"""
    CREATE TEMP TABLE tranpcard_base AS
    SELECT custnbr, newic, oldic, homtelno, bustelno, hphoneno,
           {card_cols}
    FROM ccard_numbered
    GROUP BY custnbr, newic, oldic, homtelno, bustelno, hphoneno
""")

# Filter where at least CARD1 has value
con.execute("""
    CREATE TEMP TABLE tranpcard AS
    SELECT *,
           CASE WHEN newic IN ('', ' ') OR newic IS NULL THEN oldic ELSE newic END as newic_final,
           custnbr as custno,
           homtelno as priphone,
           bustelno as secphone,
           hphoneno as mobiphon
    FROM tranpcard_base
    WHERE card1 IS NOT NULL AND card1 NOT IN ('', ' ')
""")

# ============================================================================
# Process COLLATER data
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE coll AS
    SELECT acctno, ccollno
    FROM read_parquet('{ICOLL_COLLATER}')
    UNION ALL
    SELECT acctno, ccollno
    FROM read_parquet('{COLL_COLLATER}')
    ORDER BY acctno
""")

# ============================================================================
# Merge CIS data with COLLATER
# ============================================================================

con.execute("""
    CREATE TEMP TABLE cisrm_coll AS
    SELECT 
        CAST(c.acctno AS VARCHAR) as acctno1,
        CAST(c.custno AS VARCHAR) as custno1,
        CAST(cl.ccollno AS VARCHAR) as collno1,
        c.newic, c.oldic, c.priphone, c.secphone, c.mobiphon, 
        c.bussreg, c.newicind
    FROM cisrm c
    LEFT JOIN coll cl ON c.acctno = cl.acctno
""")

con.execute("""
    CREATE TEMP TABLE cisdp_coll AS
    SELECT 
        CAST(c.acctno AS VARCHAR) as acctno1,
        CAST(c.custno AS VARCHAR) as custno1,
        CAST(cl.ccollno AS VARCHAR) as collno1,
        c.newic, c.oldic, c.priphone, c.secphone, c.mobiphon, 
        c.bussreg, c.newicind
    FROM cisdp c
    LEFT JOIN coll cl ON c.acctno = cl.acctno
""")

con.execute("""
    CREATE TEMP TABLE cisln_coll AS
    SELECT 
        CAST(c.acctno AS VARCHAR) as acctno1,
        CAST(c.custno AS VARCHAR) as custno1,
        CAST(cl.ccollno AS VARCHAR) as collno1,
        c.newic, c.oldic, c.priphone, c.secphone, c.mobiphon, 
        c.bussreg, c.newicind
    FROM cisln c
    LEFT JOIN coll cl ON c.acctno = cl.acctno
""")

# ============================================================================
# Combine all CIS sources
# ============================================================================

con.execute("""
    CREATE TEMP TABLE cis_combined AS
    SELECT acctno1 as acctno, custno1 as custno, collno1 as ccollno,
           newic, oldic, priphone, secphone, mobiphon, bussreg, newicind
    FROM cisrm_coll
    UNION ALL
    SELECT acctno1 as acctno, custno1 as custno, collno1 as ccollno,
           newic, oldic, priphone, secphone, mobiphon, bussreg, newicind
    FROM cisdp_coll
    UNION ALL
    SELECT acctno1 as acctno, custno1 as custno, collno1 as ccollno,
           newic, oldic, priphone, secphone, mobiphon, bussreg, newicind
    FROM cisln_coll
""")

# Apply NEWIC logic
con.execute("""
    CREATE TEMP TABLE cis AS
    SELECT *,
           CASE WHEN newic IN ('', ' ') OR newic IS NULL THEN 'OC' ELSE newicind END as newicind_final,
           CASE WHEN newic IN ('', ' ') OR newic IS NULL THEN oldic ELSE newic END as newic_final
    FROM cis_combined
    ORDER BY newic_final
""")

# ============================================================================
# Merge CIS with CARD data
# ============================================================================

# Get card column names
card_columns = ', '.join([f"t.card{i}" for i in range(1, c_value + 1)])

con.execute(f"""
    CREATE TEMP TABLE ciscard_base AS
    SELECT c.acctno, c.custno, c.ccollno, 
           COALESCE(c.newic_final, t.newic_final) as newic,
           c.oldic,
           COALESCE(c.priphone, t.priphone) as priphone,
           COALESCE(c.secphone, t.secphone) as secphone,
           COALESCE(c.mobiphon, t.mobiphon) as mobiphon,
           c.bussreg,
           c.newicind_final as newicind,
           {card_columns}
    FROM cis c
    FULL OUTER JOIN tranpcard t ON c.newic_final = t.newic_final
""")

# ============================================================================
# Compress and clean data
# ============================================================================

# Use Polars for string compression (more efficient than SQL)
ciscard_df = con.execute("SELECT * FROM ciscard_base").pl()

# Compress function
def compress_keep_digits(s):
    if s is None or s == '':
        return ''
    return re.sub(r'[^0-9]', '', str(s))

def compress_all(s):
    if s is None or s == '':
        return ''
    return re.sub(r'\s+', '', str(s))

# Apply compression
ciscard_df = ciscard_df.with_columns([
    pl.col('newic').map_elements(compress_all, return_dtype=pl.Utf8).alias('newic'),
    pl.col('acctno').map_elements(compress_all, return_dtype=pl.Utf8).alias('acctno'),
    pl.col('custno').map_elements(compress_all, return_dtype=pl.Utf8).alias('custno'),
    pl.col('priphone').map_elements(compress_keep_digits, return_dtype=pl.Utf8).alias('priphone'),
    pl.col('secphone').map_elements(compress_keep_digits, return_dtype=pl.Utf8).alias('secphone'),
    pl.col('mobiphon').map_elements(compress_keep_digits, return_dtype=pl.Utf8).alias('mobiphon'),
    pl.col('bussreg').map_elements(compress_all, return_dtype=pl.Utf8).alias('bussreg'),
    pl.col('ccollno').map_elements(compress_all, return_dtype=pl.Utf8).alias('ccollno'),
])

# ============================================================================
# Remove leading zeros from phone numbers
# ============================================================================

ciscard_df = ciscard_df.with_columns([
    pl.col('priphone').cast(pl.Int64, strict=False).cast(pl.Utf8).alias('pr1ph0n3'),
    pl.col('secphone').cast(pl.Int64, strict=False).cast(pl.Utf8).alias('s3cph0n3'),
    pl.col('mobiphon').cast(pl.Int64, strict=False).cast(pl.Utf8).alias('m0b1ph0n'),
])

# Replace null/empty with placeholder
placeholder = '###**###'

# Replace empty values with placeholder for all card columns
for i in range(1, c_value + 1):
    col_name = f'card{i}'
    if col_name in ciscard_df.columns:
        ciscard_df = ciscard_df.with_columns([
            pl.when(pl.col(col_name).is_null() | (pl.col(col_name) == '') | (pl.col(col_name) == ' '))
            .then(pl.lit(placeholder))
            .otherwise(pl.col(col_name))
            .alias(col_name)
        ])

# Replace empty values with placeholder for main fields
ciscard_df = ciscard_df.with_columns([
    pl.when(pl.col('newic').is_null() | (pl.col('newic') == '') | (pl.col('newic') == '.'))
    .then(pl.lit(placeholder)).otherwise(pl.col('newic')).alias('newic'),
    
    pl.when(pl.col('acctno').is_null() | (pl.col('acctno') == '') | (pl.col('acctno') == '.'))
    .then(pl.lit(placeholder)).otherwise(pl.col('acctno')).alias('acctno'),
    
    pl.when(pl.col('custno').is_null() | (pl.col('custno') == '') | (pl.col('custno') == '.'))
    .then(pl.lit(placeholder)).otherwise(pl.col('custno')).alias('custno'),
    
    pl.when(pl.col('priphone').is_null() | (pl.col('priphone') == '') | (pl.col('priphone') == '.'))
    .then(pl.lit(placeholder)).otherwise(pl.col('priphone')).alias('priphone'),
    
    pl.when(pl.col('secphone').is_null() | (pl.col('secphone') == '') | (pl.col('secphone') == '.'))
    .then(pl.lit(placeholder)).otherwise(pl.col('secphone')).alias('secphone'),
    
    pl.when(pl.col('mobiphon').is_null() | (pl.col('mobiphon') == '') | (pl.col('mobiphon') == '.'))
    .then(pl.lit(placeholder)).otherwise(pl.col('mobiphon')).alias('mobiphon'),
    
    pl.when(pl.col('bussreg').is_null() | (pl.col('bussreg') == '') | (pl.col('bussreg') == '.'))
    .then(pl.lit(placeholder)).otherwise(pl.col('bussreg')).alias('bussreg'),
    
    pl.when(pl.col('ccollno').is_null() | (pl.col('ccollno') == '') | (pl.col('ccollno') == '.'))
    .then(pl.lit(placeholder)).otherwise(pl.col('ccollno')).alias('ccollno'),
])

# ============================================================================
# Apply validation filters
# ============================================================================

def compress_excluding(text, exclude_chars):
    """Remove all characters in exclude_chars from text"""
    if text is None or text == '':
        return ''
    pattern = f"[{re.escape(exclude_chars)}]"
    return re.sub(pattern, '', str(text))

# Add check columns
ciscard_df = ciscard_df.with_columns([
    pl.col('newic').map_elements(lambda x: compress_excluding(x, NUMBER_CHARS), return_dtype=pl.Utf8).alias('numcheck'),
    pl.col('newic').map_elements(lambda x: compress_excluding(x, STRING_CHARS), return_dtype=pl.Utf8).alias('strcheck'),
    pl.col('newic').map_elements(lambda x: compress_excluding(x, ZERO_CHAR), return_dtype=pl.Utf8).alias('zrocheck'),
    pl.col('pr1ph0n3').map_elements(lambda x: compress_excluding(x, ZERO_CHAR), return_dtype=pl.Utf8).alias('zrchkpri'),
    pl.col('s3cph0n3').map_elements(lambda x: compress_excluding(x, ZERO_CHAR), return_dtype=pl.Utf8).alias('zrchksec'),
    pl.col('m0b1ph0n').map_elements(lambda x: compress_excluding(x, ZERO_CHAR), return_dtype=pl.Utf8).alias('zrchkmob'),
])

# Apply NEWIC filtering rules
ciscard_df = ciscard_df.with_columns([
    pl.when(
        (pl.col('newicind') == 'IC') & (pl.col('newic').str.lengths() < 12)
    ).then(pl.lit(placeholder))
    .when(
        (pl.col('newicind') == 'OC') & (pl.col('newic').str.lengths() < 7)
    ).then(pl.lit(placeholder))
    .when(
        pl.col('newicind').is_in(['SA', 'PC', 'BC'])
    ).then(pl.lit(placeholder))
    .when(
        (pl.col('numcheck') == ' ') & (pl.col('newic').str.lengths() < 6)
    ).then(pl.lit(placeholder))
    .when(
        pl.col('strcheck') == ' '
    ).then(pl.lit(placeholder))
    .when(
        pl.col('newic').is_in(FALSIC_LIST)
    ).then(pl.lit(placeholder))
    .when(
        pl.col('newic').str.lengths() < 4
    ).then(pl.lit(placeholder))
    .when(
        pl.col('zrocheck') == ' '
    ).then(pl.lit(placeholder))
    .otherwise(pl.col('newic'))
    .alias('newic')
])

# Apply phone number filtering rules
ciscard_df = ciscard_df.with_columns([
    pl.when(pl.col('pr1ph0n3').str.lengths() < 8)
    .then(pl.lit(placeholder)).otherwise(pl.col('priphone')).alias('priphone'),
    
    pl.when(pl.col('s3cph0n3').str.lengths() < 8)
    .then(pl.lit(placeholder)).otherwise(pl.col('secphone')).alias('secphone'),
    
    pl.when(pl.col('m0b1ph0n').str.lengths() < 8)
    .then(pl.lit(placeholder)).otherwise(pl.col('mobiphon')).alias('mobiphon'),
])

ciscard_df = ciscard_df.with_columns([
    pl.when((pl.col('zrchkpri') == ' ') | (pl.col('zrchkpri').str.lengths() < 4))
    .then(pl.lit(placeholder)).otherwise(pl.col('priphone')).alias('priphone'),
    
    pl.when((pl.col('zrchksec') == ' ') | (pl.col('zrchksec').str.lengths() < 4))
    .then(pl.lit(placeholder)).otherwise(pl.col('secphone')).alias('secphone'),
    
    pl.when((pl.col('zrchkmob') == ' ') | (pl.col('zrchkmob').str.lengths() < 4))
    .then(pl.lit(placeholder)).otherwise(pl.col('mobiphon')).alias('mobiphon'),
])

# Drop temporary check columns
ciscard_df = ciscard_df.drop(['numcheck', 'strcheck', 'zrocheck', 
                               'zrchkpri', 'zrchksec', 'zrchkmob',
                               'pr1ph0n3', 's3cph0n3', 'm0b1ph0n'])

# Save to parquet
ciscard_df.write_parquet(DLP_CUSTDATA)

# ============================================================================
# Split into multiple files (5 million records each)
# ============================================================================

nobs = len(ciscard_df)
obstart = 0
obsnum = 5000000
numfile = math.ceil(nobs / obsnum)

print(f"\nTotal records: {nobs}")
print(f"Records per file: {obsnum}")
print(f"Number of files: {numfile}")

# Generate output files
for i in range(1, numfile + 1):
    file_no = f"{i:03d}"
    output_file = DLP_DIR / f"DLP{file_no}.csv"
    
    # Calculate slice
    start_idx = (i - 1) * obsnum
    end_idx = min(i * obsnum, nobs)
    
    df_slice = ciscard_df[start_idx:end_idx]
    
    # Prepare card columns for output
    card_cols = [f'card{j}' for j in range(1, c_value + 1) if f'card{j}' in df_slice.columns]
    
    # Create output with pipe delimiters
    with open(output_file, 'w') as f:
        for row in df_slice.iter_rows(named=True):
            # Build line with pipe delimiters
            line_parts = [
                row['newic'],
                row['priphone'],
                row['secphone'],
                row['mobiphon'],
                row['acctno'],
                row['bussreg']
            ]
            
            # Add card numbers
            for card_col in card_cols:
                line_parts.append(row.get(card_col, placeholder))
            
            line = '|'.join(line_parts)
            f.write(line + '\n')
    
    print(f"Generated: {output_file}")

# ============================================================================
# Generate SFTP script
# ============================================================================

with open(SFTP_SCRIPT, 'w') as f:
    for i in range(1, numfile + 1):
        file_no = f"{i:03d}"
        f.write(f"PUT //SAP.PBB.DLP.C{file_no}.TEXT  DLP{file_no}.CSV\n")

print(f"\nGenerated SFTP script: {SFTP_SCRIPT}")
print(f"\nProcessing complete!")
print(f"Output directory: {DLP_DIR}")
print(f"Files created: {numfile}")
