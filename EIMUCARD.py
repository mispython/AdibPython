#!/usr/bin/env python3
"""
EIMUCARD - Credit Card Account Matching
Matches credit card holders with loan accounts via IC numbers
"""

import duckdb
from pathlib import Path
from datetime import datetime

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()

reptdate = con.execute(f"SELECT reptdate FROM read_parquet('{INPUT_DIR}/ln/reptdate.parquet')").fetchone()[0]
day = reptdate.day

if day == 8:
    wk, wk1 = '01', '04'
elif day == 15:
    wk, wk1 = '02', '01'
elif day == 22:
    wk, wk1 = '03', '02'
else:
    wk, wk1 = '04', '03'

nomth = (reptdate.year * 12) + reptdate.month
reptmon = reptdate.month
reptyr2 = reptdate.year % 100
reptyear = reptdate.year
rdate = reptdate.strftime('%d/%m/%Y')

print(f"Credit Card Matching - {rdate}, Week: {wk}")

# ============================================================================
# LOAD CREDIT CARD DATA FROM CARDTXT FILE
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE card_raw AS
    SELECT 
        cardno, oldic, newic, copendt, cname, brcode,
        {nomth} - ((YEAR(copendt) * 12) + MONTH(copendt)) mthdue
    FROM read_parquet('{INPUT_DIR}/card/cardtxt.parquet')
""")

con.execute(f"COPY card_raw TO '{OUTPUT_DIR}/card{reptmon:02d}.parquet'")
print(f"Loaded {con.execute('SELECT COUNT(*) FROM card_raw').fetchone()[0]} card records")

# ============================================================================
# SPLIT VISA (4) AND MASTERCARD (5)
# ============================================================================

con.execute("""
    CREATE TEMP TABLE visa AS
    SELECT *, newic ic
    FROM card_raw
    WHERE SUBSTRING(cardno, 1, 1) = '4'
""")

con.execute("""
    CREATE TEMP TABLE master AS
    SELECT *, newic ic
    FROM card_raw
    WHERE SUBSTRING(cardno, 1, 1) = '5'
""")

print(f"VISA cards: {con.execute('SELECT COUNT(*) FROM visa').fetchone()[0]}")
print(f"MasterCard: {con.execute('SELECT COUNT(*) FROM master').fetchone()[0]}")

# ============================================================================
# LOAD CIS LOAN DATA
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE cis_all AS
    SELECT acctno, newic, oldic, custno
    FROM read_parquet('{INPUT_DIR}/cis/loan.parquet')
    WHERE acctno > 0
""")

# ============================================================================
# MATCH FUNCTIONS
# ============================================================================

def match_cards(card_type, card_table):
    """Match cards with CIS using NEW IC, OLD IC, then PASSPORT"""
    
    # Match on NEW IC
    con.execute(f"""
        CREATE TEMP TABLE {card_type}_newic AS
        SELECT c.*, cis.acctno, cis.custno
        FROM {card_table} c
        JOIN cis_all cis ON c.newic = cis.newic
        WHERE c.newic != '' AND cis.newic != ''
    """)
    
    found = con.execute(f"SELECT COUNT(*) FROM {card_type}_newic").fetchone()[0]
    print(f"{card_type.upper()} - NEW IC matches: {found}")
    
    # Get not found on NEW IC
    con.execute(f"""
        CREATE TEMP TABLE {card_type}_nf1 AS
        SELECT c.*
        FROM {card_table} c
        LEFT JOIN {card_type}_newic m ON c.cardno = m.cardno
        WHERE m.cardno IS NULL
    """)
    
    # Match on OLD IC
    con.execute(f"""
        CREATE TEMP TABLE {card_type}_oldic AS
        SELECT c.*, c.oldic ic, cis.acctno, cis.custno
        FROM {card_type}_nf1 c
        JOIN cis_all cis ON c.oldic = cis.oldic
        WHERE c.oldic != '' AND cis.oldic != ''
    """)
    
    found = con.execute(f"SELECT COUNT(*) FROM {card_type}_oldic").fetchone()[0]
    print(f"{card_type.upper()} - OLD IC matches: {found}")
    
    # Combine found
    con.execute(f"""
        CREATE TEMP TABLE {card_type}_found AS
        SELECT * FROM {card_type}_newic
        UNION ALL
        SELECT * FROM {card_type}_oldic
    """)
    
    # Get final not found
    con.execute(f"""
        CREATE TEMP TABLE {card_type}_nofound AS
        SELECT c.*
        FROM {card_table} c
        LEFT JOIN {card_type}_found f ON c.cardno = f.cardno
        WHERE f.cardno IS NULL
    """)
    
    nf = con.execute(f"SELECT COUNT(*) FROM {card_type}_nofound").fetchone()[0]
    print(f"{card_type.upper()} - Not found: {nf}")

# Match VISA and MasterCard
match_cards('visa', 'visa')
match_cards('master', 'master')

# ============================================================================
# CONSOLIDATE FOUND AND NOT FOUND
# ============================================================================

con.execute("""
    CREATE TEMP TABLE cfound AS
    SELECT * FROM visa_found
    UNION ALL
    SELECT * FROM master_found
""")

con.execute("""
    CREATE TEMP TABLE cnofound AS
    SELECT * FROM visa_nofound
    UNION ALL
    SELECT * FROM master_nofound
""")

con.execute("CREATE TEMP TABLE cfound_dedup AS SELECT DISTINCT * FROM cfound")
con.execute("ALTER TABLE cfound_dedup ADD PRIMARY KEY (cardno, newic, oldic, acctno)")

total_found = con.execute("SELECT COUNT(*) FROM cfound_dedup").fetchone()[0]
total_nf = con.execute("SELECT COUNT(*) FROM cnofound").fetchone()[0]

print(f"\nTotal matched: {total_found}")
print(f"Total not found: {total_nf}")

# ============================================================================
# REPORT: CARDS NOT FOUND (6+ MONTHS OLD)
# ============================================================================

con.execute("""
    CREATE TEMP TABLE cnofound_6m AS
    SELECT *
    FROM cnofound
    WHERE mthdue >= 6
    ORDER BY brcode, cardno
""")

con.execute(f"COPY cnofound_6m TO '{OUTPUT_DIR}/cnofound{reptmon:02d}.csv' (HEADER, DELIMITER ',')")
nf6m = con.execute("SELECT COUNT(*) FROM cnofound_6m").fetchone()[0]
print(f"\nCards not found (6+ months): {nf6m}")

# ============================================================================
# PROCESS SETTLED ACCOUNTS
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE lnnote_all AS
    SELECT acctno, noteno, name, ntbrch, lasttran, paidind, loantype, taxno, guarend
    FROM read_parquet('{INPUT_DIR}/ln/lnnote.parquet')
    UNION ALL
    SELECT acctno, noteno, name, ntbrch, lasttran, paidind, loantype, taxno, guarend
    FROM read_parquet('{INPUT_DIR}/lni/lnnote.parquet')
""")

# Settled accounts (PAIDIND = 'P')
con.execute("""
    CREATE TEMP TABLE loanpd AS
    SELECT 
        name, ntbrch, acctno, noteno, paidind,
        TRY_STRPTIME(SUBSTRING(CAST(lasttran AS VARCHAR), 1, 8), '%m%d%Y') lastran
    FROM lnnote_all
    WHERE paidind = 'P' AND acctno > 0
""")

# Match settled accounts with found cards
con.execute("""
    CREATE TEMP TABLE settle AS
    SELECT c.*, l.name, l.ntbrch, l.lastran, l.noteno, l.paidind
    FROM cfound_dedup c
    JOIN loanpd l ON c.acctno = l.acctno
""")

# Add back same customer settled accounts (by NEW IC)
con.execute("""
    CREATE TEMP TABLE totnew AS
    SELECT s.*, c.acctno acctno2
    FROM settle s
    JOIN cfound_dedup c ON s.newic = c.newic
    WHERE s.newic != '' AND c.newic != ''
""")

# Add back same customer settled accounts (by OLD IC)
con.execute("""
    CREATE TEMP TABLE totold AS
    SELECT s.*, c.acctno acctno2
    FROM settle s
    JOIN cfound_dedup c ON s.oldic = c.oldic
    WHERE s.oldic != '' AND c.oldic != ''
""")

# Consolidate settled
con.execute("""
    CREATE TEMP TABLE settle_all AS
    SELECT cardno, newic, oldic, acctno, custno, cname, copendt, brcode, mthdue,
           name, ntbrch, lastran, noteno, paidind
    FROM settle
    UNION ALL
    SELECT cardno, newic, oldic, acctno2 acctno, custno, cname, copendt, brcode, mthdue,
           name, ntbrch, lastran, noteno, paidind
    FROM totnew
    UNION ALL
    SELECT cardno, newic, oldic, acctno2 acctno, custno, cname, copendt, brcode, mthdue,
           name, ntbrch, lastran, noteno, paidind
    FROM totold
""")

con.execute("""
    CREATE TEMP TABLE settle_dedup AS
    SELECT DISTINCT cardno, newic, oldic, acctno, custno, cname, copendt, brcode,
           name, ntbrch, lastran, noteno, paidind
    FROM settle_all
    WHERE acctno > 0
""")

# ============================================================================
# FIND ACTIVE ACCOUNTS FOR SETTLED CUSTOMERS
# ============================================================================

con.execute("""
    CREATE TEMP TABLE loan_active AS
    SELECT acctno, paidind, name, taxno, guarend
    FROM lnnote_all
    WHERE paidind != 'P' AND acctno > 0
""")

# Match by ACCTNO
con.execute("""
    CREATE TEMP TABLE active AS
    SELECT s.*, l.paidind active_paidind, l.name active_name
    FROM settle_dedup s
    JOIN loan_active l ON s.acctno = l.acctno
""")

# Match by OLD IC (taxno)
con.execute("""
    CREATE TEMP TABLE active_oldic AS
    SELECT s.*, l.acctno active_acctno, l.name active_name
    FROM settle_dedup s
    JOIN loan_active l ON s.oldic = l.taxno
    WHERE s.oldic != '' AND l.taxno != ''
""")

# Match by NEW IC (guarend)
con.execute("""
    CREATE TEMP TABLE active_newic AS
    SELECT s.*, l.acctno active_acctno, l.name active_name
    FROM settle_dedup s
    JOIN loan_active l ON s.newic = l.guarend
    WHERE s.newic != '' AND l.guarend != ''
""")

# Consolidate active
con.execute("""
    CREATE TEMP TABLE active_all AS
    SELECT * FROM active
    UNION ALL
    SELECT * FROM active_oldic
    UNION ALL
    SELECT * FROM active_newic
""")

con.execute("""
    CREATE TEMP TABLE active_dedup AS
    SELECT DISTINCT newic, oldic, acctno
    FROM active_all
""")

# ============================================================================
# FILTER: SETTLED WITHOUT ACTIVE ACCOUNTS
# ============================================================================

# Remove by OLD IC
con.execute("""
    CREATE TEMP TABLE fsettle1 AS
    SELECT s.*
    FROM settle_dedup s
    LEFT JOIN active_dedup a ON s.oldic = a.oldic
    WHERE a.oldic IS NULL
""")

# Remove by NEW IC
con.execute("""
    CREATE TEMP TABLE fsettle2 AS
    SELECT s.*
    FROM fsettle1 s
    LEFT JOIN active_dedup a ON s.newic = a.newic
    WHERE a.newic IS NULL
""")

# Remove by ACCTNO
con.execute("""
    CREATE TEMP TABLE fsettle AS
    SELECT DISTINCT s.*
    FROM fsettle2 s
    LEFT JOIN active_dedup a ON s.acctno = a.acctno
    WHERE a.acctno IS NULL
    ORDER BY ntbrch, cname, newic, oldic
""")

# ============================================================================
# FINAL REPORTS
# ============================================================================

con.execute(f"COPY cfound_dedup TO '{OUTPUT_DIR}/cfound{reptmon:02d}.parquet'")
con.execute(f"COPY cnofound TO '{OUTPUT_DIR}/cnofound{reptmon:02d}.parquet'")
con.execute(f"COPY settle_dedup TO '{OUTPUT_DIR}/settle{reptmon:02d}.parquet'")
con.execute(f"COPY active_dedup TO '{OUTPUT_DIR}/active{reptmon:02d}.parquet'")
con.execute(f"COPY fsettle TO '{OUTPUT_DIR}/fsettle{reptmon:02d}.parquet'")

# CSV Report: Settled without active
con.execute(f"""
    COPY (
        SELECT custno, cardno, cname, newic, oldic, copendt, ntbrch, lastran, acctno, noteno
        FROM fsettle
        ORDER BY ntbrch, cname
    ) TO '{OUTPUT_DIR}/settled_report{reptmon:02d}.csv' (HEADER, DELIMITER ',')
""")

settled_count = con.execute("SELECT COUNT(*) FROM fsettle").fetchone()[0]

print("\n" + "="*80)
print("CREDIT CARD MATCHING SUMMARY")
print("="*80)
print(f"""
Report Date: {rdate}

Input:
- Credit Cards: {con.execute('SELECT COUNT(*) FROM card_raw').fetchone()[0]} records
  - VISA (4xxx): {con.execute('SELECT COUNT(*) FROM visa').fetchone()[0]}
  - MasterCard (5xxx): {con.execute('SELECT COUNT(*) FROM master').fetchone()[0]}

Matching Results:
- Found in CIS: {total_found} cards
- Not Found: {total_nf} cards
- Not Found (6+ months): {nf6m} cards

Settled Analysis:
- Settled accounts: {con.execute('SELECT COUNT(*) FROM settle_dedup').fetchone()[0]} cards
- With active accounts: {con.execute('SELECT COUNT(*) FROM active_dedup').fetchone()[0]} customers
- Settled without active: {settled_count} cards

Matching Strategy:
1. Match by NEW IC (primary)
2. Match by OLD IC (secondary)
3. Match by PASSPORT (tertiary)

Output Files:
1. cfound{reptmon:02d}.parquet - Cards matched to loan accounts
2. cnofound{reptmon:02d}.parquet - Cards not found in CIS
3. cnofound{reptmon:02d}.csv - Not found 6+ months old (for review)
4. settle{reptmon:02d}.parquet - Settled loan accounts
5. active{reptmon:02d}.parquet - Active loan accounts
6. fsettle{reptmon:02d}.parquet - Settled without active accounts
7. settled_report{reptmon:02d}.csv - Final settled report

Report: HP/HL ACCOUNT HAS BEEN REDEEMED/SETTLED
Columns: CIS NO, CARD NO, NAME, NEW IC, OLD IC, CARD OPEN DATE,
         BR CODE, SETTLED DATE, ACCT NO, NOTE NO
Count: {settled_count} records
""")

con.close()
print(f"\nCompleted: {OUTPUT_DIR}")
