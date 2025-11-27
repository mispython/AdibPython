REPTDATE = 2025-11-26, PREVDATE = 2025-11-25
MON=11, DAY=26, YEAR=25
SAVING.csv shape: (8320, 81)
CIS shape: (224992, 100)

============================================================
INVESTIGATING ACCOUNT NUMBER MATCHING ISSUE
============================================================
SAVING ACCTNO sample:
shape: (10, 1)
┌────────┐
│ ACCTNO │
│ ---    │
│ str    │
╞════════╡
│ 1      │
│ 1      │
│ 1      │
│ 1      │
│ 1      │
│ 1      │
│ 1      │
│ 1      │
│ 1      │
│ 1      │
└────────┘

CIS ACCTNO sample:
shape: (10, 1)
┌──────────────────┐
│ ACCTNO           │
│ ---              │
│ str              │
╞══════════════════╡
│ 01950002820      │
│ 5472310900369100 │
│ 02000031427      │
│ 03000940936      │
│ 5472310900359101 │
│ 03800000232      │
│ 02468394422      │
│ 03001642036      │
│ 03098967635      │
│ 01950027005      │
└──────────────────┘

Common ACCTNOs found: 0
❌ NO COMMON ACCOUNT NUMBERS FOUND!

CIS columns that might contain account numbers:
  CUSTNO: ['00000000001', '00000000001', '00000000001']
  BANKNO: [33.0, 33.0, 33.0]
  ACCTNOC: ['01950002820', '5472310900369100', '02000031427']
  ACCTNO: ['01950002820', '5472310900369100', '02000031427']
  ACCTCODE: ['DP', 'BMG', 'LN']
  CUSTSTAT: ['O', 'O', 'O']
  CUSTBRCH: [37.0, 37.0, 37.0]
  CUSTLASTDATECC: ['20', '20', '20']
  CUSTLASTDATEYY: ['25', '25', '25']
  CUSTLASTDATEMM: ['11', '11', '11']
  CUSTLASTDATEDD: ['17', '17', '17']
  CUSTLASTOPER: ['BDSBAZA', 'BDSBAZA', 'BDSBAZA']
  CUSTSINCEDATE: [12011996336.0, 12011996336.0, 12011996336.0]
  CUSTOPENDATE: [1161986016.0, 1161986016.0, 1161986016.0]
  CUST_CODE: ['012', '012', '012']
  CUSTCONSENT: ['002', '002', '002']
  CUSTMNTDATE: ['20251117', '20251117', '20251117']
  CUSTNAME: ['YAP YIP SENG', 'YAP YIP SENG', 'YAP YIP SENG']

============================================================
TRYING DIFFERENT JOIN STRATEGIES
============================================================
Strategy 1: Direct ACCTNO join
  Results: 0 records
Strategy 2: Join with ACCTNOC
  Results: 0 records
Strategy 4: Check for partial matches
  SAVING ACCTNO lengths: [1, 2]
  CIS ACCTNO lengths: [None, 11, 14, 16, 19]

============================================================
PROCESSING WITH BEST AVAILABLE DATA
============================================================
Processed SAVING accounts: 7875
Relaxed join results: 0

Final output records: 7875
Output written to /pythonITD/mis_dev/OUTPUT/BRIGHTSTAR_SAVINGS_251126.parquet

============================================================
DATA QUALITY ASSESSMENT
============================================================
SAVING DATA ISSUES:
  - Total records: 8320
  - OPENDT: 741 non-null, but only 2 valid dates
  - PRODUCT: 1866 unique values
  - ACCTNO: 2 unique values

CIS DATA ISSUES:
  - Total records: 224992
  - ACCTNO: 116557 unique values

MATCHING ISSUES:
  - Common ACCTNOs: 0
  - Direct join results: 0

============================================================
RECOMMENDATIONS
============================================================
🚨 CRITICAL: Account numbers don't match between SAVING and CIS files!
   Possible causes:
   1. Different account number formats
   2. Different data sources or time periods
   3. Account number transformation issues
   4. Missing leading zeros or formatting differences

🔧 Required fixes:
   1. Check account number formats in both files
   2. Verify both files are from the same time period
   3. Check for leading zeros or formatting differences
   4. Contact data providers about the mismatch
