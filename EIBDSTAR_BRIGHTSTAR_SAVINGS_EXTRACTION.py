REPTDATE = 2025-11-26, PREVDATE = 2025-11-25
MON=11, DAY=26, YEAR=25
SAVING.csv shape: (8320, 81)

============================================================
INVESTIGATING OPENDT COLUMN ISSUES
============================================================
OPENDT value analysis (top 20):
shape: (5, 2)
┌────────┬───────┐
│ OPENDT ┆ count │
│ ---    ┆ ---   │
│ str    ┆ u32   │
╞════════╪═══════╡
│ null   ┆ 7579  │
│ G      ┆ 382   │
│ J      ┆ 357   │
│ 1      ┆ 1     │
│ 5      ┆ 1     │
└────────┴───────┘

OPENDT Summary:
  - Null values: 7579
  - 'J' values: 357
  - Numeric values: 2
  - Non-null values: 741

============================================================
PROCESSING STRATEGY: Include all accounts (no date filter)
============================================================
Top 15 PRODUCT values (potential savings accounts):
shape: (15, 4)
┌───────────┬───────┬─────────────┬─────────────┐
│ PRODUCT   ┆ count ┆ avg_balance ┆ max_balance │
│ ---       ┆ ---   ┆ ---         ┆ ---         │
│ str       ┆ u32   ┆ f64         ┆ f64         │
╞═══════════╪═══════╪═════════════╪═════════════╡
│ 111411318 ┆ 139   ┆ 1.143885    ┆ 4.0         │
│ 112625330 ┆ 99    ┆ 2.625       ┆ 4.0         │
│ 32720087  ┆ 83    ┆ 1.1         ┆ 4.0         │
│ 121001344 ┆ 77    ┆ 2.025974    ┆ 4.0         │
│ 12319023  ┆ 40    ┆ 4.0         ┆ 4.0         │
│ …         ┆ …     ┆ …           ┆ …           │
│ 102825301 ┆ 36    ┆ 1.5         ┆ 4.0         │
│ 111925323 ┆ 34    ┆ 1.0         ┆ 1.0         │
│ 12219022  ┆ 33    ┆ null        ┆ null        │
│ 112425328 ┆ 33    ┆ 2.142857    ┆ 4.0         │
│ 111225316 ┆ 32    ┆ null        ┆ null        │
└───────────┴───────┴─────────────┴─────────────┘
Potential savings PRODUCT codes: ['101624290', '22020051', '32019079', '110920314', '51323133', '81022222', '40225092', '110223306', '72214203', '12225022']

Using most common product for processing: 111411318
Accounts selected for processing: 139
CIS shape: (224992, 100)
CIS processed records: 224992
Joined records: 0
Final output records: 0
Output written to /pythonITD/mis_dev/OUTPUT/BRIGHTSTAR_SAVINGS_251126.parquet

============================================================
FINAL SUMMARY AND RECOMMENDATIONS
============================================================
Accounts processed: 0
Output file: /pythonITD/mis_dev/OUTPUT/BRIGHTSTAR_SAVINGS_251126.parquet

❌ CRITICAL ISSUES FOUND:
1. OPENDT column contains no valid date values
2. All values are either NULL or 'J'
3. Cannot filter by opening date as intended

💡 IMMEDIATE ACTIONS NEEDED:
1. Check the source of SAVING.csv - is OPENDT populated correctly?
2. Contact the data provider about the OPENDT format
3. Verify if there's another date column that should be used
4. Check if 'J' has a specific meaning (e.g., Joint account)

📋 DATA QUALITY ISSUES:
  - Total records: 8320
  - Valid OPENDT values: 2
  - 'J' values: 357
  - Null values: 7579
