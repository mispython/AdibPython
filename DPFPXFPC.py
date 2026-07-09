============================================================
EIVD GL PROCESSING STARTED (EIVDNLGL)
============================================================
Processing date: 2026-07-08
Store directory: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIVDNLGL
============================================================
Successfully read file with encoding: ascii
Total lines: 9

First 5 lines of data:
Line 1: '20260708                                                                        '
  Positions 0-8 (GLITEM): '20260708'
  Positions 20-28 (DATEX): ''
  Positions 45-60 (BALANCE): ''

Line 2: '1S-RCF              08/07/26                    36,353,900.00                                                                        '
  Positions 0-8 (GLITEM): '1S-RCF'
  Positions 20-28 (DATEX): '08/07/26'
  Positions 45-60 (BALANCE): '36,353,900.0'

Line 3: '1S-TLF              08/07/26                   250,737,245.49                                                                        '
  Positions 0-8 (GLITEM): '1S-TLF'
  Positions 20-28 (DATEX): '08/07/26'
  Positions 45-60 (BALANCE): '250,737,245.4'

Line 4: '1S-BA F             08/07/26                     4,353,267.90                                                                        '
  Positions 0-8 (GLITEM): '1S-BA F'
  Positions 20-28 (DATEX): '08/07/26'
  Positions 45-60 (BALANCE): '4,353,267.9'

Line 5: '1S-SM F             08/07/26                             0.00                                                                        '
  Positions 0-8 (GLITEM): '1S-SM F'
  Positions 20-28 (DATEX): '08/07/26'
  Positions 45-60 (BALANCE): '0.0'

Header date found: 20260708

Parsed 7 rows from GL file
Columns: ['GLITEM', 'DATEX', 'BALANCE', 'SIGN']

Data sample:
shape: (7, 4)
┌──────────┬────────┬───────────┬──────┐
│ GLITEM   ┆ DATEX  ┆ BALANCE   ┆ SIGN │
│ ---      ┆ ---    ┆ ---       ┆ ---  │
│ str      ┆ str    ┆ f64       ┆ str  │
╞══════════╪════════╪═══════════╪══════╡
│ 1S-RCF   ┆ 080726 ┆ 3.63539e7 ┆      │
│ 1S-TLF   ┆ 080726 ┆ 2.5074e8  ┆      │
│ 1S-BA F  ┆ 080726 ┆ 4353267.9 ┆      │
│ 1S-SM F  ┆ 080726 ┆ 0.0       ┆      │
│ 1S-GUARA ┆ 080726 ┆ 5.7e6     ┆      │
│ 1S-REMIS ┆ 080726 ┆ 0.0       ┆      │
│ 1S-FIXED ┆ 080726 ┆ 0.0       ┆      │
└──────────┴────────┴───────────┴──────┘

Unique GLITEMs in file (7):
  '1S-BA F'
  '1S-FIXED'
  '1S-GUARA'
  '1S-RCF'
  '1S-REMIS'
  '1S-SM F'
  '1S-TLF'

Balance summary:
297144413.29999995

GL Date from file: 080726
REPT Date: 080726

Creating output files in: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIVDNLGL

Summary data:
shape: (3, 8)
┌───────┬────────┬───────┬─────┬────────┬──────┬──────┬─────────┐
│ ITEM  ┆ WEEK   ┆ MONTH ┆ QTR ┆ HALFYR ┆ YEAR ┆ LAST ┆ BALANCE │
│ ---   ┆ ---    ┆ ---   ┆ --- ┆ ---    ┆ ---  ┆ ---  ┆ ---     │
│ str   ┆ f64    ┆ f64   ┆ f64 ┆ f64    ┆ f64  ┆ f64  ┆ f64     │
╞═══════╪════════╪═══════╪═════╪════════╪══════╪══════╪═════════╡
│ A1.35 ┆ 7.271  ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 7.271   │
│ A1.37 ┆ 0.0    ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 0.0     │
│ A1.38 ┆ 51.018 ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 51.018  │
└───────┴────────┴───────┴─────┴────────┴──────┴──────┴─────────┘
✓ Saved: GLRMP120260708.parquet
  Rows: 3
shape: (3, 8)
┌───────┬────────┬───────┬─────┬────────┬──────┬──────┬─────────┐
│ ITEM  ┆ WEEK   ┆ MONTH ┆ QTR ┆ HALFYR ┆ YEAR ┆ LAST ┆ BALANCE │
│ ---   ┆ ---    ┆ ---   ┆ --- ┆ ---    ┆ ---  ┆ ---  ┆ ---     │
│ str   ┆ f64    ┆ f64   ┆ f64 ┆ f64    ┆ f64  ┆ f64  ┆ f64     │
╞═══════╪════════╪═══════╪═════╪════════╪══════╪══════╪═════════╡
│ A1.35 ┆ 7.271  ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 7.271   │
│ A1.37 ┆ 0.0    ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 0.0     │
│ A1.38 ┆ 51.018 ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 51.018  │
└───────┴────────┴───────┴─────┴────────┴──────┴──────┴─────────┘

============================================================
EIVD PROCESSING COMPLETE!
============================================================

Output files saved to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIVDNLGL

✓ 1 parquet files created:
  • GLRMP120260708.parquet (2,627 bytes)

============================================================
