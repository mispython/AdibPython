============================================================
Processing date: 2026-08-31 (SAS date: 24349)
============================================================

============================================================
DEBUGGING COLL FILE FORMAT
============================================================
Record length: 380

First 20 bytes: 00033f00000000133fc403078959107ff2f9f7f6

Bytes 140-160 (around ACCTNO and NOTENO):
  Position 140: 40 ( 64)
  Position 141: 40 ( 64)
  Position 142: 40 ( 64)
  Position 143: 40 ( 64)
  Position 144: C4 (196)
  Position 145: 03 (  3)
  Position 146: 07 (  7)
  Position 147: 89 (137)
  Position 148: 59 ( 89)
  Position 149: 10 ( 16)
  Position 150: 7F (127)
  Position 151: C1 (193)
  Position 152: 03 (  3)
  Position 153: 07 (  7)
  Position 154: 89 (137)
  Position 155: 59 ( 89)
  Position 156: 10 ( 16)
  Position 157: 7F (127)
  Position 158: 09 (  9)
  Position 159: 14 ( 20)

Trying different ACCTNO interpretations (position 145, 6 bytes):
  Raw hex: 03078959107f
  Packed decimal: 3078959107
  Big-endian int: 3330903969919
  Little-endian int: 139708198356739

Looking for ACCTNO pattern (starts with 2)...

LNNOTE account numbers (from earlier):
  2.0000e9 = 2000000000
  2.0001e9 = 2000100000

============================================================
STEP 1: Reading LNNOTE (using LOANTYPE=570)
============================================================
Filtered LNNOTE rows: 122

LNNOTE ACCTNO and NOTENO values:
shape: (10, 2)
┌──────────┬─────────┐
│ ACCTNO   ┆ NOTENO  │
│ ---      ┆ ---     │
│ f64      ┆ f64     │
╞══════════╪═════════╡
│ 2.0000e9 ┆ 10.0    │
│ 2.0001e9 ┆ 10.0    │
│ 2.0001e9 ┆ 30010.0 │
│ 2.0001e9 ┆ 10.0    │
│ 2.0001e9 ┆ 10.0    │
│ 2.0001e9 ┆ 10.0    │
│ 2.0001e9 ┆ 10.0    │
│ 2.0001e9 ┆ 10.0    │
│ 2.0001e9 ┆ 20010.0 │
│ 2.0001e9 ┆ 11.0    │
└──────────┴─────────┘

LOAN0: 116 rows, LOAN1: 6 rows

============================================================
Processing complete!
============================================================
