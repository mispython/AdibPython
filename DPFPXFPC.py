Reading GL text file...
First line of file: '20260708                                                                        '
Length of first line: 133
Total lines: 74

First 5 lines of data:
Line 1: '20260708                                                                        '
  Length: 133
  Positions:   0  10  20  30  40  50  60  70  80  90 100 110 120 130
  Characters: 2 0 2 6 0 7 0 8                                                                                    ...

Line 2: '1F147600            08/07/26                             0.00                                                                        '
  Length: 133
  Positions:   0  10  20  30  40  50  60  70  80  90 100 110 120 130
  Characters: 1 F 1 4 7 6 0 0                         0 8 / 0 7 / 2 6                                            ...

Line 3: '1F142630C           08/07/26                             0.00                                                                        '
  Length: 133
  Positions:   0  10  20  30  40  50  60  70  80  90 100 110 120 130
  Characters: 1 F 1 4 2 6 3 0 C                       0 8 / 0 7 / 2 6                                            ...

Line 4: '142699              08/07/26                   224,458,779.12-                                                                       '
  Length: 133
  Positions:   0  10  20  30  40  50  60  70  80  90 100 110 120 130
  Characters: 1 4 2 6 9 9                             0 8 / 0 7 / 2 6                                       2 2 4...

Line 5: '144111              08/07/26                 4,997,935,844.48-                                                                       '
  Length: 133
  Positions:   0  10  20  30  40  50  60  70  80  90 100 110 120 130
  Characters: 1 4 4 1 1 1                             0 8 / 0 7 / 2 6                                   4 , 9 9 7...

/usr/lib64/python3.9/functools.py:888: DataOrientationWarning: Row orientation inferred during DataFrame construction. Explicitly specify the orientation by passing `orient="row"` to silence this warning.
  return dispatch(args[0].__class__)(*args, **kw)
Successfully parsed with widths: [2, 2, 2, 20, 1, 15]
First row: ('20', '26', '07', '08', '', '')
Successfully read file using format: fixed-width-[2, 2, 2, 20, 1, 15]

DataFrame shape: (74, 6)
Columns: ['YY', 'MM', 'DD', 'GLITEM', 'SIGN', 'BALANCE']
Data types: [String, String, String, String, String, String]

First few rows:
shape: (10, 6)
┌─────┬─────┬─────┬──────────────────────┬──────┬─────────┐
│ YY  ┆ MM  ┆ DD  ┆ GLITEM               ┆ SIGN ┆ BALANCE │
│ --- ┆ --- ┆ --- ┆ ---                  ┆ ---  ┆ ---     │
│ str ┆ str ┆ str ┆ str                  ┆ str  ┆ str     │
╞═════╪═════╪═════╪══════════════════════╪══════╪═════════╡
│ 20  ┆ 26  ┆ 07  ┆ 08                   ┆      ┆         │
│ 1F  ┆ 14  ┆ 76  ┆ 00            08/07/ ┆ 2    ┆ 6       │
│ 1F  ┆ 14  ┆ 26  ┆ 30C           08/07/ ┆ 2    ┆ 6       │
│ 14  ┆ 26  ┆ 99  ┆ 08/07/               ┆ 2    ┆ 6       │
│ 14  ┆ 41  ┆ 11  ┆ 08/07/               ┆ 2    ┆ 6       │
│ 1F  ┆ 14  ┆ 71  ┆ 00            08/07/ ┆ 2    ┆ 6       │
│ 1F  ┆ 24  ┆ 92  ┆ 99K           08/07/ ┆ 2    ┆ 6       │
│ 14  ┆ 21  ┆ 99  ┆ 08/07/               ┆ 2    ┆ 6       │
│ 1F  ┆ 14  ┆ 46  ┆ 11FXSDC       08/07/ ┆ 2    ┆ 6       │
│ 14  ┆ 91  ┆ 20  ┆ NLF           08/07/ ┆ 2    ┆ 6       │
└─────┴─────┴─────┴──────────────────────┴──────┴─────────┘

Column statistics:
  YY: 74 non-null values, 6 unique
    Values: ['20', '13', '1M', '-\x00', '14', '1F']
  MM: 74 non-null values, 11 unique
  DD: 74 non-null values, 21 unique
  GLITEM: 74 non-null values, 55 unique
  SIGN: 74 non-null values, 3 unique
    Values: ['\x00', '', '2']
  BALANCE: 74 non-null values, 3 unique
    Values: ['', '6', '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00']

Attempting to reconstruct GLITEM from data...
Converted BALANCE to numeric

Cleaned data:
shape: (10, 6)
┌─────┬─────┬─────┬────────┬──────┬─────────┐
│ YY  ┆ MM  ┆ DD  ┆ GLITEM ┆ SIGN ┆ BALANCE │
│ --- ┆ --- ┆ --- ┆ ---    ┆ ---  ┆ ---     │
│ str ┆ str ┆ str ┆ str    ┆ str  ┆ f64     │
╞═════╪═════╪═════╪════════╪══════╪═════════╡
│ 20  ┆ 26  ┆ 07  ┆ 202607 ┆      ┆ null    │
│ 1F  ┆ 14  ┆ 76  ┆ 1F1476 ┆ 2    ┆ 6.0     │
│ 1F  ┆ 14  ┆ 26  ┆ 1F1426 ┆ 2    ┆ 6.0     │
│ 14  ┆ 26  ┆ 99  ┆ 142699 ┆ 2    ┆ 6.0     │
│ 14  ┆ 41  ┆ 11  ┆ 144111 ┆ 2    ┆ 6.0     │
│ 1F  ┆ 14  ┆ 71  ┆ 1F1471 ┆ 2    ┆ 6.0     │
│ 1F  ┆ 24  ┆ 92  ┆ 1F2492 ┆ 2    ┆ 6.0     │
│ 14  ┆ 21  ┆ 99  ┆ 142199 ┆ 2    ┆ 6.0     │
│ 1F  ┆ 14  ┆ 46  ┆ 1F1446 ┆ 2    ┆ 6.0     │
│ 14  ┆ 91  ┆ 20  ┆ 149120 ┆ 2    ┆ 6.0     │
└─────┴─────┴─────┴────────┴──────┴─────────┘

============================================================
Processing GL P1...
============================================================

============================================================
Processing GL P2...
============================================================

============================================================
Processing complete!
============================================================
