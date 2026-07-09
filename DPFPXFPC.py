Reading GL text file...
First line of file: 20260708                                                                        ...
Length of first line: 133
Could not detect delimiter, trying fixed-width format
/usr/lib64/python3.9/functools.py:888: DataOrientationWarning: Row orientation inferred during DataFrame construction. Explicitly specify the orientation by passing `orient="row"` to silence this warning.
  return dispatch(args[0].__class__)(*args, **kw)
Successfully read file using format: fixed-width

DataFrame shape: (74, 7)
Columns: ['YY', 'MM', 'DD', 'DATE', 'GLITEM', 'SIGN', 'BALANCE']
First few rows:
shape: (5, 7)
┌─────┬─────┬─────┬──────┬──────────┬──────┬─────────┐
│ YY  ┆ MM  ┆ DD  ┆ DATE ┆ GLITEM   ┆ SIGN ┆ BALANCE │
│ --- ┆ --- ┆ --- ┆ ---  ┆ ---      ┆ ---  ┆ ---     │
│ str ┆ str ┆ str ┆ str  ┆ str      ┆ str  ┆ str     │
╞═════╪═════╪═════╪══════╪══════════╪══════╪═════════╡
│ 20  ┆ 26  ┆ 07  ┆ 08   ┆          ┆      ┆         │
│ 1F  ┆ 14  ┆ 76  ┆ 00   ┆ 08/07/26 ┆      ┆         │
│ 1F  ┆ 14  ┆ 26  ┆ 30C  ┆ 08/07/26 ┆      ┆         │
│ 14  ┆ 26  ┆ 99  ┆      ┆ 08/07/26 ┆      ┆ 224     │
│ 14  ┆ 41  ┆ 11  ┆      ┆ 08/07/26 ┆      ┆ 4,997   │
└─────┴─────┴─────┴──────┴──────────┴──────┴─────────┘

Attempting to identify columns...
Column 'YY' might be BALANCE
Column 'MM' might be BALANCE
Column 'DD' might be BALANCE
Column 'DATE' might be BALANCE
Column 'GLITEM' might be DATE
Column 'BALANCE' might be GLITEM
Column 'BALANCE' might be BALANCE

Renamed columns to: ['YY', 'MM', 'DD', 'DATE', 'GLITEM', 'SIGN', 'BALANCE']
Error parsing date from file: month must be in 1..12
Using date: 080726
Applied sign adjustment to BALANCE

============================================================
Processing GL P1...
============================================================

============================================================
Processing GL P2...
============================================================

============================================================
Processing complete!
============================================================
