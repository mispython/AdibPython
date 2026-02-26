============================================================

Reading DPPSTRGS file...
  ✓ DPPSTRGS: Record read (665 bytes)

Extracting TBDATE (packed decimal)...
  Raw bytes: 30 38 9C 9C 41 00
  As big-endian integer: 53019703787776
  Warning: Low nibble 12 at byte 2 is not a digit (sign nibble in middle?)
  Warning: Low nibble 12 at byte 3 is not a digit (sign nibble in middle?)
  Unknown sign (0x0)
  Extracted digits: 303899410 (length: 9)
  TBDATE value: 303899410
  Z11.: 00303899410
  Date string: 00303899
  ✗ ERROR: Could not find valid date format
  Raw bytes: 30 38 9C 9C 41 00
  This might mean position 34 is not the date field

  Checking nearby positions...
    Position 29: 70 C7 40 01 0F 30
    Position 30: C7 40 01 0F 30 38
    Position 31: 40 01 0F 30 38 9C
    Position 32: 01 0F 30 38 9C 9C
    Position 33: 0F 30 38 9C 9C 41
    Position 35: 38 9C 9C 41 00 00
    Position 36: 9C 9C 41 00 00 00
    Position 37: 9C 41 00 00 00 C0
    Position 38: 41 00 00 00 C0 0B
    Position 39: 00 00 00 C0 0B 5A
