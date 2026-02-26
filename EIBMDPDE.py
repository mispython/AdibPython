EIBMDPDE - Deposit Position Date Extraction
============================================================

Reading DPPSTRGS file...
  ✓ DPPSTRGS: Record read (665 bytes)

Extracting TBDATE (packed decimal)...
  Raw bytes: 30 38 9C 9C 41 00
  Binary:    00110000 00111000 10011100 10011100 01000001 00000000
  Byte 0: 30 -> high=3 (0x3), low=0 (0x0)
  Byte 1: 38 -> high=3 (0x3), low=8 (0x8)
  Byte 2: 9C -> high=9 (0x9), low=12 (0xC)
  Byte 3: 9C -> high=9 (0x9), low=12 (0xC)
  Byte 4: 41 -> high=4 (0x4), low=1 (0x1)
  Byte 5: 00 -> high=0 (0x0), low=0 (0x0)
    Sign nibble: 0 (0x0) - unknown
  Extracted digits: 303899410 (length: 9)
  Trying MMDDYYYY (first 8): 30389941
  Trying MMDDYYYY (skip 1): 03899410
  Trying YYYYMMDD (first 8): 30389941
  Trying YYYYMMDD (skip 1): 03899410
  Trying DDMMYYYY (first 8): 30389941
  Trying DDMMYYYY (skip 1): 03899410
  ✗ ERROR: No valid date pattern found in digits: 303899410
  Raw bytes: 30 38 9C 9C 41 00
  This might indicate that position 34 is not the date field, or the date is in a different format
  Please check the file layout for DPPSTRGS to confirm the correct position for TBDATE
