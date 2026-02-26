EIBMDPDE - Deposit Position Date Extraction
============================================================

Reading DPPSTRGS file...
  ✓ DPPSTRGS: Record read (665 bytes)

Extracting TBDATE (packed decimal)...
  Raw bytes: 30 38 9C 9C 41 00
  Warning: Non-digit low nibble 12 at byte 2 (sign nibble in middle?)
  Warning: Non-digit low nibble 12 at byte 3 (sign nibble in middle?)
  Sign nibble: 0 (0x0)
  Packed decimal value: 303899410
  ✗ ERROR: Cannot extract/parse TBDATE: date value out of range
  Please verify the packed decimal format and position
