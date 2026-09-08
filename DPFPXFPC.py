================================================================================
DETAILED DEBUGGING - COLL FILE STRUCTURE
================================================================================

============================================================
RECORD 1
============================================================

Bytes 140-170:
  Pos 140: 40 ( 64)
  Pos 141: 40 ( 64)
  Pos 142: 40 ( 64)
  Pos 143: 40 ( 64)
  Pos 144: C4 (196)
  Pos 145: 03 (  3)
  Pos 146: 07 (  7)
  Pos 147: 89 (137)
  Pos 148: 59 ( 89)
  Pos 149: 10 ( 16)
  Pos 150: 7F (127)
  Pos 151: C1 (193)
  Pos 152: 03 (  3)
  Pos 153: 07 (  7)
  Pos 154: 89 (137)
  Pos 155: 59 ( 89)
  Pos 156: 10 ( 16)
  Pos 157: 7F (127)
  Pos 158: 09 (  9)
  Pos 159: 14 ( 20)
  Pos 160: 19 ( 25)
  Pos 161: 99 (153)
  Pos 162: 25 ( 37)
  Pos 163: 7C (124)
  Pos 164: 00 (  0)
  Pos 165: 00 (  0)
  Pos 166: 00 (  0)
  Pos 167: 00 (  0)
  Pos 168: 00 (  0)
  Pos 169: 10 ( 16)

Packed decimal values at different positions (searching for ACCTNO starting with 2):

EBCDIC decoded strings (positions 140-170):
  Position 140: 'Diß'
  Position 141: 'Diß"'
  Position 142: 'Diß"A'
  Position 143: 'Diß"A'
  Position 144: 'Diß"A'
  Position 145: 'iß"Ai'
  Position 146: 'iß"Aiß'
  Position 147: 'iß"Aiß'
  Position 148: 'ß"Aiß"'
  Position 149: '"Aiß"'
  Position 150: '"Aiß"'
  Position 150: '¬Ahç/'
  Position 151: 'Ahç/'
  Position 152: 'hç/r'
  Position 153: 'hç/r'
  Position 154: 'hç/r
æ'
  Position 155: 'ç/r
æ'
  Position 156: '/r
æ'
  Position 157: 'r
æ'
  Position 158: r
æ'
  Position 159: r
æ'
  Position 160: 'r
æ'
  Position 161: 'r
æÊ'
  Position 162: 'æÊ'
  Position 163: 'æÊ
                   '
  Position 164: 'Ê
                  '
  Position 165: 'Ê
                  '
  Position 166: 'Ê
                  '
  Position 167: 'Ê
                  '
  Position 168: 'Ê
                  '
  Position 169: 'Ê

                  '


============================================================
RECORD 3
============================================================

Bytes 140-170:
  Pos 140: 40 ( 64)
  Pos 141: 40 ( 64)
  Pos 142: 40 ( 64)
  Pos 143: 40 ( 64)
  Pos 144: C4 (196)
  Pos 145: 03 (  3)
  Pos 146: 09 (  9)
  Pos 147: 31 ( 49)
  Pos 148: 59 ( 89)
  Pos 149: 11 ( 17)
  Pos 150: 5F ( 95)
  Pos 151: C1 (193)
  Pos 152: 03 (  3)
  Pos 153: 09 (  9)
  Pos 154: 31 ( 49)
  Pos 155: 59 ( 89)
  Pos 156: 11 ( 17)
  Pos 157: 5F ( 95)
  Pos 158: 10 ( 16)
  Pos 159: 06 (  6)
  Pos 160: 19 ( 25)
  Pos 161: 99 (153)
  Pos 162: 27 ( 39)
  Pos 163: 9C (156)
  Pos 164: 00 (  0)
  Pos 165: 00 (  0)
  Pos 166: 00 (  0)
  Pos 167: 00 (  0)
  Pos 168: 89 (137)
  Pos 169: 00 (  0)

Packed decimal values at different positions (searching for ACCTNO starting with 2):

EBCDIC decoded strings (positions 140-170):
  Position 140: 'Dß'
  Position 141: 'Dß¬'
  Position 142: 'Dß¬A'
  Position 143: 'Dß¬A'
  Position 144: 'Dß¬A'
  Position 145: 'ß¬A'
  Position 146: 'ß¬Aß'
  Position 147: 'ß¬Aß'
  Position 148: 'ß¬Aß¬'
  Position 149: '¬Aß¬'
  Position 150: '¬Aß¬'
  Position 151: 'Aß¬'
  Position 152: 'ß¬r'
  Position 153: 'ß¬r
osition 154: 'ß¬r'
  Position 155: 'ß¬r'
  Position 156: '¬r'
  Position 157: '¬r'
  Position 158: 'r'
  Position 159: 'ri'
  Position 160: 'ri'
  Position 161: 'riÄ'
  Position 162: 'iÄ'
  Position 163: 'æiÄ
                    '
  Position 164: 'iÄ
                   '
  Position 165: 'iÄ
                   '
  Position 166: 'iÄ
                   '
  Position 167: 'iÄ
                   '
  Position 168: 'iÄ
                   '
  Position 169: 'Ä

                  '


================================================================================
LNNOTE SAMPLE DATA
================================================================================
ACCTNO and NOTENO values:
  Record 0: ACCTNO=2000000125, NOTENO=20010
  Record 1: ACCTNO=2000000319, NOTENO=10
  Record 2: ACCTNO=2000000707, NOTENO=10
  Record 3: ACCTNO=2000000901, NOTENO=12
  Record 4: ACCTNO=2000000901, NOTENO=15
  Record 5: ACCTNO=2000000901, NOTENO=16
  Record 6: ACCTNO=2000000901, NOTENO=10013
  Record 7: ACCTNO=2000000901, NOTENO=20014
  Record 8: ACCTNO=2000001023, NOTENO=11
  Record 9: ACCTNO=2000001023, NOTENO=13

ACCTNO range: 2000000125 to 2000002503
NOTENO range: 10 to 30010

================================================================================
ANALYSIS
================================================================================
COLL file ACCTNO appears to be 3078959107 (10 digits, starts with 3)
LNNOTE ACCTNO range is around 2000000000 (10 digits, starts with 2)

Possible explanations:
  1. COLL ACCTNO uses a different numbering system
  2. COLL ACCTNO might be at a different byte position
  3. There might be a mapping table needed
  4. COLL ACCTNO might be encoded differently (EBCDIC vs binary)

Let's check if 3078959107 in EBCDIC represents '2000000000' or similar...
  3078959107 = 0xB7853003
  2000000000 = 0x77359400

The COLL file might store ACCTNO as EBCDIC characters, not packed decimal!
For example, '2000000000' as EBCDIC would be different bytes than packed decimal

EBCDIC representation check:
  '2000000000' as EBCDIC bytes: f2f0f0f0f0f0f0f0f0f0
  As packed decimal: 0

Trying to read ACCTNO as EBCDIC from COLL file:

================================================================================
DEBUGGING COMPLETE
================================================================================
