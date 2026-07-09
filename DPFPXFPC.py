Reading GL text file with encoding detection...
Detected encoding: ascii
Successfully read file with encoding: ascii

Successfully read 74 lines with encoding: ascii

Parsing 74 lines...

First 5 lines after decoding:
Line 1: '20260708                                                                        '
  Length: 133
  Hex (first 20 chars): 32 30 32 36 30 37 30 38 20 20 20 20 20 20 20 20 20 20 20 20

Line 2: '1F147600            08/07/26                             0.00                                                                        '
  Length: 133
  Hex (first 20 chars): 31 46 31 34 37 36 30 30 20 20 20 20 20 20 20 20 20 20 20 20

Line 3: '1F142630C           08/07/26                             0.00                                                                        '
  Length: 133
  Hex (first 20 chars): 31 46 31 34 32 36 33 30 43 20 20 20 20 20 20 20 20 20 20 20

Line 4: '142699              08/07/26                   224,458,779.12-                                                                       '
  Length: 133
  Hex (first 20 chars): 31 34 32 36 39 39 20 20 20 20 20 20 20 20 20 20 20 20 20 20

Line 5: '144111              08/07/26                 4,997,935,844.48-                                                                       '
  Length: 133
  Hex (first 20 chars): 31 34 34 31 31 31 20 20 20 20 20 20 20 20 20 20 20 20 20 20

Found header date: 20260708

Parsed 67 rows of data
Columns: ['YY', 'MM', 'DD', 'GLITEM', 'DATE', 'BALANCE', 'SIGN']

Data sample:
shape: (10, 7)
┌─────┬─────┬─────┬─────────┬──────────┬───────────┬──────┐
│ YY  ┆ MM  ┆ DD  ┆ GLITEM  ┆ DATE     ┆ BALANCE   ┆ SIGN │
│ --- ┆ --- ┆ --- ┆ ---     ┆ ---      ┆ ---       ┆ ---  │
│ str ┆ str ┆ str ┆ str     ┆ str      ┆ f64       ┆ str  │
╞═════╪═════╪═════╪═════════╪══════════╪═══════════╪══════╡
│ 20  ┆ 26  ┆ 07  ┆ 1F14760 ┆ 08/07/26 ┆ 0.0       ┆      │
│ 20  ┆ 26  ┆ 07  ┆ 1F14263 ┆ 08/07/26 ┆ 0.0       ┆      │
│ 20  ┆ 26  ┆ 07  ┆ 142699  ┆ 08/07/26 ┆ -2.2446e8 ┆ -    │
│ 20  ┆ 26  ┆ 07  ┆ 144111  ┆ 08/07/26 ┆ -4.9979e9 ┆ -    │
│ 20  ┆ 26  ┆ 07  ┆ 1F14710 ┆ 08/07/26 ┆ -0.0      ┆ -    │
│ 20  ┆ 26  ┆ 07  ┆ 1F24929 ┆ 08/07/26 ┆ -9.0      ┆ -    │
│ 20  ┆ 26  ┆ 07  ┆ 142199  ┆ 08/07/26 ┆ -3.7929e7 ┆ -    │
│ 20  ┆ 26  ┆ 07  ┆ 1F14461 ┆ 08/07/26 ┆ 1.0       ┆      │
│ 20  ┆ 26  ┆ 07  ┆ 149120N ┆ 08/07/26 ┆ -2.2252e8 ┆ -    │
│ 20  ┆ 26  ┆ 07  ┆ 1F13460 ┆ 08/07/26 ┆ 64.0      ┆      │
└─────┴─────┴─────┴─────────┴──────────┴───────────┴──────┘

Unique GLITEMs in file (45):
  132110
  134200
  137070
  139110
  142199
  142699
  142699S
  142699U
  144111
  149120N
  1F13211
  1F13212
  1F13311
  1F13313
  1F13314
  1F13362
  1F13364
  1F13460
  1F13701
  1F13761
  ... and 25 more

============================================================
Processing GL P1...
============================================================

Processing P1...
DataFrame shape: (67, 7)

Unique GLITEMs in file (45):
  132110
  134200
  137070
  139110
  142199
  142699
  142699S
  142699U
  144111
  149120N
  1F13211
  1F13212
  1F13311
  1F13313
  1F13314
  1F13362
  1F13364
  1F13460
  1F13701
  1F13761
  1F13765
  1F13911
  1F13961
  1F14130
  1F14219
  1F14251
  1F14259
  1F14260
  1F14263
  1F14269
  1F14311
  1F14312
  1F14313
  1F14362
  1F14411
  1F14414
  1F14461
  1F14710
  1F14760
  1F24761
  1F24912
  1F24923
  1F24929
  34111
  34170
Matched: '1F14760' -> 'F147600' (B1.18)
Matched: '1F14362' -> 'F143620FNFBI' (B2.21)
Matched: '1F14311' -> 'F143110VCB' (A2.21)
Matched: '1F24929' -> 'F249299K' (A1.20)
Matched: '149120N' -> '49120' (A1.20)
Matched: '1F14710' -> 'F147100' (A1.18)
Matched: '1F14312' -> 'F143120ODNVB' (A2.21)
Matched: '1F13765' -> 'F137650FXCDS' (B2.08)
Matched: '142699' -> '42699' (B1.14)
Matched: '137070' -> '37070' (A2.08)
Matched: '1F13311' -> 'F133110ODVIB' (A2.01)
Matched: '1F13362' -> 'F133620FNFBI' (B2.01)
Matched: '142699S' -> '42699' (B1.14)
Matched: '142699U' -> '42699' (B1.14)
Matched: '1F14263' -> 'F142630C' (B1.12)
Matched: '1F13761' -> 'F137610FXSH' (B2.08)
Matched: '144111' -> '44111' (A1.18)
Matched: '142199' -> '42199' (A1.20)
Matched: '1F13212' -> 'F132121BBNM' (A2.01)
Matched: '1F14461' -> 'F144611FXSDC' (B1.18)
Created DataFrame with 20 rows for P1

Processed data for P1:
shape: (11, 9)
┌───────┬───────┬───────┬─────┬───┬──────┬─────────────┬─────────────┬─────────────┐
│ ITEM  ┆ WEEK  ┆ MONTH ┆ QTR ┆ … ┆ YEAR ┆ LAST        ┆ TOTAL       ┆ BALANCE     │
│ ---   ┆ ---   ┆ ---   ┆ --- ┆   ┆ ---  ┆ ---         ┆ ---         ┆ ---         │
│ str   ┆ f64   ┆ f64   ┆ f64 ┆   ┆ f64  ┆ f64         ┆ f64         ┆ f64         │
╞═══════╪═══════╪═══════╪═════╪═══╪══════╪═════════════╪═════════════╪═════════════╡
│ B2.21 ┆ 0.0   ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0         ┆ 0.0         ┆ 0.0         │
│ B2.01 ┆ 0.0   ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0         ┆ 0.0         ┆ 0.0         │
│ A2.21 ┆ 0.0   ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0         ┆ 0.0         ┆ 0.0         │
│ A2.08 ┆ 0.0   ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0         ┆ 0.0         ┆ 0.0         │
│ B1.12 ┆ 0.0   ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0         ┆ 0.0         ┆ 0.0         │
│ …     ┆ …     ┆ …     ┆ …   ┆ … ┆ …    ┆ …           ┆ …           ┆ …           │
│ A1.20 ┆ 0.0   ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0         ┆ -260449.742 ┆ -260449.742 │
│ B1.18 ┆ 0.001 ┆ 0.001 ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0         ┆ 0.0         ┆ 0.001       │
│ A1.18 ┆ 0.0   ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0         ┆ -4.9979e6   ┆ -4.9979e6   │
│ B2.08 ┆ 0.0   ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0         ┆ 0.0         ┆ 0.0         │
│ B1.14 ┆ 0.0   ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ -428093.697 ┆ 0.0         ┆ -428093.697 │
└───────┴───────┴───────┴─────┴───┴──────┴─────────────┴─────────────┴─────────────┘
Saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGLGLRMP120260708.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 275176

Error creating SAS dataset GLRMP120260708: sasdata() got multiple values for argument 'table'

GLRMP120260708:
shape: (2, 9)
┌───────┬──────┬───────┬─────┬───┬──────┬──────┬─────────────┬─────────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ … ┆ YEAR ┆ LAST ┆ TOTAL       ┆ BALANCE     │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆   ┆ ---  ┆ ---  ┆ ---         ┆ ---         │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆   ┆ f64  ┆ f64  ┆ f64         ┆ f64         │
╞═══════╪══════╪═══════╪═════╪═══╪══════╪══════╪═════════════╪═════════════╡
│ A1.20 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ -260449.742 ┆ -260449.742 │
│ A1.18 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ -4.9979e6   ┆ -4.9979e6   │
└───────┴──────┴───────┴─────┴───┴──────┴──────┴─────────────┴─────────────┘
Saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGLGLFXP120260708.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 275200

Error creating SAS dataset GLFXP120260708: sasdata() got multiple values for argument 'table'

GLFXP120260708:
shape: (1, 9)
┌───────┬───────┬───────┬─────┬───┬──────┬──────┬───────┬─────────┐
│ ITEM  ┆ WEEK  ┆ MONTH ┆ QTR ┆ … ┆ YEAR ┆ LAST ┆ TOTAL ┆ BALANCE │
│ ---   ┆ ---   ┆ ---   ┆ --- ┆   ┆ ---  ┆ ---  ┆ ---   ┆ ---     │
│ str   ┆ f64   ┆ f64   ┆ f64 ┆   ┆ f64  ┆ f64  ┆ f64   ┆ f64     │
╞═══════╪═══════╪═══════╪═════╪═══╪══════╪══════╪═══════╪═════════╡
│ B1.18 ┆ 0.001 ┆ 0.001 ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0   ┆ 0.001   │
└───────┴───────┴───────┴─────┴───┴──────┴──────┴───────┴─────────┘
Saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGLGLRMFXP120260708.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 275220

Error creating SAS dataset GLRMFXP120260708: sasdata() got multiple values for argument 'table'

GLRMFXP120260708:
shape: (2, 9)
┌───────┬──────┬───────┬─────┬───┬──────┬─────────────┬───────┬─────────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ … ┆ YEAR ┆ LAST        ┆ TOTAL ┆ BALANCE     │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆   ┆ ---  ┆ ---         ┆ ---   ┆ ---         │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆   ┆ f64  ┆ f64         ┆ f64   ┆ f64         │
╞═══════╪══════╪═══════╪═════╪═══╪══════╪═════════════╪═══════╪═════════════╡
│ B1.12 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0         ┆ 0.0   ┆ 0.0         │
│ B1.14 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ -428093.697 ┆ 0.0   ┆ -428093.697 │
└───────┴──────┴───────┴─────┴───┴──────┴─────────────┴───────┴─────────────┘
Saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGLGLUTRMP120260708.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 275240

Error creating SAS dataset GLUTRMP120260708: sasdata() got multiple values for argument 'table'

GLUTRMP120260708:
shape: (3, 9)
┌───────┬──────┬───────┬─────┬───┬──────┬──────┬───────┬─────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ … ┆ YEAR ┆ LAST ┆ TOTAL ┆ BALANCE │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆   ┆ ---  ┆ ---  ┆ ---   ┆ ---     │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆   ┆ f64  ┆ f64  ┆ f64   ┆ f64     │
╞═══════╪══════╪═══════╪═════╪═══╪══════╪══════╪═══════╪═════════╡
│ A2.21 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0   ┆ 0.0     │
│ A2.08 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0   ┆ 0.0     │
│ A2.01 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.001 ┆ 0.001   │
└───────┴──────┴───────┴─────┴───┴──────┴──────┴───────┴─────────┘
Saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGLGLUTFXP120260708.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 275259

Error creating SAS dataset GLUTFXP120260708: sasdata() got multiple values for argument 'table'

GLUTFXP120260708:
shape: (3, 9)
┌───────┬──────┬───────┬─────┬───┬──────┬──────┬───────┬─────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ … ┆ YEAR ┆ LAST ┆ TOTAL ┆ BALANCE │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆   ┆ ---  ┆ ---  ┆ ---   ┆ ---     │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆   ┆ f64  ┆ f64  ┆ f64   ┆ f64     │
╞═══════╪══════╪═══════╪═════╪═══╪══════╪══════╪═══════╪═════════╡
│ B2.21 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0   ┆ 0.0     │
│ B2.01 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0   ┆ 0.0     │
│ B2.08 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0   ┆ 0.0     │
└───────┴──────┴───────┴─────┴───┴──────┴──────┴───────┴─────────┘

============================================================
Processing GL P2...
============================================================

Processing P2...
DataFrame shape: (67, 7)

Unique GLITEMs in file (45):
  132110
  134200
  137070
  139110
  142199
  142699
  142699S
  142699U
  144111
  149120N
  1F13211
  1F13212
  1F13311
  1F13313
  1F13314
  1F13362
  1F13364
  1F13460
  1F13701
  1F13761
  1F13765
  1F13911
  1F13961
  1F14130
  1F14219
  1F14251
  1F14259
  1F14260
  1F14263
  1F14269
  1F14311
  1F14312
  1F14313
  1F14362
  1F14411
  1F14414
  1F14461
  1F14710
  1F14760
  1F24761
  1F24912
  1F24923
  1F24929
  34111
  34170
Matched: '1F14461' -> 'F144611FXSDC' (B1.18)
Matched: '142699U' -> '42699' (B1.14)
Matched: '1F13761' -> 'F137610FXSH' (B2.08)
Matched: '1F24929' -> 'F249299K' (A1.20)
Matched: '142199' -> '42199' (A1.20)
Matched: '1F13362' -> 'F133620FNFBI' (B2.01)
Matched: '142699' -> '42699' (B1.14)
Matched: '1F14710' -> 'F147100' (A1.18)
Matched: '142699S' -> '42699' (B1.14)
Matched: '1F14263' -> 'F142630C' (B1.12)
Matched: '144111' -> '44111' (A1.18)
Matched: '1F13765' -> 'F137650FXCDS' (B2.08)
Matched: '1F13212' -> 'F132121BBNM' (A2.01)
Matched: '1F14311' -> 'F143110VCB' (A2.21)
Matched: '1F14760' -> 'F147600' (B1.18)
Matched: '149120N' -> '49120' (A1.20)
Matched: '1F14362' -> 'F143620FNFBI' (B2.21)
Matched: '1F14312' -> 'F143120ODNVB' (A2.21)
Matched: '137070' -> '37070' (A2.08)
Matched: '1F13311' -> 'F133110ODVIB' (A2.01)
Created DataFrame with 20 rows for P2

Processed data for P2:
shape: (11, 9)
┌───────┬───────┬───────┬─────┬───┬──────┬─────────────┬───────────┬─────────────┐
│ ITEM  ┆ WEEK  ┆ MONTH ┆ QTR ┆ … ┆ YEAR ┆ LAST        ┆ TOTAL     ┆ BALANCE     │
│ ---   ┆ ---   ┆ ---   ┆ --- ┆   ┆ ---  ┆ ---         ┆ ---       ┆ ---         │
│ str   ┆ f64   ┆ f64   ┆ f64 ┆   ┆ f64  ┆ f64         ┆ f64       ┆ f64         │
╞═══════╪═══════╪═══════╪═════╪═══╪══════╪═════════════╪═══════════╪═════════════╡
│ B1.18 ┆ 0.001 ┆ 0.001 ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0         ┆ 0.0       ┆ 0.001       │
│ A2.01 ┆ 0.0   ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0         ┆ 0.001     ┆ 0.001       │
│ B1.14 ┆ 0.0   ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ -428093.697 ┆ 0.0       ┆ -428093.697 │
│ A2.08 ┆ 0.0   ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0         ┆ 0.0       ┆ 0.0         │
│ B2.21 ┆ 0.0   ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0         ┆ 0.0       ┆ 0.0         │
│ …     ┆ …     ┆ …     ┆ …   ┆ … ┆ …    ┆ …           ┆ …         ┆ …           │
│ B1.12 ┆ 0.0   ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0         ┆ 0.0       ┆ 0.0         │
│ B2.01 ┆ 0.0   ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0         ┆ 0.0       ┆ 0.0         │
│ B2.08 ┆ 0.0   ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0         ┆ 0.0       ┆ 0.0         │
│ A1.18 ┆ 0.0   ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0         ┆ -4.9979e6 ┆ -4.9979e6   │
│ A2.21 ┆ 0.0   ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0         ┆ 0.0       ┆ 0.0         │
└───────┴───────┴───────┴─────┴───┴──────┴─────────────┴───────────┴─────────────┘
Saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGLGLRMP220260708.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 275278

Error creating SAS dataset GLRMP220260708: sasdata() got multiple values for argument 'table'

GLRMP220260708:
shape: (2, 9)
┌───────┬──────┬───────┬─────┬───┬──────┬──────┬─────────────┬─────────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ … ┆ YEAR ┆ LAST ┆ TOTAL       ┆ BALANCE     │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆   ┆ ---  ┆ ---  ┆ ---         ┆ ---         │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆   ┆ f64  ┆ f64  ┆ f64         ┆ f64         │
╞═══════╪══════╪═══════╪═════╪═══╪══════╪══════╪═════════════╪═════════════╡
│ A1.20 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ -260449.742 ┆ -260449.742 │
│ A1.18 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ -4.9979e6   ┆ -4.9979e6   │
└───────┴──────┴───────┴─────┴───┴──────┴──────┴─────────────┴─────────────┘
Saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGLGLFXP220260708.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 275298

Error creating SAS dataset GLFXP220260708: sasdata() got multiple values for argument 'table'

GLFXP220260708:
shape: (1, 9)
┌───────┬───────┬───────┬─────┬───┬──────┬──────┬───────┬─────────┐
│ ITEM  ┆ WEEK  ┆ MONTH ┆ QTR ┆ … ┆ YEAR ┆ LAST ┆ TOTAL ┆ BALANCE │
│ ---   ┆ ---   ┆ ---   ┆ --- ┆   ┆ ---  ┆ ---  ┆ ---   ┆ ---     │
│ str   ┆ f64   ┆ f64   ┆ f64 ┆   ┆ f64  ┆ f64  ┆ f64   ┆ f64     │
╞═══════╪═══════╪═══════╪═════╪═══╪══════╪══════╪═══════╪═════════╡
│ B1.18 ┆ 0.001 ┆ 0.001 ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0   ┆ 0.001   │
└───────┴───────┴───────┴─────┴───┴──────┴──────┴───────┴─────────┘
Saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGLGLRMFXP220260708.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 275318

Error creating SAS dataset GLRMFXP220260708: sasdata() got multiple values for argument 'table'

GLRMFXP220260708:
shape: (2, 9)
┌───────┬──────┬───────┬─────┬───┬──────┬─────────────┬───────┬─────────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ … ┆ YEAR ┆ LAST        ┆ TOTAL ┆ BALANCE     │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆   ┆ ---  ┆ ---         ┆ ---   ┆ ---         │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆   ┆ f64  ┆ f64         ┆ f64   ┆ f64         │
╞═══════╪══════╪═══════╪═════╪═══╪══════╪═════════════╪═══════╪═════════════╡
│ B1.14 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ -428093.697 ┆ 0.0   ┆ -428093.697 │
│ B1.12 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0         ┆ 0.0   ┆ 0.0         │
└───────┴──────┴───────┴─────┴───┴──────┴─────────────┴───────┴─────────────┘
Saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGLGLUTRMP220260708.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 275337

Error creating SAS dataset GLUTRMP220260708: sasdata() got multiple values for argument 'table'

GLUTRMP220260708:
shape: (3, 9)
┌───────┬──────┬───────┬─────┬───┬──────┬──────┬───────┬─────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ … ┆ YEAR ┆ LAST ┆ TOTAL ┆ BALANCE │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆   ┆ ---  ┆ ---  ┆ ---   ┆ ---     │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆   ┆ f64  ┆ f64  ┆ f64   ┆ f64     │
╞═══════╪══════╪═══════╪═════╪═══╪══════╪══════╪═══════╪═════════╡
│ A2.01 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.001 ┆ 0.001   │
│ A2.08 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0   ┆ 0.0     │
│ A2.21 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0   ┆ 0.0     │
└───────┴──────┴───────┴─────┴───┴──────┴──────┴───────┴─────────┘
Saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGLGLUTFXP220260708.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 275356

Error creating SAS dataset GLUTFXP220260708: sasdata() got multiple values for argument 'table'

GLUTFXP220260708:
shape: (3, 9)
┌───────┬──────┬───────┬─────┬───┬──────┬──────┬───────┬─────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ … ┆ YEAR ┆ LAST ┆ TOTAL ┆ BALANCE │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆   ┆ ---  ┆ ---  ┆ ---   ┆ ---     │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆   ┆ f64  ┆ f64  ┆ f64   ┆ f64     │
╞═══════╪══════╪═══════╪═════╪═══╪══════╪══════╪═══════╪═════════╡
│ B2.21 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0   ┆ 0.0     │
│ B2.01 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0   ┆ 0.0     │
│ B2.08 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0   ┆ 0.0     │
└───────┴──────┴───────┴─────┴───┴──────┴──────┴───────┴─────────┘

============================================================
Processing complete!
============================================================
SAS Connection terminated. Subprocess id was 275356
SAS Connection terminated. Subprocess id was 275337
SAS Connection terminated. Subprocess id was 275318
SAS Connection terminated. Subprocess id was 275298
SAS Connection terminated. Subprocess id was 275278
SAS Connection terminated. Subprocess id was 275259
SAS Connection terminated. Subprocess id was 275240
SAS Connection terminated. Subprocess id was 275220
SAS Connection terminated. Subprocess id was 275200
SAS Connection terminated. Subprocess id was 275176
