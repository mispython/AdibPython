============================================================
GL FILE PROCESSING STARTED
============================================================
Processing date: 2026-07-08
Store directory: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGL
============================================================

Reading GL text file...
Total lines: 74

Successfully read 74 rows
Columns: ['YY', 'MM', 'DD', 'GLITEM', 'DATE', 'BALANCE', 'SIGN']

Data sample:
shape: (10, 7)
┌─────┬─────┬─────┬──────────┬──────────┬─────────────┬──────┐
│ YY  ┆ MM  ┆ DD  ┆ GLITEM   ┆ DATE     ┆ BALANCE     ┆ SIGN │
│ --- ┆ --- ┆ --- ┆ ---      ┆ ---      ┆ ---         ┆ ---  │
│ str ┆ str ┆ str ┆ str      ┆ str      ┆ f64         ┆ str  │
╞═════╪═════╪═════╪══════════╪══════════╪═════════════╪══════╡
│ 26  ┆ 07  ┆ 08  ┆ 20260708 ┆          ┆ 0.0         ┆     │
│ 26  ┆ 07  ┆ 08  ┆ 1F147600 ┆ 08/07/26 ┆ 0.0         ┆      │
│ 26  ┆ 07  ┆ 08  ┆ 1F142630 ┆ 08/07/26 ┆ 0.0         ┆      │
│ 26  ┆ 07  ┆ 08  ┆ 142699   ┆ 08/07/26 ┆ 2.2446e8    ┆      │
│ 26  ┆ 07  ┆ 08  ┆ 144111   ┆ 08/07/26 ┆ 4.9979e9    ┆      │
│ 26  ┆ 07  ┆ 08  ┆ 1F147100 ┆ 08/07/26 ┆ 6.9164e9    ┆      │
│ 26  ┆ 07  ┆ 08  ┆ 1F249299 ┆ 08/07/26 ┆ 1.9506e9    ┆      │
│ 26  ┆ 07  ┆ 08  ┆ 142199   ┆ 08/07/26 ┆ 3.7928701e7 ┆      │
│ 26  ┆ 07  ┆ 08  ┆ 1F144611 ┆ 08/07/26 ┆ 0.0         ┆      │
│ 26  ┆ 07  ┆ 08  ┆ 149120NL ┆ 08/07/26 ┆ 2.2252e8    ┆      │
└─────┴─────┴─────┴──────────┴──────────┴─────────────┴──────┘

Unique GLITEMs in file (47):
  -
  132110
  134200
  137070
  139110
  142199
  142699
  142699SG
  142699US
  144111
  149120NL
  1F132110
  1F132121
  1F133110
  1F133130
  1F133140
  1F133620
  1F133640
  1F134600
  1F137010
  1F137610
  1F137650
  1F139111
  1F139610
  1F141301
  1F142199
  1F142510
  1F142599
  1F142600
  1F142630
  ... and 17 more

============================================================
Processing GL P1...
============================================================

Processing P1...
DataFrame shape: (74, 7)

Unique GLITEMs in file (47):
  -
  132110
  134200
  137070
  139110
  142199
  142699
  142699SG
  142699US
  144111
  149120NL
  1F132110
  1F132121
  1F133110
  1F133130
  1F133140
  1F133620
  1F133640
  1F134600
  1F137010
  ... and 27 more
Matched: '1F137610' -> 'F137610FXSH' (B2.08)
Matched: '1F142199' -> '42199' (A1.20)
Matched: '1F142630' -> 'F142630C' (B1.12)
Matched: '1F132121' -> 'F132121BBNM' (A2.01)
Matched: '142699' -> '42699' (B1.14)
Matched: '1F143620' -> 'F143620FNFBI' (B2.21)
Matched: '1F137650' -> 'F137650FXCDS' (B2.08)
Matched: '1F249120' -> '49120' (A1.20)
Matched: '1F147100' -> 'F147100' (A1.18)
Matched: '1F144111' -> '44111' (A1.18)
Matched: '1F144611' -> 'F144611FXSDC' (B1.18)
Matched: '1F133620' -> 'F133620FNFBI' (B2.01)
Matched: '1F147600' -> 'F147600' (B1.18)
Matched: '137070' -> '37070' (A2.08)
Matched: '1F142699' -> '42699' (B1.14)
Matched: '1F249299' -> 'F249299K' (A1.20)
Matched: '1F143120' -> 'F143120ODNVB' (A2.21)
Matched: '144111' -> '44111' (A1.18)
Matched: '1F143110' -> 'F143110VCB' (A2.21)
Matched: '1F133110' -> 'F133110ODVIB' (A2.01)
Matched: '142199' -> '42199' (A1.20)
Created DataFrame with 21 rows for P1

Processed data for P1:
shape: (11, 9)
┌───────┬────────────┬────────────┬─────┬───┬──────┬───────────┬──────────┬────────────┐
│ ITEM  ┆ WEEK       ┆ MONTH      ┆ QTR ┆ … ┆ YEAR ┆ LAST      ┆ TOTAL    ┆ BALANCE    │
│ ---   ┆ ---        ┆ ---        ┆ --- ┆   ┆ ---  ┆ ---       ┆ ---      ┆ ---        │
│ str   ┆ f64        ┆ f64        ┆ f64 ┆   ┆ f64  ┆ f64       ┆ f64      ┆ f64        │
╞═══════╪════════════╪════════════╪═════╪═══╪══════╪═══════════╪══════════╪════════════╡
│ B2.08 ┆ 1597.498   ┆ 1597.498   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0       ┆ 0.0      ┆ 1597.498   │
│ A2.08 ┆ 0.0        ┆ 0.0        ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0       ┆ 0.0      ┆ 0.0        │
│ A1.20 ┆ 0.0        ┆ 0.0        ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0       ┆ 2.2566e6 ┆ 2.2566e6   │
│ B1.14 ┆ 0.0        ┆ 0.0        ┆ 0.0 ┆ … ┆ 0.0  ┆ 225923.71 ┆ 0.0      ┆ 225923.71  │
│ A1.18 ┆ 0.0        ┆ 0.0        ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0       ┆ 1.6912e7 ┆ 1.6912e7   │
│ …     ┆ …          ┆ …          ┆ …   ┆ … ┆ …    ┆ …         ┆ …        ┆ …          │
│ A2.21 ┆ 0.0        ┆ 0.0        ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0       ┆ 1.5225e6 ┆ 1.5225e6   │
│ B1.12 ┆ 0.0        ┆ 0.0        ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0       ┆ 0.0      ┆ 0.0        │
│ B2.01 ┆ 116648.76  ┆ 116648.76  ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0       ┆ 0.0      ┆ 116648.76  │
│ B2.21 ┆ 257584.266 ┆ 257584.266 ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0       ┆ 0.0      ┆ 257584.266 │
│ A2.01 ┆ 0.0        ┆ 0.0        ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0       ┆ 1.1184e6 ┆ 1.1184e6   │
└───────┴────────────┴────────────┴─────┴───┴──────┴───────────┴──────────┴────────────┘
Saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGLGLRMP120260708.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 278563

Error creating SAS dataset GLRMP120260708: sasdata() got multiple values for argument 'table'

GLRMP120260708:
shape: (2, 9)
┌───────┬──────┬───────┬─────┬───┬──────┬──────┬──────────┬──────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ … ┆ YEAR ┆ LAST ┆ TOTAL    ┆ BALANCE  │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆   ┆ ---  ┆ ---  ┆ ---      ┆ ---      │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆   ┆ f64  ┆ f64  ┆ f64      ┆ f64      │
╞═══════╪══════╪═══════╪═════╪═══╪══════╪══════╪══════════╪══════════╡
│ A1.20 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 2.2566e6 ┆ 2.2566e6 │
│ A1.18 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 1.6912e7 ┆ 1.6912e7 │
└───────┴──────┴───────┴─────┴───┴──────┴──────┴──────────┴──────────┘
Saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGLGLFXP120260708.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 278592

Error creating SAS dataset GLFXP120260708: sasdata() got multiple values for argument 'table'

GLFXP120260708:
shape: (1, 9)
┌───────┬──────┬───────┬─────┬───┬──────┬──────┬───────┬─────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ … ┆ YEAR ┆ LAST ┆ TOTAL ┆ BALANCE │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆   ┆ ---  ┆ ---  ┆ ---   ┆ ---     │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆   ┆ f64  ┆ f64  ┆ f64   ┆ f64     │
╞═══════╪══════╪═══════╪═════╪═══╪══════╪══════╪═══════╪═════════╡
│ B1.18 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0   ┆ 0.0     │
└───────┴──────┴───────┴─────┴───┴──────┴──────┴───────┴─────────┘
Saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGLGLRMFXP120260708.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 278611

Error creating SAS dataset GLRMFXP120260708: sasdata() got multiple values for argument 'table'

GLRMFXP120260708:
shape: (2, 9)
┌───────┬──────┬───────┬─────┬───┬──────┬───────────┬───────┬───────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ … ┆ YEAR ┆ LAST      ┆ TOTAL ┆ BALANCE   │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆   ┆ ---  ┆ ---       ┆ ---   ┆ ---       │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆   ┆ f64  ┆ f64       ┆ f64   ┆ f64       │
╞═══════╪══════╪═══════╪═════╪═══╪══════╪═══════════╪═══════╪═══════════╡
│ B1.14 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 225923.71 ┆ 0.0   ┆ 225923.71 │
│ B1.12 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0       ┆ 0.0   ┆ 0.0       │
└───────┴──────┴───────┴─────┴───┴──────┴───────────┴───────┴───────────┘
Saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGLGLUTRMP120260708.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 278631

Error creating SAS dataset GLUTRMP120260708: sasdata() got multiple values for argument 'table'

GLUTRMP120260708:
shape: (3, 9)
┌───────┬──────┬───────┬─────┬───┬──────┬──────┬──────────┬──────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ … ┆ YEAR ┆ LAST ┆ TOTAL    ┆ BALANCE  │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆   ┆ ---  ┆ ---  ┆ ---      ┆ ---      │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆   ┆ f64  ┆ f64  ┆ f64      ┆ f64      │
╞═══════╪══════╪═══════╪═════╪═══╪══════╪══════╪══════════╪══════════╡
│ A2.08 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0      ┆ 0.0      │
│ A2.21 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 1.5225e6 ┆ 1.5225e6 │
│ A2.01 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 1.1184e6 ┆ 1.1184e6 │
└───────┴──────┴───────┴─────┴───┴──────┴──────┴──────────┴──────────┘
Saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGLGLUTFXP120260708.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 278651

Error creating SAS dataset GLUTFXP120260708: sasdata() got multiple values for argument 'table'

GLUTFXP120260708:
shape: (3, 9)
┌───────┬────────────┬────────────┬─────┬───┬──────┬──────┬───────┬────────────┐
│ ITEM  ┆ WEEK       ┆ MONTH      ┆ QTR ┆ … ┆ YEAR ┆ LAST ┆ TOTAL ┆ BALANCE    │
│ ---   ┆ ---        ┆ ---        ┆ --- ┆   ┆ ---  ┆ ---  ┆ ---   ┆ ---        │
│ str   ┆ f64        ┆ f64        ┆ f64 ┆   ┆ f64  ┆ f64  ┆ f64   ┆ f64        │
╞═══════╪════════════╪════════════╪═════╪═══╪══════╪══════╪═══════╪════════════╡
│ B2.08 ┆ 1597.498   ┆ 1597.498   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0   ┆ 1597.498   │
│ B2.01 ┆ 116648.76  ┆ 116648.76  ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0   ┆ 116648.76  │
│ B2.21 ┆ 257584.266 ┆ 257584.266 ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0   ┆ 257584.266 │
└───────┴────────────┴────────────┴─────┴───┴──────┴──────┴───────┴────────────┘

============================================================
Processing GL P2...
============================================================

Processing P2...
DataFrame shape: (74, 7)

Unique GLITEMs in file (47):
  -
  132110
  134200
  137070
  139110
  142199
  142699
  142699SG
  142699US
  144111
  149120NL
  1F132110
  1F132121
  1F133110
  1F133130
  1F133140
  1F133620
  1F133640
  1F134600
  1F137010
  ... and 27 more
Matched: '1F132121' -> 'F132121BBNM' (A2.01)
Matched: '1F144111' -> '44111' (A1.18)
Matched: '1F133110' -> 'F133110ODVIB' (A2.01)
Matched: '142199' -> '42199' (A1.20)
Matched: '1F133620' -> 'F133620FNFBI' (B2.01)
Matched: '144111' -> '44111' (A1.18)
Matched: '1F249299' -> 'F249299K' (A1.20)
Matched: '1F142199' -> '42199' (A1.20)
Matched: '1F143120' -> 'F143120ODNVB' (A2.21)
Matched: '1F142630' -> 'F142630C' (B1.12)
Matched: '1F137650' -> 'F137650FXCDS' (B2.08)
Matched: '142699' -> '42699' (B1.14)
Matched: '1F142699' -> '42699' (B1.14)
Matched: '1F147600' -> 'F147600' (B1.18)
Matched: '1F143620' -> 'F143620FNFBI' (B2.21)
Matched: '1F143110' -> 'F143110VCB' (A2.21)
Matched: '1F249120' -> '49120' (A1.20)
Matched: '1F144611' -> 'F144611FXSDC' (B1.18)
Matched: '1F137610' -> 'F137610FXSH' (B2.08)
Matched: '1F147100' -> 'F147100' (A1.18)
Matched: '137070' -> '37070' (A2.08)
Created DataFrame with 21 rows for P2

Processed data for P2:
shape: (11, 9)
┌───────┬───────────┬───────────┬─────┬───┬──────┬───────────┬──────────┬───────────┐
│ ITEM  ┆ WEEK      ┆ MONTH     ┆ QTR ┆ … ┆ YEAR ┆ LAST      ┆ TOTAL    ┆ BALANCE   │
│ ---   ┆ ---       ┆ ---       ┆ --- ┆   ┆ ---  ┆ ---       ┆ ---      ┆ ---       │
│ str   ┆ f64       ┆ f64       ┆ f64 ┆   ┆ f64  ┆ f64       ┆ f64      ┆ f64       │
╞═══════╪═══════════╪═══════════╪═════╪═══╪══════╪═══════════╪══════════╪═══════════╡
│ A2.01 ┆ 0.0       ┆ 0.0       ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0       ┆ 1.1184e6 ┆ 1.1184e6  │
│ A1.18 ┆ 0.0       ┆ 0.0       ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0       ┆ 1.6912e7 ┆ 1.6912e7  │
│ B2.08 ┆ 1597.498  ┆ 1597.498  ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0       ┆ 0.0      ┆ 1597.498  │
│ A2.21 ┆ 0.0       ┆ 0.0       ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0       ┆ 1.5225e6 ┆ 1.5225e6  │
│ A1.20 ┆ 0.0       ┆ 0.0       ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0       ┆ 2.2566e6 ┆ 2.2566e6  │
│ …     ┆ …         ┆ …         ┆ …   ┆ … ┆ …    ┆ …         ┆ …        ┆ …         │
│ B2.01 ┆ 116648.76 ┆ 116648.76 ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0       ┆ 0.0      ┆ 116648.76 │
│ A2.08 ┆ 0.0       ┆ 0.0       ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0       ┆ 0.0      ┆ 0.0       │
│ B1.14 ┆ 0.0       ┆ 0.0       ┆ 0.0 ┆ … ┆ 0.0  ┆ 225923.71 ┆ 0.0      ┆ 225923.71 │
│ B1.12 ┆ 0.0       ┆ 0.0       ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0       ┆ 0.0      ┆ 0.0       │
│ B1.18 ┆ 0.0       ┆ 0.0       ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0       ┆ 0.0      ┆ 0.0       │
└───────┴───────────┴───────────┴─────┴───┴──────┴───────────┴──────────┴───────────┘
Saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGLGLRMP220260708.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 278670

Error creating SAS dataset GLRMP220260708: sasdata() got multiple values for argument 'table'

GLRMP220260708:
shape: (2, 9)
┌───────┬──────┬───────┬─────┬───┬──────┬──────┬──────────┬──────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ … ┆ YEAR ┆ LAST ┆ TOTAL    ┆ BALANCE  │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆   ┆ ---  ┆ ---  ┆ ---      ┆ ---      │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆   ┆ f64  ┆ f64  ┆ f64      ┆ f64      │
╞═══════╪══════╪═══════╪═════╪═══╪══════╪══════╪══════════╪══════════╡
│ A1.18 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 1.6912e7 ┆ 1.6912e7 │
│ A1.20 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 2.2566e6 ┆ 2.2566e6 │
└───────┴──────┴───────┴─────┴───┴──────┴──────┴──────────┴──────────┘
Saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGLGLFXP220260708.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 278689

Error creating SAS dataset GLFXP220260708: sasdata() got multiple values for argument 'table'

GLFXP220260708:
shape: (1, 9)
┌───────┬──────┬───────┬─────┬───┬──────┬──────┬───────┬─────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ … ┆ YEAR ┆ LAST ┆ TOTAL ┆ BALANCE │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆   ┆ ---  ┆ ---  ┆ ---   ┆ ---     │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆   ┆ f64  ┆ f64  ┆ f64   ┆ f64     │
╞═══════╪══════╪═══════╪═════╪═══╪══════╪══════╪═══════╪═════════╡
│ B1.18 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0   ┆ 0.0     │
└───────┴──────┴───────┴─────┴───┴──────┴──────┴───────┴─────────┘
Saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGLGLRMFXP220260708.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 278708

Error creating SAS dataset GLRMFXP220260708: sasdata() got multiple values for argument 'table'

GLRMFXP220260708:
shape: (2, 9)
┌───────┬──────┬───────┬─────┬───┬──────┬───────────┬───────┬───────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ … ┆ YEAR ┆ LAST      ┆ TOTAL ┆ BALANCE   │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆   ┆ ---  ┆ ---       ┆ ---   ┆ ---       │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆   ┆ f64  ┆ f64       ┆ f64   ┆ f64       │
╞═══════╪══════╪═══════╪═════╪═══╪══════╪═══════════╪═══════╪═══════════╡
│ B1.14 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 225923.71 ┆ 0.0   ┆ 225923.71 │
│ B1.12 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0       ┆ 0.0   ┆ 0.0       │
└───────┴──────┴───────┴─────┴───┴──────┴───────────┴───────┴───────────┘
Saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGLGLUTRMP220260708.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 278727

Error creating SAS dataset GLUTRMP220260708: sasdata() got multiple values for argument 'table'

GLUTRMP220260708:
shape: (3, 9)
┌───────┬──────┬───────┬─────┬───┬──────┬──────┬──────────┬──────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ … ┆ YEAR ┆ LAST ┆ TOTAL    ┆ BALANCE  │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆   ┆ ---  ┆ ---  ┆ ---      ┆ ---      │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆   ┆ f64  ┆ f64  ┆ f64      ┆ f64      │
╞═══════╪══════╪═══════╪═════╪═══╪══════╪══════╪══════════╪══════════╡
│ A2.01 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 1.1184e6 ┆ 1.1184e6 │
│ A2.21 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 1.5225e6 ┆ 1.5225e6 │
│ A2.08 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0      ┆ 0.0      │
└───────┴──────┴───────┴─────┴───┴──────┴──────┴──────────┴──────────┘
Saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGLGLUTFXP220260708.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 278746

Error creating SAS dataset GLUTFXP220260708: sasdata() got multiple values for argument 'table'

GLUTFXP220260708:
shape: (3, 9)
┌───────┬────────────┬────────────┬─────┬───┬──────┬──────┬───────┬────────────┐
│ ITEM  ┆ WEEK       ┆ MONTH      ┆ QTR ┆ … ┆ YEAR ┆ LAST ┆ TOTAL ┆ BALANCE    │
│ ---   ┆ ---        ┆ ---        ┆ --- ┆   ┆ ---  ┆ ---  ┆ ---   ┆ ---        │
│ str   ┆ f64        ┆ f64        ┆ f64 ┆   ┆ f64  ┆ f64  ┆ f64   ┆ f64        │
╞═══════╪════════════╪════════════╪═════╪═══╪══════╪══════╪═══════╪════════════╡
│ B2.08 ┆ 1597.498   ┆ 1597.498   ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0   ┆ 1597.498   │
│ B2.21 ┆ 257584.266 ┆ 257584.266 ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0   ┆ 257584.266 │
│ B2.01 ┆ 116648.76  ┆ 116648.76  ┆ 0.0 ┆ … ┆ 0.0  ┆ 0.0  ┆ 0.0   ┆ 116648.76  │
└───────┴────────────┴────────────┴─────┴───┴──────┴──────┴───────┴────────────┘

============================================================
PROCESSING COMPLETE!
============================================================

Output files saved to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNLGL

Files created:

============================================================
SAS Connection terminated. Subprocess id was 278746
SAS Connection terminated. Subprocess id was 278727
SAS Connection terminated. Subprocess id was 278708
SAS Connection terminated. Subprocess id was 278689
SAS Connection terminated. Subprocess id was 278670
SAS Connection terminated. Subprocess id was 278651
SAS Connection terminated. Subprocess id was 278631
SAS Connection terminated. Subprocess id was 278611
SAS Connection terminated. Subprocess id was 278592
SAS Connection terminated. Subprocess id was 278563
