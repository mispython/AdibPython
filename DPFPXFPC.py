Current date: 2026-08-05
Report date (yesterday): 2026-08-04
Expected date format (DDMMYY): 040826

Reading GL file...
File date: 2026-07-31 (DDMMYY: 310726)
Expected date (DDMMYY): 040826
WARNING: GL file extraction date (310726) does not match expected date (040826)
Using file date for processing...

Processing 68 data lines
Line 27: Not enough parts (1): -
Line 28: Not enough parts (1): -
Line 30: Not enough parts (1): -
Line 35: Not enough parts (1): -
Line 58: Not enough parts (1): -

Created DataFrame with 57 records
Sample GLITEMs: ['F132121BBNM', 'F143110VFBI', '144111', 'ML34170', 'F143620USDOP', '149120', 'F133110ODVIB', 'ML34111', 'F144140CAGA', 'F142199C']

First few rows:
shape: (5, 7)
┌──────────────┬──────────┬──────┬──────────┬──────┬─────┬─────┐
│ GLITEM       ┆ DATE     ┆ SIGN ┆ BALANCE  ┆ YY   ┆ MM  ┆ DD  │
│ ---          ┆ ---      ┆ ---  ┆ ---      ┆ ---  ┆ --- ┆ --- │
│ str          ┆ str      ┆ str  ┆ f64      ┆ i64  ┆ i64 ┆ i64 │
╞══════════════╪══════════╪══════╪══════════╪══════╪═════╪═════╡
│ 149120       ┆ 31/07/26 ┆ -    ┆ 1.5017e8 ┆ 2026 ┆ 7   ┆ 31  │
│ 142199       ┆ 31/07/26 ┆ -    ┆ 3.6472e7 ┆ 2026 ┆ 7   ┆ 31  │
│ F144611FXSDC ┆ 31/07/26 ┆ +    ┆ 0.0      ┆ 2026 ┆ 7   ┆ 31  │
│ F142630C     ┆ 31/07/26 ┆ +    ┆ 0.0      ┆ 2026 ┆ 7   ┆ 31  │
│ 142699       ┆ 31/07/26 ┆ -    ┆ 1.7933e8 ┆ 2026 ┆ 7   ┆ 31  │
└──────────────┴──────────┴──────┴──────────┴──────┴─────┴─────┘

Processing P1 conditions...
Using SAS Config named: default
SAS Connection established. Subprocess id is 212831


61   
62   libname mylib    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL'  ;
NOTE: Libref MYLIB was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL
63   
Saved SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL/GLRMP120260731.sas7bdat
Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL/GLRMP120260731.parquet

GLRMP120260731:
shape: (2, 8)
┌───────┬───────────┬───────┬─────┬────────┬──────┬──────┬───────────┐
│ ITEM  ┆ WEEK      ┆ MONTH ┆ QTR ┆ HALFYR ┆ YEAR ┆ LAST ┆ BALANCE   │
│ ---   ┆ ---       ┆ ---   ┆ --- ┆ ---    ┆ ---  ┆ ---  ┆ ---       │
│ str   ┆ f64       ┆ f64   ┆ f64 ┆ f64    ┆ f64  ┆ f64  ┆ f64       │
╞═══════╪═══════════╪═══════╪═════╪════════╪══════╪══════╪═══════════╡
│ A1.20 ┆ -1.9671e6 ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ -1.9671e6 │
│ A1.18 ┆ 0.0       ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 0.0       │
└───────┴───────────┴───────┴─────┴────────┴──────┴──────┴───────────┘
Using SAS Config named: default
SAS Connection established. Subprocess id is 212870


61   
62   libname mylib    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL'  ;
NOTE: Libref MYLIB was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL
63   
Saved SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL/GLFXP120260731.sas7bdat
Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL/GLFXP120260731.parquet

GLFXP120260731:
shape: (1, 8)
┌───────┬──────┬───────┬─────┬────────┬──────┬──────┬─────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ HALFYR ┆ YEAR ┆ LAST ┆ BALANCE │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆ ---    ┆ ---  ┆ ---  ┆ ---     │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆ f64    ┆ f64  ┆ f64  ┆ f64     │
╞═══════╪══════╪═══════╪═════╪════════╪══════╪══════╪═════════╡
│ B1.18 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 0.0     │
└───────┴──────┴───────┴─────┴────────┴──────┴──────┴─────────┘
Using SAS Config named: default
SAS Connection established. Subprocess id is 212894


61   
62   libname mylib    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL'  ;
NOTE: Libref MYLIB was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL
63   
Saved SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL/GLRMFXP120260731.sas7bdat
Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL/GLRMFXP120260731.parquet

GLRMFXP120260731:
shape: (1, 8)
┌───────┬──────┬───────┬─────┬────────┬──────┬──────┬─────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ HALFYR ┆ YEAR ┆ LAST ┆ BALANCE │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆ ---    ┆ ---  ┆ ---  ┆ ---     │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆ f64    ┆ f64  ┆ f64  ┆ f64     │
╞═══════╪══════╪═══════╪═════╪════════╪══════╪══════╪═════════╡
│ B1.12 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 0.0     │
└───────┴──────┴───────┴─────┴────────┴──────┴──────┴─────────┘
Using SAS Config named: default
SAS Connection established. Subprocess id is 212917


61   
62   libname mylib    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL'  ;
NOTE: Libref MYLIB was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL
63   
Saved SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL/GLUTRMP120260731.sas7bdat
Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL/GLUTRMP120260731.parquet

GLUTRMP120260731:
shape: (2, 8)
┌───────┬─────────────┬───────┬─────┬────────┬──────┬──────┬─────────────┐
│ ITEM  ┆ WEEK        ┆ MONTH ┆ QTR ┆ HALFYR ┆ YEAR ┆ LAST ┆ BALANCE     │
│ ---   ┆ ---         ┆ ---   ┆ --- ┆ ---    ┆ ---  ┆ ---  ┆ ---         │
│ str   ┆ f64         ┆ f64   ┆ f64 ┆ f64    ┆ f64  ┆ f64  ┆ f64         │
╞═══════╪═════════════╪═══════╪═════╪════════╪══════╪══════╪═════════════╡
│ A2.21 ┆ -448767.597 ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ -448767.597 │
│ A2.01 ┆ 362337.912  ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 362337.912  │
└───────┴─────────────┴───────┴─────┴────────┴──────┴──────┴─────────────┘
Using SAS Config named: default
SAS Connection established. Subprocess id is 212940


61   
62   libname mylib    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL'  ;
NOTE: Libref MYLIB was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL
63   
Saved SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL/GLUTFXP120260731.sas7bdat
Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL/GLUTFXP120260731.parquet

GLUTFXP120260731:
shape: (3, 8)
┌───────┬────────────┬───────┬─────┬────────┬──────┬──────┬────────────┐
│ ITEM  ┆ WEEK       ┆ MONTH ┆ QTR ┆ HALFYR ┆ YEAR ┆ LAST ┆ BALANCE    │
│ ---   ┆ ---        ┆ ---   ┆ --- ┆ ---    ┆ ---  ┆ ---  ┆ ---        │
│ str   ┆ f64        ┆ f64   ┆ f64 ┆ f64    ┆ f64  ┆ f64  ┆ f64        │
╞═══════╪════════════╪═══════╪═════╪════════╪══════╪══════╪════════════╡
│ B2.21 ┆ -44861.543 ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ -44861.543 │
│ B2.01 ┆ -44861.543 ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ -44861.543 │
│ B2.08 ┆ 1600.829   ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 1600.829   │
└───────┴────────────┴───────┴─────┴────────┴──────┴──────┴────────────┘

Processing P2 conditions...
Using SAS Config named: default
SAS Connection established. Subprocess id is 212963


61   
62   libname mylib    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL'  ;
NOTE: Libref MYLIB was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL
63   
Saved SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL/GLRMP220260731.sas7bdat
Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL/GLRMP220260731.parquet

GLRMP220260731:
shape: (2, 8)
┌───────┬───────────┬───────┬─────┬────────┬──────┬──────┬───────────┐
│ ITEM  ┆ WEEK      ┆ MONTH ┆ QTR ┆ HALFYR ┆ YEAR ┆ LAST ┆ BALANCE   │
│ ---   ┆ ---       ┆ ---   ┆ --- ┆ ---    ┆ ---  ┆ ---  ┆ ---       │
│ str   ┆ f64       ┆ f64   ┆ f64 ┆ f64    ┆ f64  ┆ f64  ┆ f64       │
╞═══════╪═══════════╪═══════╪═════╪════════╪══════╪══════╪═══════════╡
│ A1.18 ┆ 0.0       ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 0.0       │
│ A1.20 ┆ -1.9671e6 ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ -1.9671e6 │
└───────┴───────────┴───────┴─────┴────────┴──────┴──────┴───────────┘
Using SAS Config named: default
SAS Connection established. Subprocess id is 212986


61   
62   libname mylib    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL'  ;
NOTE: Libref MYLIB was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL
63   
Saved SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL/GLFXP220260731.sas7bdat
Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL/GLFXP220260731.parquet

GLFXP220260731:
shape: (1, 8)
┌───────┬──────┬───────┬─────┬────────┬──────┬──────┬─────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ HALFYR ┆ YEAR ┆ LAST ┆ BALANCE │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆ ---    ┆ ---  ┆ ---  ┆ ---     │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆ f64    ┆ f64  ┆ f64  ┆ f64     │
╞═══════╪══════╪═══════╪═════╪════════╪══════╪══════╪═════════╡
│ B1.18 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 0.0     │
└───────┴──────┴───────┴─────┴────────┴──────┴──────┴─────────┘
Using SAS Config named: default
SAS Connection established. Subprocess id is 213018


61   
62   libname mylib    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL'  ;
NOTE: Libref MYLIB was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL
63   
Saved SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL/GLRMFXP220260731.sas7bdat
Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL/GLRMFXP220260731.parquet

GLRMFXP220260731:
shape: (1, 8)
┌───────┬──────┬───────┬─────┬────────┬──────┬──────┬─────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ HALFYR ┆ YEAR ┆ LAST ┆ BALANCE │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆ ---    ┆ ---  ┆ ---  ┆ ---     │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆ f64    ┆ f64  ┆ f64  ┆ f64     │
╞═══════╪══════╪═══════╪═════╪════════╪══════╪══════╪═════════╡
│ B1.12 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 0.0     │
└───────┴──────┴───────┴─────┴────────┴──────┴──────┴─────────┘
Using SAS Config named: default
SAS Connection established. Subprocess id is 213041


61   
62   libname mylib    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL'  ;
NOTE: Libref MYLIB was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL
63   
Saved SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL/GLUTRMP220260731.sas7bdat
Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL/GLUTRMP220260731.parquet

GLUTRMP220260731:
shape: (2, 8)
┌───────┬─────────────┬───────┬─────┬────────┬──────┬──────┬─────────────┐
│ ITEM  ┆ WEEK        ┆ MONTH ┆ QTR ┆ HALFYR ┆ YEAR ┆ LAST ┆ BALANCE     │
│ ---   ┆ ---         ┆ ---   ┆ --- ┆ ---    ┆ ---  ┆ ---  ┆ ---         │
│ str   ┆ f64         ┆ f64   ┆ f64 ┆ f64    ┆ f64  ┆ f64  ┆ f64         │
╞═══════╪═════════════╪═══════╪═════╪════════╪══════╪══════╪═════════════╡
│ A2.21 ┆ -448767.597 ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ -448767.597 │
│ A2.01 ┆ 362337.912  ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 362337.912  │
└───────┴─────────────┴───────┴─────┴────────┴──────┴──────┴─────────────┘
Using SAS Config named: default
SAS Connection established. Subprocess id is 213064


61   
62   libname mylib    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL'  ;
NOTE: Libref MYLIB was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL
63   
Saved SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL/GLUTFXP220260731.sas7bdat
Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMNLGL/GLUTFXP220260731.parquet

GLUTFXP220260731:
shape: (3, 8)
┌───────┬────────────┬───────┬─────┬────────┬──────┬──────┬────────────┐
│ ITEM  ┆ WEEK       ┆ MONTH ┆ QTR ┆ HALFYR ┆ YEAR ┆ LAST ┆ BALANCE    │
│ ---   ┆ ---        ┆ ---   ┆ --- ┆ ---    ┆ ---  ┆ ---  ┆ ---        │
│ str   ┆ f64        ┆ f64   ┆ f64 ┆ f64    ┆ f64  ┆ f64  ┆ f64        │
╞═══════╪════════════╪═══════╪═════╪════════╪══════╪══════╪════════════╡
│ B2.21 ┆ -44861.543 ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ -44861.543 │
│ B2.08 ┆ 1600.829   ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 1600.829   │
│ B2.01 ┆ -44861.543 ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ -44861.543 │
└───────┴────────────┴───────┴─────┴────────┴──────┴──────┴────────────┘

Processing complete!
SAS Connection terminated. Subprocess id was 213064
SAS Connection terminated. Subprocess id was 213041
SAS Connection terminated. Subprocess id was 213018
SAS Connection terminated. Subprocess id was 212986
SAS Connection terminated. Subprocess id was 212963
SAS Connection terminated. Subprocess id was 212940
SAS Connection terminated. Subprocess id was 212917
SAS Connection terminated. Subprocess id was 212894
SAS Connection terminated. Subprocess id was 212870
SAS Connection terminated. Subprocess id was 212831
