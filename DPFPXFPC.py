Current date: 2026-08-05
Report date (yesterday): 2026-08-04
Expected date format (DDMMYY): 040826

Reading GL file...
File date: 2026-07-31 (DDMMYY: 310726)
Expected date (DDMMYY): 040826
WARNING: GL file extraction date (310726) does not match expected date (040826)
Using file date for processing...

Processing 17 data lines
Line 6: Not enough parts (1): -
Line 16: Not enough parts (1): -

Created DataFrame with 13 records
Sample GLITEMs: ['F142199C', '132110', 'F132121BBNM', 'F143130', 'F249120BP', '139110', '149120', 'F147100', 'F142199E', '137070']

First few rows:
shape: (5, 4)
┌─────────────┬──────────┬──────┬───────────┐
│ GLITEM      ┆ DATEX    ┆ SIGN ┆ BALANCE   │
│ ---         ┆ ---      ┆ ---  ┆ ---       │
│ str         ┆ str      ┆ str  ┆ f64       │
╞═════════════╪══════════╪══════╪═══════════╡
│ 137070      ┆ 31/07/26 ┆ +    ┆ 0.0       │
│ 132110      ┆ 31/07/26 ┆ +    ┆ 1.0089e9  │
│ 139110      ┆ 31/07/26 ┆ +    ┆ 1.6607e7  │
│ 149120      ┆ 31/07/26 ┆ -    ┆ 570339.41 │
│ F132121BBNM ┆ 31/07/26 ┆ +    ┆ 1.0154e7  │
└─────────────┴──────────┴──────┴───────────┘

Processing Pass 1 (A2.21)...
No data for GLRMP120260731
No data for GLFXP120260731
Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMNLGL/GLUTRMP120260731.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 218316


61   
62   libname mylib    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMNLGL'  ;
NOTE: Libref MYLIB was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMNLGL
63   
Saved SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMNLGL/GLUTRMP120260731.sas7bdat
SAS Connection terminated. Subprocess id was 218316

GLUTRMP120260731:
shape: (2, 8)
┌───────┬───────────┬───────┬─────┬────────┬──────┬──────┬───────────┐
│ ITEM  ┆ WEEK      ┆ MONTH ┆ QTR ┆ HALFYR ┆ YEAR ┆ LAST ┆ BALANCE   │
│ ---   ┆ ---       ┆ ---   ┆ --- ┆ ---    ┆ ---  ┆ ---  ┆ ---       │
│ str   ┆ f64       ┆ f64   ┆ f64 ┆ f64    ┆ f64  ┆ f64  ┆ f64       │
╞═══════╪═══════════╪═══════╪═════╪════════╪══════╪══════╪═══════════╡
│ A2.01 ┆ 10154.309 ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 10154.309 │
│ A2.08 ┆ 0.0       ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 0.0       │
└───────┴───────────┴───────┴─────┴────────┴──────┴──────┴───────────┘
No data for GLUTFXP120260731

Processing Pass 2 (A2.14)...
No data for GLRMP220260731
No data for GLFXP220260731
Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMNLGL/GLUTRMP220260731.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 218361


61   
62   libname mylib    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMNLGL'  ;
NOTE: Libref MYLIB was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMNLGL
63   
Saved SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMNLGL/GLUTRMP220260731.sas7bdat
SAS Connection terminated. Subprocess id was 218361

GLUTRMP220260731:
shape: (2, 8)
┌───────┬───────────┬───────┬─────┬────────┬──────┬──────┬───────────┐
│ ITEM  ┆ WEEK      ┆ MONTH ┆ QTR ┆ HALFYR ┆ YEAR ┆ LAST ┆ BALANCE   │
│ ---   ┆ ---       ┆ ---   ┆ --- ┆ ---    ┆ ---  ┆ ---  ┆ ---       │
│ str   ┆ f64       ┆ f64   ┆ f64 ┆ f64    ┆ f64  ┆ f64  ┆ f64       │
╞═══════╪═══════════╪═══════╪═════╪════════╪══════╪══════╪═══════════╡
│ A2.01 ┆ 10154.309 ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 10154.309 │
│ A2.08 ┆ 0.0       ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 0.0       │
└───────┴───────────┴───────┴─────┴────────┴──────┴──────┴───────────┘
No data for GLUTFXP220260731

Processing complete!
