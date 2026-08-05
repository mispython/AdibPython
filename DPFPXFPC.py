Current date: 2026-08-05
Report date (yesterday): 2026-08-04
Expected date format (DDMMYY): 040826

Reading GL file...
File date: 2026-07-31 (DDMMYY: 310726)
Expected date (DDMMYY): 040826
WARNING: GL file extraction date (310726) does not match expected date (040826)
Using file date for processing...

Processing 9 data lines
Line 6: Parse error - could not convert string to float: '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00': -

Created DataFrame with 7 records
Sample GLITEMs: ['S-RCF', 'S-FIXED DEP', 'S-BA F', 'S-SM F', 'S-TLF', 'S-GUARANTEE', 'S-REMISIERFD']

First few rows:
shape: (7, 4)
┌──────────────┬──────────┬──────┬───────────┐
│ GLITEM       ┆ DATEX    ┆ SIGN ┆ BALANCE   │
│ ---          ┆ ---      ┆ ---  ┆ ---       │
│ str          ┆ str      ┆ str  ┆ f64       │
╞══════════════╪══════════╪══════╪═══════════╡
│ S-TLF        ┆ 31/07/26 ┆      ┆ 2.4499e8  │
│ S-RCF        ┆ 31/07/26 ┆      ┆ 4.43539e7 │
│ S-BA F       ┆ 31/07/26 ┆      ┆ 4.9294e6  │
│ S-SM F       ┆ 31/07/26 ┆      ┆ 1.9831e8  │
│ S-GUARANTEE  ┆ 31/07/26 ┆      ┆ 5.7e6     │
│ S-REMISIERFD ┆ 31/07/26 ┆      ┆ 0.0       │
│ S-FIXED DEP  ┆ 31/07/26 ┆      ┆ 0.0       │
└──────────────┴──────────┴──────┴───────────┘

Processing Investment outputs...
Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIVMNLGL/GLRMP120260731.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 222527


61   
62   libname mylib    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIVMNLGL'  ;
NOTE: Libref MYLIB was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIVMNLGL
63   
Saved SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIVMNLGL/GLRMP120260731.sas7bdat
SAS Connection terminated. Subprocess id was 222527

GLRMP120260731:
shape: (4, 8)
┌───────┬────────────┬───────┬─────┬────────┬──────┬──────┬────────────┐
│ ITEM  ┆ WEEK       ┆ MONTH ┆ QTR ┆ HALFYR ┆ YEAR ┆ LAST ┆ BALANCE    │
│ ---   ┆ ---        ┆ ---   ┆ --- ┆ ---    ┆ ---  ┆ ---  ┆ ---        │
│ str   ┆ f64        ┆ f64   ┆ f64 ┆ f64    ┆ f64  ┆ f64  ┆ f64        │
╞═══════╪════════════╪═══════╪═════╪════════╪══════╪══════╪════════════╡
│ A1.37 ┆ -39662.418 ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ -39662.418 │
│ A1.35 ┆ -8870.78   ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ -8870.78   │
│ A1.38 ┆ -49983.808 ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ -49983.808 │
│ A1.36 ┆ -1140.0    ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ -1140.0    │
└───────┴────────────┴───────┴─────┴────────┴──────┴──────┴────────────┘
Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIVMNLGL/GLUTRMP120260731.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 222576


61   
62   libname mylib    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIVMNLGL'  ;
NOTE: Libref MYLIB was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIVMNLGL
63   
Saved SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIVMNLGL/GLUTRMP120260731.sas7bdat
SAS Connection terminated. Subprocess id was 222576

GLUTRMP120260731:
shape: (1, 8)
┌───────┬──────┬───────┬─────┬────────┬──────┬──────┬─────────┐
│ ITEM  ┆ WEEK ┆ MONTH ┆ QTR ┆ HALFYR ┆ YEAR ┆ LAST ┆ BALANCE │
│ ---   ┆ ---  ┆ ---   ┆ --- ┆ ---    ┆ ---  ┆ ---  ┆ ---     │
│ str   ┆ f64  ┆ f64   ┆ f64 ┆ f64    ┆ f64  ┆ f64  ┆ f64     │
╞═══════╪══════╪═══════╪═════╪════════╪══════╪══════╪═════════╡
│ A2.01 ┆ 0.0  ┆ 0.0   ┆ 0.0 ┆ 0.0    ┆ 0.0  ┆ 0.0  ┆ 0.0     │
└───────┴──────┴───────┴─────┴────────┴──────┴──────┴─────────┘

Processing complete!
