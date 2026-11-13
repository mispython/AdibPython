2026-11-13 15:06:41 | INFO    | ==============================================================================
2026-11-13 15:06:41 | INFO    | EIBDAWSA START
2026-11-13 15:06:41 | INFO    | ==============================================================================
2026-11-13 15:06:41 | INFO    | Report date : 12/11/2026
2026-11-13 15:06:41 | INFO    | Input       : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDAWSA/DPTRBLGS.parquet
2026-11-13 15:06:41 | INFO    | Output dir  : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA
2026-11-13 15:06:41 | INFO    | Target      : MISOUT.awsa11
2026-11-13 15:06:41 | INFO    | Mode        : replace
2026-11-13 15:06:41 | INFO    | Initialising DuckDB (mem=12GB, threads=8)
100% ▕██████████████████████████████████████▏ (00:00:02.50 elapsed)     
 99% ▕█████████████████████████████████████▌▏ (<1 second remaining)     2026-11-13 15:06:44 | INFO    | Extracted 8006 rows (products [93, 204, 207, 215])
2026-11-13 15:06:44 | INFO    | ALWSA/ALWSS pass: 8006 rows
2026-11-13 15:06:44 | INFO    | ALWBC pass: 0 rows
2026-11-13 15:06:44 | INFO    | Combined ALWSA+ALWBC: 8006 rows
2026-11-13 15:06:44 | INFO    | Aggregating 8006 rows …
2026-11-13 15:06:44 | INFO    | Aggregated to 456 rows
2026-11-13 15:06:44 | INFO    | Reconciliation OK: 8006 accounts, RM 26,214,065.09
2026-11-13 15:06:44 | INFO    | ------------------------------------------------------------------------------
2026-11-13 15:06:44 | INFO    | Total accounts     : 8006
2026-11-13 15:06:44 | INFO    | Total balance      : RM 26,214,065.09
2026-11-13 15:06:44 | INFO    | Total average amt  : RM 33,055,595.00
2026-11-13 15:06:44 | INFO    | New accts this year: 0
2026-11-13 15:06:44 | INFO    |   Product 204      : 3237
2026-11-13 15:06:44 | INFO    |   Product 207      : 160
2026-11-13 15:06:44 | INFO    |   Product 215      : 4609
2026-11-13 15:06:44 | INFO    | ------------------------------------------------------------------------------
2026-11-13 15:06:45 | INFO    | Connecting to SAS (cfg=default) …
SAS Connection established. Subprocess id is 1945968


21   
22   libname MISOUT    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA'  ;
NOTE: Libref MISOUT was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA
23   
2026-11-13 15:06:45 | INFO    | Libref MISOUT → /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA
2026-11-13 15:06:45 | INFO    | Ensuring prior MISOUT.awsa11 is removed (if present)
2026-11-13 15:06:45 | INFO    | df2sd → MISOUT.awsa11 (456 rows)
2026-11-13 15:06:45 | INFO    | SAS confirms MISOUT.awsa11 exists
SAS Connection terminated. Subprocess id was 1945968
2026-11-13 15:06:48 | INFO    | SAS dataset written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA/awsa11.sas7bdat (131072 bytes)
2026-11-13 15:06:48 | INFO    | EIBDAWSA END — success. Output: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA/awsa11.sas7bdat
