2026-11-13 14:59:38 | INFO    | ==============================================================================
2026-11-13 14:59:38 | INFO    | EIBDAWSA START
2026-11-13 14:59:38 | INFO    | ==============================================================================
2026-11-13 14:59:38 | INFO    | Report date : 12/11/2026
2026-11-13 14:59:38 | INFO    | Input       : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDAWSA/DPTRBLGS.parquet
2026-11-13 14:59:38 | INFO    | Output dir  : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA
2026-11-13 14:59:38 | INFO    | Target      : MISOUT.AWSA11
2026-11-13 14:59:38 | INFO    | Mode        : replace
2026-11-13 14:59:38 | INFO    | Initialising DuckDB (mem=12GB, threads=8)
100% ▕██████████████████████████████████████▏ (00:00:02.00 elapsed)     
 99% ▕█████████████████████████████████████▌▏ (<1 second remaining)     2026-11-13 14:59:40 | INFO    | Extracted 8006 rows (products [93, 204, 207, 215])
2026-11-13 14:59:40 | INFO    | ALWSA/ALWSS pass: 8006 rows
2026-11-13 14:59:41 | INFO    | ALWBC: no rows
2026-11-13 14:59:41 | INFO    | Combined ALWSA+ALWBC: 8006 rows
2026-11-13 14:59:41 | INFO    | Aggregating 8006 rows …
2026-11-13 14:59:41 | INFO    | Aggregated to 456 rows
2026-11-13 14:59:41 | INFO    | Reconciliation OK: 8006 accounts, RM 26,214,065.09
2026-11-13 14:59:41 | INFO    | ------------------------------------------------------------------------------
2026-11-13 14:59:41 | INFO    | Total accounts     : 8006
2026-11-13 14:59:41 | INFO    | Total balance      : RM 26,214,065.09
2026-11-13 14:59:41 | INFO    | Total average amt  : RM 33,055,595.00
2026-11-13 14:59:41 | INFO    | New accts this year: 0
2026-11-13 14:59:41 | INFO    |   Product 204      : 3237
2026-11-13 14:59:41 | INFO    |   Product 207      : 160
2026-11-13 14:59:41 | INFO    |   Product 215      : 4609
2026-11-13 14:59:41 | INFO    | ------------------------------------------------------------------------------
2026-11-13 14:59:41 | INFO    | Connecting to SAS (cfg=default) …
SAS Connection established. Subprocess id is 1716492


21   
22   libname MISOUT    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA'  ;
NOTE: Libref MISOUT was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA
23   
2026-11-13 14:59:41 | INFO    | Replacing MISOUT.AWSA11
INFO:EIBDAWSA:Replacing MISOUT.AWSA11
SAS Connection terminated. Subprocess id was 1716492
2026-11-13 14:59:44 | ERROR   | EIBDAWSA failed: Expected file not on disk: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA/AWSA11.sas7bdat
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDAWSA.py", line 495, in main
    target = write_sas(agg, table_name, cfg, logger, append=args.append)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDAWSA.py", line 419, in write_sas
    raise RuntimeError(f"Expected file not on disk: {target}")
RuntimeError: Expected file not on disk: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA/AWSA11.sas7bdat
ERROR:EIBDAWSA:EIBDAWSA failed: Expected file not on disk: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA/AWSA11.sas7bdat
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDAWSA.py", line 495, in main
    target = write_sas(agg, table_name, cfg, logger, append=args.append)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDAWSA.py", line 419, in write_sas
    raise RuntimeError(f"Expected file not on disk: {target}")
RuntimeError: Expected file not on disk: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA/AWSA11.sas7bdat
