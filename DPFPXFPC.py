PBBDPFMT imported successfully
============================================================
EIBQINST - Trustee and Client Account Quarterly Reporting
============================================================

Report Date: 2026-07-31 (Week: 4)

Loading data...
  FLOAT: 18927 records
  IBGPIDM: 7609 records
  REMIT/UNCLAIM: 6385 records
  DEP: 920763 records
  CLIENT: 3338 records

============================================================
Processing Trustee Accounts...
============================================================
  Trustee SA/CA/FD (with purpose filter): 162 records

Trustee >60k: 37 accounts
Trustee <=60k: 6 accounts

Writing Trustee output files...
  Written to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQINST/trustee_high.txt
  Written to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQINST/trustee_low.txt

TRUSTEE >60000 by Branch:
  Branch 2.0: RM 6,034,191.79
  Branch 4.0: RM 1,614,488.67
  Branch 18.0: RM 938,255.52
  Branch 168.0: RM 3,752,676.82
  Branch 196.0: RM 10,235,196.87

TRUSTEE <=60000 by Branch:
  Branch 18.0: RM 105,384.65
  Branch 168.0: RM 39,798.43
  Branch 196.0: RM 27,266.23

============================================================
Processing Client Accounts...
============================================================
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQINST.py", line 835, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQINST.py", line 669, in main
    saca_client_df = load_saca_client()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQINST.py", line 390, in load_saca_client
    result = pl.concat(dfs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.SchemaError: type String is incompatible with expected type Null
