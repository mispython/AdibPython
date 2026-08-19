============================================================
EIBQINST - Trustee and Client Account Quarterly Reporting
============================================================

Report Date: 2026-07-31 (Week: 4)

Processing Trustee Accounts...
  FLOAT: 18927 records loaded
  IBGPIDM: 7609 records loaded
Error loading REMIT/UNCLAIM: unable to vstack, column names don't match: "acctno" and "paymode"
  REMIT: 0 records loaded
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQINST.py", line 648, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQINST.py", line 403, in main
    saca = pl.concat(dfs_to_concat)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.ShapeError: unable to vstack, column names don't match: "purpose" and "product"
