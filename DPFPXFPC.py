Warning: PBBLNFMT module not found
============================================================
EIBMTLCR - Top Depositors Report
============================================================

Report Date: 31/07/2026
Report Month: 07
Exclusions: CIS=99, EQU=62

Processing M&I...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMTLCR.py", line 1071, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMTLCR.py", line 950, in main
    mni_sum, mni_detail = process_mni(rep_vars, excl_cis)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMTLCR.py", line 149, in process_mni
    cmm = pl.concat([cmm, vostro])
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.ShapeError: unable to append to a DataFrame of width 26 with a DataFrame of width 10


the PBBLNFMT.py is stored in the same path as the program run in.
