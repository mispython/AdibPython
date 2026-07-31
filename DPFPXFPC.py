Processing for month: 07 (Report Date: 2026-07-30)
Report Date based on: current date minus 1 day
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMTRUT.py", line 142, in <module>
    SA = apply_sas_format(SA, source_col="PRODUCT", format_dict=pbbdpfmt.SAPROD, out_col="PRODCD")
AttributeError: module 'PBBDPFMT' has no attribute 'SAPROD'
