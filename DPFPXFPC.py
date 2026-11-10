EIIDMELW - Islamic BNM EL Daily Deposit Extract
============================================================

Reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDMELW/DPTRBLGS.parquet ...
Error parsing TBDATE: Unsupported TBDATE type: <class 'float'>


btw ive changed the PBBDPFMT to PBBDPFMT_AII, the correct one

BNM_DIR         = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDMELW/'          # OUTPUT directory
DPTRBL_PARQUET  = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDMELW/DPTRBLGS.parquet'   # INPUT

and above is the updated dir
