EIMRESHI - HP Loan Summary & Detail Report
============================================================
============================================================
HP_PRODUCTS (from PBBLNFMT.HPD): [128, 130, 131, 132, 380, 381, 700, 705, 720, 725]
Report title products (SAS hardcoded): 128,130,380,381,700,705
Report Date: 30/09/2026
Week: 4  |  RDATE: 300926
LNNOTE file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m09.sas7bdat
LOANTEMP file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOI/loantemp_iccd2.sas7bdat
Output CCDTXT1: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOI/EIMRESHI_SUMMARY_ALL_CCDTXT1.txt
Output CCDTXT2: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOI/EIMRESHI_DETAIL_NPL_CCDTXT2.txt
============================================================

Reading loan data from SAS files...
  Reading loantemp.sas7bdat...
  LOANTEMP raw rows: 631,802
  LOANTEMP after filtering: 404,862 rows
  Reading LNNOTE (chunked, filtered as it streams)...
  Error: 'tuple' object has no attribute 'columns'
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHI.py", line 298, in <module>
    df_lnnote = read_lnnote_filtered(
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHI.py", line 193, in read_lnnote_filtered
    actual_cols = set(_first.columns.tolist())
AttributeError: 'tuple' object has no attribute 'columns'
