Starting LONPAC data processing...
Reading from: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMLNPC
Writing to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMLNPC
Processing PA data...
sys:1: UserWarning: CSV malformed: expected 8 rows, actual 16 rows, in chunk starting at byte offset 2724628, length 24032
sys:1: UserWarning: CSV malformed: expected 58 rows, actual 113 rows, in chunk starting at byte offset 2554902, length 169726
sys:1: UserWarning: CSV malformed: expected 58 rows, actual 113 rows, in chunk starting at byte offset 2385176, length 169726
sys:1: UserWarning: CSV malformed: expected 57 rows, actual 113 rows, in chunk starting at byte offset 2215450, length 169726
sys:1: UserWarning: CSV malformed: expected 60 rows, actual 114 rows, in chunk starting at byte offset 2044222, length 171228
sys:1: UserWarning: CSV malformed: expected 56 rows, actual 114 rows, in chunk starting at byte offset 1872994, length 171228
sys:1: UserWarning: CSV malformed: expected 55 rows, actual 113 rows, in chunk starting at byte offset 1703268, length 169726
sys:1: UserWarning: CSV malformed: expected 53 rows, actual 114 rows, in chunk starting at byte offset 1532040, length 171228
sys:1: UserWarning: CSV malformed: expected 59 rows, actual 113 rows, in chunk starting at byte offset 1362314, length 169726
sys:1: UserWarning: CSV malformed: expected 56 rows, actual 113 rows, in chunk starting at byte offset 1192588, length 169726
sys:1: UserWarning: CSV malformed: expected 56 rows, actual 113 rows, in chunk starting at byte offset 1022862, length 169726
sys:1: UserWarning: CSV malformed: expected 54 rows, actual 113 rows, in chunk starting at byte offset 853136, length 169726
sys:1: UserWarning: CSV malformed: expected 57 rows, actual 114 rows, in chunk starting at byte offset 681908, length 171228
sys:1: UserWarning: CSV malformed: expected 57 rows, actual 114 rows, in chunk starting at byte offset 510680, length 171228
sys:1: UserWarning: CSV malformed: expected 58 rows, actual 114 rows, in chunk starting at byte offset 339452, length 171228
sys:1: UserWarning: CSV malformed: expected 55 rows, actual 113 rows, in chunk starting at byte offset 169726, length 169726
sys:1: UserWarning: CSV malformed: expected 57 rows, actual 113 rows, in chunk starting at byte offset 0, length 169726
Error during processing: module 'pyreadstat' has no attribute 'write_sas7bdat'
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMLNPC.py", line 519, in process_lonpac_data
    process_pa_data(input_path, output_path, reptyear4)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMLNPC.py", line 181, in process_pa_data
    write_sas_dataset(df_prod, output_dir / "paprod.sas7bdat")
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMLNPC.py", line 79, in write_sas_dataset
    pyreadstat.write_sas7bdat(
AttributeError: module 'pyreadstat' has no attribute 'write_sas7bdat'


MAYBE COULD TRY SASPY TO WRITE SAS DAATSET
