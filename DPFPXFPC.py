REPTDATE: 2026-08-31 10:22:14.191742
REPTMON: 08
REPTYEAR2: 26
REPTDAY: 31
NOWK: 4
TESTING MODE: Reading max 50000 rows per file

Reading CRFTABL...
Read 50000 rows from crftabl.txt (limited to 50000)
CRFT records after filter: 50000

Reading MAST file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/btmast08426.sas7bdat
Read 50000 rows from btmast08426.sas7bdat (limited to 50000)
Warning: Could not convert acctno to integer: 'ExprStringNameSpace' object has no attribute 'strip'
MAST columns: ['custcode', 'ficode', 'acctno', 'postcode', 'tfid', 'custcodx', 'retailid', 'state', 'score1', 'score2', 'busregn', 'birthdtx', 'sector', 'settled', 'sm_status', 'ia_lru', 'sm_date', 'review_dt', 'acct_block_ind', 'acct_block_reason', 'ascore_ltst', 'ascore_previous', 'industrial_sector_cd', 'legal_action_cd', 'legal_action_dt', 'e_invoice_ind', 'ccpt_ltst_review_dt', 'fdb_tag', 'fdb_tag_dt', 'fdb_scoring_dt', 'acct_write_off_status', 'dnbfisme', 'birthdt', 'apvdate', 'subacct', 'btrel', 'intrecv', 'dcurbal', 'icurbal', 'dbalance', 'collater', 'branch', 'applcode', 'ccrisfac', 'tranxmt', 'currency', 'apprlimt', 'aa_num', 'sindicat', 'ori_aalimit', 'climate_prin_taxonomy_class', 'source_income_currency_cd', 'aa_approved_dt', 'fac_block_ind', 'fac_block_reason', 'levels', 'keyfaci', 'level1', 'level2', 'level3', 'level4', 'level5', 'd', 'i', 'dirctind', 'custcd', 'issdte', 'exprdate', 'dcurbalx', 'icurbalx', 'dundrawn', 'iundrawn', 'origmt', 'sectorcd', 'sectorx', 'sectorz', 'sectorzz', 'amtind', 'acctnox']
MAST acctno dtype: String
CRFT acctno dtype: Int64
MAST unique acctno records: 18301
CRFT records after MAST join: 23367
CRFT final records: 23367

Reading MNITB.CURRENT: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/intg_dp_acct_current_m08.sas7bdat
Read 50000 rows from intg_dp_acct_current_m08.sas7bdat (limited to 50000)
CA records: 49996

Reading MNILN.LNNOTE: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m08.sas7bdat
Read 50000 rows from enrh_ln_note_m08.sas7bdat (limited to 50000)
LN records: 49964

Reading COLL and DESC files...
Warning: COLL binary file reading needs implementation for packed decimal format
File: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831
Warning: DESC file reading needs implementation for fixed-width format
File: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831
COLL records after merge: 0

Combining CA, LN, CRFT...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRCGCS.py", line 430, in <module>
    aaa = pl.concat(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.SchemaError: type Float64 is incompatible with expected type Int32




also note that LCCRISEX and LCCRISEX_DESC are both uppercase for the file naming
