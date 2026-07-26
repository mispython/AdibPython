============================================================
EIMIR101 SAS to Python Conversion
============================================================

1. Processing REPTDATE (yesterday)...
   Report Date: 250726 (2026-07-25)

2. Loading data...
Loaded 663747 records from loantemp.sas7bdat
Columns: ['ACCTNO', 'NOTENO', 'CAP', 'NAME', 'LSTTRNCD', 'CURBAL', 'COLLDESC', 'CENSUS', 'ORGBAL', 'FEEDUE', 'LOANSTAT', 'BORSTAT', 'PAYAMT', 'BILDUE', 'BILTOT', 'BILPAY', 'LSTTRNAM', 'DELQCD', 'USER5', 'BLDATE', 'BALANCE', 'PRODUCT', 'BRANCH', 'ISSDTE', 'NOISTLPD', 'LASTRAN', 'MATURDT', 'THISDATE', 'CHECKDT', 'DAYDIFF', 'ARREAR2', 'ARREAR']
Loaded 0 records from LKP_BRANCH
   Loans: 663747, Branches: 0

3. Categorizing loans...
   Categorized records: 389474

4. Merging with branch data...
   Merged records: 389474

5. Generating Report A (EIMAR101-A)...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR101.py", line 545, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR101.py", line 472, in main
    summary_a = calculate_branch_summaries(report_a)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR101.py", line 191, in calculate_branch_summaries
    grouped = filtered.groupby(['CAT', 'BRANCH', 'ARREAR2']).agg({
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/groupby/generic.py", line 1432, in aggregate
    result = op.agg()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/apply.py", line 190, in agg
    return self.agg_dict_like()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/apply.py", line 423, in agg_dict_like
    return self.agg_or_apply_dict_like(op_name="agg")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/apply.py", line 1608, in agg_or_apply_dict_like
    result_index, result_data = self.compute_dict_like(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/apply.py", line 462, in compute_dict_like
    func = self.normalize_dictlike_arg(op_name, selected_obj, func)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/apply.py", line 663, in normalize_dictlike_arg
    raise KeyError(f"Column(s) {list(cols)} do not exist")
KeyError: "Column(s) ['ACCOUNT_NO', 'BRHCODE'] do not exist"
