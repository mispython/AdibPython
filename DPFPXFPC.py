SAS Connection established. Subprocess id is 3056649

EIMBNM01: Starting Public Bank Berhad loan summary reports...
  Report date: 2026-08-31  MM=08 YY=2026 WK=4
  loan_base rows: 2109074
  dispay_df rows: 1820970
Traceback (most recent call last):
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/ops/array_ops.py", line 218, in _na_arithmetic_op
    result = func(left, right)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expressions.py", line 242, in evaluate
    return _evaluate(op, op_str, a, b)  # type: ignore[misc]
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expressions.py", line 131, in _evaluate_numexpr
    result = _evaluate_standard(op, op_str, a, b)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expressions.py", line 73, in _evaluate_standard
    return op(a, b)
TypeError: can't multiply sequence by non-int of type 'float'

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMBNM01.py", line 1317, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMBNM01.py", line 951, in main
    bnm_loan = merge_loan_cl_fee(bnm_loan, cl_fee)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMBNM01.py", line 424, in merge_loan_cl_fee
    merged['clfee'] = (merged['duetotal'].fillna(0.0) *
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/ops/common.py", line 76, in new_method
    return method(self, other)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/arraylike.py", line 202, in __mul__
    return self._arith_method(other, operator.mul)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/series.py", line 6135, in _arith_method
    return base.IndexOpsMixin._arith_method(self, other, op)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/base.py", line 1382, in _arith_method
    result = ops.arithmetic_op(lvalues, rvalues, op)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/ops/array_ops.py", line 283, in arithmetic_op
    res_values = _na_arithmetic_op(left, right, op)  # type: ignore[arg-type]
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/ops/array_ops.py", line 227, in _na_arithmetic_op
    result = _masked_arith_op(left, right, op)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/ops/array_ops.py", line 163, in _masked_arith_op
    result[mask] = op(xrav[mask], yrav[mask])
TypeError: can't multiply sequence by non-int of type 'float'
SAS Connection terminated. Subprocess id was 3056649
