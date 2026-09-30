REPTMON=09, NOWK=4, REPTYEAR=2026, RDATE=29/09/26, SDATE=23/09/26
L124PBBD: reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1/loan094.sas7bdat ...
SAS Connection established. Subprocess id is 3216093

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1839: UserWarning: Note that Indexes are not transferred over as columns. Only actual columns are transferred
  warnings.warn("Note that Indexes are not transferred over as columns. Only actual columns are transferred")
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS Connection terminated. Subprocess id was 3216093
L124 written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/l124094.sas7bdat  (0 rows)
L124PBBD: reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1/uloan094.sas7bdat ...
SAS Connection established. Subprocess id is 3216141

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1839: UserWarning: Note that Indexes are not transferred over as columns. Only actual columns are transferred
  warnings.warn("Note that Indexes are not transferred over as columns. Only actual columns are transferred")
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS Connection terminated. Subprocess id was 3216141
UL124 written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/ul124094.sas7bdat  (0 rows)
Traceback (most recent call last):
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/scope.py", line 231, in resolve
    return self.resolvers[key]
  File "/usr/lib64/python3.9/collections/__init__.py", line 941, in __getitem__
    return self.__missing__(key)            # support subclasses that define __missing__
  File "/usr/lib64/python3.9/collections/__init__.py", line 933, in __missing__
    raise KeyError(key)
KeyError: 'entity_cd'

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/scope.py", line 242, in resolve
    return self.temps[key]
KeyError: 'entity_cd'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWP124.py", line 214, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWP124.py", line 188, in main
    run_lalwp124()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/LALWP124.py", line 155, in main
    loan_df  = read_sas7bdat(l124_path,  where="entity_cd == 'PIBB'")
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/LALWP124.py", line 89, in read_sas7bdat
    df = df.query(where)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/frame.py", line 4823, in query
    res = self.eval(expr, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/frame.py", line 4949, in eval
    return _eval(expr, inplace=inplace, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/eval.py", line 336, in eval
    parsed_expr = Expr(expr, engine=engine, parser=parser, env=env)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 805, in __init__
    self.terms = self.parse()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 824, in parse
    return self._visitor.visit(self.expr)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 411, in visit
    return visitor(node, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 417, in visit_Module
    return self.visit(expr, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 411, in visit
    return visitor(node, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 420, in visit_Expr
    return self.visit(node.value, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 411, in visit
    return visitor(node, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 715, in visit_Compare
    return self.visit(binop)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 411, in visit
    return visitor(node, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 531, in visit_BinOp
    op, op_class, left, right = self._maybe_transform_eq_ne(node)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 451, in _maybe_transform_eq_ne
    left = self.visit(node.left, side="left")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 411, in visit
    return visitor(node, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 541, in visit_Name
    return self.term_type(node.id, self.env, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/ops.py", line 91, in __init__
    self._value = self._resolve_name()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/ops.py", line 115, in _resolve_name
    res = self.env.resolve(local_name, is_local=is_local)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/scope.py", line 244, in resolve
    raise UndefinedVariableError(key, is_local) from err
pandas.errors.UndefinedVariableError: name 'entity_cd' is not defined
