Starting EIQPROM2 processing...
Report Date: 31/07/26
Report Month: 07

Step 1: Loading and filtering PROMOTE.LOAN data...
  Available columns in LOAN: ['ACCTNO', 'NAME', 'GUAREND', 'NOTENO', 'ORGBAL', 'NETPROC', 'SPREAD', 'FLAG1', 'BORSTAT', 'COLLMAKE', 'NTINDEX', 'SCORE1', 'COLLYEAR', 'DELQCD', 'MODELDES', 'MAILCODE', 'IA_LRU', 'AANO', 'EXPRDATE', 'COMMNO', 'PRODUCT', 'BRANCH', 'ISSDTE', 'APPRLIMT', 'BALANCE', 'DAYDIFF', 'REMTERM', 'NOTE1', 'NOTE2', 'DAYSLATE', 'BLDAT', 'BLPDDAT', 'DAYS', 'CORGAMT', 'LMTAPPR', 'REPAID', 'BRCH', 'CPRLANDU', 'CPRPROPD', 'HOLDEXPD', 'MRESERVE', 'EXPDT', 'EXCL', 'CUSTNO', 'SECCUST', 'NEWIC', 'EMAILADD', 'REINPROD', 'NEW']
  Records in RLSLIST: 55773
  RLSLIST columns: ['ACCTNO', 'NAME', 'GUAREND', 'NOTENO', 'ORGBAL', 'NETPROC', 'SPREAD', 'FLAG1', 'BORSTAT', 'COLLMAKE', 'NTINDEX', 'SCORE1', 'COLLYEAR', 'DELQCD', 'MODELDES', 'MAILCODE', 'IA_LRU', 'AANO', 'EXPRDATE', 'COMMNO', 'PRODUCT', 'BRANCH', 'ISSDTE', 'APPRLIMT', 'BALANCE', 'DAYDIFF', 'REMTERM', 'NOTE1', 'NOTE2', 'DAYSLATE', 'BLDAT', 'BLPDDAT', 'DAYS', 'CORGAMT', 'LMTAPPR', 'REPAID', 'BRCH', 'CPRLANDU', 'CPRPROPD', 'HOLDEXPD', 'MRESERVE', 'EXPDT', 'EXCL', 'CUSTNO', 'SECCUST', 'NEWIC', 'EMAILADD', 'REINPROD', 'NEW']

Step 2: Processing PBB data...
  LNNAME columns: ['NAMELN1', 'NAMELN2', 'NAMELN3', 'NAMELN4', 'NAMELN5', 'ACCTNO', 'SECPHONE', 'PRIPHONE']
  Records in PBBNAME after merge: 50173
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIEMCRLS.py", line 210, in <module>
    mailpbb = pbbname.filter(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 5325, in filter
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "MAILCODE"; valid columns: ["NAMELN1", "NAMELN2", "NAMELN3", "NAMELN4", "NAMELN5", "ACCTNO", "SECPHONE", "PRIPHONE", "GUAREND", "ID", "MASK_IDS"]


need anything from my end?
