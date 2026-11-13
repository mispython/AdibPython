======================================================================
STEP 1 — which EIBMHPFS.py is Python importing?
======================================================================
======================================================================
STEP 1 — which EIBMHPFS.py is Python importing?
======================================================================
  module file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMHPFS.py

======================================================================
STEP 2 — does that file contain the fixed function?
======================================================================
  has parse_sas_date:    False   <- should be True
  has parse_z11_to_date: False   <- should be False

  >>> BUG CONFIRMED: the fixed function is NOT loaded.
  >>> The file you edited is not the file Python is importing,
      or the file was not saved, or .pyc cache is stale.

======================================================================
STEP 3 — what does the function source look like?
======================================================================

======================================================================
STEP 4 — sanity-test the function on your actual data values
======================================================================
  Test failed: partially initialized module 'EIBMHPFS' has no attribute 'parse_z11_to_date' (most likely due to a circular import)

======================================================================
DONE
======================================================================
  module file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMHPFS.py

======================================================================
STEP 2 — does that file contain the fixed function?
======================================================================
  has parse_sas_date:    False   <- should be True
  has parse_z11_to_date: False   <- should be False

  >>> BUG CONFIRMED: the fixed function is NOT loaded.
  >>> The file you edited is not the file Python is importing,
      or the file was not saved, or .pyc cache is stale.

======================================================================
STEP 3 — what does the function source look like?
======================================================================

======================================================================
STEP 4 — sanity-test the function on your actual data values
======================================================================
  Test failed: module 'EIBMHPFS' has no attribute 'parse_z11_to_date'

======================================================================
DONE
======================================================================
