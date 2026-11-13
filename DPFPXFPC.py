======================================================================
1. WHICH FILE IS ACTUALLY BEING IMPORTED
======================================================================
  module file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMHPFS.py
  file mtime:  Fri Nov 13 15:17:40 2026
  file size:   46,561 bytes

======================================================================
2. WHICH FUNCTION IS DEFINED IN THE IMPORTED MODULE
======================================================================
  has parse_sas_date:    True
  has parse_z11_to_date: False

  --- Source of parse_sas_date ---
  def parse_sas_date(val) -> Optional[date]:
      """Parse a date from a SAS7BDAT column.
  
      Handles:
        - SAS numeric dates (days since 1960-01-01): 14633 -> 2000-01-20
        - Python date objects (passed through)
        - DDMMYY8 strings with YEARCUTOFF=1950
        - Null / NaN -> None
        - Garbage numeric values outside 0..60000 -> None
      """
      if val is None:
          return None
      if isinstance(val, float) and val != val:      # NaN
          return None
      if isinstance(val, date):
          return val
      if isinstance(val, (int, float)):
          try:
              days = int(val)
              if days < 0 or days > 60000:

======================================================================
3. DOES THE FUNCTION ACTUALLY WORK ON SAS DATES
======================================================================
  parse_sas_date(14633       ) = 2000-01-24
  parse_sas_date(23197       ) = 2023-07-06
  parse_sas_date(14435.0     ) = 1999-07-10
  parse_sas_date(0           ) = 1960-01-01
  parse_sas_date(None        ) = None

  Expected: 14633 -> 2000-01-20, 23197 -> 2023-07-05, 14435.0 -> 1999-07-11
            If any show 'None' for the numeric values, the fix is NOT loaded.

======================================================================
4. OTHER COPIES OF EIBMHPFS.py ON THE FILESYSTEM
======================================================================


======================================================================
DONE
======================================================================
