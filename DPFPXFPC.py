[    0.00s] computed date macros
[   41.98s] read MNITB_CURRENT (1132084 rows)
[    0.02s] filtered CA (0 rows)
[   14.01s] read LIMIT_OVERDFT (1093140 rows)
[    0.18s] parsed ODLMT (154889 rows)
[    0.02s] joined ODLMT (0 rows)
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDP169.py", line 254, in <module>
    gp3 = read_fixed_width(
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDP169.py", line 152, in read_fixed_width
    text = raw_bytes.decode(encoding, errors="replace")
LookupError: unknown encoding: utf8-lossy
