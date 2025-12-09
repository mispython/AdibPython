Traceback (most recent call last):
  File "/pythonITD/mis_dev/sas_migration/CIGR/output/EIBWCIGR.py", line 65, in <module>
    cisbasel_df = read_fixed_width_file(CIS_FILE, cisbasel_columns, cisbasel_widths, cisbasel_dtypes)
  File "/pythonITD/mis_dev/sas_migration/CIGR/output/EIBWCIGR.py", line 26, in read_fixed_width_file
    lines = [line.rstrip("\n") for line in f]
  File "/pythonITD/mis_dev/sas_migration/CIGR/output/EIBWCIGR.py", line 26, in <listcomp>
    lines = [line.rstrip("\n") for line in f]
  File "/usr/lib64/python3.9/codecs.py", line 322, in decode
    (result, consumed) = self._buffer_decode(data, self.errors, final)
UnicodeDecodeError: 'utf-8' codec can't decode byte 0xbe in position 7: invalid start byte
