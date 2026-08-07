import pyreadstat

for name, path in [
    ("CAMV", "data/camv3107.sas7bdat"),
    ("FDMV", "data/fdmv3107.sas7bdat"),
]:
    df, meta = pyreadstat.read_sas7bdat(path, metadataonly=True)
    print(f"\n=== {name} ===")
    for col, dtype in zip(meta.column_names, meta.readstat_variable_types):
        print(f"{col}  ({dtype})")
