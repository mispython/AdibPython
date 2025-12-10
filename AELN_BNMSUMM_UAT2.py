this is the path file

# Base paths
BASE_INPUT_PATH = Path("/pythonITD/mis_dev/source_data")
BASE_OUTPUT_PATH = Path("/host/mis/parquet")
BASE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# File paths
REPTDATE_FILE = BASE_INPUT_PATH / "reptdate.sas7bdat"
EGOLD_FILE = BASE_INPUT_PATH / "EGOLD.txt"
OTHER_FILE = BASE_INPUT_PATH / "OTHR.txt"

below are the error



Traceback (most recent call last):
  File "/pythonITD/mis_dev/sas_migration/EGOLD/EIBDEGLD.py", line 17, in <module>
    REPTDATE_df = pl.read_sas(REPTDATE_FILE)
  File "/pythonITD/mis_dev/lib64/python3.9/site-packages/polars/__init__.py", line 487, in __getattr__
    raise AttributeError(msg)
AttributeError: module 'polars' has no attribute 'read_sas'
