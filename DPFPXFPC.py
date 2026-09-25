python -c "
import pyarrow.parquet as pq
pf = pq.ParquetFile('/stgsrcsys/host/holding/DPDARPGS_FB_20260924.parquet')
print('rows       :', pf.metadata.num_rows)
print('row groups :', pf.num_row_groups)
print('schema     :', pf.schema_arrow)
"
