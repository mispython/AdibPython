# Check if SAS is accessible
ls -la /host/mis/parquet/crm/year=2025/month=11/otc_detail/

# Check file permissions
ls -ld /host/mis/parquet/crm/year=2025/month=11/

# Check if any SAS files exist anywhere
find /host/mis/parquet/crm -name "*.sas7bdat" -type f 2>/dev/null
