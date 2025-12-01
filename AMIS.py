(mis) [sas@svdwh001 mis]$ ls -la /host/mis/parquet/crm/year=2025/month=11/otc_detail/
total 0
drwxrwxr-x+ 2 sas sas  6 Dec  1 14:18 .
drwxrwxr-x+ 3 sas sas 81 Dec  1 14:18 ..
(mis) [sas@svdwh001 mis]$ ls -ld /host/mis/parquet/crm/year=2025/month=11/
drwxrwxr-x+ 3 sas sas 81 Dec  1 14:18 '/host/mis/parquet/crm/year=2025/month=11/'
(mis) [sas@svdwh001 mis]$ find /host/mis/parquet/crm -name "*.sas7bdat" -type f 2>/dev/null
(mis) [sas@svdwh001 mis]$
