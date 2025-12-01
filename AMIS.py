1       import polars as pl
2       import os
3       import saspy
4       from datetime import datetime, timedelta
5       from pathlib import Path
6       sas = saspy.SASsession() if saspy else None
Using SAS Config named: default
SAS Connection established. Subprocess id is 89728

7       REPTDATE = datetime.today() - timedelta(days=1)
8       print(f"PROCESSING DATE: {REPTDATE:%Y-%m-%d}")
PROCESSING DATE: 2025-11-30
9       print(f"CURRENT DIRECTORY: {os.getcwd()}")
CURRENT DIRECTORY: /sas/pythonITD/mis
10      print(f"USER: {os.getenv('USER')}")
USER: sas
11      BASE_PATH = "/host/mis/parquet/crm"
12      CURRENT_MONTH_PATH = f"{BASE_PATH}/year={REPTDATE.year}/month={REPTDATE.month:02d}"
13      print(f"\nPATH CHECK:")

PATH CHECK:
14      print(f"BASE_PATH: {BASE_PATH}")
BASE_PATH: /host/mis/parquet/crm
15      print(f"CURRENT_MONTH_PATH: {CURRENT_MONTH_PATH}")
CURRENT_MONTH_PATH: /host/mis/parquet/crm/year=2025/month=11
16      print(f"\nDIRECTORY CHECK:")

DIRECTORY CHECK:
17      print(f"BASE_PATH exists: {os.path.exists(BASE_PATH)}")
BASE_PATH exists: True
18      print(f"CURRENT_MONTH_PATH exists: {os.path.exists(CURRENT_MONTH_PATH)}")
CURRENT_MONTH_PATH exists: True
19      if not os.path.exists(CURRENT_MONTH_PATH):
20      print(f"\nPERMISSIONS CHECK:")

PERMISSIONS CHECK:
21      if os.path.exists(CURRENT_MONTH_PATH):
22          print(f"Can read: {os.access(CURRENT_MONTH_PATH, os.R_OK)}")
Can read: True
23          print(f"Can write: {os.access(CURRENT_MONTH_PATH, os.W_OK)}")
Can write: True
24          print(f"Can execute: {os.access(CURRENT_MONTH_PATH, os.X_OK)}")
Can execute: True
25      print(f"\nEXISTING FILES in {CURRENT_MONTH_PATH}:")

EXISTING FILES in /host/mis/parquet/crm/year=2025/month=11:
26      if os.path.exists(CURRENT_MONTH_PATH):
27          files = os.listdir(CURRENT_MONTH_PATH)
28          for file in sorted(files):
29              filepath = os.path.join(CURRENT_MONTH_PATH, file)
30              if os.path.isfile(filepath):
31                  size = os.path.getsize(filepath)
32                  modified = datetime.fromtimestamp(os.path.getmtime(filepath))
33                  print(f"  {file} ({size:,} bytes, modified: {modified:%Y-%m-%d %H:%M:%S})")
  CHANNEL_SUM.parquet (1,535 bytes, modified: 2025-12-01 20:46:49)
  CHANNEL_UPDATE.parquet (3,113 bytes, modified: 2025-12-01 19:30:39)
  OTC_DETAIL.parquet (4,718 bytes, modified: 2025-12-01 21:38:10)
  otc_detail_1125.sas7bdat (131,072 bytes, modified: 2025-12-01 21:38:10)
34      print(f"\n" + "=" * 80)

================================================================================
35      print("TESTING FILE WRITE")
TESTING FILE WRITE
36      print("=" * 80)
================================================================================
37      test_channel = pl.DataFrame({
38          "CHANNEL": ["ATM", "EBK", "OTC"],
39          "TOLPROMPT": [100, 200, 300],
40          "TOLUPDATE": [10, 20, 30],
41          "MONTH": ["NOV25", "NOV25", "NOV25"]
42      test_update = pl.DataFrame({
43          "DESC": ["TOTAL PROMPT BASE", "TOTAL UPDATED"],
44          "ATM": [1000, 2000],
45          "EBK": [500, 600],
46          "OTC": [300, 400],
47          "TOTAL": [1800, 3000],
48          "DATE": ["30/11/2025", "30/11/2025"]
49      test_otc = pl.DataFrame({
50          "BRANCHNO": list(range(2, 10)),
51          "TOLPROMPT": [10, 20, 30, 40, 50, 60, 70, 80],
52          "TOLUPDATE": [1, 2, 3, 4, 5, 6, 7, 8]
53      channel_path = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
54      update_path = f"{CURRENT_MONTH_PATH}/CHANNEL_UPDATE.parquet"
55      otc_path = f"{CURRENT_MONTH_PATH}/OTC_DETAIL.parquet"
56      print(f"\nWRITING TEST FILES:")

WRITING TEST FILES:
57      print(f"1. {channel_path}")
1. /host/mis/parquet/crm/year=2025/month=11/CHANNEL_SUM.parquet
58      print(f"2. {update_path}")
2. /host/mis/parquet/crm/year=2025/month=11/CHANNEL_UPDATE.parquet
59      print(f"3. {otc_path}")
3. /host/mis/parquet/crm/year=2025/month=11/OTC_DETAIL.parquet
60      try:
61          test_channel.write_parquet(channel_path)
62          print(f"✓ CHANNEL_SUM written: {channel_path}")
✓ CHANNEL_SUM written: /host/mis/parquet/crm/year=2025/month=11/CHANNEL_SUM.parquet
63          print(f"  File size: {os.path.getsize(channel_path):,} bytes")
  File size: 1,984 bytes
64      try:
65          test_update.write_parquet(update_path)
66          print(f"✓ CHANNEL_UPDATE written: {update_path}")
✓ CHANNEL_UPDATE written: /host/mis/parquet/crm/year=2025/month=11/CHANNEL_UPDATE.parquet
67          print(f"  File size: {os.path.getsize(update_path):,} bytes")
  File size: 4,962 bytes
68      try:
69          test_otc.write_parquet(otc_path)
70          print(f"✓ OTC_DETAIL written: {otc_path}")
✓ OTC_DETAIL written: /host/mis/parquet/crm/year=2025/month=11/OTC_DETAIL.parquet
71          print(f"  File size: {os.path.getsize(otc_path):,} bytes")
  File size: 1,451 bytes
72      print(f"\n" + "=" * 80)

================================================================================
73      print("VERIFYING FILE CREATION")
VERIFYING FILE CREATION
74      print("=" * 80)
================================================================================
75      print(f"\nCHECKING FILES in {CURRENT_MONTH_PATH}:")

CHECKING FILES in /host/mis/parquet/crm/year=2025/month=11:
76      if os.path.exists(CURRENT_MONTH_PATH):
77          files = os.listdir(CURRENT_MONTH_PATH)
78          parquet_files = [f for f in files if f.endswith('.parquet')]
79          if parquet_files:
80              print(f"Found {len(parquet_files)} Parquet files:")
Found 3 Parquet files:
81              for file in sorted(parquet_files):
82                  filepath = os.path.join(CURRENT_MONTH_PATH, file)
83                  if os.path.isfile(filepath):
84                      size = os.path.getsize(filepath)
85                      created = datetime.fromtimestamp(os.path.getctime(filepath))
86                      print(f"  ✓ {file} ({size:,} bytes, created: {created:%H:%M:%S})")
  ✓ CHANNEL_SUM.parquet (1,984 bytes, created: 21:40:22)
  ✓ CHANNEL_UPDATE.parquet (4,962 bytes, created: 21:40:22)
  ✓ OTC_DETAIL.parquet (1,451 bytes, created: 21:40:22)
87          sas_files = [f for f in files if f.endswith('.sas7bdat')]
88          if sas_files:
89              print(f"\nFound {len(sas_files)} SAS files:")

Found 1 SAS files:
90              for file in sorted(sas_files):
91                  filepath = os.path.join(CURRENT_MONTH_PATH, file)
92                  size = os.path.getsize(filepath)
93                  print(f"  ⚡ {file} ({size:,} bytes)")
  ⚡ otc_detail_1125.sas7bdat (131,072 bytes)
94      print(f"\n" + "=" * 80)

================================================================================
95      print("CHECKING PARENT DIRECTORIES")
CHECKING PARENT DIRECTORIES
96      print("=" * 80)
================================================================================
97      month_dir = f"{BASE_PATH}/year={REPTDATE.year}"
98      print(f"\nFiles in {month_dir}:")

Files in /host/mis/parquet/crm/year=2025:
99      if os.path.exists(month_dir):
100         months = os.listdir(month_dir)
101         for month in sorted(months):
102             month_path = os.path.join(month_dir, month)
103             if os.path.isdir(month_path):
104                 files = os.listdir(month_path)
105                 parquet_count = len([f for f in files if f.endswith('.parquet')])
106                 sas_count = len([f for f in files if f.endswith('.sas7bdat')])
107                 print(f"  {month}: {parquet_count} Parquet, {sas_count} SAS files")
  month=10: 2 Parquet, 0 SAS files
  month=11: 3 Parquet, 1 SAS files
108     print(f"\n" + "=" * 80)

================================================================================
109     print("DIAGNOSTIC SUMMARY")
DIAGNOSTIC SUMMARY
110     print("=" * 80)
================================================================================
111     print(f"\nISSUES TO CHECK:")

ISSUES TO CHECK:
112     print(f"1. File permissions on {CURRENT_MONTH_PATH}")
1. File permissions on /host/mis/parquet/crm/year=2025/month=11
113     print(f"2. Disk space on /host/mis/parquet/crm/")
2. Disk space on /host/mis/parquet/crm/
114     print(f"3. SAS overwriting Parquet files during transfer")
3. SAS overwriting Parquet files during transfer
115     print(f"4. Wrong path being used")
4. Wrong path being used
116     print(f"\nQUICK FIXES:")

QUICK FIXES:
117     print(f"1. Run: ls -la {CURRENT_MONTH_PATH}/")
1. Run: ls -la /host/mis/parquet/crm/year=2025/month=11/
118     print(f"2. Run: df -h /host/mis/parquet/crm/")
2. Run: df -h /host/mis/parquet/crm/
119     print(f"3. Check if SAS library path conflicts with Parquet path")
3. Check if SAS library path conflicts with Parquet path
120     print(f"4. Add delay between Parquet write and SAS transfer")
4. Add delay between Parquet write and SAS transfer
121     print(f"\n" + "=" * 80)

