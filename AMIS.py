1       import polars as pl
2       import os
3       import saspy
4       from datetime import datetime, timedelta
5       from pathlib import Path
6       sas = saspy.SASsession() if saspy else None
Using SAS Config named: default
SAS Connection established. Subprocess id is 4022620

7       REPTDATE = datetime.today() - timedelta(days=1)
8       REPTYEAR = f"{REPTDATE.year % 100:02d}"
9       REPTMON = f"{REPTDATE.month:02d}"
10      REPTDAY = f"{REPTDATE.day:02d}"
11      print(f"PROCESSING DATE: {REPTDATE:%Y-%m-%d}")
PROCESSING DATE: 2025-11-30
12      BASE_PATH = "/host/mis/parquet/crm"
13      CRMWH_PATH = f"{BASE_PATH}/CRMWH"
14      CURRENT_MONTH_PATH = f"{BASE_PATH}/year={REPTDATE.year}/month={REPTMON}"
15      os.makedirs(CRMWH_PATH, exist_ok=True)
16      os.makedirs(CURRENT_MONTH_PATH, exist_ok=True)
17      def read_bcode_file():
18      def process_channel_sum():
19      def process_otc_detail():
20      def process_channel_update():
21      def write_sas_dataset(df, output_path, dataset_name):
22      print("\n" + "=" * 60)

============================================================
23      print(f"PROCESSING FOR {REPTDATE:%Y-%m-%d}")
PROCESSING FOR 2025-11-30
24      print("=" * 60)
============================================================
25      print("\n>>> 1. CHANNEL SUMMARY (EIBMCHNL)")

>>> 1. CHANNEL SUMMARY (EIBMCHNL)
26      channel_df = process_channel_sum()
27          try:
28              channel_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_ALL_SUMMARY.parquet"
29              if not os.path.exists(channel_path):
30              df = pl.read_parquet(channel_path)
31              print(f"  Columns: {df.columns}")
  Columns: ['CHANNEL', 'PROMPT', 'UPDATED']
32              channel_df = df.select([
33                  pl.col("CHANNEL").str.to_uppercase().alias("CHANNEL"),
34                  pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT"),
35                  pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE")
36                  pl.lit(REPTDATE.strftime("%b%y").upper()).alias("MONTH")
37              print(f"  Records: {len(channel_df)}")
  Records: 3
38              return channel_df
39      print("\n>>> 2. CHANNEL UPDATE (EIBMCHN2)")

>>> 2. CHANNEL UPDATE (EIBMCHN2)
40      update_df = process_channel_update()
41          try:
42              update_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_FULL_SUMMARY.parquet"
43              if not os.path.exists(update_path):
44              df = pl.read_parquet(update_path)
45              print(f"  Columns: {df.columns}")
  Columns: ['LINE', 'ATM', 'EBK', 'OTC', 'TOTAL', 'REPORT_DATE']
46              update_df = df.head(2)
47              update_df = update_df.with_row_index().with_columns(
48                  pl.when(pl.col("index") == 0).then("TOTAL PROMPT BASE")
49                   .when(pl.col("index") == 1).then("TOTAL UPDATED")
50                   .alias("DESC")
51          except Exception as e:
52              print(f"  Error: {e}")
  Error: TOTAL PROMPT BASE
53              return pl.DataFrame()
54      print("\n>>> 3. OTC DETAIL (EIBMCHNL - MERGE)")

>>> 3. OTC DETAIL (EIBMCHNL - MERGE)
55      otc_detail = process_otc_detail()
56          try:
57              bcode_df = read_bcode_file()
58          records = []
59          try:
60              with open("/sasdata/rawdata/lookup/LKP_BRANCH", 'r') as f:
61                  for line in f:
62                      if line.strip():
63                          try:
64                              line_padded = line.ljust(80)
65                              branchno_str = line_padded[1:4].strip()
66                              if branchno_str:
67                                  branchno = int(branchno_str)
68                                  brstatus = line_padded[49:50].strip()
69                                  if brstatus in ['O', 'A']:
70                                      records.append({"BRANCHNO": branchno})
71              if records:
72                  df = pl.DataFrame(records).unique().sort("BRANCHNO")
73                  print(f"  BCODE active branches: {len(df)}")
  BCODE active branches: 269
74                  print(f"  BRANCHNO range: {df['BRANCHNO'].min()} to {df['BRANCHNO'].max()}")
  BRANCHNO range: 2 to 704
75                  print(f"  Sample BRANCHNO: {df['BRANCHNO'].head(10).to_list()}")
  Sample BRANCHNO: [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
76                  return df
77              if len(bcode_df) == 0:
78              otc_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_OTC_SUMMARY.parquet"
79              if not os.path.exists(otc_path):
80              otc_df = pl.read_parquet(otc_path)
81              print(f"  OTC columns: {otc_df.columns}")
  OTC columns: ['CHANNEL', 'PROMPT', 'UPDATED']
82              print(f"  OTC records: {len(otc_df)}")
  OTC records: 264
83              print(f"  Sample OTC CHANNEL values: {otc_df['CHANNEL'].head(10).to_list()}")
  Sample OTC CHANNEL values: ['00002', '00003', '00004', '00005', '00006', '00007', '00008', '00009', '00010', '00011']
84              print(f"\n  DEBUG - Analyzing CHANNEL values:")

  DEBUG - Analyzing CHANNEL values:
85              otc_df_debug = otc_df.with_columns([
86                  pl.col("CHANNEL").alias("CHANNEL_original"),
87                  pl.col("CHANNEL").str.strip().alias("CHANNEL_stripped"),
88          except Exception as e:
89              print(f"  Error: {e}")
  Error: 'ExprStringNameSpace' object has no attribute 'strip'
90              import traceback
91              traceback.print_exc()
Traceback (most recent call last):
  File "/sas/pythonITD/mis/job/LOAN/EIBMCHNL.py", line 128, in process_otc_detail
    pl.col("CHANNEL").str.strip().alias("CHANNEL_stripped"),
AttributeError: 'ExprStringNameSpace' object has no attribute 'strip'
92              return pl.DataFrame()
93      print("\n" + "=" * 60)

============================================================
94      print("WRITING OUTPUT FILES")
WRITING OUTPUT FILES
95      print("=" * 60)
============================================================
96      if len(channel_df) > 0:
97          output_path = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
98          channel_df.write_parquet(output_path)
99          print(f"✓ 1. CHANNEL_SUM.parquet: {output_path}")
✓ 1. CHANNEL_SUM.parquet: /host/mis/parquet/crm/year=2025/month=11/CHANNEL_SUM.parquet
100     if len(update_df) > 0:
101     if len(otc_detail) > 0:
102     print("\n" + "=" * 60)

============================================================
103     print("FINAL VERIFICATION")
FINAL VERIFICATION
104     print("=" * 60)
============================================================
105     print(f"\nOutputs in {CURRENT_MONTH_PATH}:")

Outputs in /host/mis/parquet/crm/year=2025/month=11:
106     if os.path.exists(CURRENT_MONTH_PATH):
107         sas_files = [f for f in os.listdir(CURRENT_MONTH_PATH) if f.endswith('.sas7bdat')]
108         parquet_files = [f for f in os.listdir(CURRENT_MONTH_PATH) if f.endswith('.parquet')]
109         all_files = sorted(sas_files + parquet_files)
110         if all_files:
111             for file in all_files:
112                 filepath = os.path.join(CURRENT_MONTH_PATH, file)
113                 size = os.path.getsize(filepath)
114                 print(f"  {file} ({size:,} bytes)")
  CHANNEL_SUM.parquet (1,535 bytes)
  CHANNEL_UPDATE.parquet (6,743 bytes)
115     print(f"\n" + "=" * 60)

============================================================
116     print(f"PROCESS COMPLETE")
PROCESS COMPLETE
117     print(f"Date: {REPTDATE:%Y-%m-%d}")
Date: 2025-11-30
118     print(f"Outputs: {len(channel_df) > 0} CHANNEL_SUM, {len(update_df) > 0} CHANNEL_UPDATE, {len(otc_detail) > 0} OTC_DETAIL")
Outputs: True CHANNEL_SUM, False CHANNEL_UPDATE, False OTC_DETAIL
119     print("=" * 60)
============================================================
