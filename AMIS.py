1       import polars as pl
2       import os
3       import saspy
4       from datetime import datetime, timedelta
5       from pathlib import Path
6       sas = saspy.SASsession() if saspy else None
Using SAS Config named: default
SAS Connection established. Subprocess id is 4027096

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
46              print(f"  Total records: {len(df)}")
  Total records: 2
47              if len(df) >= 2:
48                  update_df = df.head(2)
49                  desc_col = None
50                  for col in ['LINE', 'DESC', 'DESCRIPTION']:
51                      if col in update_df.columns:
52                          desc_col = col
53                          break
54                  if desc_col:
55                      update_df = update_df.with_columns([
56                          pl.col(desc_col).alias("DESC"),
57                          pl.col("ATM").cast(pl.Int64),
58                          pl.col("EBK").cast(pl.Int64),
59                          pl.col("OTC").cast(pl.Int64),
60                          pl.col("TOTAL").cast(pl.Int64)
61                  update_df = update_df.with_columns(
62                      pl.lit(REPTDATE.strftime("%d/%m/%Y")).alias("DATE")
63                  print(f"  Update records: {len(update_df)}")
  Update records: 2
64                  return update_df
65      print("\n>>> 3. OTC DETAIL (EIBMCHNL - MERGE)")

>>> 3. OTC DETAIL (EIBMCHNL - MERGE)
66      otc_detail = process_otc_detail()
67          try:
68              bcode_df = read_bcode_file()
69          records = []
70          try:
71              with open("/sasdata/rawdata/lookup/LKP_BRANCH", 'r') as f:
72                  for line in f:
73                      if line.strip():
74                          try:
75                              line_padded = line.ljust(80)
76                              branchno_str = line_padded[1:4].strip()
77                              if branchno_str:
78                                  branchno = int(branchno_str)
79                                  brstatus = line_padded[49:50].strip()
80                                  if brstatus in ['O', 'A']:
81                                      records.append({"BRANCHNO": branchno})
82              if records:
83                  df = pl.DataFrame(records).unique().sort("BRANCHNO")
84                  print(f"  BCODE active branches: {len(df)}")
  BCODE active branches: 269
85                  print(f"  BRANCHNO range: {df['BRANCHNO'].min()} to {df['BRANCHNO'].max()}")
  BRANCHNO range: 2 to 704
86                  print(f"  Sample BRANCHNO: {df['BRANCHNO'].head(10).to_list()}")
  Sample BRANCHNO: [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
87                  return df
88              if len(bcode_df) == 0:
89              otc_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_OTC_SUMMARY.parquet"
90              if not os.path.exists(otc_path):
91              otc_df = pl.read_parquet(otc_path)
92              print(f"  OTC columns: {otc_df.columns}")
  OTC columns: ['CHANNEL', 'PROMPT', 'UPDATED']
93              print(f"  OTC records: {len(otc_df)}")
  OTC records: 264
94              print(f"  Sample CHANNEL: {otc_df['CHANNEL'].head(10).to_list()}")
  Sample CHANNEL: ['00002', '00003', '00004', '00005', '00006', '00007', '00008', '00009', '00010', '00011']
95              otc_clean = otc_df.with_columns(
96                  pl.col("CHANNEL").str.replace_all("^0+", "").cast(pl.Int64).alias("BRANCHNO")
97                  pl.col("BRANCHNO"),
98                  pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT"),
99                  pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE")
100             ).select([
101             print(f"  OTC after conversion: {len(otc_clean)} records")
  OTC after conversion: 264 records
102             print(f"  Sample BRANCHNO: {otc_clean['BRANCHNO'].head(10).to_list()}")
  Sample BRANCHNO: [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
103             merged = bcode_df.join(otc_clean, on="BRANCHNO", how="left")
104             otc_detail = merged.with_columns([
105                 pl.col("TOLPROMPT").fill_null(0),
106                 pl.col("TOLUPDATE").fill_null(0)
107             ]).sort("BRANCHNO")
108             print(f"\n  FINAL OTC_DETAIL:")

  FINAL OTC_DETAIL:
109             print(f"    Total records: {len(otc_detail)}")
    Total records: 269
110             print(f"    TOLPROMPT sum: {otc_detail['TOLPROMPT'].sum():,}")
    TOLPROMPT sum: 18,244
111             print(f"    TOLUPDATE sum: {otc_detail['TOLUPDATE'].sum():,}")
    TOLUPDATE sum: 18,202
112             return otc_detail
113     print("\n" + "=" * 60)

============================================================
114     print("WRITING OUTPUT FILES")
WRITING OUTPUT FILES
115     print("=" * 60)
============================================================
116     if len(channel_df) > 0:
117         output_path = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
118         channel_df.write_parquet(output_path)
119         print(f"✓ 1. CHANNEL_SUM.parquet: {output_path}")
✓ 1. CHANNEL_SUM.parquet: /host/mis/parquet/crm/year=2025/month=11/CHANNEL_SUM.parquet
120     if len(update_df) > 0:
121         output_path = f"{CURRENT_MONTH_PATH}/CHANNEL_UPDATE.parquet"
122         update_df.write_parquet(output_path)
123         print(f"✓ 2. CHANNEL_UPDATE.parquet: {output_path}")
✓ 2. CHANNEL_UPDATE.parquet: /host/mis/parquet/crm/year=2025/month=11/CHANNEL_UPDATE.parquet
124     if len(otc_detail) > 0:
125         dataset_name = f"OTC_DETAIL_{REPTMON}{REPTYEAR}"
126         sas_path = f"{CURRENT_MONTH_PATH}"
127         print(f"\nWriting {dataset_name} to SAS...")

Writing OTC_DETAIL_1125 to SAS...
128         sas_success = write_sas_dataset(otc_detail, sas_path, dataset_name)
129         if sas is None:
130         if len(df) == 0:
131         try:
132             os.makedirs(output_path, exist_ok=True)
133             lib_result = sas.submit(f"libname SASOUT '{output_path}';")
134             if "ERROR" in lib_result["LOG"]:
135             print(f"  Writing {len(df)} records to {dataset_name}...")
  Writing 269 records to OTC_DETAIL_1125...
136             write_result = sas.df2sd(df.to_pandas(), table=dataset_name, libref="SASOUT")
137             if hasattr(write_result, 'LOG') and "ERROR" in write_result.LOG:
138             verify = sas.submit(f"""
139                     select count(*) as N from SASOUT.{dataset_name};
140             sas.submit("libname SASOUT clear;")
141             sas_file = f"{output_path}/{dataset_name}.sas7bdat"
142             if os.path.exists(sas_file):
143                 print(f"  ✗ SAS file not found: {sas_file}")
  ✗ SAS file not found: /host/mis/parquet/crm/year=2025/month=11/OTC_DETAIL_1125.sas7bdat
144                 return False
145         if sas_success:
146             parquet_path = f"{CRMWH_PATH}/{dataset_name}.parquet"
147             otc_detail.write_parquet(parquet_path)
148             print(f"✓ 3. {dataset_name}.parquet (fallback): {parquet_path}")
✓ 3. OTC_DETAIL_1125.parquet (fallback): /host/mis/parquet/crm/CRMWH/OTC_DETAIL_1125.parquet
149     print("\n" + "=" * 60)

============================================================
150     print("FINAL CHECK")
FINAL CHECK
151     print("=" * 60)
============================================================
152     print(f"\nFiles in {CURRENT_MONTH_PATH}:")

Files in /host/mis/parquet/crm/year=2025/month=11:
153     if os.path.exists(CURRENT_MONTH_PATH):
154         for file in sorted(os.listdir(CURRENT_MONTH_PATH)):
155             filepath = os.path.join(CURRENT_MONTH_PATH, file)
156             if os.path.isfile(filepath):
157                 size = os.path.getsize(filepath)
158                 print(f"  {file} ({size:,} bytes)")
  CHANNEL_SUM.parquet (1,535 bytes)
  CHANNEL_UPDATE.parquet (3,113 bytes)
  otc_detail_1125.sas7bdat (131,072 bytes)
159     print(f"\n" + "=" * 60)

============================================================
160     print(f"COMPLETE")
COMPLETE
161     print("=" * 60)
============================================================
