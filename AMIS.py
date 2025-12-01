Program start : 2025/12/01 14:52:57

1       import polars as pl
2       import os
3       import saspy
4       from datetime import datetime, timedelta
5       from pathlib import Path
6       sas = saspy.SASsession() if saspy else None
Using SAS Config named: default
SAS Connection established. Subprocess id is 3933323

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
17      def process_channel_sum():
18      def process_otc_detail():
19      def process_channel_update():
20      def write_sas_dataset(df, output_path, dataset_name):
21      print("\n>>> PROCESSING EIBMCHNL (CHANNEL SUMMARY)")

>>> PROCESSING EIBMCHNL (CHANNEL SUMMARY)
22      channel_df = process_channel_sum()
23          try:
24              channel_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_ALL_SUMMARY.parquet"
25              if not os.path.exists(channel_path):
26              df = pl.read_parquet(channel_path)
27              print(f"  Channel file columns: {df.columns}")
  Channel file columns: ['CHANNEL', 'PROMPT', 'UPDATED']
28              if "BRANCHNO" not in df.columns:
29                  for col in df.columns:
30                      if "branch" in col.lower() or "brn" in col.lower():
31              channel_df = df.select([
32                  pl.col("CHANNEL").str.to_uppercase(),
33                  pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT"),
34                  pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE")
35                  pl.lit(REPTDATE.strftime("%b%y").upper()).alias("MONTH")
36              print(f"  Channel records: {len(channel_df)}")
  Channel records: 3
37              return channel_df
38      print("\n>>> PROCESSING EIBMCHN2 (CHANNEL UPDATE)")

>>> PROCESSING EIBMCHN2 (CHANNEL UPDATE)
39      update_df = process_channel_update()
40          try:
41              update_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_FULL_SUMMARY.parquet"
42              if not os.path.exists(update_path):
43              df = pl.read_parquet(update_path)
44              print(f"  Update file columns: {df.columns}")
  Update file columns: ['LINE', 'ATM', 'EBK', 'OTC', 'TOTAL', 'REPORT_DATE']
45              update_df = df.head(2)
46              available_cols = update_df.columns
47              print(f"  Available columns in update: {available_cols}")
  Available columns in update: ['LINE', 'ATM', 'EBK', 'OTC', 'TOTAL', 'REPORT_DATE']
48              result_cols = {}
49              for col in available_cols:
50                  if col.upper() == "ATM":
51                      result_cols["ATM"] = pl.col(col).cast(pl.Int64)
52                      break
53              for col in available_cols:
54                  if col.upper() == "EBK":
55                      result_cols["EBK"] = pl.col(col).cast(pl.Int64)
56                      break
57              for col in available_cols:
58                  if col.upper() == "OTC":
59                      result_cols["OTC"] = pl.col(col).cast(pl.Int64)
60                      break
61              for col in available_cols:
62                  if col.upper() == "TOTAL":
63                      result_cols["TOTAL"] = pl.col(col).cast(pl.Int64)
64                      break
65              if len(result_cols) < 4:
66              update_df = update_df.select(list(result_cols.values()))
67              update_df = update_df.with_columns([
68                  pl.when(pl.col("ATM").is_first()).then("TOTAL PROMPT BASE")
69          except Exception as e:
70              print(f"  Error processing update: {e}")
  Error processing update: 'Expr' object has no attribute 'is_first'
71              return pl.DataFrame()
72      print("\n>>> CREATING OTC_DETAIL")

>>> CREATING OTC_DETAIL
73      otc_detail = process_otc_detail()
74          try:
75              bcode_records = []
76              with open("/sasdata/rawdata/lookup/LKP_BRANCH", 'r') as f:
77                  for line in f:
78                      if line.strip():
79                          try:
80                              branchno = int(line[1:4].strip())
81                              bcode_records.append({"BRANCHNO": branchno})
82              bcode_df = pl.DataFrame(bcode_records).unique().sort("BRANCHNO")
83              print(f"  BCODE branches: {len(bcode_df)}")
  BCODE branches: 375
84              branch_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_OTC_SUMMARY.parquet"
85              if os.path.exists(branch_path):
86                  branch_df = pl.read_parquet(branch_path)
87                  print(f"  BRANCH file columns: {branch_df.columns}")
  BRANCH file columns: ['CHANNEL', 'PROMPT', 'UPDATED']
88                  if "BRANCHNO" not in branch_df.columns:
89                      print(f"  WARNING: BRANCHNO column not found in {branch_path}")
  WARNING: BRANCHNO column not found in /host/cis/parquet/year=2025/month=11/day=30/CIPHONET_OTC_SUMMARY.parquet
90                      print(f"  Available columns: {branch_df.columns}")
  Available columns: ['CHANNEL', 'PROMPT', 'UPDATED']
91                      branch_col = None
92                      for col in branch_df.columns:
93                          if "branch" in col.lower() or "brn" in col.lower():
94                      if branch_col:
95                          print(f"  No branch column found, creating empty branch data")
  No branch column found, creating empty branch data
96                          branch_df = pl.DataFrame({
97                              "BRANCHNO": [],
98                              "PROMPT": [],
99                              "UPDATED": []
100                 branch_df = branch_df.select([
101                     pl.col("BRANCHNO").cast(pl.Int64),
102                     pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT").fill_null(0),
103                     pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE").fill_null(0)
104             print(f"  BRANCH records: {len(branch_df)}")
  BRANCH records: 0
105             if len(bcode_df) > 0 and len(branch_df) > 0:
106             elif len(bcode_df) > 0:
107                 otc_detail = bcode_df.with_columns([
108                     pl.lit(0).alias("TOLPROMPT"),
109                     pl.lit(0).alias("TOLUPDATE")
110             otc_detail = otc_detail.with_columns([
111                 pl.col("TOLPROMPT").fill_null(0),
112                 pl.col("TOLUPDATE").fill_null(0)
113             return otc_detail
114     if len(channel_df) > 0:
115         output_path = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
116         channel_df.write_parquet(output_path)
117         print(f"\n✓ CHANNEL_SUM written: {output_path}")

✓ CHANNEL_SUM written: /host/mis/parquet/crm/year=2025/month=11/CHANNEL_SUM.parquet
118     if len(update_df) > 0:
119     if len(otc_detail) > 0:
120         print(f"\n✓ OTC_DETAIL DataFrame created: {len(otc_detail)} records")

✓ OTC_DETAIL DataFrame created: 375 records
121         sas_output_path = f"{CURRENT_MONTH_PATH}/otc_detail"
122         sas_success = write_sas_dataset(otc_detail, sas_output_path, "OTC_DETAIL")
123         if sas is None:
124         if len(df) == 0:
125         try:
126             os.makedirs(output_path, exist_ok=True)
127             lib_log = sas.submit(f"libname SASOUT '{output_path}';")
128             if "ERROR" in lib_log["LOG"]:
129             print(f"  Writing {len(df)} records to SAS...")
  Writing 375 records to SAS...
130             write_result = sas.df2sd(df.to_pandas(), table=dataset_name, libref="SASOUT")
131             if hasattr(write_result, 'LOG') and "ERROR" in write_result.LOG:
132             print(f"  ✓ SAS write completed")
  ✓ SAS write completed
133             verify_log = sas.submit(f"""
134                     select count(*) as N from SASOUT.{dataset_name};
135             print(f"  Verification: {verify_log['LOG'][:100]}...")
  Verification:
75   ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') devi...
136             sas.submit("libname SASOUT clear;")
137             sas_file = f"{output_path}/{dataset_name}.sas7bdat"
138             if os.path.exists(sas_file):
139                 print(f"  ✗ File not found: {sas_file}")
  ✗ File not found: /host/mis/parquet/crm/year=2025/month=11/otc_detail/OTC_DETAIL.sas7bdat
140                 return False
141         if not sas_success:
142             print(f"\nFalling back to Parquet format...")

Falling back to Parquet format...
143             parquet_path = f"{CRMWH_PATH}/OTC_DETAIL_{REPTMON}{REPTYEAR}.parquet"
144             otc_detail.write_parquet(parquet_path)
145             print(f"✓ OTC_DETAIL written as Parquet: {parquet_path}")
✓ OTC_DETAIL written as Parquet: /host/mis/parquet/crm/CRMWH/OTC_DETAIL_1125.parquet
146     print("\n" + "=" * 60)

============================================================
147     print("PROCESSING SUMMARY")
PROCESSING SUMMARY
148     print("=" * 60)
============================================================
149     print(f"Date: {REPTDATE:%Y-%m-%d}")
Date: 2025-11-30
150     print(f"Output Month: {REPTDATE.year}-{REPTMON}")
Output Month: 2025-11
151     print(f"Channel Summary: {len(channel_df)} records")
Channel Summary: 3 records
152     print(f"Channel Update: {len(update_df)} records")
Channel Update: 0 records
153     print(f"OTC Detail: {len(otc_detail)} branches")
OTC Detail: 375 branches
154     if len(otc_detail) > 0:
155         print(f"\nOTC_DETAIL Statistics:")

OTC_DETAIL Statistics:
156         print(f"  Total TOLPROMPT: {otc_detail['TOLPROMPT'].sum()}")
  Total TOLPROMPT: 0
157         print(f"  Total TOLUPDATE: {otc_detail['TOLUPDATE'].sum()}")
  Total TOLUPDATE: 0
158         print(f"  Branches with data: {otc_detail.filter(pl.col('TOLPROMPT') > 0).height}")
  Branches with data: 0
159     print("=" * 60)
============================================================
