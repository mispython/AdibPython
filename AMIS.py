1       import polars as pl
2       import os
3       import saspy
4       from datetime import datetime, timedelta
5       from pathlib import Path
6       import pandas as pd
7       sas = saspy.SASsession() if saspy else None
Using SAS Config named: default
SAS Connection established. Subprocess id is 91333

8       REPTDATE = datetime.today() - timedelta(days=1)
9       REPTYEAR = f"{REPTDATE.year % 100:02d}"
10      REPTMON = f"{REPTDATE.month:02d}"
11      REPTDAY = f"{REPTDATE.day:02d}"
12      PREV_MONTH_DATE = REPTDATE.replace(day=1) - timedelta(days=1)
13      PREV_YEAR = PREV_MONTH_DATE.year
14      PREV_MONTH = PREV_MONTH_DATE.month
15      print(f"PROCESSING DATE: {REPTDATE:%Y-%m-%d} (TODAY()-1)")
PROCESSING DATE: 2025-11-30 (TODAY()-1)
16      print(f"ACCUMULATING WITH: {PREV_YEAR}-{PREV_MONTH:02d} (previous month)")
ACCUMULATING WITH: 2025-10 (previous month)
17      BASE_PATH = "/host/mis/parquet/crm"
18      CURRENT_MONTH_PATH = f"{BASE_PATH}/year={REPTDATE.year}/month={REPTMON}"
19      PREV_MONTH_PATH = f"{BASE_PATH}/year={PREV_YEAR}/month={PREV_MONTH:02d}"
20      os.makedirs(CURRENT_MONTH_PATH, exist_ok=True)
21      def read_bcode_file():
22      def process_channel_sum():
23      def process_otc_detail():
24      def process_channel_update():
25      def write_otc_sas_dataset(df, dataset_name):
26      def assign_libname(lib_name, sas_path):
27      def set_data(df_polars, lib_name, ctrl_name, cur_data, prev_data):
28      print("\n" + "=" * 80)

================================================================================
29      print(f"PROCESSING WITH PREVIOUS MONTH ACCUMULATION")
PROCESSING WITH PREVIOUS MONTH ACCUMULATION
30      print("=" * 80)
================================================================================
31      print(f"\n>>> 1. CHANNEL SUMMARY (EIBMCHNL)")

>>> 1. CHANNEL SUMMARY (EIBMCHNL)
32      print(f"Accumulating with {PREV_YEAR}-{PREV_MONTH:02d} data...")
Accumulating with 2025-10 data...
33      channel_df = process_channel_sum()
34          try:
35              channel_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_ALL_SUMMARY.parquet"
36              if not os.path.exists(channel_path):
37              df = pl.read_parquet(channel_path)
38              print(f"  Columns: {df.columns}")
  Columns: ['CHANNEL', 'PROMPT', 'UPDATED']
39              channel_df = df.select([
40                  pl.col("CHANNEL").str.to_uppercase().alias("CHANNEL"),
41                  pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT"),
42                  pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE"),
43                  pl.lit(REPTDATE.strftime("%b%y").upper()).alias("MONTH")
44              print(f"  Today's records: {len(channel_df)}")
  Today's records: 3
45              print(f"  MONTH: {REPTDATE.strftime('%b%y').upper()}")
  MONTH: NOV25
46              prev_month_data = pl.DataFrame()
47              prev_sum_path = f"{PREV_MONTH_PATH}/CHANNEL_SUM.parquet"
48              if os.path.exists(prev_sum_path):
49                  prev_month_data = pl.read_parquet(prev_sum_path)
50                  print(f"  Previous month data: {len(prev_month_data)} records")
  Previous month data: 537 records
51              curr_sum_path = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
52              curr_month_data = pl.DataFrame()
53              if os.path.exists(curr_sum_path):
54                  curr_month_data = pl.read_parquet(curr_sum_path)
55                  print(f"  Current month existing: {len(curr_month_data)} records")
  Current month existing: 3 records
56              all_data = []
57              if len(prev_month_data) > 0:
58                  all_data.append(prev_month_data)
59                  print(f"  Adding previous month: {len(prev_month_data)}")
  Adding previous month: 537
60              if len(curr_month_data) > 0:
61                  today_month = REPTDATE.strftime("%b%y").upper()
62                  if today_month in curr_month_data['MONTH'].unique().to_list():
63                      print(f"  Removing existing {today_month} data from current month...")
  Removing existing NOV25 data from current month...
64                      curr_month_data = curr_month_data.filter(pl.col("MONTH") != today_month)
65                      print(f"  After removal: {len(curr_month_data)}")
  After removal: 0
66                  all_data.append(curr_month_data)
67              all_data.append(channel_df)
68              if all_data:
69                  final_df = pl.concat(all_data, how="vertical")
70          except Exception as e:
71              print(f"  Error: {e}")
  Error: type Int64 is incompatible with expected type Float64
72              return pl.DataFrame()
73      print(f"\n>>> 2. CHANNEL UPDATE (EIBMCHN2)")

>>> 2. CHANNEL UPDATE (EIBMCHN2)
74      print(f"Accumulating with {PREV_YEAR}-{PREV_MONTH:02d} data...")
Accumulating with 2025-10 data...
75      update_df = process_channel_update()
76          try:
77              update_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_FULL_SUMMARY.parquet"
78              if not os.path.exists(update_path):
79              df = pl.read_parquet(update_path)
80              print(f"  Columns: {df.columns}")
  Columns: ['LINE', 'ATM', 'EBK', 'OTC', 'TOTAL', 'REPORT_DATE']
81              if len(df) >= 2:
82                  update_df = df.head(2)
83                  update_df = update_df.with_row_index().with_columns(
84                      pl.when(pl.col("index") == 0).then("TOTAL PROMPT BASE")
85                       .when(pl.col("index") == 1).then("TOTAL UPDATED")
86                       .alias("DESC")
87          except Exception as e:
88              print(f"  Error: {e}")
  Error: TOTAL PROMPT BASE
89              return pl.DataFrame()
90      print(f"\n>>> 3. OTC DETAIL (EIBMCHNL - MERGE)")

>>> 3. OTC DETAIL (EIBMCHNL - MERGE)
91      print("Processing ALL 375 BCODE branches...")
Processing ALL 375 BCODE branches...
92      otc_detail = process_otc_detail()
93          try:
94              bcode_df = read_bcode_file()
95          records = []
96          try:
97              with open("/sasdata/rawdata/lookup/LKP_BRANCH", 'r') as f:
98                  for line in f:
99                      if line.strip():
100                         try:
101                             line_padded = line.ljust(80)
102                             branchno_str = line_padded[1:4].strip()
103                             if branchno_str:
104                                 branchno = int(branchno_str)
105                                 records.append({"BRANCHNO": branchno})
106             if records:
107                 df = pl.DataFrame(records).unique().sort("BRANCHNO")
108                 print(f"  BCODE total branches: {len(df)}")
  BCODE total branches: 375
109                 return df
110             if len(bcode_df) == 0:
111             otc_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_OTC_SUMMARY.parquet"
112             if not os.path.exists(otc_path):
113             otc_df = pl.read_parquet(otc_path)
114             print(f"  OTC columns: {otc_df.columns}")
  OTC columns: ['CHANNEL', 'PROMPT', 'UPDATED']
115             print(f"  OTC records: {len(otc_df)}")
  OTC records: 264
116             otc_clean = otc_df.with_columns(
117                 pl.col("CHANNEL").cast(pl.Int64).alias("BRANCHNO")
118                 pl.col("BRANCHNO"),
119                 pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT"),
120                 pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE")
121             ).select([
122             print(f"  OTC after conversion: {len(otc_clean)} records")
  OTC after conversion: 264 records
123             merged = bcode_df.join(otc_clean, on="BRANCHNO", how="left")
124             otc_detail = merged.with_columns([
125                 pl.col("TOLPROMPT").fill_null(0),
126                 pl.col("TOLUPDATE").fill_null(0)
127             ]).sort("BRANCHNO")
128             print(f"\n  FINAL OTC_DETAIL:")

  FINAL OTC_DETAIL:
129             print(f"    Total records: {len(otc_detail)} (should be 375)")
    Total records: 375 (should be 375)
130             print(f"    TOLPROMPT sum: {otc_detail['TOLPROMPT'].sum():,}")
    TOLPROMPT sum: 18,244
131             return otc_detail
132     print("\n" + "=" * 80)

================================================================================
133     print("WRITING OUTPUT FILES")
WRITING OUTPUT FILES
134     print("=" * 80)
================================================================================
135     if len(channel_df) > 0:
136     if len(update_df) > 0:
137     if len(otc_detail) > 0:
138         output_path = f"{CURRENT_MONTH_PATH}/OTC_DETAIL.parquet"
139         otc_detail.write_parquet(output_path)
140         print(f"✓ OTC_DETAIL.parquet: {output_path}")
✓ OTC_DETAIL.parquet: /host/mis/parquet/crm/year=2025/month=11/OTC_DETAIL.parquet
141     print("\n>>> TRANSFERRING TO SAS DATASETS")

>>> TRANSFERRING TO SAS DATASETS
142     if sas:
143         try:
144             assign_libname("crm", "/stgsrcsys/host/uat")
145         log = sas.submit(f"libname {lib_name} '{sas_path}';")
146         return log
147             assign_libname("ctrl_crm", "/sas/python/virt_edw/Data_Warehouse/SASTABLE")
148             if len(channel_df) > 0:
149             if len(update_df) > 0:
150             if len(otc_detail) > 0:
151                 dataset_name = f"OTC_DETAIL_{REPTMON}{REPTYEAR}"
152                 print(f"\nTransferring OTC_DETAIL to SAS...")

Transferring OTC_DETAIL to SAS...
153                 sas_success = write_otc_sas_dataset(otc_detail, dataset_name)
154         if sas is None or len(df) == 0:
155         try:
156             sas_path = f"{CURRENT_MONTH_PATH}"
157             os.makedirs(sas_path, exist_ok=True)
158             lib_result = sas.submit(f"libname OTCLIB '{sas_path}';")
159             if "ERROR" in lib_result["LOG"]:
160             print(f"  Creating {dataset_name}...")
  Creating OTC_DETAIL_1125...
161             write_result = sas.df2sd(df.to_pandas(), table=dataset_name, libref="OTCLIB")
162             if hasattr(write_result, 'LOG') and "ERROR" in write_result.LOG:
163             sas_file = f"{sas_path}/{dataset_name}.sas7bdat"
164             if os.path.exists(sas_file):
165                 print(f"  ✗ SAS file not found")
  ✗ SAS file not found
166                 return False
167                 if not sas_success:
168                     log3 = set_data(otc_detail, "crm", "ctrl_crm", dataset_name, "otc_detail_ctl")
169         df_pandas = df_polars.to_pandas()
170         sas.df2sd(df_pandas, table=cur_data, libref='work')
171         log = sas.submit(f"""
172             where libname = upcase("{ctrl_name}")
173                  and memname = upcase("{prev_data}");
174         df_meta = sas.sasdata("colmeta", libref="work").to_df()
175         if len(df_meta) > 0:
176             cols = df_meta["name"].dropna().tolist()
177             col_list = ", ".join(cols)
178             casted_cols = []
179             for _, row in df_meta.iterrows():
180                 col = row["name"]
181                 length = row['length']
182                 if str(row['type']).strip().lower() == 'char' and pd.notnull(length) and length > 0:
183                     casted_cols.append(col)
184             casted_cols_str = ",\n ".join(casted_cols)
185             log = sas.submit(f"""
186                      create table {lib_name}.{cur_data} as
187                      select {col_list} from {ctrl_name}.{prev_data}(obs=0)
188                      select {casted_cols_str} from work.{cur_data};
189         return log
190             print(f"\n✓ SAS datasets created with previous month accumulation")

✓ SAS datasets created with previous month accumulation
191     print("\n" + "=" * 80)

================================================================================
192     print("VERIFICATION - WITH PREVIOUS MONTH ACCUMULATION")
VERIFICATION - WITH PREVIOUS MONTH ACCUMULATION
193     print("=" * 80)
================================================================================
194     print(f"\n1. ACCUMULATION DETAILS:")

1. ACCUMULATION DETAILS:
195     print(f"   - Processing date: {REPTDATE:%Y-%m-%d}")
   - Processing date: 2025-11-30
196     print(f"   - Previous month: {PREV_YEAR}-{PREV_MONTH:02d}")
   - Previous month: 2025-10
197     print(f"   - Previous month path: {PREV_MONTH_PATH}")
   - Previous month path: /host/mis/parquet/crm/year=2025/month=10
198     if len(channel_df) > 0:
199     if len(update_df) > 0:
200     if len(otc_detail) > 0:
201         print(f"\n4. OTC_DETAIL:")

4. OTC_DETAIL:
202         print(f"   - Total branches: {len(otc_detail)} (should be 375)")
   - Total branches: 375 (should be 375)
203         print(f"   - First BRANCHNO: {otc_detail['BRANCHNO'].min()} (should be 2)")
   - First BRANCHNO: 1 (should be 2)
204     print("\n" + "=" * 80)

================================================================================
205     print("PROCESS COMPLETE - WITH PREVIOUS MONTH ACCUMULATION")
PROCESS COMPLETE - WITH PREVIOUS MONTH ACCUMULATION
206     print("=" * 80)
================================================================================
