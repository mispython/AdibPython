1	import polars as pl
2	import os
3	import saspy
4	from datetime import datetime, timedelta
5	from pathlib import Path
6	sas = saspy.SASsession() if saspy else None
Using SAS Config named: default
SAS Connection established. Subprocess id is 4186856

7	REPTDATE = datetime.today() - timedelta(days=1)
8	REPTYEAR = f"{REPTDATE.year % 100:02d}"
9	REPTMON = f"{REPTDATE.month:02d}"
10	REPTDAY = f"{REPTDATE.day:02d}"
11	print(f"PROCESSING DATE: {REPTDATE:%Y-%m-%d}")
PROCESSING DATE: 2025-11-30
12	BASE_PATH = "/host/mis/parquet/crm"
13	CURRENT_MONTH_PATH = f"{BASE_PATH}/year={REPTDATE.year}/month={REPTMON}"
14	os.makedirs(CURRENT_MONTH_PATH, exist_ok=True)
15	def read_bcode_file():
16	def process_channel_sum():
17	def process_otc_detail():
18	def process_channel_update():
19	print("\n" + "=" * 60)

============================================================
20	print(f"PROCESSING FOR {REPTDATE:%Y-%m-%d}")
PROCESSING FOR 2025-11-30
21	print("=" * 60)
============================================================
22	print("\n>>> 1. CHANNEL SUMMARY (EIBMCHNL)")

>>> 1. CHANNEL SUMMARY (EIBMCHNL)
23	channel_df = process_channel_sum()
24	    try:
25	        channel_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_ALL_SUMMARY.parquet"
26	        if not os.path.exists(channel_path):
27	        df = pl.read_parquet(channel_path)
28	        print(f"  Columns: {df.columns}")
  Columns: ['CHANNEL', 'PROMPT', 'UPDATED']
29	        channel_df = df.select([
30	            pl.col("CHANNEL").str.to_uppercase().alias("CHANNEL"),
31	            pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT"),
32	            pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE")
33	            pl.lit(REPTDATE.strftime("%b%y").upper()).alias("MONTH")
34	        print(f"  Records: {len(channel_df)}")
  Records: 3
35	        return channel_df
36	print("\n>>> 2. CHANNEL UPDATE (EIBMCHN2)")

>>> 2. CHANNEL UPDATE (EIBMCHN2)
37	update_df = process_channel_update()
38	    try:
39	        update_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_FULL_SUMMARY.parquet"
40	        if not os.path.exists(update_path):
41	        df = pl.read_parquet(update_path)
42	        print(f"  Columns: {df.columns}")
  Columns: ['LINE', 'ATM', 'EBK', 'OTC', 'TOTAL', 'REPORT_DATE']
43	        print(f"  Total records: {len(df)}")
  Total records: 2
44	        if len(df) >= 2:
45	            update_df = df.head(2)
46	            desc_col = None
47	            for col in ['LINE', 'DESC', 'DESCRIPTION']:
48	                if col in update_df.columns:
49	                    desc_col = col
50	                    break
51	            if desc_col:
52	                update_df = update_df.with_columns([
53	                    pl.col(desc_col).alias("DESC"),
54	                    pl.col("ATM").cast(pl.Int64),
55	                    pl.col("EBK").cast(pl.Int64),
56	                    pl.col("OTC").cast(pl.Int64),
57	                    pl.col("TOTAL").cast(pl.Int64)
58	            update_df = update_df.with_columns(
59	                pl.lit(REPTDATE.strftime("%d/%m/%Y")).alias("DATE")
60	            print(f"  Update records: {len(update_df)}")
  Update records: 2
61	            return update_df
62	print("\n>>> 3. OTC DETAIL (EIBMCHNL - MERGE)")

>>> 3. OTC DETAIL (EIBMCHNL - MERGE)
63	otc_detail = process_otc_detail()
64	    try:
65	        bcode_df = read_bcode_file()
66	    records = []
67	    try:
68	        with open("/sasdata/rawdata/lookup/LKP_BRANCH", 'r') as f:
69	            for line in f:
70	                if line.strip():
71	                    try:
72	                        line_padded = line.ljust(80)
73	                        branchno_str = line_padded[1:4].strip()
74	                        if branchno_str:
75	                            branchno = int(branchno_str)
76	                            brstatus = line_padded[49:50].strip()
77	                            if brstatus in ['O', 'A']:
78	                                records.append({"BRANCHNO": branchno})
79	        if records:
80	            df = pl.DataFrame(records).unique().sort("BRANCHNO")
81	            print(f"  BCODE active branches: {len(df)}")
  BCODE active branches: 269
82	            print(f"  BRANCHNO range: {df['BRANCHNO'].min()} to {df['BRANCHNO'].max()}")
  BRANCHNO range: 2 to 704
83	            print(f"  Sample BRANCHNO: {df['BRANCHNO'].head(10).to_list()}")
  Sample BRANCHNO: [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
84	            return df
85	        if len(bcode_df) == 0:
86	        otc_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_OTC_SUMMARY.parquet"
87	        if not os.path.exists(otc_path):
88	        otc_df = pl.read_parquet(otc_path)
89	        print(f"  OTC columns: {otc_df.columns}")
  OTC columns: ['CHANNEL', 'PROMPT', 'UPDATED']
90	        print(f"  OTC records: {len(otc_df)}")
  OTC records: 264
91	        print(f"  Sample CHANNEL: {otc_df['CHANNEL'].head(10).to_list()}")
  Sample CHANNEL: ['00002', '00003', '00004', '00005', '00006', '00007', '00008', '00009', '00010', '00011']
92	        otc_clean = otc_df.with_columns(
93	            pl.col("CHANNEL").str.replace_all("^0+", "").cast(pl.Int64).alias("BRANCHNO")
94	            pl.col("BRANCHNO"),
95	            pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT"),
96	            pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE")
97	        ).select([
98	        print(f"  OTC after conversion: {len(otc_clean)} records")
  OTC after conversion: 264 records
99	        print(f"  Sample BRANCHNO: {otc_clean['BRANCHNO'].head(10).to_list()}")
  Sample BRANCHNO: [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
100	        merged = bcode_df.join(otc_clean, on="BRANCHNO", how="left")
101	        otc_detail = merged.with_columns([
102	            pl.col("TOLPROMPT").fill_null(0),
103	            pl.col("TOLUPDATE").fill_null(0)
104	        ]).sort("BRANCHNO")
105	        print(f"\n  FINAL OTC_DETAIL:")

  FINAL OTC_DETAIL:
106	        print(f"    Total records: {len(otc_detail)}")
    Total records: 269
107	        print(f"    TOLPROMPT sum: {otc_detail['TOLPROMPT'].sum():,}")
    TOLPROMPT sum: 18,244
108	        print(f"    TOLUPDATE sum: {otc_detail['TOLUPDATE'].sum():,}")
    TOLUPDATE sum: 18,202
109	        return otc_detail
110	print("\n" + "=" * 60)

============================================================
111	print("WRITING OUTPUT FILES")
WRITING OUTPUT FILES
112	print("=" * 60)
============================================================
113	if len(channel_df) > 0:
114	    output_path = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
115	    channel_df.write_parquet(output_path)
116	    print(f"✓ 1. CHANNEL_SUM.parquet: {output_path}")
✓ 1. CHANNEL_SUM.parquet: /host/mis/parquet/crm/year=2025/month=11/CHANNEL_SUM.parquet
117	if len(update_df) > 0:
118	    output_path = f"{CURRENT_MONTH_PATH}/CHANNEL_UPDATE.parquet"
119	    update_df.write_parquet(output_path)
120	    print(f"✓ 2. CHANNEL_UPDATE.parquet: {output_path}")
✓ 2. CHANNEL_UPDATE.parquet: /host/mis/parquet/crm/year=2025/month=11/CHANNEL_UPDATE.parquet
121	if len(otc_detail) > 0:
122	    output_path = f"{CURRENT_MONTH_PATH}/OTC_DETAIL.parquet"
123	    otc_detail.write_parquet(output_path)
124	    print(f"✓ 2. CHANNEL_UPDATE.parquet: {output_path}")
✓ 2. CHANNEL_UPDATE.parquet: /host/mis/parquet/crm/year=2025/month=11/OTC_DETAIL.parquet
125	channel_sum_ctl    = "channel_sum_ctl"
126	channel_update_ctl = "channel_update_ctl"
127	otc_ctl      = "otc_detail_ctl"
128	sum_data     = "channel_sum"
129	update_data  = "channel_update"
130	otc_data     = f"otc_detail_{REPTYEAR}{REPTMON}"
131	def assign_libname(lib_name, sas_path):
132	def set_data(df, lib_name, ctrl_name, cur_data, prev_data):
133	assign_libname("crm" , "/stgsrcsys/host/uat")
134	    log = sas.submit(f"""libname {lib_name} '{sas_path}';""")
135	    return log
136	assign_libname("ctrl_crm", "/sas/python/virt_edw/Data_Warehouse/SASTABLE")
137	log1 = set_data(channel_df, "crm", "ctrl_crm", sum_data , channel_sum_ctl)
138	    sas.df2sd(df,table=cur_data, libref='work')
Traceback (most recent call last):
  File "/sas/pythonITD/mis/job/CIS/MIS_CRM_CHANNEL.py", line 313, in <module>
    log1 = set_data(channel_df, "crm", "ctrl_crm", sum_data , channel_sum_ctl)
  File "/sas/pythonITD/mis/job/CIS/MIS_CRM_CHANNEL.py", line 271, in set_data
    sas.df2sd(df,table=cur_data, libref='work')
  File "/sas/pythonITD/mis/lib64/python3.9/site-packages/saspy/sasbase.py", line 1577, in df2sd
    return self.dataframe2sasdata(df, table, libref, results, keep_outer_quotes, embedded_newlines, LF, CR, colsep, colrep,
  File "/sas/pythonITD/mis/lib64/python3.9/site-packages/saspy/sasbase.py", line 1690, in dataframe2sasdata
    charlens = self.df_char_lengths(df, encode_errors, char_lengths)
  File "/sas/pythonITD/mis/lib64/python3.9/site-packages/saspy/sasbase.py", line 1476, in df_char_lengths
    if df.dtypes[name].kind in ('O','S','U','V'):
TypeError: list indices must be integers or slices, not str
