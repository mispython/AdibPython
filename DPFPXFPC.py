****************************************************
* Job:      MIS_ELN_BNMSUMM                        *
* Location: /sas/pythonITD/mis/job/ELDS            *
*                                                  *
* Modified: Monday, November 02, 2026, 10:38:40 AM *
****************************************************
Program start : 2026/11/23 09:15:30

1	"""
2	import polars as pl
3	import pyreadstat
4	import duckdb
5	from datetime import datetime, timedelta
6	from pathlib import Path
7	import sys
8	import re
9	BASE_INPUT_PATH = Path("/host/mis/input/ELDS")
10	ELDS_DATA_PATH = Path("/parquet/dwh/ELDS")
11	ELDS_DATA_PATH.mkdir(parents=True, exist_ok=True)
12	REPTDATE = datetime.today() - timedelta(days=1)
13	PREVDATE = REPTDATE - timedelta(days=1)
14	FILE_DT = REPTDATE.strftime('%Y%m%d')
15	SAS_ORIGIN = datetime(1960, 1, 1)
16	RDATE = (REPTDATE - SAS_ORIGIN).days
17	print("="*70)
======================================================================
18	print(f"ELDS BNM Summary Processing - {REPTDATE.strftime('%Y-%m-%d')}")
ELDS BNM Summary Processing - 2026-11-22
19	print("="*70)
======================================================================
20	BNMSUMM1 = BASE_INPUT_PATH / f"bnmsummary1_{FILE_DT}.csv"
21	BNMSUMM2 = BASE_INPUT_PATH / f"bnmsummary2_{FILE_DT}.csv"
22	OUTPUT_DATA_PATH = Path(f"{ELDS_DATA_PATH}/year={REPTDATE.year}/month={REPTDATE.month:02d}/day={REPTDATE.day:02d}")
23	OUTPUT_DATA_PATH.mkdir(parents=True, exist_ok=True)
24	EHP_SRC = '/sas/eHP/intermediate/intg_app_ehp_fs_dwh_bnmsummary1.sas7bdat'
25	EHP2_SRC = '/sas/eHP/intermediate/intg_app_ehp_fs_dwh_bnmsummary2.sas7bdat'
26	    'MAANO': ('char', 50),
27	    'STAGE': ('char', 2),
28	    'APPLICATION': ('char', 10),
29	    'DTECOMPLETE': ('char', 10),
30	    'AANO': ('char', 50),
31	    'APPKEY': ('char', 10),
32	    'PRIORITY_SECTOR': ('char', 2),
33	    'DSRISS3': ('num', 8),
34	    'FIN_CONCEPT': ('char', 60),
35	    'LN_UTILISE_LOCAT_CD': ('char', 2),
36	    'SPECIALFUND': ('num', 8),
37	    'ASSET_PURCH_AMT': ('num', 8),
38	    'PURPOSE_LOAN': ('num', 8),
39	    'STRUPCO_3YR': ('char', 2),
40	    'FACICODE': ('num', 8),
41	    'AMTAPPLY': ('num', 8),
42	    'AMOUNT': ('num', 8),
43	    'APPTYPE': ('char', 1),
44	    'REJREASON': ('char', 5),
45	    'DATEXT': ('char', 10),
46	    'ACCTNO': ('num', 8),
47	    'EIR': ('num', 8),
48	    'EREQNO': ('char', 15),
49	    'REFIN_FLG': ('char', 3),
50	    'INDINTERIM': ('char', 1),
51	    'DATE': ('num', 8),
52	    'STATUS': ('char', 30),
53	    'CCPT_TAG': ('char', 5),
54	    'CIR': ('num', 8),
55	    'PRICING_TYPE': ('char', 5),
56	    'LU_ADD1': ('char', 40),
57	    'LU_ADD2': ('char', 40),
58	    'LU_ADD3': ('char', 40),
59	    'LU_ADD4': ('char', 40),
60	    'LU_TOWN_CITY': ('char', 20),
61	    'LU_POSTCODE': ('char', 5),
62	    'LU_STATE_CD': ('char', 2),
63	    'LU_COUNTRY_CD': ('char', 3),
64	    'PROP_STATUS': ('char', 5),
65	    'DTCOMPLETE': ('char', 25),
66	    'LU_SOURCE': ('char', 5),
67	    'EVENT_TYPE': ('char', 2),
68	SUMM1_SCHEMA = {
69	    'MAANO': ('char', 50),
70	    'STAGE': ('char', 2),
71	    'APPLICATION': ('char', 10),
72	    'DTECOMPLETE': ('char', 10),
73	    'IDNO': ('char', 30),
74	    'ENTKEY': ('char', 11),
75	    'APPLNAME': ('char', 150),
76	    'COUNTRY': ('char', 5),
77	    'DBIRTH': ('char', 10),
78	    'ENTITY_TYPE': ('num', 8),
79	    'CORP_STATUS_CD': ('char', 2),
80	    'INDUSTRIAL_STATUS': ('char', 5),
81	    'RESIDENCY_STATUS_CD': ('char', 1),
82	    'ANNSUBTSALARY': ('num', 8),
83	    'GENDER': ('char', 1),
84	    'OCCUPATION': ('char', 5),
85	    'EMPNAME': ('char', 150),
86	    'EMPLOY_SECTOR_CD': ('char', 10),
87	    'EMPLOY_TYPE_CD': ('char', 5),
88	    'POSTCODE': ('char', 5),
89	    'STATE_CD': ('char', 2),
90	    'COUNTRY_CD': ('char', 3),
91	    'ROLE': ('char', 1),
92	    'ICPP': ('char', 30),
93	    'MARRIED': ('char', 10),
94	    'CUSTOMER_CODE': ('char', 2),
95	    'CISNUMBER': ('char', 20),
96	    'NO_OF_EMPLOYEE': ('char', 10),
97	    'ANNUAL_TURNOVER': ('char', 20),
98	    'SMESIZE': ('char', 5),
99	    'RACE': ('char', 2),
100	    'INDUSTRIAL_SECTOR_CD': ('char', 10),
101	    'OCCUPAT_MASCO_CD': ('char', 10),
102	    'EREQNO': ('char', 15),
103	    'DSRISS3': ('char', 10),
104	    'INDINTERIM': ('char', 1),
105	    'DATE': ('num', 8),
106	    'DTCOMPLETE': ('char', 25),
107	SUMM2_SCHEMA = {
108	def apply_production_schema(df: pl.DataFrame, schema: dict, dataset_name: str) -> pl.DataFrame:
109	def apply_sas_eof_check(df: pl.DataFrame, source_name: str, is_sas_dataset: bool = False) -> pl.DataFrame:
110	def save_with_cumulative_append(df: pl.DataFrame, dataset_name: str, schema: dict,
111	                                prev_date: datetime, output_path: Path, data_path: Path):
112	print("\n" + "="*70)

======================================================================
113	print("PROCESSING SUMM1 (Loan Applications)")
PROCESSING SUMM1 (Loan Applications)
114	print("="*70)
======================================================================
115	print(f"\nReading {BNMSUMM1.name}...")

Reading bnmsummary1_20261122.csv...
116	if not BNMSUMM1.exists():
117	try:
118	    SUMM1 = pl.read_csv(
119	        BNMSUMM1,
120	        separator=",",
121	        has_header=False,
122	        ignore_errors=True,
123	        truncate_ragged_lines=True,
124	        infer_schema_length=0,
125	        skip_rows=1
126	    print(f"  âœ“ Raw CSV: {SUMM1.height:,} rows, {SUMM1.width} columns")
  âœ“ Raw CSV: 23 rows, 40 columns
127	    csv_column_names = [
128	    if SUMM1.width < 40:
129	    SUMM1 = SUMM1.select([pl.col(f"column_{i+1}") for i in range(40)])
130	    SUMM1.columns = csv_column_names
131	    if SUMM1.width != 40:
132	SUMM1 = apply_sas_eof_check(SUMM1, "bnmsummary1_csv", is_sas_dataset=False)
133	    if "MAANO" not in df.columns:
134	    if is_sas_dataset:
135	    df_str = df.with_columns(pl.col("MAANO").cast(pl.Utf8, strict=False))
136	    header_mask = df_str["MAANO"].str.starts_with("FH")
137	    if header_mask.any():
138	    if df.height == 0:
139	    control_row = df_str[-1, :]
140	    ctrl_val = str(control_row["MAANO"][0]).strip()
141	    match = re.match(r"FT0*(\d+)", ctrl_val)
142	    if not match:
143	    control_count = int(match.group(1))
144	    print(f"  ðŸ“Š {source_name}: Control count = {control_count}")
  ðŸ“Š bnmsummary1_csv: Control count = 22
145	    df_data = df.slice(0, df.height - 1)
146	    actual_count = df_data.height
147	    print(f"  ðŸ“Š {source_name}: Actual data rows = {actual_count}")
  ðŸ“Š bnmsummary1_csv: Actual data rows = 22
148	    if control_count != actual_count:
149	        print(f"  âœ“ Record count matches control ({actual_count})")
  âœ“ Record count matches control (22)
150	    df_data = df_data.with_columns([
151	        pl.lit("Y").alias("INDINTERIM"),
152	        pl.lit(RDATE).alias("DATE")
153	    return df_data
154	print(f"\nReading {Path(EHP_SRC).name}...")

Reading intg_app_ehp_fs_dwh_bnmsummary1.sas7bdat...
155	if not Path(EHP_SRC).exists():
156	SUMM1_EHP_df, _ = pyreadstat.read_sas7bdat(EHP_SRC)
157	print(f"âœ“ SAS7BDAT loaded: {len(SUMM1_EHP_df):,} rows")
âœ“ SAS7BDAT loaded: 524 rows
158	SUMM1_EHP = pl.from_pandas(SUMM1_EHP_df)
159	print(f"  ðŸ“‹ Mapping EHP column names (CORRECTED - added missing mappings)...")
  ðŸ“‹ Mapping EHP column names (CORRECTED - added missing mappings)...
160	    'MAANO': 'MAANO',
161	    'STAGE': 'STAGE',
162	    'APPLICATION': 'APPLICATION',
163	    'APPKEY': 'APPKEY',
164	    'DATE_COMPLETED': 'DTECOMPLETE',
165	    'DATE': 'DATEXT',
166	    'DTCOMPLETE': 'DTCOMPLETE',
167	    'PRIORITY_SECTOR': 'PRIORITY_SECTOR',
168	    'APPTYPE': 'APPTYPE',
169	    'STATUS': 'STATUS',
170	    'EVENT_TYPE': 'EVENT_TYPE',
171	    'DEBT_SERVICE_RATIO': 'DSRISS3',
172	    'SPECIAL_FUND': 'SPECIALFUND',
173	    'ASSET_PURCHASE': 'ASSET_PURCH_AMT',
174	    'PURPOSE_LOAN': 'PURPOSE_LOAN',
175	    'AMOUNT': 'AMTAPPLY',
176	    'AMT_APPROVED': 'AMOUNT',
177	    'ACCOUNT_NO': 'ACCTNO',
178	    'EIR': 'EIR',
179	    'CIR': 'CIR',
180	    'PRICING_TYPE': 'PRICING_TYPE',
181	    'STARTUP_FINANCING': 'STRUPCO_3YR',
182	    'FACILITY_TYPE': 'FACICODE',
183	    'REJECT_REASON': 'REJREASON',
184	    'FIN_CONCEPT': 'FIN_CONCEPT',
185	    'REFINANCING': 'REFIN_FLG',
186	    'CCPT_TAG': 'CCPT_TAG',
187	    'LOCATION': 'LN_UTILISE_LOCAT_CD',
188	    'ADDRESS_1': 'LU_ADD1',
189	    'ADDRESS_2': 'LU_ADD2',
190	    'CITY': 'LU_TOWN_CITY',
191	    'POSTCODE': 'LU_POSTCODE',
192	    'STATECODE': 'LU_STATE_CD',
193	    'COUNTRY': 'LU_COUNTRY_CD',
194	    'EREQ_NUMBER': 'EREQNO',
195	ehp_column_map = {
196	rename_dict = {k: v for k, v in ehp_column_map.items() if k in SUMM1_EHP.columns}
197	if rename_dict:
198	    SUMM1_EHP = SUMM1_EHP.rename(rename_dict)
199	    print(f"  âœ“ Renamed {len(rename_dict)} columns")
  âœ“ Renamed 34 columns
200	SUMM1_EHP = apply_sas_eof_check(SUMM1_EHP, "bnmsummary1_ehp", is_sas_dataset=True)
201	        print(f"  â„¹ SAS dataset - no FH/FT markers expected")
  â„¹ SAS dataset - no FH/FT markers expected
202	        df = df.with_columns([
203	            pl.lit("Y").alias("INDINTERIM"),
204	            pl.lit(RDATE).alias("DATE")
205	        return df
206	print(f"\nApplying production schema (CORRECTED)...")

Applying production schema (CORRECTED)...
207	SUMM1 = apply_production_schema(SUMM1, SUMM1_SCHEMA, "CSV")
208	    print(f"  ðŸ“‹ Applying production schema to {dataset_name}...")
  ðŸ“‹ Applying production schema to CSV...
209	    expressions = []
210	    for col_name, (col_type, col_len) in schema.items():
211	        if col_name not in df.columns:
212	        if col_type == 'char':
213	            if col_name == 'CUSTOMER_CODE':
214	            elif col_name in ['POSTCODE', 'STATE_CD',
215	                    pl.col(col_name)
216	                    .cast(pl.Utf8, strict=False)
217	                    .str.slice(0, col_len)
218	                    .alias(col_name)
219	                expr = (
220	        expressions.append(expr)
221	                pl.col(col_name)
222	                .cast(pl.Utf8, strict=False)
223	                .str.replace_all(r'[^0-9.\-]', '')
224	                .replace('', None)
225	                .cast(pl.Float64, strict=False)
226	                .alias(col_name)
227	            expr = (
228	                    pl.col(col_name)
229	                    .cast(pl.Utf8, strict=False)
230	                    .str.pad_start(col_len, '0')
231	                    .alias(col_name)
232	                expr = (
233	    df_transformed = df.select(expressions)
234	    schema_cols = list(schema.keys())
235	    df_transformed = df_transformed.select([c for c in schema_cols if c in df_transformed.columns])
236	    print(f"  âœ“ Schema applied: {len(df_transformed.columns)} columns, {df_transformed.height:,} rows")
  âœ“ Schema applied: 42 columns, 22 rows
237	    return df_transformed
238	SUMM1_EHP = apply_production_schema(SUMM1_EHP, SUMM1_SCHEMA, "EHP")
  ðŸ“‹ Applying production schema to EHP...
239	            if col_type == 'char':
240	                expressions.append(pl.lit(None).cast(pl.Utf8).alias(col_name))
  âœ“ Schema applied: 42 columns, 524 rows
241	print(f"\nCombining CSV + EHP...")

Combining CSV + EHP...
242	SUMM1_COMBINED = pl.concat([SUMM1, SUMM1_EHP], how="vertical_relaxed")
243	print(f"âœ“ Combined: {SUMM1_COMBINED.height:,} rows")
âœ“ Combined: 546 rows
244	final_summ1_count = save_with_cumulative_append(
245	    SUMM1_COMBINED, "SUMM1", SUMM1_SCHEMA, PREVDATE, OUTPUT_DATA_PATH, ELDS_DATA_PATH
246	    print(f"\nðŸ“¦ Building cumulative file for {dataset_name}...")

ðŸ“¦ Building cumulative file for SUMM1...
247	    print(f"  Today's new records: {df.height:,} rows")
  Today's new records: 546 rows
248	    print(f"  Today's date (RDATE): {RDATE}")
  Today's date (RDATE): 24432
249	    prev_parquet = Path(f"{data_path}/year={prev_date.year}/month={prev_date.month:02d}/day={prev_date.day:02d}/{dataset_name}.parquet")
250	    print(f"  Looking for previous cumulative: {prev_parquet.name}")
  Looking for previous cumulative: SUMM1.parquet
251	    temp_today = output_path / f"{dataset_name}_today_temp.parquet"
252	    print(f"  ðŸ’¾ Saving today's data to temp...")
  ðŸ’¾ Saving today's data to temp...
253	    duckdb.sql(f"""
254	        TO '{temp_today}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
255	    print(f"  âœ“ Temp file created: {df.height:,} rows")
  âœ“ Temp file created: 546 rows
256	    output_file = output_path / f"{dataset_name}.parquet"
257	    if not prev_parquet.exists():
258	        try:
259	            prev_count = duckdb.query(f"SELECT COUNT(*) FROM read_parquet('{prev_parquet}')").fetchone()[0]
260	            print(f"  âœ“ Previous cumulative from {prev_date.strftime('%Y-%m-%d')}: {prev_count:,} rows")
  âœ“ Previous cumulative from 2026-11-21: 1,845,324 rows
261	            print(f"  ðŸ”„ Streaming append via DuckDB...")
  ðŸ”„ Streaming append via DuckDB...
262	            print(f"    Combining: {prev_count:,} (previous) + {df.height:,} (today)")
    Combining: 1,845,324 (previous) + 546 (today)
263	            duckdb.sql(f"""
264	                    SELECT * FROM read_parquet('{prev_parquet}')
265	                    SELECT * FROM read_parquet('{temp_today}')
266	                TO '{output_file}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
267	            final_count = duckdb.query(f"SELECT COUNT(*) FROM read_parquet('{output_file}')").fetchone()[0]
268	            print(f"  âœ“ New cumulative: {final_count:,} rows")
  âœ“ New cumulative: 1,845,870 rows
269	            print(f"    Verified: {prev_count:,} + {df.height:,} = {final_count:,}")
    Verified: 1,845,324 + 546 = 1,845,870
270	    return final_count
271	print(f"\nâœ“ SUMM1 Complete: {final_summ1_count:,} total rows")

âœ“ SUMM1 Complete: 1,845,870 total rows
272	print("\n" + "="*70)

======================================================================
273	print("PROCESSING SUMM2 (Borrower Details)")
PROCESSING SUMM2 (Borrower Details)
274	print("="*70)
======================================================================
275	print(f"\nReading {BNMSUMM2.name}...")

Reading bnmsummary2_20261122.csv...
276	if not BNMSUMM2.exists():
277	try:
278	    SUMM2 = pl.read_csv(
279	        BNMSUMM2,
280	        separator=",",
281	        has_header=False,
282	        ignore_errors=True,
283	        truncate_ragged_lines=True,
284	        infer_schema_length=0,
285	        skip_rows=1
286	    print(f"  âœ“ Raw CSV: {SUMM2.height:,} rows, {SUMM2.width} columns")
  âœ“ Raw CSV: 31 rows, 37 columns
287	    csv_column_names = [
288	    if SUMM2.width < 36:
289	    SUMM2 = SUMM2.select([pl.col(f"column_{i+1}") for i in range(36)])
290	    SUMM2.columns = csv_column_names
291	SUMM2 = apply_sas_eof_check(SUMM2, "bnmsummary2_csv", is_sas_dataset=False)
  ðŸ“Š bnmsummary2_csv: Control count = 30
  ðŸ“Š bnmsummary2_csv: Actual data rows = 30
  âœ“ Record count matches control (30)
292	print(f"\nReading {Path(EHP2_SRC).name}...")

Reading intg_app_ehp_fs_dwh_bnmsummary2.sas7bdat...
293	if not Path(EHP2_SRC).exists():
294	SUMM2_EHP_df, _ = pyreadstat.read_sas7bdat(EHP2_SRC)
295	print(f"âœ“ SAS7BDAT loaded: {len(SUMM2_EHP_df):,} rows")
âœ“ SAS7BDAT loaded: 693 rows
296	SUMM2_EHP = pl.from_pandas(SUMM2_EHP_df)
297	print(f"  ðŸ“‹ Mapping EHP column names...")
  ðŸ“‹ Mapping EHP column names...
298	    'DATECOMPLETE': 'DTECOMPLETE',
299	    'NAME': 'APPLNAME',
300	    'NATIONALITY': 'COUNTRY',
301	    'DOB': 'DBIRTH',
302	    'CORP_STATUS': 'CORP_STATUS_CD',
303	    'INDUSTRIAL_SECTOR': 'INDUSTRIAL_STATUS',
304	    'RESIDENCY_STATUS': 'RESIDENCY_STATUS_CD',
305	    'INCOME': 'ANNSUBTSALARY',
306	    'EMPLOYER_NAME': 'EMPNAME',
307	    'EMPLOYMENT_SECTOR': 'EMPLOY_SECTOR_CD',
308	    'EMPLOYMENT_TYPE': 'EMPLOY_TYPE_CD',
309	    'STATE': 'STATE_CD',
310	    'COUNTRY': 'COUNTRY_CD',
311	    'MARITALSTATUS': 'MARRIED',
312	    'CUSTCODE': 'CUSTOMER_CODE',
313	    'CISNO': 'CISNUMBER',
314	    'NUM_EMPLOYEE': 'NO_OF_EMPLOYEE',
315	    'MSIC': 'INDUSTRIAL_SECTOR_CD',
316	    'MASCO': 'OCCUPAT_MASCO_CD',
317	    'DSR': 'DSRISS3'
318	ehp2_column_map = {
319	rename_dict = {k: v for k, v in ehp2_column_map.items() if k in SUMM2_EHP.columns}
320	if rename_dict:
321	    SUMM2_EHP = SUMM2_EHP.rename(rename_dict)
322	    print(f"  âœ“ Renamed {len(rename_dict)} columns")
  âœ“ Renamed 20 columns
323	SUMM2_EHP = apply_sas_eof_check(SUMM2_EHP, "bnmsummary2_ehp", is_sas_dataset=True)
  â„¹ SAS dataset - no FH/FT markers expected
324	print(f"\nApplying production schema...")

Applying production schema...
325	SUMM2 = apply_production_schema(SUMM2, SUMM2_SCHEMA, "CSV")
  ðŸ“‹ Applying production schema to CSV...
326	                    pl.when(
327	                        pl.col(col_name).is_null() |
328	                        (pl.col(col_name).cast(pl.Utf8, strict=False).str.strip_chars() == '')
329	                    .then(None)  # Keep as NULL if empty
330	                        pl.col(col_name)
331	                        .cast(pl.Utf8, strict=False)
332	                        .str.pad_start(col_len, '0')
333	                    .alias(col_name)
334	                expr = (
  âœ“ Schema applied: 38 columns, 30 rows
335	SUMM2_EHP = apply_production_schema(SUMM2_EHP, SUMM2_SCHEMA, "EHP")
  ðŸ“‹ Applying production schema to EHP...
  âœ“ Schema applied: 38 columns, 693 rows
336	print(f"\nCombining CSV + EHP...")

Combining CSV + EHP...
337	SUMM2_COMBINED = pl.concat([SUMM2, SUMM2_EHP], how="vertical_relaxed")
338	print(f"âœ“ Combined: {SUMM2_COMBINED.height:,} rows")
âœ“ Combined: 723 rows
339	final_summ2_count = save_with_cumulative_append(
340	    SUMM2_COMBINED, "SUMM2", SUMM2_SCHEMA, PREVDATE, OUTPUT_DATA_PATH, ELDS_DATA_PATH

ðŸ“¦ Building cumulative file for SUMM2...
  Today's new records: 723 rows
  Today's date (RDATE): 24432
  Looking for previous cumulative: SUMM2.parquet
  ðŸ’¾ Saving today's data to temp...
  âœ“ Temp file created: 723 rows
  âœ“ Previous cumulative from 2026-11-21: 2,438,080 rows
  ðŸ”„ Streaming append via DuckDB...
    Combining: 2,438,080 (previous) + 723 (today)
  âœ“ New cumulative: 2,438,803 rows
    Verified: 2,438,080 + 723 = 2,438,803
341	print(f"\nâœ“ SUMM2 Complete: {final_summ2_count:,} rows")

âœ“ SUMM2 Complete: 2,438,803 rows
342	print("\n" + "="*70)

======================================================================
343	print("PROCESSING COMPLETE")
PROCESSING COMPLETE
344	print("="*70)
======================================================================
345	print(f"Date: {REPTDATE.strftime('%Y-%m-%d')}")
Date: 2026-11-22
346	print(f"Output Directory: {OUTPUT_DATA_PATH}")
Output Directory: /parquet/dwh/ELDS/year=2026/month=11/day=22
347	print(f"\nResults:")

Results:
348	print(f"  SUMM1.parquet: {final_summ1_count:,} rows, 40 columns")
  SUMM1.parquet: 1,845,870 rows, 40 columns
349	print(f"  SUMM2.parquet: {final_summ2_count:,} rows, 37 columns")
  SUMM2.parquet: 2,438,803 rows, 37 columns

Total time elapsed : 00:00:20
Program end : 2026/11/23 09:15:51
