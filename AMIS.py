>>> READING TODAY'S DATA
30                       ) if parquet_exists(TODAY_CHANNEL_SUM) else pl.DataFrame()
31          try:
32              return pq.read_schema(str(path)) is not None
33      today_channel = (pl.read_parquet(TODAY_CHANNEL_SUM)
34                       .rename({"PROMPT": "TOLPROMPT", "UPDATED": "TOLUPDATE"})
35                       .select(["CHANNEL", "TOLPROMPT", "TOLUPDATE"])
36                       .with_columns(pl.lit(REPTDATE.strftime("%b%y").upper()).alias("MONTH"))
37                      ) if parquet_exists(TODAY_CHANNEL_UPDATE) else pl.DataFrame()
38      today_update = (pl.read_parquet(TODAY_CHANNEL_UPDATE)
39                          pl.when(pl.col("index") == 1).then("TOTAL PROMPT BASE")
40                           .when(pl.col("index") == 2).then("TOTAL UPDATED")
41                           .alias("LINE")
Traceback (most recent call last):
  File "/sas/pythonITD/mis/job/LOAN/EIBMCHNL.py", line 97, in <module>
    today_update = (pl.read_parquet(TODAY_CHANNEL_UPDATE)
  File "/sas/pythonITD/mis/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 9141, in with_columns
    return self.lazy().with_columns(*exprs, **named_exprs).collect(_eager=True)
  File "/sas/pythonITD/mis/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2032, in collect
    return wrap_df(ldf.collect(callback))
polars.exceptions.ColumnNotFoundError: TOTAL PROMPT BASE
SAS Connection terminated. Subprocess id was 3899207
