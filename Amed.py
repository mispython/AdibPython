1       import pandas as pd
2       import pyarrow as pa
3       import pyarrow.parquet as pq
4       import saspy
5       import sys
6       import os
7       from datetime import datetime,timedelta
8       import numpy as np
9       batch_dt = datetime.today() - timedelta(days=1)
10      day_str = f"{batch_dt.day:02d}"
11      BATCH_MODE = sys.argv[1]
12      print("BATCH MODE = ", BATCH_MODE)
BATCH MODE =  M
13      if BATCH_MODE == 'M':
14          print("Monthly")
Monthly
15          batch_dt_monthly = (batch_dt.replace(day=1) - timedelta(days=1))
16          month_str = f"{batch_dt_monthly.month:02d}"
17          year_str = f"{batch_dt_monthly.year % 100:02d}"
18          print(f"Processing month: {month_str}/{year_str}")
