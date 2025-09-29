# Program: EICRMEOD_END_OF_DAY_DATE_GEN
# Purpose: Generate reporting dates (EOD, previous month start, month-year tag)
# Tools: Python datetime

import datetime
import os

# -----------------------------
# Step 1. Compute dates
# -----------------------------
today = datetime.date.today()
reptdate = today - datetime.timedelta(days=1)   # yesterday
prevdate = today.replace(day=1) - datetime.timedelta(days=1)  # last day prev month
prevmonth_start = prevdate.replace(day=1)       # start of prev month

# Formats
reptdate_str = reptdate.strftime("%Y%m%d")  # YYMMDDN8. → 20250928
prevdate_str = prevmonth_start.strftime("%Y%m%d")
mmyy_str     = prevmonth_start.strftime("%m%y")  # MMYYN4. → 0925

# -----------------------------
# Step 2. Ensure output folder
# -----------------------------
os.makedirs("output", exist_ok=True)

# -----------------------------
# Step 3. Write to files
# -----------------------------
with open("output/DAYFL.txt", "w") as f:
    f.write(reptdate_str + "\n")

with open("output/MTHFL.txt", "w") as f:
    f.write(prevdate_str + "\n")

with open("output/ALLFL.txt", "w") as f:
    f.write(mmyy_str + "\n")

print("EICRMEOD_END_OF_DAY_DATE_GEN completed.")
print("DAYFL :", reptdate_str)
print("MTHFL :", prevdate_str)
print("ALLFL :", mmyy_str)
