NOWK: 4, NOWK1: 3, REPTMON: 07, REPTMON1: 07
REPTYEAR: 2026, REPTDAY: 30, RDATE: 300726, SDATE: 230726
Processing LNNOTE (large file) in chunks...
NOTE: Checking data types and sample values...
First chunk schema:
Schema([('ACCTNO', Float64), ('LOANTYPE', Float64), ('NTBRCH', Float64), ('COLLDESC', String), ('BORSTAT', String), ('COLLYEAR', Float64), ('BALANCE', Float64)])

First 5 rows:
shape: (5, 7)
┌──────────┬──────────┬────────┬─────────────────────────────────┬─────────┬──────────┬─────────────┐
│ ACCTNO   ┆ LOANTYPE ┆ NTBRCH ┆ COLLDESC                        ┆ BORSTAT ┆ COLLYEAR ┆ BALANCE     │
│ ---      ┆ ---      ┆ ---    ┆ ---                             ┆ ---     ┆ ---      ┆ ---         │
│ f64      ┆ f64      ┆ f64    ┆ str                             ┆ str     ┆ f64      ┆ f64         │
╞══════════╪══════════╪════════╪═════════════════════════════════╪═════════╪══════════╪═════════════╡
│ 2.0001e9 ┆ 103.0    ┆ 702.0  ┆ NISSAN          X-TRAIL 2.0L (… ┆         ┆ 2016.0   ┆ 0.0022797   │
│ 2.0030e9 ┆ 103.0    ┆ 811.0  ┆ TOYOTA          INNOVA 2.0E MT… ┆         ┆ 2016.0   ┆ 4975.729431 │
│ 2.0056e9 ┆ 103.0    ┆ 811.0  ┆ HONDA           HR-V 1.8L V   … ┆         ┆ 2018.0   ┆ 29744.30395 │
│ 2.0065e9 ┆ 103.0    ┆ 60.0   ┆ PROTON          EXORA 1.6     … ┆         ┆ 2015.0   ┆ 5227.427592 │
│ 2.0069e9 ┆ 103.0    ┆ 800.0  ┆ HONDA           CRV 2.0 2WD   … ┆         ┆ 2018.0   ┆ -0.00203    │
└──────────┴──────────┴────────┴─────────────────────────────────┴─────────┴──────────┴─────────────┘

Unique LOANTYPE values: [102.0, 103.0, 104.0, 105.0, 110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 116.0, 120.0, 124.0, 127.0, 128.0, 133.0, 134.0, 135.0, 136.0, 138.0]
Unique BORSTAT values: ['W', '', 'P', 'K', 'X']
BALANCE stats: min=-1348811.2999999998, max=1018072602.7368581
Processed 10 chunks from LNNOTE...
Total chunks processed: 18

WARNING: No records found matching HP criteria!
Trying alternative filter without LOANTYPE restriction...

Found data without LOANTYPE filter. Sample LOANTYPE values: ['422.0', '654.0', '141.0', '113.0', '413.0', '112.0', '412.0', '663.0', '120.0', '184.0']

Processing NAME8...
NAME8 records: 1206155
Sample NAME8 data:
shape: (3, 3)
┌──────────────┬──────────────┬───────────────────┐
│ ACCTNO       ┆ LINETHRE     ┆ LINEFOUR          │
│ ---          ┆ ---          ┆ ---               │
│ str          ┆ str          ┆ str               │
╞══════════════╪══════════════╪═══════════════════╡
│ 2000083516.0 ┆ MR20825097B  ┆ PN8JAAT32TCA24302 │
│ 2002980905.0 ┆ 1TRA105044   ┆ PN111NV4003030003 │
│ 2005611033.0 ┆ R18ZG7906950 ┆ PMHRU5870HD706923 │
└──────────────┴──────────────┴───────────────────┘

Processing LOANTEMP...
LOANTEMP records: 663747
Sample LOANTEMP data:
shape: (3, 2)
┌──────────────┬────────┐
│ ACCTNO       ┆ ARREAR │
│ ---          ┆ ---    │
│ str          ┆ f64    │
╞══════════════╪════════╡
│ 2002980905.0 ┆ 1.0    │
│ 2005611033.0 ┆ 1.0    │
│ 2006460400.0 ┆ 1.0    │
└──────────────┴────────┘

Merging datasets...
WARNING: No data to merge!
Merged REPO records: 0
REPO records: 0
REPO1 records: 0

Generating REPOTXT.txt...
REPOTXT.txt generated successfully
Generating REPOTXT1.txt...
REPOTXT1.txt generated successfully

PROCESSING COMPLETED SUCCESSFULLY
