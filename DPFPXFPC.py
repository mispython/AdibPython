============================================================
EIBQINST - Trustee and Client Account Quarterly Reporting
============================================================

Report Date: 2026-07-31 (Week: 4)

Loading data...
  FLOAT: 18927 records
  IBGPIDM: 7609 records
  REMIT/UNCLAIM: 6385 records
  DEP: 920763 records
  CLIENT: 3338 records

============================================================
Processing Trustee Accounts...
============================================================
  Trustee SA/CA/FD (with purpose filter): 162 records

Trustee >60k: 37 accounts
Trustee <=60k: 6 accounts

Writing Trustee output files...
  Written to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQINST/trustee_high.txt
  Written to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQINST/trustee_low.txt

TRUSTEE >60000 by Branch:
  Branch 168.0: RM 3,752,676.82
  Branch 18.0: RM 938,255.52
  Branch 196.0: RM 10,235,196.87
  Branch 2.0: RM 6,034,191.79
  Branch 4.0: RM 1,614,488.67

TRUSTEE <=60000 by Branch:
  Branch 168.0: RM 39,798.43
  Branch 18.0: RM 105,384.65
  Branch 196.0: RM 27,266.23

============================================================
Processing Client Accounts...
============================================================
  Client SA/CA/FD (without purpose filter): 6204839 records
  Debug - Client accounts: 3338
  Debug - SACA client accounts: 6204839
  Debug - Overlap: 3334
  Debug - Client after join with SACA: 3334
  Debug - Client after join with DEP: 519

Client >60k: 403 accounts
Client <=60k: 116 accounts

Writing Client output files...
  Written to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQINST/client_high.txt
  Written to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQINST/client_low.txt

CLIENT >60000 by Branch:
  Branch 2.0: RM 10,801,838.09
  Branch 3.0: RM 39,851,820.04
  Branch 4.0: RM 1,027,281.08
  Branch 5.0: RM 3,199,911.12
  Branch 6.0: RM 13,631,780.30
  Branch 7.0: RM 161,746,862.59
  Branch 8.0: RM 8,240,099.64
  Branch 9.0: RM 1,082,254.54
  Branch 10.0: RM 2,537,535.82
  Branch 13.0: RM 7,279,497.65
  Branch 15.0: RM 1,201,973.38
  Branch 17.0: RM 2,164,917.02
  Branch 19.0: RM 44,445,117.72
  Branch 21.0: RM 749,531.84
  Branch 27.0: RM 820,861.42
  Branch 29.0: RM 8,927,108.33
  Branch 30.0: RM 591,614.68
  Branch 32.0: RM 122,444.68
  Branch 33.0: RM 2,303,833.17
  Branch 34.0: RM 304,750.31
  Branch 35.0: RM 71,229,619.67
  Branch 36.0: RM 7,539,742.22
  Branch 37.0: RM 7,731,053.33
  Branch 38.0: RM 335,361.07
  Branch 42.0: RM 1,923,781.41
  Branch 43.0: RM 2,856,838.88
  Branch 44.0: RM 9,814,811.72
  Branch 45.0: RM 832,682.52
  Branch 47.0: RM 519,528.39
  Branch 49.0: RM 705,762.21
  Branch 50.0: RM 1,746,433.24
  Branch 51.0: RM 10,970,049.23
  Branch 53.0: RM 202,788.53
  Branch 55.0: RM 641,055.41
  Branch 56.0: RM 9,118,846.17
  Branch 57.0: RM 19,284,714.65
  Branch 58.0: RM 143,122.85
  Branch 59.0: RM 3,570,653.07
  Branch 60.0: RM 3,820,093.49
  Branch 61.0: RM 3,506,130.65
  Branch 62.0: RM 11,255,604.40
  Branch 64.0: RM 2,441,572.40
  Branch 65.0: RM 5,179,947.85
  Branch 66.0: RM 31,907,307.49
  Branch 71.0: RM 61,217.09
  Branch 76.0: RM 552,914.19
  Branch 78.0: RM 6,013,652.11
  Branch 80.0: RM 2,376,494.55
  Branch 83.0: RM 258,978.77
  Branch 91.0: RM 809,098.59
  Branch 92.0: RM 7,393,594.55
  Branch 94.0: RM 2,687,347.57
  Branch 95.0: RM 2,387,702.46
  Branch 97.0: RM 2,762,842.40
  Branch 106.0: RM 3,763,014.21
  Branch 107.0: RM 2,531,133.55
  Branch 110.0: RM 18,323,834.07
  Branch 112.0: RM 490,803.56
  Branch 113.0: RM 1,339,302.37
  Branch 114.0: RM 763,079.92
  Branch 120.0: RM 336,090.52
  Branch 121.0: RM 595,655.20
  Branch 122.0: RM 303,336.98
  Branch 123.0: RM 70,898.24
  Branch 124.0: RM 2,841,220.17
  Branch 125.0: RM 1,138,574.19
  Branch 126.0: RM 1,669,195.07
  Branch 127.0: RM 352,119.93
  Branch 129.0: RM 6,528,855.11
  Branch 130.0: RM 2,046,627.88
  Branch 131.0: RM 2,672,233.82
  Branch 133.0: RM 103,536.50
  Branch 135.0: RM 1,133,298.07
  Branch 136.0: RM 11,422,499.62
  Branch 137.0: RM 144,754.01
  Branch 138.0: RM 61,838.25
  Branch 140.0: RM 176,160.87
  Branch 141.0: RM 3,703,015.72
  Branch 143.0: RM 202,409.97
  Branch 144.0: RM 205,132.94
  Branch 148.0: RM 2,723,696.38
  Branch 150.0: RM 1,709,909.04
  Branch 153.0: RM 483,157.10
  Branch 154.0: RM 517,664.22
  Branch 156.0: RM 566,108.16
  Branch 157.0: RM 9,387,519.09
  Branch 159.0: RM 1,192,258.32
  Branch 160.0: RM 4,608,617.10
  Branch 161.0: RM 2,227,550.94
  Branch 162.0: RM 2,168,338.39
  Branch 165.0: RM 1,778,629.74
  Branch 168.0: RM 357,759,854.21
  Branch 169.0: RM 2,323,284.93
  Branch 170.0: RM 465,518.88
  Branch 171.0: RM 2,243,701.39
  Branch 172.0: RM 455,207.51
  Branch 174.0: RM 6,077,586.73
  Branch 175.0: RM 162,846.04
  Branch 177.0: RM 750,522.17
  Branch 179.0: RM 8,674,146.66
  Branch 183.0: RM 901,663.38
  Branch 185.0: RM 2,201,577.42
  Branch 195.0: RM 433,756.44
  Branch 196.0: RM 1,240,585.17
  Branch 197.0: RM 234,908.73
  Branch 198.0: RM 66,554.31
  Branch 199.0: RM 977,072.10
  Branch 201.0: RM 3,909,272.73
  Branch 202.0: RM 1,378,838.92
  Branch 203.0: RM 760,262.08
  Branch 204.0: RM 3,157,236.45
  Branch 206.0: RM 157,066.97
  Branch 207.0: RM 215,198.57
  Branch 208.0: RM 16,514,546.77
  Branch 209.0: RM 4,420,404.30
  Branch 210.0: RM 1,099,878.35
  Branch 216.0: RM 3,419,816.92
  Branch 217.0: RM 9,906,472.05
  Branch 222.0: RM 1,448,107.03
  Branch 224.0: RM 4,916,278.17
  Branch 226.0: RM 1,090,204.64
  Branch 228.0: RM 814,534.53
  Branch 232.0: RM 1,562,107.76
  Branch 233.0: RM 2,887,191.12
  Branch 241.0: RM 1,429,539.65
  Branch 242.0: RM 731,144.49
  Branch 249.0: RM 299,428.62
  Branch 252.0: RM 1,722,882.08
  Branch 256.0: RM 4,905,444.49
  Branch 257.0: RM 91,016.83
  Branch 262.0: RM 177,207.58
  Branch 264.0: RM 9,226,718.30
  Branch 267.0: RM 2,026,344.47
  Branch 268.0: RM 732,695.82
  Branch 269.0: RM 1,064,273.84
  Branch 270.0: RM 943,556.83
  Branch 273.0: RM 326,376.80
  Branch 274.0: RM 155,131.70
  Branch 276.0: RM 1,417,847.36
  Branch 280.0: RM 2,227,639.26
  Branch 281.0: RM 138,953.97
  Branch 283.0: RM 2,437,839.86
  Branch 284.0: RM 395,263.32
  Branch 287.0: RM 4,770,645.29
  Branch 288.0: RM 167,165.89
  Branch 289.0: RM 405,903.21
  Branch 296.0: RM 1,657,088.83

CLIENT <=60000 by Branch:
  Branch 2.0: RM 53,702.34
  Branch 3.0: RM 23,138.56
  Branch 5.0: RM 38,165.67
  Branch 6.0: RM 162,112.97
  Branch 7.0: RM 88,417.31
  Branch 9.0: RM 80,108.58
  Branch 10.0: RM 27,938.18
  Branch 11.0: RM 1,039.65
  Branch 13.0: RM 40,139.70
  Branch 19.0: RM 57,194.57
  Branch 23.0: RM 10,215.37
  Branch 28.0: RM 39,621.29
  Branch 29.0: RM 51,563.90
  Branch 38.0: RM 946.67
  Branch 42.0: RM 47,202.69
  Branch 45.0: RM 25,143.24
  Branch 47.0: RM 17,164.28
  Branch 49.0: RM 33,147.44
  Branch 50.0: RM 32,908.86
  Branch 54.0: RM 19,780.14
  Branch 55.0: RM 50,023.97
  Branch 58.0: RM 46,053.91
  Branch 60.0: RM 19,864.45
  Branch 62.0: RM 44,364.30
  Branch 64.0: RM 35,098.63
  Branch 79.0: RM 31,462.37
  Branch 80.0: RM 54,763.48
  Branch 95.0: RM 15,188.75
  Branch 96.0: RM 1,489.30
  Branch 110.0: RM 7,358.52
  Branch 112.0: RM 66,568.90
  Branch 120.0: RM 88,932.90
  Branch 121.0: RM 10,812.97
  Branch 130.0: RM 84,039.60
  Branch 131.0: RM 1,098.38
  Branch 138.0: RM 33,257.10
  Branch 143.0: RM 39,052.60
  Branch 146.0: RM 28,663.47
  Branch 150.0: RM 98,116.40
  Branch 156.0: RM 78,542.28
  Branch 159.0: RM 14,725.98
  Branch 160.0: RM 44,486.93
  Branch 165.0: RM 35,109.50
  Branch 168.0: RM 48,187.87
  Branch 169.0: RM 51,879.17
  Branch 171.0: RM 12,065.40
  Branch 172.0: RM 55,606.49
  Branch 175.0: RM 16,552.52
  Branch 177.0: RM 59,227.95
  Branch 196.0: RM 14,378.37
  Branch 197.0: RM 49,984.92
  Branch 198.0: RM 11,625.15
  Branch 202.0: RM 11,628.23
  Branch 204.0: RM 29,496.65
  Branch 205.0: RM 24,776.57
  Branch 208.0: RM 96,988.45
  Branch 217.0: RM 3,708.81
  Branch 222.0: RM 1,056.76
  Branch 224.0: RM 16,442.74
  Branch 243.0: RM 62,172.19
  Branch 266.0: RM 49,857.25
  Branch 268.0: RM 59,616.53
  Branch 270.0: RM 50,052.74
  Branch 273.0: RM 28,425.54
  Branch 275.0: RM 22,051.78
  Branch 276.0: RM 8,374.53
  Branch 280.0: RM 8,896.79
  Branch 288.0: RM 47,474.12

============================================================
Checking for duplicate accounts...
============================================================

Found 6 duplicate accounts:
  Account 1120702022.0 appears in: TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE
  Account 1375054827.0 appears in: TRUSTEE, TRUSTEE, TRUSTEE
  Account 1371928624.0 appears in: TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE
  Account 1347898536.0 appears in: TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE
  Account 1194869701.0 appears in: TRUSTEE, TRUSTEE
  Account 1286513018.0 appears in: TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE

============================================================
SUMMARY
============================================================

Trustee Accounts:
  Total: RM 22,747,258.98
  >60k: RM 22,574,809.67 (37 accounts)
  <=60k: RM 172,449.31 (6 accounts)

Client Accounts:
  Total: RM 1,116,590,011.31
  >60k: RM 1,113,970,757.69 (403 accounts)
  <=60k: RM 2,619,253.62 (116 accounts)

============================================================
✓ EIBQINST Complete
============================================================
