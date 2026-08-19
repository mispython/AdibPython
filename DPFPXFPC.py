============================================================
EIIQINST - Islamic Trustee and Client Account Reporting
============================================================

Report Period: 12/2025 (Week: 4)
SDESC: PUBLIC BANK BERHAD

Loading data...
  FLOAT: 18927 rows
  IBGPIDM: 7609 rows
  REMIT: 6385 rows
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQINST.py:72: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  df['acctno'] = df['acctno'].astype(str).str.strip()
  DEP: 3325593 rows
  CLIENT: 617 rows

Processing Trustee Accounts...
  SA/CA/FD: 9 rows
  Trustee >60k: 0 accounts
  Trustee <=60k: 1 accounts
  Output: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIQINST/islamic_trustee_low.txt

TRUSTEE <=60000 by Branch:
  Branch 161.0: RM 18,305.23

Processing Client Accounts...
  CLIENT master: 617 rows
  Client >60k: 281 accounts
  Client <=60k: 336 accounts
  Output: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIQINST/islamic_client_high.txt
  Output: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIQINST/islamic_client_low.txt

CLIENT >60000 by Branch:
  Branch 3.0: RM 8,207,789.80
  Branch 4.0: RM 1,078,211.83
  Branch 5.0: RM 1,966,217.34
  Branch 7.0: RM 6,459,150.50
  Branch 8.0: RM 992,382.74
  Branch 9.0: RM 414,943.31
  Branch 10.0: RM 1,238,427.88
  Branch 13.0: RM 13,145,660.67
  Branch 14.0: RM 436,423.90
  Branch 15.0: RM 790,267.05
  Branch 16.0: RM 254,044.39
  Branch 18.0: RM 60,369.56
  Branch 19.0: RM 5,980,710.57
  Branch 21.0: RM 225,418.11
  Branch 22.0: RM 1,255,073.40
  Branch 23.0: RM 2,538,210.47
  Branch 24.0: RM 3,391,424.41
  Branch 26.0: RM 197,360.49
  Branch 28.0: RM 605,546.17
  Branch 29.0: RM 88,393.06
  Branch 30.0: RM 2,404,392.63
  Branch 32.0: RM 712,489.92
  Branch 34.0: RM 5,675,759.35
  Branch 35.0: RM 450,838.36
  Branch 36.0: RM 15,616,458.39
  Branch 37.0: RM 313,251.35
  Branch 38.0: RM 80,517.39
  Branch 39.0: RM 200,058.59
  Branch 41.0: RM 420,981.29
  Branch 42.0: RM 800,162.97
  Branch 44.0: RM 390,950.61
  Branch 45.0: RM 4,330,679.47
  Branch 46.0: RM 1,108,597.87
  Branch 47.0: RM 720,851.17
  Branch 50.0: RM 280,408.08
  Branch 51.0: RM 167,436.16
  Branch 53.0: RM 845,177.19
  Branch 56.0: RM 2,883,903.41
  Branch 57.0: RM 509,208.56
  Branch 61.0: RM 10,376,725.07
  Branch 64.0: RM 1,792,812.54
  Branch 66.0: RM 3,952,838.37
  Branch 67.0: RM 103,978.00
  Branch 74.0: RM 2,119,720.73
  Branch 77.0: RM 1,067,679.83
  Branch 79.0: RM 152,079.92
  Branch 83.0: RM 96,120.99
  Branch 87.0: RM 1,441,694.81
  Branch 88.0: RM 217,730.59
  Branch 90.0: RM 703,914.24
  Branch 91.0: RM 74,653.14
  Branch 94.0: RM 162,310.68
  Branch 95.0: RM 275,423.02
  Branch 96.0: RM 243,045.82
  Branch 104.0: RM 169,893.68
  Branch 106.0: RM 457,522.05
  Branch 107.0: RM 250,251.97
  Branch 109.0: RM 548,714.17
  Branch 110.0: RM 2,740,133.93
  Branch 111.0: RM 2,293,154.56
  Branch 112.0: RM 365,409.69
  Branch 113.0: RM 558,641.78
  Branch 117.0: RM 615,613.83
  Branch 123.0: RM 1,526,054.55
  Branch 125.0: RM 81,699.00
  Branch 126.0: RM 16,453,250.84
  Branch 127.0: RM 200,977.79
  Branch 129.0: RM 570,826.01
  Branch 130.0: RM 517,582.42
  Branch 131.0: RM 1,043,915.39
  Branch 136.0: RM 4,577,991.38
  Branch 137.0: RM 1,027,054.04
  Branch 139.0: RM 1,114,343.90
  Branch 140.0: RM 1,997,844.15
  Branch 141.0: RM 803,817.84
  Branch 143.0: RM 160,145.49
  Branch 145.0: RM 488,337.66
  Branch 147.0: RM 1,227,600.68
  Branch 148.0: RM 1,806,427.18
  Branch 150.0: RM 492,724.44
  Branch 151.0: RM 176,828.03
  Branch 154.0: RM 481,948.77
  Branch 158.0: RM 144,008.82
  Branch 159.0: RM 62,731.36
  Branch 160.0: RM 1,085,118.09
  Branch 161.0: RM 4,402,553.47
  Branch 163.0: RM 190,317.68
  Branch 164.0: RM 2,251,377.95
  Branch 165.0: RM 1,080,028.56
  Branch 168.0: RM 1,769,157.27
  Branch 172.0: RM 890,609.72
  Branch 174.0: RM 351,106.99
  Branch 176.0: RM 3,202,523.62
  Branch 177.0: RM 1,117,915.21
  Branch 178.0: RM 245,166.90
  Branch 179.0: RM 2,818,839.01
  Branch 180.0: RM 743,220.03
  Branch 185.0: RM 27,050,149.22
  Branch 190.0: RM 103,161.06
  Branch 194.0: RM 114,955.53
  Branch 196.0: RM 4,027,265.79
  Branch 198.0: RM 3,264,706.29
  Branch 199.0: RM 151,842.01
  Branch 202.0: RM 104,541.17
  Branch 204.0: RM 5,306,841.19
  Branch 207.0: RM 6,339,394.29
  Branch 208.0: RM 983,679.33
  Branch 209.0: RM 847,526.56
  Branch 217.0: RM 5,973,280.40
  Branch 222.0: RM 6,255,622.01
  Branch 224.0: RM 851,718.38
  Branch 231.0: RM 1,877,428.45
  Branch 232.0: RM 86,522.65
  Branch 237.0: RM 3,834,388.96
  Branch 241.0: RM 72,069.71
  Branch 244.0: RM 113,625.94
  Branch 254.0: RM 4,241,280.04
  Branch 256.0: RM 6,672,624.31
  Branch 258.0: RM 1,015,700.68
  Branch 260.0: RM 2,051,214.32
  Branch 269.0: RM 3,596,849.68
  Branch 270.0: RM 1,987,678.30
  Branch 273.0: RM 549,921.56
  Branch 274.0: RM 5,165,865.90
  Branch 275.0: RM 715,297.56
  Branch 278.0: RM 427,902.41
  Branch 281.0: RM 488,015.01
  Branch 282.0: RM 777,721.49
  Branch 284.0: RM 596,679.99
  Branch 288.0: RM 3,570,841.58
  Branch 290.0: RM 220,675.79
  Branch 293.0: RM 1,255,452.67
  Branch 294.0: RM 193,449.94
  Branch 295.0: RM 534,352.20
  Branch 296.0: RM 352,758.50
  Branch 701.0: RM 2,312,064.20
  Branch 703.0: RM 458,460.31
  Branch 704.0: RM 6,036,920.66

CLIENT <=60000 by Branch:
  Branch 2.0: RM 61,712.14
  Branch 3.0: RM 3,979.60
  Branch 4.0: RM 247.42
  Branch 5.0: RM 57,966.67
  Branch 6.0: RM 172.73
  Branch 7.0: RM 107,974.28
  Branch 8.0: RM 46,352.19
  Branch 9.0: RM 2,425.65
  Branch 10.0: RM 3,013.27
  Branch 13.0: RM 114,032.03
  Branch 14.0: RM 1,320.22
  Branch 16.0: RM 1,532.50
  Branch 19.0: RM 4,511.98
  Branch 22.0: RM 220.00
  Branch 23.0: RM 55,475.42
  Branch 24.0: RM 27,306.84
  Branch 25.0: RM 22,058.03
  Branch 28.0: RM 210.00
  Branch 30.0: RM 100,054.69
  Branch 31.0: RM 0.10
  Branch 33.0: RM 249.11
  Branch 34.0: RM 52,735.88
  Branch 36.0: RM 955.09
  Branch 37.0: RM 10,725.58
  Branch 42.0: RM 25,050.92
  Branch 45.0: RM 1,010.00
  Branch 46.0: RM 37,347.92
  Branch 47.0: RM 50,171.95
  Branch 49.0: RM 331.11
  Branch 50.0: RM 1,709.81
  Branch 52.0: RM 12,089.81
  Branch 53.0: RM 9,172.33
  Branch 57.0: RM 53,924.49
  Branch 58.0: RM 1,140.20
  Branch 61.0: RM 16,747.20
  Branch 66.0: RM 7,798.07
  Branch 70.0: RM 9,412.15
  Branch 72.0: RM 62.81
  Branch 77.0: RM 7,281.51
  Branch 79.0: RM 342.60
  Branch 81.0: RM 6,874.42
  Branch 83.0: RM 5,309.97
  Branch 88.0: RM 49,406.17
  Branch 91.0: RM 634.16
  Branch 94.0: RM 38,583.00
  Branch 96.0: RM 4,232.00
  Branch 97.0: RM 760.27
  Branch 102.0: RM 30,742.19
  Branch 104.0: RM 236.05
  Branch 106.0: RM 25,342.00
  Branch 107.0: RM 22,119.25
  Branch 108.0: RM 3,882.63
  Branch 109.0: RM 2,873.32
  Branch 110.0: RM 12,225.75
  Branch 111.0: RM 6,494.50
  Branch 112.0: RM 79,816.07
  Branch 113.0: RM 33,294.19
  Branch 114.0: RM 6.00
  Branch 118.0: RM 78,235.86
  Branch 123.0: RM 26,797.23
  Branch 126.0: RM 7,322.31
  Branch 128.0: RM 8,961.94
  Branch 129.0: RM 4,310.53
  Branch 130.0: RM 11,236.61
  Branch 131.0: RM 451.82
  Branch 136.0: RM 92,357.74
  Branch 137.0: RM 3,772.51
  Branch 139.0: RM 34,006.73
  Branch 140.0: RM 14,313.03
  Branch 141.0: RM 5,908.78
  Branch 142.0: RM 3.00
  Branch 143.0: RM 108,637.29
  Branch 146.0: RM 6,755.43
  Branch 147.0: RM 177.84
  Branch 148.0: RM 14.50
  Branch 150.0: RM 11,817.80
  Branch 151.0: RM 983.35
  Branch 153.0: RM 5,712.27
  Branch 154.0: RM 99,362.06
  Branch 156.0: RM 18,220.11
  Branch 158.0: RM 822.15
  Branch 159.0: RM 73,031.01
  Branch 161.0: RM 947.41
  Branch 162.0: RM 11,663.33
  Branch 163.0: RM 6,755.06
  Branch 164.0: RM 108,054.83
  Branch 165.0: RM 477.00
  Branch 168.0: RM 5,192.10
  Branch 171.0: RM 53,983.56
  Branch 173.0: RM 19,438.15
  Branch 174.0: RM 4,792.50
  Branch 176.0: RM 44,076.82
  Branch 177.0: RM 59,414.24
  Branch 179.0: RM 66,912.07
  Branch 180.0: RM 23,256.11
  Branch 185.0: RM 87,054.94
  Branch 195.0: RM 32.00
  Branch 196.0: RM 2,261.80
  Branch 198.0: RM 39,436.19
  Branch 202.0: RM 427.25
  Branch 204.0: RM 4,721.29
  Branch 205.0: RM 10,668.50
  Branch 206.0: RM 50,372.20
  Branch 207.0: RM 18,113.05
  Branch 208.0: RM 49,372.58
  Branch 209.0: RM 879.40
  Branch 217.0: RM 1,447.98
  Branch 224.0: RM 3,115.50
  Branch 225.0: RM 8,111.20
  Branch 228.0: RM 0.00
  Branch 231.0: RM 3,701.37
  Branch 235.0: RM 4,364.06
  Branch 241.0: RM 56.15
  Branch 243.0: RM 156.54
  Branch 244.0: RM 50,372.00
  Branch 245.0: RM 2,106.29
  Branch 248.0: RM 27,355.15
  Branch 251.0: RM 1,837.68
  Branch 254.0: RM 21,024.98
  Branch 258.0: RM 31,534.75
  Branch 264.0: RM 4,700.00
  Branch 266.0: RM 50,111.61
  Branch 269.0: RM 20,513.21
  Branch 270.0: RM 12,649.81
  Branch 274.0: RM 37,656.37
  Branch 275.0: RM -302,168.50
  Branch 278.0: RM 59,149.88
  Branch 283.0: RM 4,950.00
  Branch 284.0: RM 16,619.57
  Branch 285.0: RM 3,030.50
  Branch 287.0: RM 24,192.50
  Branch 288.0: RM 4,910.00
  Branch 293.0: RM 1,544.31
  Branch 294.0: RM 3,030.00
  Branch 295.0: RM 101,074.56
  Branch 296.0: RM 40,038.29
  Branch 701.0: RM 43,377.65
  Branch 702.0: RM 304.62
  Branch 703.0: RM 70,047.75
  Branch 704.0: RM 106,589.15

Checking for duplicate accounts...
  No duplicate accounts found

============================================================
SUMMARY
============================================================

Trustee Accounts:
  Total: RM 18,305.23
  >60k: RM 0.00 (0 accounts)
  <=60k: RM 18,305.23 (1 accounts)

Client Accounts:
  Total: RM 286,673,377.55
  >60k: RM 283,702,702.10 (281 accounts)
  <=60k: RM 2,970,675.45 (336 accounts)

============================================================
✓ EIIQINST Complete
