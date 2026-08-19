PBBDPFMT imported successfully
============================================================
EIBQINST - Trustee and Client Account Quarterly Reporting
============================================================

Report Date: 2025-12-14 (Week: 4)

Loading data...
  FLOAT: 18927 records
  IBGPIDM: 7609 records
  REMIT/UNCLAIM: 6385 records
  DEP: 6002227 records
  CLIENT: 3338 records

============================================================
Processing Trustee Accounts...
============================================================
  Trustee SA/CA/FD (with purpose filter): 162 records

Trustee >60k: 56 accounts
Trustee <=60k: 102 accounts

Writing Trustee output files...
  Written to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQINST/trustee_high.txt
  Written to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQINST/trustee_low.txt

TRUSTEE >60000 by Branch:
  Branch 168.0: RM 12,275,529.21
  Branch 18.0: RM 938,255.52
  Branch 196.0: RM 10,235,196.87
  Branch 2.0: RM 6,034,191.79
  Branch 4.0: RM 1,614,488.67
  Branch 66.0: RM 78,460.45

TRUSTEE <=60000 by Branch:
  Branch 112.0: RM 311.86
  Branch 122.0: RM 8,517.61
  Branch 140.0: RM 8,751.60
  Branch 152.0: RM 51,376.75
  Branch 161.0: RM 13,316.12
  Branch 168.0: RM 598,820.44
  Branch 179.0: RM 7,956.69
  Branch 18.0: RM 155,055.65
  Branch 196.0: RM 27,266.23
  Branch 2.0: RM 3,374.34
  Branch 23.0: RM 34,842.99
  Branch 260.0: RM 2,502.87
  Branch 3.0: RM 9,650.30
  Branch 41.0: RM 50,600.34
  Branch 50.0: RM 52,035.69
  Branch 54.0: RM 26,232.46
  Branch 6.0: RM 25,499.75
  Branch 61.0: RM 17.40
  Branch 64.0: RM 9,990.00
  Branch 78.0: RM 68.77

============================================================
Processing Client Accounts...
============================================================
  Client SA/CA/FD (without purpose filter): 6204839 records
  Debug - Client accounts: 3338
  Debug - SACA client accounts: 6204839
  Debug - Overlap: 3334
  Debug - Client after join with SACA: 3334
  Debug - Client after join with DEP: 3334

Client >60k: 1845 accounts
Client <=60k: 1489 accounts

Writing Client output files...
  Written to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQINST/client_high.txt
  Written to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQINST/client_low.txt

CLIENT >60000 by Branch:
  Branch 2.0: RM 20,985,567.47
  Branch 3.0: RM 61,203,479.88
  Branch 4.0: RM 7,785,633.32
  Branch 5.0: RM 8,235,926.73
  Branch 6.0: RM 45,564,256.35
  Branch 7.0: RM 262,564,077.01
  Branch 8.0: RM 29,604,235.51
  Branch 9.0: RM 6,472,380.33
  Branch 10.0: RM 11,361,903.59
  Branch 13.0: RM 20,641,455.46
  Branch 15.0: RM 6,001,700.15
  Branch 16.0: RM 1,971,906.07
  Branch 17.0: RM 3,888,621.78
  Branch 18.0: RM 4,987,101.11
  Branch 19.0: RM 73,452,680.29
  Branch 20.0: RM 1,602,807.62
  Branch 21.0: RM 3,709,287.43
  Branch 22.0: RM 1,152,716.05
  Branch 23.0: RM 440,258.19
  Branch 24.0: RM 3,497,201.00
  Branch 25.0: RM 1,699,832.42
  Branch 26.0: RM 4,544,296.41
  Branch 27.0: RM 2,909,897.11
  Branch 28.0: RM 13,751,098.14
  Branch 29.0: RM 16,559,978.11
  Branch 30.0: RM 1,713,172.17
  Branch 32.0: RM 15,726,326.24
  Branch 33.0: RM 7,909,342.02
  Branch 34.0: RM 3,563,871.80
  Branch 35.0: RM 81,114,060.53
  Branch 36.0: RM 14,317,785.98
  Branch 37.0: RM 25,916,434.92
  Branch 38.0: RM 4,258,266.04
  Branch 39.0: RM 2,659,873.83
  Branch 40.0: RM 1,835,864.38
  Branch 41.0: RM 4,552,282.11
  Branch 42.0: RM 8,913,014.94
  Branch 43.0: RM 5,027,771.09
  Branch 44.0: RM 20,578,885.00
  Branch 45.0: RM 39,815,963.24
  Branch 46.0: RM 2,409,764.25
  Branch 47.0: RM 16,280,849.23
  Branch 48.0: RM 1,093,683.59
  Branch 49.0: RM 866,349.49
  Branch 50.0: RM 12,527,768.69
  Branch 51.0: RM 37,355,382.65
  Branch 52.0: RM 3,048,214.29
  Branch 53.0: RM 2,646,668.55
  Branch 54.0: RM 828,356.18
  Branch 55.0: RM 3,764,979.77
  Branch 56.0: RM 46,175,494.99
  Branch 57.0: RM 29,424,818.90
  Branch 58.0: RM 17,602,651.10
  Branch 59.0: RM 12,123,042.83
  Branch 60.0: RM 36,718,204.48
  Branch 61.0: RM 5,522,403.49
  Branch 62.0: RM 14,061,380.75
  Branch 63.0: RM 575,228.96
  Branch 64.0: RM 7,559,168.51
  Branch 65.0: RM 30,124,198.34
  Branch 66.0: RM 44,464,369.13
  Branch 67.0: RM 1,409,749.06
  Branch 68.0: RM 11,066,468.59
  Branch 69.0: RM 138,127.47
  Branch 71.0: RM 977,248.75
  Branch 72.0: RM 93,152.40
  Branch 73.0: RM 832,546.68
  Branch 76.0: RM 3,251,783.00
  Branch 77.0: RM 1,483,851.30
  Branch 78.0: RM 16,064,368.55
  Branch 79.0: RM 1,946,798.31
  Branch 80.0: RM 5,451,769.21
  Branch 83.0: RM 336,475.63
  Branch 85.0: RM 1,227,135.35
  Branch 87.0: RM 667,380.78
  Branch 88.0: RM 4,911,428.28
  Branch 90.0: RM 6,764,924.71
  Branch 91.0: RM 11,014,136.52
  Branch 92.0: RM 13,375,868.28
  Branch 93.0: RM 385,251.04
  Branch 94.0: RM 5,547,595.72
  Branch 95.0: RM 6,084,723.87
  Branch 96.0: RM 1,064,412.10
  Branch 97.0: RM 5,559,776.77
  Branch 103.0: RM 7,291,775.57
  Branch 104.0: RM 87,798.15
  Branch 105.0: RM 192,783.70
  Branch 106.0: RM 13,989,862.19
  Branch 107.0: RM 4,802,132.15
  Branch 108.0: RM 1,763,299.02
  Branch 109.0: RM 794,591.93
  Branch 110.0: RM 68,421,521.75
  Branch 111.0: RM 4,054,196.33
  Branch 112.0: RM 5,634,654.07
  Branch 113.0: RM 15,731,609.93
  Branch 114.0: RM 2,095,610.97
  Branch 115.0: RM 3,272,481.95
  Branch 116.0: RM 2,687,425.62
  Branch 118.0: RM 841,536.69
  Branch 120.0: RM 7,492,460.02
  Branch 121.0: RM 13,439,586.35
  Branch 122.0: RM 1,599,982.73
  Branch 123.0: RM 990,916.07
  Branch 124.0: RM 7,172,015.42
  Branch 125.0: RM 11,686,702.26
  Branch 126.0: RM 18,493,477.08
  Branch 127.0: RM 884,773.43
  Branch 128.0: RM 1,130,163.00
  Branch 129.0: RM 16,053,933.79
  Branch 130.0: RM 17,041,255.84
  Branch 131.0: RM 13,457,060.04
  Branch 133.0: RM 236,974.07
  Branch 135.0: RM 6,941,193.82
  Branch 136.0: RM 15,611,383.78
  Branch 137.0: RM 144,754.01
  Branch 138.0: RM 1,232,867.05
  Branch 139.0: RM 931,688.88
  Branch 140.0: RM 5,334,277.51
  Branch 141.0: RM 13,786,638.57
  Branch 143.0: RM 3,259,480.46
  Branch 144.0: RM 205,132.94
  Branch 145.0: RM 589,136.37
  Branch 146.0: RM 1,658,251.47
  Branch 147.0: RM 85,726.17
  Branch 148.0: RM 24,901,674.39
  Branch 149.0: RM 176,509.00
  Branch 150.0: RM 10,999,257.17
  Branch 151.0: RM 698,460.77
  Branch 153.0: RM 7,333,809.08
  Branch 154.0: RM 4,395,741.31
  Branch 156.0: RM 9,782,493.72
  Branch 157.0: RM 24,903,848.06
  Branch 158.0: RM 11,654,748.61
  Branch 159.0: RM 7,009,779.57
  Branch 160.0: RM 5,355,465.44
  Branch 161.0: RM 2,617,668.96
  Branch 162.0: RM 14,210,014.87
  Branch 163.0: RM 1,717,238.47
  Branch 164.0: RM 74,835.89
  Branch 165.0: RM 4,017,132.09
  Branch 167.0: RM 614,911.72
  Branch 168.0: RM 391,996,071.59
  Branch 169.0: RM 3,639,120.67
  Branch 170.0: RM 730,410.69
  Branch 171.0: RM 3,764,527.82
  Branch 172.0: RM 8,198,389.45
  Branch 174.0: RM 9,638,041.28
  Branch 175.0: RM 562,225.54
  Branch 176.0: RM 15,350,736.22
  Branch 177.0: RM 5,964,592.41
  Branch 178.0: RM 715,986.10
  Branch 179.0: RM 31,787,092.25
  Branch 180.0: RM 917,730.19
  Branch 183.0: RM 8,720,232.63
  Branch 184.0: RM 13,747,304.18
  Branch 185.0: RM 16,305,386.04
  Branch 186.0: RM 5,670,914.38
  Branch 190.0: RM 256,111.38
  Branch 191.0: RM 2,102,824.72
  Branch 192.0: RM 379,791.71
  Branch 193.0: RM 1,409,994.57
  Branch 194.0: RM 6,903,055.92
  Branch 195.0: RM 1,102,968.58
  Branch 196.0: RM 8,550,541.59
  Branch 197.0: RM 8,350,590.09
  Branch 198.0: RM 4,516,864.82
  Branch 199.0: RM 12,778,977.20
  Branch 201.0: RM 5,097,708.24
  Branch 202.0: RM 22,495,035.46
  Branch 203.0: RM 1,398,526.67
  Branch 204.0: RM 7,058,054.83
  Branch 205.0: RM 1,683,315.75
  Branch 206.0: RM 2,854,493.01
  Branch 207.0: RM 3,369,611.16
  Branch 208.0: RM 34,257,957.79
  Branch 209.0: RM 18,355,655.71
  Branch 210.0: RM 3,324,499.05
  Branch 211.0: RM 3,900,036.28
  Branch 216.0: RM 7,389,998.01
  Branch 217.0: RM 13,070,555.35
  Branch 221.0: RM 655,064.81
  Branch 222.0: RM 2,227,560.75
  Branch 224.0: RM 17,253,591.20
  Branch 225.0: RM 4,277,368.85
  Branch 226.0: RM 1,329,211.69
  Branch 228.0: RM 882,760.48
  Branch 230.0: RM 1,728,123.52
  Branch 231.0: RM 248,147.50
  Branch 232.0: RM 2,911,498.54
  Branch 233.0: RM 6,961,290.48
  Branch 235.0: RM 414,721.45
  Branch 237.0: RM 445,479.91
  Branch 239.0: RM 1,241,520.30
  Branch 241.0: RM 32,513,573.91
  Branch 242.0: RM 6,390,185.05
  Branch 243.0: RM 1,390,650.34
  Branch 244.0: RM 1,852,101.36
  Branch 245.0: RM 929,215.85
  Branch 247.0: RM 996,533.81
  Branch 248.0: RM 1,777,654.22
  Branch 249.0: RM 3,722,424.18
  Branch 251.0: RM 924,858.10
  Branch 252.0: RM 4,611,731.50
  Branch 254.0: RM 1,197,564.43
  Branch 256.0: RM 5,643,525.19
  Branch 257.0: RM 2,414,412.87
  Branch 259.0: RM 979,139.39
  Branch 260.0: RM 2,042,218.95
  Branch 262.0: RM 9,276,269.18
  Branch 263.0: RM 183,287.50
  Branch 264.0: RM 13,527,480.29
  Branch 265.0: RM 445,910.92
  Branch 267.0: RM 8,540,377.66
  Branch 268.0: RM 10,241,020.76
  Branch 269.0: RM 23,183,677.36
  Branch 270.0: RM 3,161,461.15
  Branch 273.0: RM 4,391,755.67
  Branch 274.0: RM 15,525,491.74
  Branch 275.0: RM 4,584,493.79
  Branch 276.0: RM 7,925,692.56
  Branch 278.0: RM 3,131,808.94
  Branch 280.0: RM 12,280,230.51
  Branch 281.0: RM 5,986,411.30
  Branch 283.0: RM 4,781,833.30
  Branch 284.0: RM 657,538.89
  Branch 285.0: RM 156,412.97
  Branch 286.0: RM 468,399.29
  Branch 287.0: RM 23,214,765.94
  Branch 288.0: RM 1,644,287.10
  Branch 289.0: RM 3,119,853.28
  Branch 290.0: RM 3,650,006.11
  Branch 291.0: RM 564,934.15
  Branch 292.0: RM 2,446,529.89
  Branch 293.0: RM 1,456,371.69
  Branch 294.0: RM 511,558.89
  Branch 295.0: RM 169,512.22
  Branch 296.0: RM 10,083,773.12

CLIENT <=60000 by Branch:
  Branch 2.0: RM 208,577.13
  Branch 3.0: RM 206,515.66
  Branch 4.0: RM 58,258.80
  Branch 5.0: RM 257,867.79
  Branch 6.0: RM 566,695.17
  Branch 7.0: RM 185,015.68
  Branch 8.0: RM -116,129.46
  Branch 9.0: RM 219,854.70
  Branch 10.0: RM 173,651.95
  Branch 11.0: RM 1,039.65
  Branch 13.0: RM 98,951.69
  Branch 14.0: RM 22,301.50
  Branch 15.0: RM 57,355.51
  Branch 16.0: RM 61,888.68
  Branch 17.0: RM 97,024.55
  Branch 18.0: RM 84,462.93
  Branch 19.0: RM 242,738.64
  Branch 20.0: RM 42,419.92
  Branch 21.0: RM 102,656.16
  Branch 22.0: RM 46,988.37
  Branch 23.0: RM 74,432.07
  Branch 24.0: RM 104,566.03
  Branch 25.0: RM 8,028.26
  Branch 26.0: RM 99,489.83
  Branch 27.0: RM 69,354.61
  Branch 28.0: RM 152,244.69
  Branch 29.0: RM 105,762.18
  Branch 30.0: RM 1,725.43
  Branch 31.0: RM 226.14
  Branch 32.0: RM 35,505.89
  Branch 33.0: RM 256,745.16
  Branch 34.0: RM 54,715.06
  Branch 35.0: RM 56,342.71
  Branch 36.0: RM 154,743.63
  Branch 37.0: RM 106,636.84
  Branch 38.0: RM 155,499.31
  Branch 39.0: RM 7,210.63
  Branch 40.0: RM 34,448.13
  Branch 41.0: RM 103,343.19
  Branch 42.0: RM 65,202.47
  Branch 43.0: RM 63,818.62
  Branch 44.0: RM 64,778.11
  Branch 45.0: RM -668,610.68
  Branch 46.0: RM 56,167.92
  Branch 47.0: RM 151,069.13
  Branch 48.0: RM 6,935.21
  Branch 49.0: RM 33,147.44
  Branch 50.0: RM 292,273.26
  Branch 51.0: RM 142,055.81
  Branch 52.0: RM 25,324.58
  Branch 53.0: RM 85,256.66
  Branch 54.0: RM 53,825.33
  Branch 55.0: RM 96,149.44
  Branch 56.0: RM 86,663.36
  Branch 57.0: RM 102,663.65
  Branch 58.0: RM 116,348.88
  Branch 59.0: RM 32,626.50
  Branch 60.0: RM 129,700.96
  Branch 61.0: RM 42,541.07
  Branch 62.0: RM 95,386.83
  Branch 63.0: RM 5,444.45
  Branch 64.0: RM 69,591.26
  Branch 66.0: RM 194,248.64
  Branch 67.0: RM 4,115.27
  Branch 68.0: RM 70,572.59
  Branch 69.0: RM 23,931.91
  Branch 70.0: RM 730.00
  Branch 71.0: RM 4,101.00
  Branch 73.0: RM 422.00
  Branch 75.0: RM 12,475.59
  Branch 76.0: RM 4,618.00
  Branch 77.0: RM 2.07
  Branch 78.0: RM 35,498.67
  Branch 79.0: RM 97,710.33
  Branch 80.0: RM 68,250.14
  Branch 81.0: RM 120,418.84
  Branch 83.0: RM 66,153.79
  Branch 87.0: RM 4,544.70
  Branch 88.0: RM 139,998.06
  Branch 89.0: RM 6,060.93
  Branch 90.0: RM 62,363.95
  Branch 91.0: RM 183,344.43
  Branch 92.0: RM 68,631.56
  Branch 93.0: RM 3,718.20
  Branch 95.0: RM 18,833.52
  Branch 96.0: RM 45,135.14
  Branch 97.0: RM 48,199.19
  Branch 102.0: RM 291.14
  Branch 103.0: RM 34,886.75
  Branch 104.0: RM 228.25
  Branch 106.0: RM 85,979.54
  Branch 107.0: RM 51,268.92
  Branch 108.0: RM 26,850.37
  Branch 110.0: RM 62,068.79
  Branch 111.0: RM 69,721.92
  Branch 112.0: RM 242,577.73
  Branch 113.0: RM 10,643.87
  Branch 114.0: RM 79,742.55
  Branch 115.0: RM 45,456.70
  Branch 116.0: RM 479.70
  Branch 117.0: RM 17,947.91
  Branch 118.0: RM 38,281.84
  Branch 120.0: RM 148,970.39
  Branch 121.0: RM 77,159.84
  Branch 122.0: RM 112.95
  Branch 123.0: RM 3,406.17
  Branch 124.0: RM 203,031.86
  Branch 125.0: RM 185,710.44
  Branch 126.0: RM 266,510.31
  Branch 127.0: RM 18,626.93
  Branch 128.0: RM 17,633.60
  Branch 129.0: RM 223,832.26
  Branch 130.0: RM 241,159.89
  Branch 131.0: RM 64,177.44
  Branch 133.0: RM 95,523.09
  Branch 135.0: RM 107,211.64
  Branch 136.0: RM 165,629.72
  Branch 137.0: RM 25,011.56
  Branch 138.0: RM 39,731.20
  Branch 139.0: RM 12,766.22
  Branch 140.0: RM 102,762.36
  Branch 141.0: RM 98,347.59
  Branch 143.0: RM 57,522.45
  Branch 145.0: RM 45,164.95
  Branch 146.0: RM 56,779.87
  Branch 147.0: RM 671.49
  Branch 148.0: RM 19,254.15
  Branch 149.0: RM 129.85
  Branch 150.0: RM 265,424.30
  Branch 151.0: RM 55,155.77
  Branch 152.0: RM 26,471.87
  Branch 153.0: RM 134,449.67
  Branch 154.0: RM 91,719.37
  Branch 155.0: RM 7,073.89
  Branch 156.0: RM 206,919.17
  Branch 157.0: RM 141,326.87
  Branch 158.0: RM 787.68
  Branch 159.0: RM 25,378.48
  Branch 160.0: RM 44,486.93
  Branch 161.0: RM 122,781.42
  Branch 162.0: RM 39,123.14
  Branch 163.0: RM 82,749.34
  Branch 164.0: RM 8,538.66
  Branch 165.0: RM 91,479.94
  Branch 168.0: RM 525,861.67
  Branch 169.0: RM 172,021.51
  Branch 170.0: RM 64,740.61
  Branch 171.0: RM 28,706.49
  Branch 172.0: RM 285,936.41
  Branch 173.0: RM 27,771.32
  Branch 174.0: RM -152.96
  Branch 175.0: RM 16,557.23
  Branch 176.0: RM 51,280.69
  Branch 177.0: RM 60,688.96
  Branch 178.0: RM 24,857.72
  Branch 179.0: RM 23,452.51
  Branch 180.0: RM 17,878.67
  Branch 183.0: RM 165,446.23
  Branch 184.0: RM 100,177.49
  Branch 185.0: RM 137,717.66
  Branch 186.0: RM 76,694.94
  Branch 190.0: RM 22,935.14
  Branch 194.0: RM 2,117.84
  Branch 195.0: RM 2,650.00
  Branch 196.0: RM 193,014.26
  Branch 197.0: RM 73,933.44
  Branch 198.0: RM 108,428.98
  Branch 199.0: RM 82,130.65
  Branch 201.0: RM 16,498.12
  Branch 202.0: RM 11,837.93
  Branch 204.0: RM 62,419.93
  Branch 205.0: RM 148,872.09
  Branch 206.0: RM 60,042.55
  Branch 207.0: RM 42,524.11
  Branch 208.0: RM 241,418.20
  Branch 209.0: RM 133,338.63
  Branch 210.0: RM 75,632.18
  Branch 216.0: RM 26,774.12
  Branch 217.0: RM 18,358.52
  Branch 221.0: RM 39,916.85
  Branch 222.0: RM 33,833.38
  Branch 224.0: RM 39,389.28
  Branch 225.0: RM 46,085.96
  Branch 226.0: RM 10,023.01
  Branch 228.0: RM 41,492.99
  Branch 230.0: RM 40,665.78
  Branch 231.0: RM 25,168.60
  Branch 232.0: RM 11,449.79
  Branch 233.0: RM 36,109.63
  Branch 235.0: RM 23,463.49
  Branch 237.0: RM 5,810.97
  Branch 240.0: RM 3,114.88
  Branch 241.0: RM 108,291.09
  Branch 242.0: RM 48,472.92
  Branch 243.0: RM 62,172.19
  Branch 245.0: RM 884.97
  Branch 247.0: RM 13,723.86
  Branch 248.0: RM 171,779.72
  Branch 249.0: RM 90,484.44
  Branch 251.0: RM 2,718.68
  Branch 252.0: RM 3,527.50
  Branch 256.0: RM 51,026.49
  Branch 257.0: RM 4,820.48
  Branch 258.0: RM 10,113.00
  Branch 259.0: RM 2,071.08
  Branch 262.0: RM -90,623.88
  Branch 263.0: RM 59,016.58
  Branch 264.0: RM 442.35
  Branch 265.0: RM 4,065.16
  Branch 266.0: RM 115,107.53
  Branch 267.0: RM 108,788.54
  Branch 268.0: RM 62,815.29
  Branch 269.0: RM 254,636.37
  Branch 270.0: RM 107,785.18
  Branch 273.0: RM 140,226.53
  Branch 274.0: RM 163,702.65
  Branch 275.0: RM 23,589.15
  Branch 276.0: RM 27,050.37
  Branch 278.0: RM 55,988.16
  Branch 280.0: RM 147,334.97
  Branch 281.0: RM 2,103.14
  Branch 282.0: RM 30,489.92
  Branch 283.0: RM 170,389.42
  Branch 284.0: RM 111,293.40
  Branch 285.0: RM 33,715.08
  Branch 286.0: RM 26,101.34
  Branch 287.0: RM 14,627.79
  Branch 288.0: RM 62,568.02
  Branch 289.0: RM 59,855.48
  Branch 292.0: RM 92,261.79
  Branch 293.0: RM 34,198.21
  Branch 296.0: RM 16,093.94

============================================================
Checking for duplicate accounts...
============================================================

Found 6 duplicate accounts:
  Account 1120702022.0 appears in: TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE
  Account 1347898536.0 appears in: TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE
  Account 1371928624.0 appears in: TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE
  Account 1286513018.0 appears in: TRUSTEE, TRUSTEE, TRUSTEE, TRUSTEE
  Account 1194869701.0 appears in: TRUSTEE, TRUSTEE
  Account 1375054827.0 appears in: TRUSTEE, TRUSTEE, TRUSTEE

============================================================
SUMMARY
============================================================

Trustee Accounts:
  Total: RM 32,262,310.37
  >60k: RM 31,176,122.51 (56 accounts)
  <=60k: RM 1,086,187.86 (102 accounts)

Client Accounts:
  Total: RM 2,687,374,588.40
  >60k: RM 2,670,493,066.76 (1845 accounts)
  <=60k: RM 16,881,521.64 (1489 accounts)

============================================================
✓ EIBQINST Complete
============================================================
