Using report date: 31/03/26
Report month: 03-2026
Starting EIQPROM2 processing...
Report Date: 31/03/26
Report Month: 03

Step 1: Loading and filtering PROMOTE.LOAN data...
Loading file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIEMCRLS/loan03.sas7bdat
Total records in LOAN file: 180643
REPAID column type: Float64
REPAID min: -1681335.0300019996
REPAID max: 15003207.6379664
Records after REPAID > 100000 filter: 62203
Records after deduplication by GUAREND: 54276
Unique GUAREND values: 54276
Total records: 54276
Duplicate GUARENDs remaining: 0
Final records in RLSLIST: 54276
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIEMCRLS.py:209: DeprecationWarning: `pl.count()` is deprecated. Please use `pl.len()` instead.
(Deprecated in version 0.20.5)
  branch_counts = rlslist.group_by('BRANCH').agg(pl.count().alias('count'))
Branch distribution in RLSLIST:
  Branch 237.0: 55 records
  Branch 24.0: 422 records
  Branch 29.0: 158 records
  Branch 46.0: 208 records
  Branch 77.0: 131 records
  Branch 48.0: 161 records
  Branch 702.0: 9 records
  Branch 56.0: 156 records
  Branch 248.0: 65 records
  Branch 197.0: 186 records
  Branch 86.0: 54 records
  Branch 70.0: 27 records
  Branch 5.0: 314 records
  Branch 113.0: 107 records
  Branch 139.0: 58 records
  Branch 249.0: 281 records
  Branch 252.0: 71 records
  Branch 228.0: 217 records
  Branch 141.0: 80 records
  Branch 202.0: 173 records
  Branch 121.0: 326 records
  Branch 168.0: 187 records
  Branch 274.0: 205 records
  Branch 89.0: 377 records
  Branch 111.0: 311 records
  Branch 156.0: 370 records
  Branch 269.0: 207 records
  Branch 179.0: 152 records
  Branch 103.0: 164 records
  Branch 242.0: 157 records
  Branch 92.0: 219 records
  Branch 50.0: 293 records
  Branch 247.0: 192 records
  Branch 273.0: 296 records
  Branch 257.0: 134 records
  Branch 41.0: 141 records
  Branch 165.0: 220 records
  Branch 138.0: 83 records
  Branch 266.0: 113 records
  Branch 108.0: 124 records
  Branch 124.0: 185 records
  Branch 6.0: 346 records
  Branch 7.0: 1066 records
  Branch 176.0: 540 records
  Branch 185.0: 188 records
  Branch 267.0: 131 records
  Branch 177.0: 417 records
  Branch 153.0: 194 records
  Branch 62.0: 281 records
  Branch 35.0: 148 records
  Branch 292.0: 20 records
  Branch 15.0: 244 records
  Branch 125.0: 90 records
  Branch 260.0: 57 records
  Branch 51.0: 125 records
  Branch 192.0: 20 records
  Branch 285.0: 92 records
  Branch 161.0: 232 records
  Branch 217.0: 586 records
  Branch 144.0: 216 records
  Branch 127.0: 226 records
  Branch 147.0: 161 records
  Branch 178.0: 81 records
  Branch 291.0: 46 records
  Branch 76.0: 158 records
  Branch 205.0: 141 records
  Branch 175.0: 66 records
  Branch 239.0: 74 records
  Branch 40.0: 141 records
  Branch 150.0: 400 records
  Branch 184.0: 226 records
  Branch 80.0: 145 records
  Branch 140.0: 188 records
  Branch 74.0: 20 records
  Branch 276.0: 80 records
  Branch 191.0: 46 records
  Branch 158.0: 176 records
  Branch 72.0: 124 records
  Branch 174.0: 360 records
  Branch 63.0: 70 records
  Branch 19.0: 184 records
  Branch 258.0: 143 records
  Branch 96.0: 108 records
  Branch 270.0: 143 records
  Branch 278.0: 82 records
  Branch 232.0: 116 records
  Branch 105.0: 351 records
  Branch 173.0: 143 records
  Branch 66.0: 125 records
  Branch 64.0: 263 records
  Branch 58.0: 506 records
  Branch 68.0: 546 records
  Branch 32.0: 197 records
  Branch 226.0: 85 records
  Branch 123.0: 321 records
  Branch 244.0: 111 records
  Branch 264.0: 209 records
  Branch 65.0: 256 records
  Branch 172.0: 381 records
  Branch 283.0: 167 records
  Branch 9.0: 229 records
  Branch 43.0: 505 records
  Branch 259.0: 99 records
  Branch 703.0: 21 records
  Branch 95.0: 310 records
  Branch 114.0: 185 records
  Branch 117.0: 79 records
  Branch 79.0: 377 records
  Branch 39.0: 68 records
  Branch 234.0: 245 records
  Branch 4.0: 502 records
  Branch 13.0: 386 records
  Branch 201.0: 444 records
  Branch 59.0: 407 records
  Branch 295.0: 34 records
  Branch 289.0: 59 records
  Branch 265.0: 99 records
  Branch 78.0: 205 records
  Branch 44.0: 260 records
  Branch 207.0: 333 records
  Branch 186.0: 222 records
  Branch 294.0: 31 records
  Branch 81.0: 155 records
  Branch 91.0: 621 records
  Branch 152.0: 226 records
  Branch 198.0: 173 records
  Branch 263.0: 65 records
  Branch 69.0: 128 records
  Branch 90.0: 334 records
  Branch 163.0: 122 records
  Branch 704.0: 132 records
  Branch 73.0: 162 records
  Branch 164.0: 202 records
  Branch 203.0: 335 records
  Branch 288.0: 49 records
  Branch 110.0: 850 records
  Branch 145.0: 160 records
  Branch 701.0: 15 records
  Branch 87.0: 99 records
  Branch 106.0: 159 records
  Branch 251.0: 115 records
  Branch 159.0: 186 records
  Branch 261.0: 85 records
  Branch 211.0: 168 records
  Branch 130.0: 354 records
  Branch 222.0: 683 records
  Branch 122.0: 223 records
  Branch 148.0: 162 records
  Branch 55.0: 165 records
  Branch 281.0: 271 records
  Branch 17.0: 229 records
  Branch 209.0: 303 records
  Branch 190.0: 78 records
  Branch 3.0: 172 records
  Branch 30.0: 116 records
  Branch 231.0: 196 records
  Branch 170.0: 145 records
  Branch 136.0: 140 records
  Branch 104.0: 64 records
  Branch 34.0: 215 records
  Branch 115.0: 91 records
  Branch 241.0: 128 records
  Branch 36.0: 275 records
  Branch 107.0: 269 records
  Branch 160.0: 152 records
  Branch 85.0: 166 records
  Branch 284.0: 64 records
  Branch 154.0: 239 records
  Branch 57.0: 443 records
  Branch 129.0: 175 records
  Branch 20.0: 197 records
  Branch 167.0: 31 records
  Branch 37.0: 489 records
  Branch 220.0: 30 records
  Branch 290.0: 202 records
  Branch 225.0: 170 records
  Branch 16.0: 141 records
  Branch 112.0: 226 records
  Branch 25.0: 77 records
  Branch 189.0: 79 records
  Branch 118.0: 148 records
  Branch 128.0: 133 records
  Branch 42.0: 577 records
  Branch 49.0: 114 records
  Branch 22.0: 125 records
  Branch 256.0: 105 records
  Branch 26.0: 186 records
  Branch 14.0: 94 records
  Branch 287.0: 678 records
  Branch 83.0: 149 records
  Branch 18.0: 223 records
  Branch 2.0: 168 records
  Branch 33.0: 301 records
  Branch 245.0: 98 records
  Branch 221.0: 341 records
  Branch 67.0: 167 records
  Branch 196.0: 127 records
  Branch 21.0: 838 records
  Branch 224.0: 671 records
  Branch 60.0: 343 records
  Branch 206.0: 256 records
  Branch 296.0: 29 records
  Branch 230.0: 155 records
  Branch 61.0: 616 records
  Branch 54.0: 244 records
  Branch 146.0: 161 records
  Branch 183.0: 306 records
  Branch 204.0: 265 records
  Branch 195.0: 140 records
  Branch 23.0: 191 records
  Branch 38.0: 203 records
  Branch 171.0: 137 records
  Branch 31.0: 250 records
  Branch 28.0: 485 records
  Branch 155.0: 100 records
  Branch 47.0: 583 records
  Branch 135.0: 141 records
  Branch 71.0: 143 records
  Branch 93.0: 109 records
  Branch 194.0: 151 records
  Branch 240.0: 162 records
  Branch 157.0: 197 records
  Branch 88.0: 220 records
  Branch 262.0: 133 records
  Branch 10.0: 493 records
  Branch 109.0: 104 records
  Branch 282.0: 53 records
  Branch 210.0: 124 records
  Branch 254.0: 57 records
  Branch 268.0: 310 records
  Branch 142.0: 59 records
  Branch 11.0: 126 records
  Branch 149.0: 78 records
  Branch 286.0: 292 records
  Branch 151.0: 187 records
  Branch 169.0: 145 records
  Branch 293.0: 49 records
  Branch 75.0: 67 records
  Branch 143.0: 90 records
  Branch 133.0: 106 records
  Branch 116.0: 212 records
  Branch 126.0: 222 records
  Branch 137.0: 123 records
  Branch 233.0: 129 records
  Branch 162.0: 152 records
  Branch 94.0: 169 records
  Branch 208.0: 265 records
  Branch 8.0: 246 records
  Branch 275.0: 135 records
  Branch 180.0: 151 records
  Branch 97.0: 101 records
  Branch 102.0: 108 records
  Branch 120.0: 182 records
  Branch 45.0: 592 records
  Branch 235.0: 123 records
  Branch 280.0: 105 records
  Branch 193.0: 65 records
  Branch 243.0: 78 records
  Branch 27.0: 211 records
  Branch 199.0: 328 records
  Branch 52.0: 315 records
  Branch 131.0: 183 records
  Branch 53.0: 229 records

Step 2: Processing PBB data...
Total records in LN.LNNAME: 5534955
LN.LNNAME columns: ['NAMELN1', 'NAMELN2', 'NAMELN3', 'NAMELN4', 'NAMELN5', 'ACCTNO', 'SECPHONE', 'PRIPHONE']
Records in PBBNAME after merge: 48954
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIEMCRLS.py:240: DeprecationWarning: `pl.count()` is deprecated. Please use `pl.len()` instead.
(Deprecated in version 0.20.5)
  brch_counts = pbbname.group_by('BRCH').agg(pl.count().alias('count'))
BRCH distribution in PBBNAME:
  MKH: 56 records
  BDR: 93 records
  BBT: 301 records
  BCG: 131 records
  BAM: 331 records
  KDA: 88 records
  JJG: 146 records
  TMH: 83 records
  JTZ: 215 records
  SS2: 256 records
  JLT: 228 records
  PJN: 212 records
  TEE: 44 records
  KKR: 49 records
  PLT: 251 records
  MRD: 17 records
  SPK: 160 records
  PRA: 316 records
  JTS: 60 records
  DUA: 165 records
  PPG: 298 records
  SMY: 101 records
  KLG: 198 records
  SDK: 225 records
  RAU: 124 records
  JIH: 130 records
  KBG: 113 records
  JAH: 568 records
  SCA: 137 records
  JAS: 55 records
  STL: 73 records
  TMM: 112 records
  KKL: 41 records
  TIH: 163 records
  INN: 188 records
  PSG: 150 records
  JPN: 218 records
  JPR: 79 records
  SEA: 138 records
  KKI: 61 records
  TMA: 71 records
  SPL: 161 records
  KDN: 44 records
  JLP: 90 records
  BWK: 118 records
  BPR: 179 records
  HSL: 94 records
  CPE: 164 records
  KPR: 99 records
  TCL: 186 records
  LMM: 361 records
  BSL: 42 records
  TSK: 315 records
  KHG: 189 records
  TMJ: 341 records
  KBS: 187 records
  TML: 100 records
  BIH: 283 records
  GHS: 23 records
  CTD: 137 records
  MTK: 154 records
  LDU: 79 records
  SKC: 202 records
  JTH: 255 records
  CAH: 54 records
  WMU: 75 records
  CLN: 44 records
  BPT: 738 records
  BJL: 19 records
  TSA: 473 records
  JSI: 250 records
  KGR: 167 records
  KMT: 180 records
  BSA: 44 records
  BHU: 231 records
  SBU: 497 records
  SRM: 211 records
  SPT: 45 records
  SJM: 321 records
  TMR: 315 records
  GMS: 58 records
  KLC: 167 records
  KTI: 323 records
  TTJ: 80 records
  BBB: 176 records
  TCS: 129 records
  JAI: 83 records
  GRN: 113 records
  TNM: 165 records
  SKN: 201 records
  BCM: 299 records
  PIH: 165 records
  JDK: 91 records
  TMD: 108 records
  TPN: 223 records
  PNS: 157 records
  SST: 157 records
  TPD: 161 records
  S14: 170 records
  TMW: 104 records
  TMB: 544 records
  MSL: 127 records
  LBN: 162 records
  NTL: 204 records
  JSB: 118 records
  BSY: 130 records
  TDI: 112 records
  TIN: 283 records
  JPI: 72 records
  MLK: 458 records
  TCT: 124 records
  TDC: 664 records
  BTA: 60 records
  DJA: 124 records
  IMO: 272 records
  BSD: 125 records
  TMG: 172 records
  BSJ: 225 records
  BBM: 163 records
  SMG: 110 records
  JTA: 124 records
  SIK: 16 records
  CMR: 148 records
  SRB: 530 records
  BSP: 62 records
  OUG: 145 records
  SPG: 182 records
  KTN: 234 records
  PDA: 428 records
  BSI: 108 records
  PPR: 111 records
  TAI: 270 records
  JCL: 294 records
  KMY: 72 records
  SLY: 177 records
  TSM: 199 records
  TPI: 358 records
  KUM: 211 records
  BKR: 25 records
  JBU: 1036 records
  KAP: 155 records
  RSH: 359 records
  APG: 198 records
  KKM: 28 records
  ATR: 169 records
  KJG: 147 records
  STW: 257 records
  PRS: 94 records
  DGG: 210 records
  KPT: 40 records
  BKI: 126 records
  JMR: 103 records
  PLI: 112 records
  MSI: 358 records
  PDG: 341 records
  TDA: 144 records
  PSA: 18 records
  MIN: 77 records
  AKH: 272 records
  PBR: 94 records
  PKG: 94 records
  MRI: 264 records
  TBM: 246 records
  SAT: 527 records
  GRT: 105 records
  PSE: 152 records
  LBG: 54 records
  PTT: 39 records
  EDU: 103 records
  BBG: 23 records
  BMC: 63 records
  STP: 149 records
  KSR: 93 records
  KLS: 53 records
  TPG: 518 records
  BBA: 158 records
  MTH: 139 records
  USJ: 168 records
  STG: 191 records
  TRI: 203 records
  JKL: 193 records
  ASR: 399 records
  SSA: 139 records
  TMI: 218 records
  WSS: 192 records
  BEN: 127 records
  PJA: 135 records
  BFT: 67 records
  BNH: 55 records
  NLI: 136 records
  JRT: 56 records
  JBH: 191 records
  TKK: 148 records
  KBD: 42 records
  TDY: 187 records
  BPJ: 166 records
  SAM: 58 records
  UTM: 222 records
  KLI: 587 records
  KUG: 602 records
  SGK: 99 records
  SJA: 191 records
  TWU: 208 records
  SGM: 536 records
  CKI: 138 records
  SBH: 131 records
  IGN: 298 records
  BTL: 328 records
  KBU: 345 records
  KJA: 79 records
  KKG: 64 records
  JHL: 105 records
  TKA: 167 records
  JRC: 157 records
  LHA: 276 records
  SUA: 642 records
  JYK: 204 records
  KTU: 76 records
  BSR: 164 records
  KNG: 67 records
  JSS: 156 records
  KCY: 173 records
  PRJ: 84 records
  MUA: 455 records
  GMG: 82 records
  SRK: 144 records
  BPI: 131 records
  BBP: 295 records
  MLB: 134 records
  JPP: 98 records
  SSH: 145 records
  MKA: 194 records
  BTW: 252 records
  BDA: 114 records
  BTG: 427 records
  JKA: 116 records
  IWS: 1 records
  PTS: 159 records
  RWG: 155 records
  KPH: 48 records
  MSG: 91 records
  SAN: 94 records
  PDN: 251 records
  SBM: 120 records
  LDO: 189 records
  JRU: 406 records
  BMM: 548 records
  BMJ: 13 records
  JRL: 128 records
  PJO: 156 records
  SNG: 125 records
  JBB: 133 records
  SGB: 118 records
  TJJ: 818 records
  PKL: 403 records
  BTR: 69 records
  KPG: 238 records
  SPI: 396 records
  KKU: 272 records
  RLU: 163 records
Records in PBBNAME (non-email): 38764
Records in MAILPBB (email): 10190

Writing EMCPBB file...
EMCPBB records written: 38764

Processing MAILPBB email statements...
Writing EMLPBB file...
EMLPBB records written: 10190
Writing EMXPBB index file...
EMXPBB records written: 10190

Step 3: Processing PIB data...
Total records in LNI.LNNAME: 1661817
Records in PIBNAME after merge: 5323
Records in PIBNAME (non-email): 4242
Records in MAILPIB (email): 1081

Writing EMCPIB file...
EMCPIB records written: 4242

Processing MAILPIB email statements...
Writing EMLPIB file...
EMLPIB records written: 1081
Writing EMXPIB index file...
EMXPIB records written: 1081

Step 4: Generating report...
Total records for report: 43006
Report written to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIEMCRLS/eiqprom2_report.txt

======================================================================
EIEMCRLS processing completed successfully!
======================================================================
Report Date: 31/03/26
Data Month: 03-2026
Total non-email records (PBB + PIB): 43006
======================================================================
