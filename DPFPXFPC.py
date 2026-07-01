Running EIBDCITX for 29/06/26 (WK=4) - Processing YESTERDAY'S data
================================================================================

Loading input files...
  ✓ Loaded DPFL: 89,493 rows from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/DPFL.txt
  ✓ Loaded EQFL: 421 rows from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/UTSASDCID_20260629.txt
  ✓ Loaded CRA: 1,563 rows from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/DPCRATXT_20260629
  Note: pyreadstat doesn't support column selection, loading full file then filtering...
  ✓ Loaded EQRATE: 57 rows from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/eqrate260629.sas7bdat
  Loading MNITB Saving...
  ✓ Loading from Parquet cache: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/cache/intg_dp_acct_saving.parquet
  ✓ Loaded MNITB Saving: 6,634,478 rows
  Loading MNITB Current...
  ✓ Loading from Parquet cache: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/cache/intg_dp_acct_current.parquet
  ✓ Loaded MNITB Current: 1,118,698 rows
  Note: pyreadstat doesn't support column selection, loading full file then filtering...
  ✓ Loaded DCID: 210 rows from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDCITX/dcid0629.sas7bdat

================================================================================

Processing DPST...
  DPST after merge: 89,493 rows

Processing EQ data...
  EQ after date filter: 300 rows
  EQC: 150 rows, EQI: 150 rows

Processing Customer Leg...
  EQDCI after join: 5 rows
  DEPO combined: 7,753,176 rows
  CRA after processing: 34 rows
  Combined EQDCI: 39 rows
  Customer MYR: 38 rows, FCY: 1 rows

Processing Interbank Leg...
  Interbank MYR: 0 rows, FCY: 0 rows

Writing DCITXT output to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCITXT.txt...
  ✓ DCITXT written to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCITXT.txt

Building DCI final output...
  Combined customer and interbank data: 38 rows
  DCI final: 2 aggregated records

Writing output files...
  ✓ Parquet written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCI_260629.parquet

Writing SAS dataset to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/BNMK_DCI064.sas7bdat...
  Connecting to SAS session...
Using SAS Config named: default
SAS Connection established. Subprocess id is 1054817

  Writing SAS dataset: BNMK_DCI064...
The libref specified is not assigned in this SAS Session.
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
  ✓ SAS dataset written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/BNMK_DCI064.sas7bdat
  ✓ CSV written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDCITX/DCI_260629.csv

================================================================================
EIBDCITX completed successfully for 29/06/26 (Yesterday's data)!
================================================================================
SAS Connection terminated. Subprocess id was 1054817



python output:

                                                    PUBLIC BANK BERHAD                                                            09:23 Wednesday, July 01, 2026   1
                                                                                  DAILY EXTRACTION OF DCI/CRA CUSTOMER FOR MYR AS AT 29/06/26
 Obs CUSTICKETNO                    TICKETNO CUSTNAME                    CUSTCODE BRANCH    INVCURAC    ALTCURAC INVCURR ALTCURR     INVAMT     ALTAMT TENOR      SPOTRT    DCIRT STATUSIND        STARTDT    MATDT     ACCINT   ACCINTRM   PREMPAID PREMPAIDRM
   1          SUA/CRA001/000001                              NG EK CHEONG     95.0      287   4353940104                   MYR            100,000.00         0.00      5    1.0000000  0.00000  Outstanding     19/06/25     19/06/30       0.00       0.00       0.00       0.00
   2          SSH/CRA005/000005                            LADDA SAE-YUAN     96.0      230   4562117718                   MYR            100,000.00         0.00      3    1.0000000  4.06000  Outstanding     28/01/26     29/01/29     700.77     700.77       0.00       0.00
   3          SMY/CRA006/000019                             YANG CHAO-WEN     96.0      022   4686513215                   MYR            200,000.00         0.00      4    1.0000000  4.10000  Outstanding     13/03/26     13/03/30     336.99     336.99       0.00       0.00
   4          BSR/CRA001/000001                       LEOW KIM LENG WENDY     96.0      129   5021907425                   MYR            100,000.00         0.00      5    1.0000000  0.00000  Outstanding     19/06/25     19/06/30       0.00       0.00       0.00       0.00
   5          PTS/CRA003/000015                                 LI YULING     95.0      126   5042180833                   MYR            450,000.00         0.00      5    1.0000000  4.20000  Outstanding     10/10/25     10/10/30   4,194.25   4,194.25       0.00       0.00
   6          MKA/CRA001/000009                             WONG SIEW HWA     95.0      269   5047240203                   MYR            100,000.00         0.00      5    1.0000000  0.00000  Outstanding     19/06/25     19/06/30       0.00       0.00       0.00       0.00
   7          KLC/CRA001/000016                              CHOU DACHANG     96.0      168   5047518128                   MYR            400,000.00         0.00      5    1.0000000  0.00000  Outstanding     19/06/25     19/06/30       0.00       0.00       0.00       0.00
   8          KLC/CRA001/000017                                   GUO YUE     96.0      168   5051139819                   MYR            300,000.00         0.00      5    1.0000000  0.00000  Outstanding     19/06/25     19/06/30       0.00       0.00       0.00       0.00
   9          TJJ/CRA007/000041                      SIM GUAY KEK JASMINE     95.0      110   5051621229                   MYR            100,000.00         0.00      4    1.0000000  4.20000  Outstanding     23/04/26     23/04/30     782.47     782.47       0.00       0.00
  10          KLC/CRA001/000046                            SHUANGYAN QIAN     96.0      168   5054699212                   MYR            300,000.00         0.00      5    1.0000000  0.00000  Outstanding     19/06/25     19/06/30       0.00       0.00       0.00       0.00
  11          KLC/CRA003/000072                            SHUANGYAN QIAN     96.0      168   5054699212                   MYR            200,000.00         0.00      5    1.0000000  4.20000  Outstanding     10/10/25     10/10/30   1,864.11   1,864.11       0.00       0.00
  12          JRC/CRA005/000008                                  JIN, WEI     96.0      003   5069443023                   MYR            100,000.00         0.00      3    1.0000000  4.06000  Outstanding     28/01/26     29/01/29     700.77     700.77       0.00       0.00
  13          JYK/CRA005/000098                              KHOO KIM KEE     95.0      009   5076602735                   MYR            100,000.00         0.00      3    1.0000000  4.06000  Outstanding     28/01/26     29/01/29     700.77     700.77       0.00       0.00
  14          JYK/CRA006/000110                              KHOO KIM KEE     95.0      009   5076602735                   MYR            200,000.00         0.00      4    1.0000000  4.10000  Outstanding     13/03/26     13/03/30     336.99     336.99       0.00       0.00
  15          JYK/CRA007/000115                              KHOO KIM KEE     95.0      009   5076602735                   MYR            200,000.00         0.00      4    1.0000000  4.20000  Outstanding     23/04/26     23/04/30   1,564.93   1,564.93       0.00       0.00
  16          KLC/CRA001/000018                  CHOU, SA-LI @ SALLY CHOU     96.0      168   5087103233                   MYR          2,000,000.00         0.00      5    1.0000000  0.00000  Outstanding     19/06/25     19/06/30       0.00       0.00       0.00       0.00
  17          JRC/CRA005/000009                                  SHEN JIA     96.0      003   5088021518                   MYR            100,000.00         0.00      3    1.0000000  4.06000  Outstanding     28/01/26     29/01/29     700.77     700.77       0.00       0.00
  18          JRC/CRA006/000012                                 HU YANHUI     96.0      003   5096094209                   MYR            100,000.00         0.00      4    1.0000000  4.10000  Outstanding     13/03/26     13/03/30     168.49     168.49       0.00       0.00
  19          KKU/CRA003/000019                                  CHEN YAN     96.0      033   5105402200                   MYR            100,000.00         0.00      5    1.0000000  4.20000  Outstanding     10/10/25     10/10/30     932.05     932.05       0.00       0.00
  20          PSG/CRA001/000026                             LIEW HUN SANG     95.0      146   5106947033                   MYR            150,000.00         0.00      5    1.0000000  0.00000  Outstanding     19/06/25     19/06/30       0.00       0.00       0.00       0.00
  21          PSG/CRA007/000038                             LIEW HUN SANG     95.0      146   5106947033                   MYR            100,000.00         0.00      4    1.0000000  4.20000  Outstanding     23/04/26     23/04/30     782.47     782.47       0.00       0.00
  22          KLC/CRA003/000071                                   LI, YAN     96.0      168   5113263318                   MYR            500,000.00         0.00      5    1.0000000  4.20000  Outstanding     10/10/25     10/10/30   4,660.27   4,660.27       0.00       0.00
  23          JRC/CRA006/000011                               LIU JIANMIN     96.0      003   5113353120                   MYR            100,000.00         0.00      4    1.0000000  4.10000  Outstanding     13/03/26     13/03/30     168.49     168.49       0.00       0.00
  24          PTS/CRA006/000025                        AGNES KUA HUN CHOO     95.0      126   5119986028                   MYR            100,000.00         0.00      4    1.0000000  4.10000  Outstanding     13/03/26     13/03/30     168.49     168.49       0.00       0.00
  25          TNM/CRA008/000001                             DENG JIANFENG     96.0      197   5121299507                   MYR            200,000.00         0.00      4    1.0000000  4.10000  Outstanding     20/05/26     20/05/30     921.10     921.10       0.00       0.00
  26          MLB/CRA006/000006                           FOONG CHEE SING     95.0      080   5121486835                   MYR            200,000.00         0.00      4    1.0000000  4.10000  Outstanding     13/03/26     13/03/30     336.99     336.99       0.00       0.00
  27          BPI/CRA008/000049                                LI HAIQING     96.0      270   6342159803                   MYR            100,000.00         0.00      4    1.0000000  4.10000  Outstanding     20/05/26     20/05/30     460.55     460.55       0.00       0.00
  28          MKA/CRA001/000002                              YOO AEKYOUNG     95.0      269   6362746314                   MYR            100,000.00         0.00      5    1.0000000  0.00000  Outstanding     19/06/25     19/06/30       0.00       0.00       0.00       0.00
  29          MKA/CRA006/000016                              YOO AEKYOUNG     95.0      269   6362746314                   MYR            750,000.00         0.00      4    1.0000000  4.10000  Outstanding     13/03/26     13/03/30   1,263.70   1,263.70       0.00       0.00
  30          JYK/CRA008/000143                             LOOI WAN LIMM     95.0      009   6399781123                   MYR            100,000.00         0.00      4    1.0000000  4.10000  Outstanding     20/05/26     20/05/30     460.55     460.55       0.00       0.00
  31          JYK/CRA008/000144                              TAN KWEE WAA     95.0      009   6476816310                   MYR            200,000.00         0.00      4    1.0000000  4.10000  Outstanding     20/05/26     20/05/30     921.10     921.10       0.00       0.00
  32          SUA/CRA001/000003                                  AGUSTINA     96.0      287   6498056116                   MYR            100,000.00         0.00      5    1.0000000  0.00000  Outstanding     19/06/25     19/06/30       0.00       0.00       0.00       0.00
  33          TJJ/CRA008/000047                             KOH CHIN MENG     95.0      110   3244592306                   MYR            200,000.00         0.00      4    1.0000000  4.10000  Outstanding     20/05/26     20/05/30     921.10     921.10       0.00       0.00
  34          TJJ/CRA008/000046                              LUM PAK YUEN     95.0      110   3250550305                   MYR            500,000.00         0.00      4    1.0000000  4.10000  Outstanding     20/05/26     20/05/30   2,302.74   2,302.74       0.00       0.00
  35                     Z29759     Z29758                      LI HUIMIN     96.0      168   5071525805   3599635531      MYR      EUR   300,000.00    64,377.68      7    1.0000000  5.00000  Outstanding     29/06/26     06/07/26      82.19      82.19     129.45     129.45
  36                     Z29761     Z29760                CHONG POH KWANG     95.0      028   5114913203   3596806721      MYR      SGD   100,000.00    31,486.15      7    1.0000000  6.40000  Outstanding     29/06/26     06/07/26      35.07      35.07      70.00      70.00
  37                     Z29183     Z29182                   RAKHEE SINGH     96.0      168   5073069010   3596280202      MYR      USD   113,876.00    28,299.20     30    1.0000000  4.20000  Outstanding     09/06/26     09/07/26     288.28     288.28     135.72     135.72
  38                     Z29353     Z29352        YUAN XIONGBING/LIU HONG     96.0      126   5060004119   3595603734      MYR      USD   100,000.00    24,636.61     30    1.0000000  3.20000  Outstanding     15/06/26     15/07/26     140.27     140.27      36.99      36.99
                                                                             ========== ========== ========== ==========
                                                                              26,896.72  26,896.72     372.16     372.16
                                                                              26,896.72  26,896.72     372.16     372.16

                                                    PUBLIC BANK BERHAD                                                            09:23 Wednesday, July 01, 2026   2
                                                                                  DAILY EXTRACTION OF DCI/CRA CUSTOMER FOR FCY AS AT 29/06/26
 Obs CUSTICKETNO                    TICKETNO CUSTNAME                    CUSTCODE BRANCH    INVCURAC    ALTCURAC INVCURR ALTCURR     INVAMT     ALTAMT TENOR      SPOTRT    DCIRT STATUSIND        STARTDT    MATDT     ACCINT   ACCINTRM   PREMPAID PREMPAIDRM
   1                     Z29379     Z29378               CHIENG HOCK KOCK     95.0      126   3590115215   3114593916      SGD      MYR    40,000.00   126,760.00     30    3.1472033  1.70000  Outstanding     15/06/26     15/07/26      29.81      93.82      19.73      62.09
                                                                             ========== ========== ========== ==========
                                                                                  29.81      93.82      19.73      62.09




actual production output:

                                                                                                                       PUBLIC BANK BERHAD                                                                                       08:00 Tuesday, June 30, 2026   1
                                                                                                  DAILY EXTRACTION OF DCI/CRA CUSTOMER FOR MYR AS AT 29/06/26
 Obs CUSTICKETNO                    TICKETNO CUSTNAME                    CUSTCODE BRANCH    INVCURAC    ALTCURAC INVCURR ALTCURR     INVAMT     ALTAMT TENOR      SPOTRT    DCIRT STATUSIND        STARTDT    MATDT     ACCINT   ACCINTRM   PREMPAID PREMPAIDRM
   1 TJJ/CRA008/000047                       KOH CHIN MENG                   95    110    3244592306               MYR            200000.00                4   1.0000000  4.10000 Outstanding     20/05/26 20/05/30     921.10     921.10       0.00       0.00
   2 TJJ/CRA008/000046                       LUM PAK YUEN                    95    110    3250550305               MYR            500000.00                4   1.0000000  4.10000 Outstanding     20/05/26 20/05/30    2302.74    2302.74       0.00       0.00
   3 SUA/CRA001/000001                       NG EK CHEONG                    95    287    4353940104               MYR            100000.00                5   1.0000000  0.00000 Outstanding     19/06/25 19/06/30       0.00       0.00       0.00       0.00
   4 SSH/CRA005/000005                       LADDA SAE-YUAN                  96    230    4562117718               MYR            100000.00                3   1.0000000  4.06000 Outstanding     28/01/26 29/01/29     700.77     700.77       0.00       0.00
   5 SMY/CRA006/000019                       YANG CHAO-WEN                   96    022    4686513215               MYR            200000.00                4   1.0000000  4.10000 Outstanding     13/03/26 13/03/30     336.99     336.99       0.00       0.00
   6 BSR/CRA001/000001                       LEOW KIM LENG WENDY             96    129    5021907425               MYR            100000.00                5   1.0000000  0.00000 Outstanding     19/06/25 19/06/30       0.00       0.00       0.00       0.00
   7 PTS/CRA003/000015                       LI YULING                       95    126    5042180833               MYR            450000.00                5   1.0000000  4.20000 Outstanding     10/10/25 10/10/30    4194.25    4194.25       0.00       0.00
   8 MKA/CRA001/000009                       WONG SIEW HWA                   95    269    5047240203               MYR            100000.00                5   1.0000000  0.00000 Outstanding     19/06/25 19/06/30       0.00       0.00       0.00       0.00
   9 KLC/CRA001/000016                       CHOU DACHANG                    96    168    5047518128               MYR            400000.00                5   1.0000000  0.00000 Outstanding     19/06/25 19/06/30       0.00       0.00       0.00       0.00
  10 KLC/CRA001/000017                       GUO YUE                         96    168    5051139819               MYR            300000.00                5   1.0000000  0.00000 Outstanding     19/06/25 19/06/30       0.00       0.00       0.00       0.00
  11 TJJ/CRA007/000041                       SIM GUAY KEK JASMINE            95    110    5051621229               MYR            100000.00                4   1.0000000  4.20000 Outstanding     23/04/26 23/04/30     782.47     782.47       0.00       0.00
  12 KLC/CRA001/000046                       SHUANGYAN QIAN                  96    168    5054699212               MYR            300000.00                5   1.0000000  0.00000 Outstanding     19/06/25 19/06/30       0.00       0.00       0.00       0.00
  13 KLC/CRA003/000072                       SHUANGYAN QIAN                  96    168    5054699212               MYR            200000.00                5   1.0000000  4.20000 Outstanding     10/10/25 10/10/30    1864.11    1864.11       0.00       0.00
  14 JRC/CRA005/000008                       JIN, WEI                        96    003    5069443023               MYR            100000.00                3   1.0000000  4.06000 Outstanding     28/01/26 29/01/29     700.77     700.77       0.00       0.00
  15 JYK/CRA005/000098                       KHOO KIM KEE                    95    009    5076602735               MYR            100000.00                3   1.0000000  4.06000 Outstanding     28/01/26 29/01/29     700.77     700.77       0.00       0.00
  16 JYK/CRA006/000110                       KHOO KIM KEE                    95    009    5076602735               MYR            200000.00                4   1.0000000  4.10000 Outstanding     13/03/26 13/03/30     336.99     336.99       0.00       0.00
  17 JYK/CRA007/000115                       KHOO KIM KEE                    95    009    5076602735               MYR            200000.00                4   1.0000000  4.20000 Outstanding     23/04/26 23/04/30    1564.93    1564.93       0.00       0.00
  18 KLC/CRA001/000018                       CHOU, SA-LI @ SALLY CHOU        96    168    5087103233               MYR           2000000.00                5   1.0000000  0.00000 Outstanding     19/06/25 19/06/30       0.00       0.00       0.00       0.00
  19 JRC/CRA005/000009                       SHEN JIA                        96    003    5088021518               MYR            100000.00                3   1.0000000  4.06000 Outstanding     28/01/26 29/01/29     700.77     700.77       0.00       0.00
  20 JRC/CRA006/000012                       HU YANHUI                       96    003    5096094209               MYR            100000.00                4   1.0000000  4.10000 Outstanding     13/03/26 13/03/30     168.49     168.49       0.00       0.00
  21 KKU/CRA003/000019                       CHEN YAN                        96    033    5105402200               MYR            100000.00                5   1.0000000  4.20000 Outstanding     10/10/25 10/10/30     932.05     932.05       0.00       0.00
  22 PSG/CRA001/000026                       LIEW HUN SANG                   95    146    5106947033               MYR            150000.00                5   1.0000000  0.00000 Outstanding     19/06/25 19/06/30       0.00       0.00       0.00       0.00
  23 PSG/CRA007/000038                       LIEW HUN SANG                   95    146    5106947033               MYR            100000.00                4   1.0000000  4.20000 Outstanding     23/04/26 23/04/30     782.47     782.47       0.00       0.00
  24 KLC/CRA003/000071                       LI, YAN                         96    168    5113263318               MYR            500000.00                5   1.0000000  4.20000 Outstanding     10/10/25 10/10/30    4660.27    4660.27       0.00       0.00
  25 JRC/CRA006/000011                       LIU JIANMIN                     96    003    5113353120               MYR            100000.00                4   1.0000000  4.10000 Outstanding     13/03/26 13/03/30     168.49     168.49       0.00       0.00
  26 PTS/CRA006/000025                       AGNES KUA HUN CHOO              95    126    5119986028               MYR            100000.00                4   1.0000000  4.10000 Outstanding     13/03/26 13/03/30     168.49     168.49       0.00       0.00
  27 TNM/CRA008/000001                       DENG JIANFENG                   96    197    5121299507               MYR            200000.00                4   1.0000000  4.10000 Outstanding     20/05/26 20/05/30     921.10     921.10       0.00       0.00
  28 MLB/CRA006/000006                       FOONG CHEE SING                 95    080    5121486835               MYR            200000.00                4   1.0000000  4.10000 Outstanding     13/03/26 13/03/30     336.99     336.99       0.00       0.00
  29 BPI/CRA008/000049                       LI HAIQING                      96    270    6342159803               MYR            100000.00                4   1.0000000  4.10000 Outstanding     20/05/26 20/05/30     460.55     460.55       0.00       0.00
  30 MKA/CRA001/000002                       YOO AEKYOUNG                    95    269    6362746314               MYR            100000.00                5   1.0000000  0.00000 Outstanding     19/06/25 19/06/30       0.00       0.00       0.00       0.00
  31 MKA/CRA006/000016                       YOO AEKYOUNG                    95    269    6362746314               MYR            750000.00                4   1.0000000  4.10000 Outstanding     13/03/26 13/03/30    1263.70    1263.70       0.00       0.00
  32 JYK/CRA008/000143                       LOOI WAN LIMM                   95    009    6399781123               MYR            100000.00                4   1.0000000  4.10000 Outstanding     20/05/26 20/05/30     460.55     460.55       0.00       0.00
  33 JYK/CRA008/000144                       TAN KWEE WAA                    95    009    6476816310               MYR            200000.00                4   1.0000000  4.10000 Outstanding     20/05/26 20/05/30     921.10     921.10       0.00       0.00
  34 SUA/CRA001/000003                       AGUSTINA                        96    287    6498056116               MYR            100000.00                5   1.0000000  0.00000 Outstanding     19/06/25 19/06/30       0.00       0.00       0.00       0.00
  35 Z29183                         Z29182   RAKHEE SINGH                    96    168    5073069010  3596280202   MYR     USD    113876.00   28299.20    30   1.0000000  4.20000 Outstanding     09/06/26 09/07/26     288.28     288.28     135.72     135.72
  36 Z29353                         Z29352   YUAN XIONGBING/LIU HONG         96    126    5060004119  3595603734   MYR     USD    100000.00   24636.61    30   1.0000000  3.20000 Outstanding     15/06/26 15/07/26     140.27     140.27      36.99      36.99
  37 Z29759                         Z29758   LI HUIMIN                       96    168    5071525805  3599635531   MYR     EUR    300000.00   64377.68     7   1.0000000  5.00000 Outstanding     29/06/26 06/07/26      82.19      82.19     129.45     129.45
  38 Z29761                         Z29760   CHONG POH KWANG                 95    028    5114913203  3596806721   MYR     SGD    100000.00   31486.15     7   1.0000000  6.40000 Outstanding     29/06/26 06/07/26      35.07      35.07      70.00      70.00
                                                                                                                                                                                                                    ========== ========== ========== ==========
                                                                                                                                                                                                                      26896.72   26896.72     372.16     372.16
                                                                                                                       PUBLIC BANK BERHAD                                                                                       08:00 Tuesday, June 30, 2026   2
                                                                                                  DAILY EXTRACTION OF DCI/CRA CUSTOMER FOR FCY AS AT 29/06/26
 Obs CUSTICKETNO                    TICKETNO CUSTNAME                    CUSTCODE BRANCH    INVCURAC    ALTCURAC INVCURR ALTCURR     INVAMT     ALTAMT TENOR      SPOTRT    DCIRT STATUSIND        STARTDT    MATDT     ACCINT   ACCINTRM   PREMPAID PREMPAIDRM
  1  Z29379                         Z29378   CHIENG HOCK KOCK                95    126    3590115215  3114593916   SGD     MYR     40000.00  126760.00    30   3.1472033  1.70000 Outstanding     15/06/26 15/07/26      29.81      93.82      19.73      62.09
  2  KLC/CRA004/000106                       SAYAKA UEMURA                   96    168    3593992027               USD            305000.00                5   4.0707500  5.00000 Outstanding     21/11/25 21/11/30    1694.44    6897.64       0.00       0.00
  3  KLI/CRA004/000003                       CAI LI                          96    061    3594093907               USD             25000.00                5   4.0707500  5.00000 Outstanding     21/11/25 21/11/30     138.89     565.39       0.00       0.00
  4  BDA/CRA004/000010                       HSIAO CHIH CHE                  96    066    3594932310               USD             35000.00                5   4.0707500  5.00000 Outstanding     21/11/25 21/11/30     194.44     791.52       0.00       0.00
  5  KLC/CRA004/000109                       CHEN HUI                        96    168    3595661023               USD             25000.00                5   4.0707500  5.00000 Outstanding     21/11/25 21/11/30     138.89     565.39       0.00       0.00
  6  KLC/CRA004/000113                       CHEN HUI                        96    168    3595661023               USD             25000.00                5   4.0707500  5.00000 Outstanding     21/11/25 21/11/30     138.89     565.39       0.00       0.00
  7  JRC/CRA004/000006                       JIN, WEI                        96    003    3595972828               USD             25000.00                5   4.0707500  5.00000 Outstanding     21/11/25 21/11/30     138.89     565.39       0.00       0.00
  8  KKM/CRA004/000012                       SELVIA CHANDRA                  96    294    3599041111               USD            505000.00                5   4.0707500  5.00000 Outstanding     21/11/25 21/11/30    2805.56   11420.73       0.00       0.00
  9                                 X16      ANZ BANKING GROUP LTD MEL       84                                    USD           17117503.8             1826   4.0707500  3.53509 Outstanding     21/11/25 21/11/30  -16710.11  -68022.68       0.00       0.00
                                                                                                                                                                                                                    ========== ========== ========== ==========
                                                                                                                                                                                                                     -11430.30  -46557.42      19.73      62.09



now it output the CUSTICKETNO, but just a bit difference on the value. why is that so? can you ensure that it is exactly similar to the production output?
