REPTDATE: 2026-08-31 16:21:18.136050
REPTMON: 08
REPTYEAR2: 26
REPTDAY: 31
NOWK: 4
TESTING MODE:
  - SAS7BDAT files: limited to 50000 rows
  - CRFTABL text file: limited to 50000 rows
  - COLL/DESC files: reading ALL rows

============================================================
Processing CRFTABL...
============================================================
Read 50000 rows from crftabl.txt (limited to 50000)
CRFT records after filter: 50000

Reading MAST file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/btmast08426.sas7bdat
Read 50000 rows from btmast08426.sas7bdat (limited to 50000)
MAST unique acctno records: 18301
CRFT records after MAST join: 23367
CRFT final records: 23367
CRFT acctno range: 2500000007 to 2502047417

============================================================
Processing MNITB.CURRENT...
============================================================
Read 50000 rows from intg_dp_acct_current_m08.sas7bdat (limited to 50000)
CA records: 49996
CA acctno range: 3000000333 to 3091967615

============================================================
Processing MNILN.LNNOTE...
============================================================
Read 50000 rows from enrh_ln_note_m08.sas7bdat (limited to 50000)
LN records: 49964
LN acctno range: 2000000125 to 2005085515

============================================================
Processing COLL and DESC files...
============================================================

Reading COLL file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831
File size: 0.79 GB
Detected record length: 152
Total records: 5607815
Processed 1000000 records...
Processed 2000000 records...
Processed 3000000 records...
Processed 4000000 records...
Processed 5000000 records...
Total valid COLL records: 1121563
COLL acctno range: 297098250 to 8996772503
COLL sample acctno values: [3078959107, 3093159115, 3077416629, 3077416629, 3077416629, 3094499928, 3094499928, 3077580114, 3077763128, 3077763128]

Reading DESC file (EBCDIC): /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831
File size: 4.62 GB
Record length: 220
Total records: 22559020
Processed 1000000 records, found 8 valid...
Processed 2000000 records, found 14 valid...
Processed 3000000 records, found 20 valid...
Processed 4000000 records, found 25 valid...
Processed 5000000 records, found 30 valid...
Processed 6000000 records, found 42 valid...
Processed 7000000 records, found 50 valid...
Processed 8000000 records, found 56 valid...
Processed 9000000 records, found 61 valid...
Processed 10000000 records, found 69 valid...
Processed 11000000 records, found 71 valid...
Processed 12000000 records, found 76 valid...
Processed 13000000 records, found 86 valid...
Processed 14000000 records, found 90 valid...
Processed 15000000 records, found 102 valid...
Processed 16000000 records, found 143 valid...
Processed 17000000 records, found 165 valid...
Processed 18000000 records, found 183 valid...
Processed 19000000 records, found 212 valid...
Processed 20000000 records, found 258 valid...
Processed 21000000 records, found 305 valid...
Processed 22000000 records, found 351 valid...
Total DESC records processed: 22559020
Total valid DESC records: 370
DESC sample ccollno values: [49, 1693662, 47, 1, 66, 6, 1, 86, 43, 61973]
COLL records after merge with DESC: 136
COLL unique acctno records: 135
COLL acctno range: 2064353304 to 3110042005
COLL sample acctno: [2198276931, 2200486008, 2105412807, 2168585328, 2162004628, 2168570517, 2161817314, 2192635730, 2193433227, 2200092416, 2192777002, 2906893020, 2193798104, 2195487705, 2170215931, 2906071700, 2123329902, 2204793624, 2197472420, 2168008701]

============================================================
Combining CA, LN, CRFT...
============================================================
AAA total records: 123327
AAA unique acctno: 84554
AAA acctno range: 2000000125 to 3091967615
AAA sample acctno: [2000000125, 2000000319, 2000000707, 2000000901, 2000000901, 2000000901, 2000000901, 2000000901, 2000001023, 2000001023, 2000001605, 2000001605, 2000001605, 2000001605, 2000001605, 2000001836, 2000002018, 2000002503, 2000002503, 2000002503]

============================================================
Merging AAA with COLL...
============================================================
Overlap between AAA and COLL: 0 accounts
No overlap found between AAA and COLL
This is likely because AAA is limited to 50,000 rows per file,
while COLL contains all accounts from the full dataset.
To get results, either:
  1. Increase MAX_ROWS_SAS to read more data
  2. Set MAX_ROWS_SAS = None to read all data
EXCP final records: 0

No records to write. Skipping output.
To get results, set MAX_ROWS_SAS = None to read all data
