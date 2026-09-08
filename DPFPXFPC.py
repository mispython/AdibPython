Reading SAS datasets...
CURRENT dataset: 1132084 rows before filtering
CURRENT dataset: 967521 rows after filtering (ENTITY_CD != 'PIBB')
LIMIT dataset: 1093140 rows before filtering
LIMIT dataset: 937196 rows after filtering (ENTITY_CD != 'PIBB')
NPLA dataset: 28 rows
Reading CISDP dataset in chunks...
CISDP columns available: 81
Processed 10 chunks, 1000000 rows total
Processed 20 chunks, 2000000 rows total
Processed 30 chunks, 3000000 rows total
Processed 40 chunks, 4000000 rows total
Processed 50 chunks, 5000000 rows total
Processed 60 chunks, 6000000 rows total
Processed 70 chunks, 7000000 rows total
Processed 80 chunks, 8000000 rows total
Processed 90 chunks, 9000000 rows total
Processed 100 chunks, 10000000 rows total
Processed 110 chunks, 10935758 rows total
CISDP dataset: 8666482 rows after filtering (SECCUST == '901')
CA after SCH mapping: 65 rows
Processing LIMIT data...
LMTSTART dtype: float64
LMTSTART non-null count: 47547
LIMIT processed: 930567 unique records
CA after LIMIT merge: 67 rows
CA after GP3 merge: 67 rows
CA after CISDP merge: 67 rows
Reading COLL file (EBCDIC)...
Successfully decoded with cp037
COLL file: 1837458 rows
Reading DESC file (EBCDIC)...
Successfully decoded with cp037
DESC file: 58427 rows

=== DESC SAMPLE DATA ===
        CCOLLNO CINSTCL NATGUAR     CENSUS
0   00000000133      29     NaN        NaN
1            FN     NaN     NaN        NaN
2            MY      15     NaN        NaN
3            MY     NaN     NaN        NaN
4             I     NaN     NaN          1
5            FN       N       M        115
6                   NaN     NaN        NaN
7             2     NaN     NaN        NaN
8                   NaN     NaN        NaN
9                   NaN     NaN        NaN
10           MN     NaN     NaN        NaN
11                  NaN     NaN        NaN
12                  NaN     NaN        NaN
13                  NaN     NaN        NaN
14        NTEAM      AR      AC  -05   BDR
15         -
            MY      16     NaN        NaN
16           MY       3     NaN        NaN
17           MY      16               OHOR
18           MY      16              JOHOR
19           HN     NaN     NaN         45

=== DESC DATA TYPES ===
CCOLLNO    object
CINSTCL    object
NATGUAR    object
CENSUS     object
dtype: object

=== CENSUS SAMPLE VALUES ===
0            NaN
1            NaN
2            NaN
3            NaN
4              1
5            115
6            NaN
7            NaN
8            NaN
9            NaN
10           NaN
11           NaN
12           NaN
13           NaN
14     -05   BDR
15           NaN
16           NaN
17          OHOR
18         JOHOR
19            45
20           NaN
21           NaN
22           NaN
23           NaN
24           NaN
25           NaN
26           PUR
27           NaN
28           NaN
29          3817
30             0
31           NaN
32    2     FLAT
33             3
34           NaN
35             1
36           NaN
37           NaN
38           NaN
39       T HOMES
40           NaN
41             1
42           NaN
43           NaN
44    UNTUNG SEL
45           NaN
46        PUR WP
47           NaN
48           NaN
49             6
Name: CENSUS, dtype: object

=== CENSUS UNIQUE VALUES (first 100) ===
CENSUS
1        245
0        172
2        120
8        119
3        107
        ... 
ABAH       9
38         9
KEDAH      9
67         9
40         9
Name: count, Length: 100, dtype: int64

=== CINSTCL SAMPLE VALUES ===
CINSTCL
22    3962
00    1699
N     1387
23     984
02     604
SI     332
AR     304
A      294
I      262
3      243
0      240
S      236
B      219
AN     217
P      213
24     181
HD     178
BH     177
D      175
R      160
Name: count, dtype: int64

=== NATGUAR SAMPLE VALUES ===
NATGUAR
      381
PE    368
NS    321
N     284
I     274
AS    257
OC    224
ER    222
WE    207
IA    183
E     173
B     173
AN    171
RI    169
A     149
NG    147
LA    147
S     145
SI    143
ES    118
Name: count, dtype: int64

CENSUS after conversion - non-null: 3244
CENSUS value range: -417.0 to 530830.0
CENSUS sample values (first 20 non-null): [1.0, 115.0, 45.0, 3817.0, 0.0, 3.0, 1.0, 1.0, 6.0, 4.0, 1.0, 7.0, 0.0, 0.0, 0.0, 7.0, 1.0, 33.0, 83.0, 1.0]
DESC after CR mapping: 0 rows

Trying alternative CR mapping...
DESC after alternative CR mapping: 5 rows
COLL after merge with DESC: 0 rows

=== COLL SAMPLE DATA AFTER MERGE ===
Empty DataFrame
Columns: [CCOLLNO, ACCTNO, CINSTCL, NATGUAR, CENSUS, CENSUS_CLEAN, CR]
Index: []

Sample CINSTCL values: Series([], Name: count, dtype: int64)
Sample NATGUAR values: Series([], Name: count, dtype: int64)
COLL after filtering (CINSTCL=18, NATGUAR=06): 0 rows

No records with CINSTCL=18 AND NATGUAR=06
COLL with just CINSTCL=18: 0 rows
COLL with just NATGUAR=06: 0 rows
DEP after COLL merge: 0 rows
WARNING: No records after merging with COLL data.
Sample ACCTNO from CA: [3071098223.0, 3071098223.0, 3071098223.0, 3071719525.0, 3071739802.0, 3071792929.0, 3072834609.0, 3074642220.0, 3074835227.0, 3075359720.0]
Sample ACCTNO from COLL: []
Continuing with CA data without collateral merge...
DEP after MICR merge: 67 rows
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDNPGS.py:527: FutureWarning: The behavior of array concatenation with empty entries is deprecated. In a future version, this will no longer exclude empty items when determining the result dtype. To retain the old behavior, exclude the empty entries before the concat operation.
  dep['NPLDATE'] = dep['NPLDATE_CALC'].combine_first(dep['NPLDATE'])
DEP after CVAR02 mapping: 0 rows
WARNING: No records after CVAR02 mapping. Using all records...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDNPGS.py", line 578, in <module>
    npgs = dep_filtered.merge(npla_df, on=['CVAR06','CVAR01'], how='left')
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/frame.py", line 10832, in merge
    return merge(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/reshape/merge.py", line 170, in merge
    op = _MergeOperation(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/reshape/merge.py", line 807, in __init__
    self._maybe_coerce_merge_keys()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/reshape/merge.py", line 1508, in _maybe_coerce_merge_keys
    raise ValueError(msg)
ValueError: You are trying to merge on object and float64 columns for key 'CVAR01'. If you wish to proceed you should use pd.concat
