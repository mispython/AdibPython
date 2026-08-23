Processing date: 2026-07-31
REPTMON: 07, REPTMON1: 06
RDATE: 310726, NDATE: 3107

Looking for input files:
DP file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRP159/dpipgs07.sas7bdat
LN file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRP159/lnipgs07.sas7bdat
DP file exists: True
LN file exists: True

Reading DP file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRP159/dpipgs07.sas7bdat
DP file read successfully. Rows: 0, Columns: 20

Reading LN file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRP159/lnipgs07.sas7bdat
LN file read successfully. Rows: 10, Columns: 20

Only LN data available: 10 rows

Writing MEFT.txt...

============================================================
PUBLIC BANK BERHAD
DETAIL OF ACCTS (MEF PRODUCTS) FOR SUBMISSION TO CGC @ 310726
============================================================
Report saved to MEFR.txt using NPGSRPT module

Parquet output saved to: eibrp159_output.parquet

Creating SAS7BDAT file using saspy...
Using SAS Config named: default
SAS Connection established. Subprocess id is 2941411

Creating SAS dataset...
Error creating SAS dataset:
{'LOG': '\n21   \n22   data work.npgs_output;\n23       input\n24           product\n25           censust\n26           cinstcl $\n27           natguar $\n28           cvar01\n29           cvar06\n30           cvar03 $\n31           cvar04 $\n32           cvar14 $\n33           cvar13 $\n34           cvar08\n35           cvar09\n36           cvar10\n37           cvar11\n38           cvar02 $\n39           cvar05\n40  cvar07 $\n41           cvar12 $\n42           cvar15 $\n43           branch\n44           cvarxx $\n45       ;\n46       datalines;\nNOTE: Invalid data for cvar05 in line 47 111-112.\nRULE:----+----1----+----2----+----3----+----4----+----5----+----6----+----7----+----8----+----9----+----0                           \n48       159.0 0.0 18 02 . 2977213532.0 KT0136232A YA SIN ENTERPRISE 0351  100000.0 0.0006820999999999999\n101   0.0 0.0  21257.0 IL  03027 14.0\nNOTE: Invalid data errors for file CARDS occurred outside the printed range.\nNOTE: Increase available buffer lines with the INFILE n= option.\nproduct=159 censust=0 cinstcl=18 natguar=01 cvar01=. cvar06=2976290832 cvar03=KT030606 cvar04=FENRA cvar14=TRADING cvar13=0351\ncvar08=50000 cvar09=13723.953961 cvar10=0 cvar11=0 cvar02=21213.0 cvar05=. cvar07=03027 cvar12=14.0 cvar15=159.0 branch=0 cvarxx=18\n_ERROR_=1 _N_=1\nNOTE: Invalid data for cvar05 in line 51 113-114.\n52       159.0 0.0 18 02 . 2972375303.0 727188X TRIPLE-C RECYCLE SDN. BHD. 0351  500000.0 -0.0021438 0.0 \n101  0.0  20811.0 IL  11134 184.0\nNOTE: Invalid data errors for file CARDS occurred outside the printed range.\nNOTE: Increase available buffer lines with the INFILE n= option.\nproduct=159 censust=0 cinstcl=18 natguar=01 cvar01=. cvar06=2964786118 cvar03=KB201746 cvar04=ARKED cvar14=PRINT cvar13=0351\ncvar08=150000 cvar09=0.0026808 cvar10=0 cvar11=0 cvar02=19340.0 cvar05=. cvar07=10098 cvar12=142.0 cvar15=159.0 branch=0 cvarxx=18\n_ERROR_=1 _N_=3\nNOTE: Invalid data for cvar08 in line 53 66-69.\n54       159.0 0.0 18 02 . 2972375303.0 727188X TRIPLE-C RECYCLE SDN. BHD. 0351  500000.0 -0.0021438 0.0 \n101  0.0  20811.0 IL  11134 184.0\nNOTE: Invalid data errors for file CARDS occurred outside the printed range.\nNOTE: Increase available buffer lines with the INFILE n= option.\nproduct=159 censust=0 cinstcl=18 natguar=02 cvar01=. cvar06=2972375303 cvar03=727188X cvar04=TRIPLE-C cvar14=RECYCLE cvar13=SDN.\ncvar08=. cvar09=351 cvar10=500000 cvar11=-0.0021438 cvar02=0.0 cvar05=0 cvar07=20811.0 cvar12=IL cvar15=11134 branch=184\ncvarxx=159.0 _ERROR_=1 _N_=4\nNOTE: Invalid data forcvar08 in line 55 64-68.\nNOTE: Invalid data for cvar09 in line 55 70-73.\n55       159.0 0.0 18 01 . 2973063825.0 AS0135555W KEDAI CERMIN DAN FRAME OMAR 0351  50000.0 0.0 0.0 0.0 \n101   20907.0 IL  02105 238.0\nproduct=159 censust=0 cinstcl=18 natguar=01 cvar01=. cvar06=2973063825 cvar03=AS013555 cvar04=KEDAI cvar14=CERMIN cvar13=DAN\ncvar08=. cvar09=. cvar10=351 cvar11=50000 cvar02=0.0 cvar05=0 cvar07=0.0 cvar12=20907.0 cvar15=IL branch=2105 cvarxx=238.0 _ERROR_=1\n_N_=5\nNOTE: Invalid data for cvar05 in line 56 122-123.\nNOTE: LOST CARD.\n57       ;\nNOTE: Invalid data errors for file CARDS occurred outside the printed range.\nNOTE: Increase available buffer lines with the INFILE n= option.\nproduct=159 censust=0 cinstcl=18 natguar=06 cvar01=1000581459 cvar06=2976290832 cvar03=KT030606 cvar04=FENRA cvar14=TRADING\ncvar13=0351 cvar08=50000 cvar09=13723.953961 cvar10=0 cvar11=0 cvar02=21213.0 cvar05=. cvar07=03027 cvar12=14.0 cvar15=  branch=.\ncvarxx=  _ERROR_=1 _N_=6\nNOTE: SAS went to a new line when INPUT statement reached past the end of a line.\nNOTE: The data set WORK.NPGS_OUTPUT has 5 observations and 21 variables.\nNOTE: DATA statement used (Total process time):\n      real time           0.00 seconds\n      cpu time            0.00 seconds\n      \n57      ;\n58   run;\n59   \n60   ', 'LST': ''}
SAS Connection terminated. Subprocess id was 2941411

Summary:
MEFT.txt file created with 10 records
Report saved to MEFR.txt
