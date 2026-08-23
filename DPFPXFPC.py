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
SAS Connection established. Subprocess id is 3015175

SAS log contains errors:
{'LOG': '\n21   \n22   \n23               PROC IMPORT DATAFILE="temp_sas_data.csv"\n24                   OUT=work.npgs_output\n25                   DBMS=CSV\n26                   REPLACE;\n27                   GETNAMES=YES;\n28                   GUESSINGROWS=MAX;\n29               RUN;\nNOTE: Unable to open parameter catalog: SASUSER.PARMS.PARMS.SLIST in update mode. Temporary parameter values will be saved to \nWORK.PARMS.PARMS.SLIST.\nNOTE: Unable to open SASUSER.PROFILE. WORK.PROFILE will be opened instead.\nNOTE: All profile changes will be lost at the end of the session.\n30    /**********************************************************************\n31    *   PRODUCT:   SAS\n32    *   VERSION:   9.4\n33    *   CREATOR:   External File Interface\n34    *   DATE:      23AUG26\n35    *   DESC:      Generated SAS Datastep Code\n36    *   TEMPLATE SOURCE:  (None Specified.)\n37    ***********************************************************************/\n38       data WORK.NPGS_OUTPUT    ;\n39       %let _EFIERR_ = 0; /* set the ERROR detection macro variable */\n40       infile \'temp_sas_data.csv\' delimiter = \',\' MISSOVER DSD lrecl=32767 firstobs=2 ;\n41          informat product $7. ;\n42          informat censust $5. ;\n43          informat cinstcl $4. ;\n44          informat natguar $4. ;\n45          informat cvar01 $14. ;\n46          informat cvar06 $14. ;\n47          informat cvar03 $12. ;\n48          informat cvar04 $29. ;\n49          informat cvar14 $6. ;\n50          informat cvar13 $2. ;\n51          informat cvar08 $10. ;\n52          informat cvar09 $23. ;\n53          informat cvar10 $5. ;\n54          informat cvar11 $5. ;\n55          informat cvar02 $2. ;\n56         informat cvar05 $9. ;\n57          informat cvar07 $4. ;\n58          informat cvar12 $2. ;\n59          informat cvar15 $7. ;\n60          informat branch $7. ;\n61          informat cvarxx $12. ;\n62      format product $7. ;\n63          format censust $5. ;\n64          format cinstcl $4. ;\n65          format natguar $4. ;\n66          format cvar01 $14. ;\n67          format cvar06 $14. ;\n68          format cvar03 $12. ;\n69          format cvar04 $29. ;\n70          format cvar14 $6. ;\n71          format cvar13 $2. ;\n72          format cvar08 $10. ;\n73          format cvar09 $23. ;\n74          format cvar10 $5. ;\n75          format cvar11 $5. ;\n76          format cvar02 $2. ;\n77          format cvar05 $9. ;\n78          format cvar07 $4. ;\n79          format cvar12 $2. ;\n80          format cvar15 $7. ;\n81 format branch $7. ;\n82          format cvarxx $12. ;\n83       input\n84                   product  $\n85                   censust  $\n86                   cinstcl  $\n87                   natguar  $\n88           cvar01  $\n89                   cvar06  $\n90                   cvar03  $\n91                   cvar04  $\n92                   cvar14  $\n93                   cvar13  $\n94                   cvar08  $\n95                   cvar09  $\n96                   cvar10  $\n97                   cvar11  $\n98                   cvar02  $\n99                   cvar05  $\n100                  cvar07  $\n101                  cvar12  $\n102                  cvar15  $\n103                  branch  $\n104                  cvarxx  $\n105      ;\n106      if _ERROR_ then call symputx(\'_EFIERR_\',1);  /* set ERROR detection macro variable */\n107      run;\nNOTE: The infile \'temp_sas_data.csv\' is:\n      Filename=/sas/python/virt_edw/Data_Warehouse/MIS/temp_sas_data.csv,\n      Owner Name=sas_edw_dev,\n      Group Name=sas_edw_dev_grp,\n      Access Permission=-rw-rw-r--,\n      Last Modified=23Aug2026:18:43:28,\n      File Size (bytes)=1960\n\nNOTE: 10 records were read from the infile \'temp_sas_data.csv\'.\n      The minimum record length was 166.\n      The maximum record length was 184.\nNOTE: The data set WORK.NPGS_OUTPUT has 10 observations and 21 variables.\nNOTE: DATA statement used (Total process time):\n      real time           0.00 seconds\n      cpu time           0.00 seconds\n      \n10 rows created in WORK.NPGS_OUTPUT from temp_sas_data.csv.\n  \n  \n  \nNOTE: WORK.NPGS_OUTPUT data set was successfully created.\nNOTE: The data set WORK.NPGS_OUTPUT has 10 observations and 21 variables.\nNOTE: PROCEDURE IMPORT used (Total process time):\n      real time           0.40 seconds\n      cpu time            0.06 seconds\n      \n108  \n109              libname outlib "/sas/python/virt_edw/Data_Warehouse/MIS";\nNOTE: Libref OUTLIB was successfully assigned as follows: \n      Engine:        V9 \n      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS\n110              data outlib.eibrp159_output;\n111                  set work.npgs_output;\n112              run;\nNOTE: There were 10 observations read from the data set WORK.NPGS_OUTPUT.\nNOTE: The data set OUTLIB.EIBRP159_OUTPUT has 10 observations and 21 variables.\nNOTE: DATA statement used (Total process time):\n      real time           0.00 seconds\n      cpu time            0.00 seconds\n      \n113  \n114              proc datasets lib=work nolist;\n115                  delete npgs_output;\n116              run;\nNOTE: Deleting WORK.NPGS_OUTPUT (memtype=DATA).\n117  \n118  ', 'LST': ''}
SAS Connection terminated. Subprocess id was 3015175

Summary:
MEFT.txt file created with 10 records
Report saved to MEFR.txt
