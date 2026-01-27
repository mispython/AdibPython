shape: (11_956, 3)
┌───────────────┬────────┬──────────┐
│ AANO          ┆ BRANCH ┆ CUSTCODE │
│ ---           ┆ ---    ┆ ---      │
│ str           ┆ i64    ┆ f64      │
╞═══════════════╪════════╪══════════╡
│ IMO/000750/25 ┆ 5      ┆ 67.0     │
│ SAM/000723/25 ┆ 25     ┆ 59.0     │
│ SPG/000963/25 ┆ 26     ┆ 67.0     │
│ SGM/001452/25 ┆ 47     ┆ 67.0     │
│ MRI/000923/25 ┆ 50     ┆ 66.0     │
│ …             ┆ …      ┆ …        │
│ JHL/000505/25 ┆ 196    ┆ 77.0     │
│ JRL/001062/25 ┆ 29     ┆ 78.0     │
│ JRL/001062/25 ┆ 29     ┆ 78.0     │
│ BSA/000455/25 ┆ 293    ┆ 78.0     │
│ BSA/000455/25 ┆ 293    ┆ 78.0     │
└───────────────┴────────┴──────────┘
shape: (11_956, 2)
┌───────────────┬───────────────┐
│ AANO          ┆ MAANO         │
│ ---           ┆ ---           │
│ str           ┆ str           │
╞═══════════════╪═══════════════╡
│ IMO/000750/25 ┆ IMO/000750/25 │
│ SAM/000723/25 ┆ SAM/000723/25 │
│ SPG/000963/25 ┆ SPG/000963/25 │
│ SGM/001452/25 ┆ SGM/001452/25 │
│ MRI/000923/25 ┆ MRI/000923/25 │
│ …             ┆ …             │
│ JHL/000505/25 ┆ JHL/000505/25 │
│ JRL/001062/25 ┆ JRL/001062/25 │
│ JRL/001062/25 ┆ JRL/001062/25 │
│ BSA/000455/25 ┆ BSA/000455/25 │
│ BSA/000455/25 ┆ BSA/000455/25 │
└───────────────┴───────────────┘
shape: (17_116, 38)
┌───────────────┬─────────────────────────────────┬──────┬───────┬───┬───────────┬──────────┬───────────┬────────┐
│ MAANO         ┆ FNAME                           ┆ PROP ┆ SHRES ┆ … ┆ TTYPEFAC2 ┆ TLMTAPP2 ┆ TBALANCE2 ┆ CADBCR │
│ ---           ┆ ---                             ┆ ---  ┆ ---   ┆   ┆ ---       ┆ ---      ┆ ---       ┆ ---    │
│ str           ┆ str                             ┆ str  ┆ str   ┆   ┆ str       ┆ str      ┆ str       ┆ str    │
╞═══════════════╪═════════════════════════════════╪══════╪═══════╪═══╪═══════════╪══════════╪═══════════╪════════╡
│ IMO/000750/25 ┆ BAN SOONG HENG SDN. BHD.        ┆      ┆       ┆ … ┆           ┆          ┆           ┆        │
│ IMO/000750/25 ┆ TAN WENG HUAT                   ┆      ┆       ┆ … ┆           ┆          ┆           ┆        │
│ IMO/000750/25 ┆ TAN SIONG HENG                  ┆      ┆       ┆ … ┆           ┆          ┆           ┆        │
│ IMO/000750/25 ┆ CHA SOK SEE                     ┆      ┆       ┆ … ┆           ┆          ┆           ┆        │
│ SAM/000723/25 ┆ XENERGI SDN. BHD.               ┆      ┆       ┆ … ┆           ┆          ┆           ┆        │
│ …             ┆ …                               ┆ …    ┆ …     ┆ … ┆ …         ┆ …        ┆ …         ┆ …      │
│ TMG/000556/25 ┆ MEHALA A/P SANDERAN             ┆      ┆       ┆ … ┆           ┆          ┆           ┆        │
│ JHL/000505/25 ┆ KHAIRULERWAN SHAH BIN AHMAD SA… ┆      ┆       ┆ … ┆           ┆          ┆           ┆        │
│ JHL/000505/25 ┆ SITI HAWA BINTI YASIN           ┆      ┆       ┆ … ┆           ┆          ┆           ┆        │
│ JRL/001062/25 ┆ PRAKASH A/L BALA                ┆      ┆       ┆ … ┆           ┆          ┆           ┆        │
│ BSA/000455/25 ┆ TAN SU SZE                      ┆      ┆       ┆ … ┆           ┆          ┆           ┆        │
└───────────────┴─────────────────────────────────┴──────┴───────┴───┴───────────┴──────────┴───────────┴────────┘
shape: (8_164, 16)
┌───────────────┬─────────────────────────────────┬──────────────┬────────────┬───┬───────────┬──────────────┬──────────────┬───────────┐
│ MAANO         ┆ NAME                            ┆ ID           ┆ BRRWERTYPE ┆ … ┆ ACCACTVTY ┆ CACONDUCT    ┆ REPAYMENT    ┆ BTCONDUCT │
│ ---           ┆ ---                             ┆ ---          ┆ ---        ┆   ┆ ---       ┆ ---          ┆ ---          ┆ ---       │
│ str           ┆ str                             ┆ str          ┆ str        ┆   ┆ str       ┆ str          ┆ str          ┆ str       │
╞═══════════════╪═════════════════════════════════╪══════════════╪════════════╪═══╪═══════════╪══════════════╪══════════════╪═══════════╡
│ JRC/001563/25 ┆ Versatile Materials Sdn Bhd     ┆ 762018H      ┆ Applicant  ┆ … ┆ Active    ┆ N/A          ┆ Satisfactory ┆ N/A       │
│ DUA/000369/25 ┆ Range Pharma Sdn. Bhd.          ┆ 144401U      ┆ Applicant  ┆ … ┆ Active    ┆ Satisfactory ┆ Satisfactory ┆ N/A       │
│ JAH/000807/25 ┆ One B Import & Export Sdn. Bhd… ┆ 1257331P     ┆ Applicant  ┆ … ┆ Active    ┆ Satisfactory ┆ Satisfactory ┆ N/A       │
│ JAH/000807/25 ┆ One B Import & Export Sdn. Bhd… ┆ 1257331P     ┆ Applicant  ┆ … ┆ Active    ┆ Satisfactory ┆ Satisfactory ┆ N/A       │
│ DUA/000434/25 ┆ AIM Coffee (M) Sdn. Bhd.        ┆ 1046485M     ┆ Applicant  ┆ … ┆ Active    ┆ Satisfactory ┆ Satisfactory ┆ N/A       │
│ …             ┆ …                               ┆ …            ┆ …          ┆ … ┆ …         ┆ …            ┆ …            ┆ …         │
│ PLT/000841/25 ┆ Jack Chang Hong Kwang           ┆ 780722135063 ┆ Applicant  ┆ … ┆ Inactive  ┆ Satisfactory ┆ Satisfactory ┆ N/A       │
│ BSP/000796/25 ┆ INAYATUN NISSAH BINTI ABDUL KA… ┆ 960704085742 ┆ Applicant  ┆ … ┆ Inactive  ┆ Satisfactory ┆ N/A          ┆ N/A       │
│ TDY/000741/25 ┆ Chir Kee Kong                   ┆ 810425016089 ┆ Applicant  ┆ … ┆ Inactive  ┆ Satisfactory ┆ Satisfactory ┆ N/A       │
│ TDY/000741/25 ┆ Chir Kee Kong                   ┆ 810425016089 ┆ Applicant  ┆ … ┆ Active    ┆ Satisfactory ┆ Satisfactory ┆ N/A       │
│ TDY/000741/25 ┆ Chir Kee Kong                   ┆ 810425016089 ┆ Applicant  ┆ … ┆ Inactive  ┆ Satisfactory ┆ Satisfactory ┆ N/A       │
└───────────────┴─────────────────────────────────┴──────────────┴────────────┴───┴───────────┴──────────────┴──────────────┴───────────┘
shape: (1_437, 4)
┌───────────────┬──────────┬─────────────┬───────────────────┐
│ MAANO         ┆ ACCTNO   ┆ CCOLLNO     ┆ CCOLLTYPE         │
│ ---           ┆ ---      ┆ ---         ┆ ---               │
│ str           ┆ f64      ┆ str         ┆ str               │
╞═══════════════╪══════════╪═════════════╪═══════════════════╡
│ KTN/000609/25 ┆ 2.2036e9 ┆ 00027704519 ┆ New Property(1)   │
│ ASR/000534/25 ┆ null     ┆ 00014129522 ┆ Existing Property │
│ BPT/000675/25 ┆ null     ┆ 00022802557 ┆ Existing Property │
│ BPT/000675/25 ┆ null     ┆ 00022802615 ┆ Existing Property │
│ SMY/000349/25 ┆ 2.2027e9 ┆ 00027704535 ┆ New Property(1)   │
│ …             ┆ …        ┆ …           ┆ …                 │
│ PDN/000683/24 ┆ 2.9080e9 ┆ 00026617324 ┆ New Property(1)   │
│ KHG/000294/25 ┆ 2.2004e9 ┆ 00027702786 ┆ New Property(1)   │
│ BTL/000892/25 ┆ 2.2044e9 ┆ 00027695196 ┆ New Property(1)   │
│ PDG/001344/25 ┆ 2.2043e9 ┆ 00027698869 ┆ New Property(1)   │
│ FT0000001438  ┆ null     ┆             ┆                   │
└───────────────┴──────────┴─────────────┴───────────────────┘

165  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
165! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
166  
167  
168              proc sql noprint;
169                 create table colmeta as
170                 select name, type, length
171                 from dictionary.columns
172                 where libname = upcase("ctl_elds")
173                       and memname = upcase("elna12_ctl");
NOTE: Table WORK.COLMETA created, with 0 rows and 3 columns.

174              quit
175  
176  
177  ;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
177!  *';*";*/;ods html5 (id=saspy_internal) close;ods listing;

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")

242  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
242! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
243  
244  
245                  proc sql noprint;
246                       create table elds.elna12_10425 as
247                       select  from ctl_elds.elna12_ctl(obs=0)
                                       --------           -
                                       22                 76
ERROR 22-322: Syntax error, expecting one of the following: !, !!, &, (, *, **, +, ',', -, /, <, <=, <>, =, >, >=, ?, AND, BETWEEN, 
              CONTAINS, EQ, EQT, FROM, GE, GET, GT, GTT, LE, LET, LIKE, LT, LTT, NE, NET, OR, ^=, |, ||, ~=.  

ERROR 76-322: Syntax error, statement will be ignored.

247!                      select  from ctl_elds.elna12_ctl(obs=0)
                                                          -
                                                          22
ERROR 22-322: Syntax error, expecting one of the following: !, !!, &, *, **, +, ',', -, /, <, <=, <>, =, >, >=, ?, AND, BETWEEN, 
              CONTAINS, EQ, EQT, GE, GET, GT, GTT, LE, LET, LIKE, LT, LTT, NE, NET, OR, ^=, |, ||, ~=.  

248                       union corr
249                       select  from work.elna12_10425;
NOTE: PROC SQL set option NOEXEC and will continue to check the syntax of statements.
250                  quit;
NOTE: The SAS System stopped processing this step because of errors.
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
251  
252  
253  ods html5 (id=saspy_internal) close;ods listing;


381  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
381! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
382  
383  
384              proc sql noprint;
385                 create table colmeta as
386                 select name, type, length
387                 from dictionary.columns
388                 where libname = upcase("ctl_elds")
389                       and memname = upcase("ielna12_ctl");
NOTE: Table WORK.COLMETA created, with 0 rows and 3 columns.

390              quit
391  
392  
393  ;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
393!  *';*";*/;ods html5 (id=saspy_internal) close;ods listing;

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")

458  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
458! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
459  
460  
461                  proc sql noprint;
462                       create table ields.ielna12_10425 as
463                       select  from ctl_elds.ielna12_ctl(obs=0)
                                       --------            -
                                       22                  76
ERROR 22-322: Syntax error, expecting one of the following: !, !!, &, (, *, **, +, ',', -, /, <, <=, <>, =, >, >=, ?, AND, BETWEEN, 
              CONTAINS, EQ, EQT, FROM, GE, GET, GT, GTT, LE, LET, LIKE, LT, LTT, NE, NET, OR, ^=, |, ||, ~=.  

ERROR 76-322: Syntax error, statement will be ignored.

463!                      select  from ctl_elds.ielna12_ctl(obs=0)
                                                           -
                                                           22
ERROR 22-322: Syntax error, expecting one of the following: !, !!, &, *, **, +, ',', -, /, <, <=, <>, =, >, >=, ?, AND, BETWEEN, 
              CONTAINS, EQ, EQT, GE, GET, GT, GTT, LE, LET, LIKE, LT, LTT, NE, NET, OR, ^=, |, ||, ~=.  

464                       union corr
465                       select  from work.ielna12_10425;
NOTE: PROC SQL set option NOEXEC and will continue to check the syntax of statements.
466                  quit;
NOTE: The SAS System stopped processing this step because of errors.
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
467  
468  
469  ods html5 (id=saspy_internal) close;ods listing;


557  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
557! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
558  
559  
560              proc sql noprint;
561                 create table colmeta as
562                 select name, type, length
563                 from dictionary.columns
564                 where libname = upcase("ctl_elds")
565                       and memname = upcase("acc_conduct_aa_ctl");
NOTE: Table WORK.COLMETA created, with 0 rows and 3 columns.

566              quit
567  
568  
569  ;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
569!  *';*";*/;ods html5 (id=saspy_internal) close;ods listing;

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")

634  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
634! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
635  
636  
637                  proc sql noprint;
638                       create table elds.acc_conduct_aa_10425 as
639                       select  from ctl_elds.acc_conduct_aa_ctl(obs=0)
                                       --------                   -
                                       22                         76
ERROR 22-322: Syntax error, expecting one of the following: !, !!, &, (, *, **, +, ',', -, /, <, <=, <>, =, >, >=, ?, AND, BETWEEN, 
              CONTAINS, EQ, EQT, FROM, GE, GET, GT, GTT, LE, LET, LIKE, LT, LTT, NE, NET, OR, ^=, |, ||, ~=.  

ERROR 76-322: Syntax error, statement will be ignored.

639!                      select  from ctl_elds.acc_conduct_aa_ctl(obs=0)
                                                                  -
                                                                  22
ERROR 22-322: Syntax error, expecting one of the following: !, !!, &, *, **, +, ',', -, /, <, <=, <>, =, >, >=, ?, AND, BETWEEN, 
              CONTAINS, EQ, EQT, GE, GET, GT, GTT, LE, LET, LIKE, LT, LTT, NE, NET, OR, ^=, |, ||, ~=.  

640                       union corr
641                       select  from work.acc_conduct_aa_10425;
NOTE: PROC SQL set option NOEXEC and will continue to check the syntax of statements.
642                  quit;
NOTE: The SAS System stopped processing this step because of errors.
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.01 seconds
      
643  
644  
645  ods html5 (id=saspy_internal) close;ods listing;


733  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
733! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
734  
735  
736              proc sql noprint;
737                 create table colmeta as
738                 select name, type, length
739                 from dictionary.columns
740                 where libname = upcase("ctl_elds")
741                       and memname = upcase("iacc_conduct_aa_ctl");
NOTE: Table WORK.COLMETA created, with 0 rows and 3 columns.

742              quit
743  
744  
745  ;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.01 seconds
      
745!  *';*";*/;ods html5 (id=saspy_internal) close;ods listing;

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")

810  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
810! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
811  
812  
813                  proc sql noprint;
814                       create table ields.iacc_conduct_aa_10425 as
815                       select  from ctl_elds.iacc_conduct_aa_ctl(obs=0)
                                       --------                    -
                                       22                          76
ERROR 22-322: Syntax error, expecting one of the following: !, !!, &, (, *, **, +, ',', -, /, <, <=, <>, =, >, >=, ?, AND, BETWEEN, 
              CONTAINS, EQ, EQT, FROM, GE, GET, GT, GTT, LE, LET, LIKE, LT, LTT, NE, NET, OR, ^=, |, ||, ~=.  

ERROR 76-322: Syntax error, statement will be ignored.

815!                      select  from ctl_elds.iacc_conduct_aa_ctl(obs=0)
                                                                   -
                                                                   22
ERROR 22-322: Syntax error, expecting one of the following: !, !!, &, *, **, +, ',', -, /, <, <=, <>, =, >, >=, ?, AND, BETWEEN, 
              CONTAINS, EQ, EQT, GE, GET, GT, GTT, LE, LET, LIKE, LT, LTT, NE, NET, OR, ^=, |, ||, ~=.  

816                       union corr
817                       select  from work.iacc_conduct_aa_10425;
NOTE: PROC SQL set option NOEXEC and will continue to check the syntax of statements.
818                  quit;
NOTE: The SAS System stopped processing this step because of errors.
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
819  
820  
821  ods html5 (id=saspy_internal) close;ods listing;


878  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
878! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
879  
880  
881              proc sql noprint;
882                 create table colmeta as
883                 select name, type, length
884                 from dictionary.columns
885                 where libname = upcase("ctl_elds")
886                       and memname = upcase("ecms_ctl");
NOTE: Table WORK.COLMETA created, with 0 rows and 3 columns.

887              quit
888  
889  
890  ;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.01 seconds
      
890!  *';*";*/;ods html5 (id=saspy_internal) close;ods listing;

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")

955  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
955! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
956  
957  
958                  proc sql noprint;
959                       create table elds.ecms10425 as
960                       select  from ctl_elds.ecms_ctl(obs=0)
                                       --------         -
                                       22               76
ERROR 22-322: Syntax error, expecting one of the following: !, !!, &, (, *, **, +, ',', -, /, <, <=, <>, =, >, >=, ?, AND, BETWEEN, 
              CONTAINS, EQ, EQT, FROM, GE, GET, GT, GTT, LE, LET, LIKE, LT, LTT, NE, NET, OR, ^=, |, ||, ~=.  

ERROR 76-322: Syntax error, statement will be ignored.

960!                      select  from ctl_elds.ecms_ctl(obs=0)
                                                        -
                                                        22
ERROR 22-322: Syntax error, expecting one of the following: !, !!, &, *, **, +, ',', -, /, <, <=, <>, =, >, >=, ?, AND, BETWEEN, 
              CONTAINS, EQ, EQT, GE, GET, GT, GTT, LE, LET, LIKE, LT, LTT, NE, NET, OR, ^=, |, ||, ~=.  

961                       union corr
962                       select  from work.ecms10425;
NOTE: PROC SQL set option NOEXEC and will continue to check the syntax of statements.
963                  quit;
NOTE: The SAS System stopped processing this step because of errors.
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
964  
965  
966  ods html5 (id=saspy_internal) close;ods listing;
