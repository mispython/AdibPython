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
169                 select count(*) into :table_exists
170                 from dictionary.tables
171                 where libname = upcase("ctl_elds")
172                       and memname = upcase("elna12_ctl");
173              quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.01 seconds
      
174  
175  
176  ods html5 (id=saspy_internal) close;ods listing;


178  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
178! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
179  
180  
181              proc sql noprint;
182                 create table colmeta as
183                 select name, type, length
184                 from dictionary.columns
185                 where libname = upcase("ctl_elds")
186                       and memname = upcase("elna12_ctl");
NOTE: Table WORK.COLMETA created, with 0 rows and 3 columns.

187              quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
188  
189  
190  ods html5 (id=saspy_internal) close;ods listing;

WARNING: Control table ctl_elds.elna12_ctl not found or has no columns.
Creating table elds.elna12_10425 directly from work.elna12_10425

255  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
255! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
256  
257  
258                  proc sql noprint;
259                       create table elds.elna12_10425 as
260                       select * from work.elna12_10425;
NOTE: Table ELDS.ELNA12_10425 created, with 8400 rows and 27 columns.

261                  quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.01 seconds
      
262  
263  
264  ods html5 (id=saspy_internal) close;ods listing;


392  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
392! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
393  
394  
395              proc sql noprint;
396                 select count(*) into :table_exists
397                 from dictionary.tables
398                 where libname = upcase("ctl_elds")
399                       and memname = upcase("ielna12_ctl");
400              quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.01 seconds
      
401  
402  
403  ods html5 (id=saspy_internal) close;ods listing;


405  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
405! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
406  
407  
408              proc sql noprint;
409                 create table colmeta as
410                 select name, type, length
411                 from dictionary.columns
412                 where libname = upcase("ctl_elds")
413                       and memname = upcase("ielna12_ctl");
NOTE: Table WORK.COLMETA created, with 0 rows and 3 columns.

414              quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
415  
416  
417  ods html5 (id=saspy_internal) close;ods listing;

WARNING: Control table ctl_elds.ielna12_ctl not found or has no columns.
Creating table ields.ielna12_10425 directly from work.ielna12_10425

482  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
482! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
483  
484  
485                  proc sql noprint;
486                       create table ields.ielna12_10425 as
487                       select * from work.ielna12_10425;
NOTE: Table IELDS.IELNA12_10425 created, with 2971 rows and 27 columns.

488                  quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
489  
490  
491  ods html5 (id=saspy_internal) close;ods listing;


579  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
579! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
580  
581  
582              proc sql noprint;
583                 select count(*) into :table_exists
584                 from dictionary.tables
585                 where libname = upcase("ctl_elds")
586                       and memname = upcase("acc_conduct_aa_ctl");
587              quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.01 seconds
      
588  
589  
590  ods html5 (id=saspy_internal) close;ods listing;


592  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
592! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
593  
594  
595              proc sql noprint;
596                 create table colmeta as
597                 select name, type, length
598                 from dictionary.columns
599                 where libname = upcase("ctl_elds")
600                       and memname = upcase("acc_conduct_aa_ctl");
NOTE: Table WORK.COLMETA created, with 0 rows and 3 columns.

601              quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.01 seconds
      
602  
603  
604  ods html5 (id=saspy_internal) close;ods listing;

WARNING: Control table ctl_elds.acc_conduct_aa_ctl not found or has no columns.
Creating table elds.acc_conduct_aa_10425 directly from work.acc_conduct_aa_10425

669  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
669! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
670  
671  
672                  proc sql noprint;
673                       create table elds.acc_conduct_aa_10425 as
674                       select * from work.acc_conduct_aa_10425;
NOTE: Table ELDS.ACC_CONDUCT_AA_10425 created, with 6904 rows and 15 columns.

675                  quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
676  
677  
678  ods html5 (id=saspy_internal) close;ods listing;


766  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
766! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
767  
768  
769              proc sql noprint;
770                 select count(*) into :table_exists
771                 from dictionary.tables
772                 where libname = upcase("ctl_elds")
773                       and memname = upcase("iacc_conduct_aa_ctl");
774              quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
775  
776  
777  ods html5 (id=saspy_internal) close;ods listing;


779  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
779! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
780  
781  
782              proc sql noprint;
783                 create table colmeta as
784                 select name, type, length
785                 from dictionary.columns
786                 where libname = upcase("ctl_elds")
787                       and memname = upcase("iacc_conduct_aa_ctl");
NOTE: Table WORK.COLMETA created, with 0 rows and 3 columns.

788              quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
789  
790  
791  ods html5 (id=saspy_internal) close;ods listing;

WARNING: Control table ctl_elds.iacc_conduct_aa_ctl not found or has no columns.
Creating table ields.iacc_conduct_aa_10425 directly from work.iacc_conduct_aa_10425

856  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
856! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
857  
858  
859                  proc sql noprint;
860                       create table ields.iacc_conduct_aa_10425 as
861                       select * from work.iacc_conduct_aa_10425;
NOTE: Table IELDS.IACC_CONDUCT_AA_10425 created, with 1260 rows and 15 columns.

862                  quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.01 seconds
      cpu time            0.01 seconds
      
863  
864  
865  ods html5 (id=saspy_internal) close;ods listing;


922  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
922! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
923  
924  
925              proc sql noprint;
926                 select count(*) into :table_exists
927                 from dictionary.tables
928                 where libname = upcase("ctl_elds")
929                       and memname = upcase("ecms_ctl");
930              quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
931  
932  
933  ods html5 (id=saspy_internal) close;ods listing;


935  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
935! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
936  
937  
938              proc sql noprint;
939                 create table colmeta as
940                 select name, type, length
941                 from dictionary.columns
942                 where libname = upcase("ctl_elds")
943                       and memname = upcase("ecms_ctl");
NOTE: Table WORK.COLMETA created, with 0 rows and 3 columns.

944              quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
945  
946  
947  ods html5 (id=saspy_internal) close;ods listing;

WARNING: Control table ctl_elds.ecms_ctl not found or has no columns.
Creating table elds.ecms10425 directly from work.ecms10425

1012  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
1012! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
1013  
1014  
1015                  proc sql noprint;
1016                       create table elds.ecms10425 as
1017                       select * from work.ecms10425;
NOTE: Table ELDS.ECMS10425 created, with 1437 rows and 4 columns.

1018                  quit;
NOTE: PROCEDURE SQL used (Total process time):
      real time           0.00 seconds
      cpu time            0.01 seconds
      
1019  
1020  
1021  ods html5 (id=saspy_internal) close;ods listing;
