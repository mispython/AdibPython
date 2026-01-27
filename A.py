2025-10-31 00:00:00
Using SAS Config named: default
SAS Connection established. Subprocess id is 1267801

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
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/ELDS/MIS_ELNA_WK_UAT.py", line 340, in <module>
    ELNA1["AANO"] = ELNA1["AANO"].astype("string").str.strip().str.replace(r"\.0$", "", regex=True)
AttributeError: 'Series' object has no attribute 'astype'
SAS Connection terminated. Subprocess id was 1267801
