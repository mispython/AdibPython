Processing date: 2026-07-01 11:18:34.052659
Year: 2026, Month: 07, Day: 01, RDate: 182
No data found for RDate 182 in behaveindfxfd
No data found for RDate 182 in BEHAVENONFXFD
No data found for RDate 182 in BEHAVEINDFXCA
No data found for RDate 182 in BEHAVENONFXCA
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDMSFX.py", line 185, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDMSFX.py", line 106, in main
    nlf_df = conn.execute(nlf_query).arrow()
_duckdb.CatalogException: Catalog Error: Table with name BEHAVEINDFXFD does not exist!

LINE 8:     FROM BEHAVEINDFXFD
                 ^


ALSO NOTE THAT ALL THE INPUT NAMING IS IN LOWERCASE NOT UPPERCASE
