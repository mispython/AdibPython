import saspy
sas = saspy.SASsession(cfgname='default')
sas.saslib('MISOUT', path='/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA')

print(sas.submit("""
    proc contents data=MISOUT.AWSA11 short varnum;
    run;
""")['LST'])

print(sas.submit("""
    proc sql;
        select count(*) as nrows,
               sum(NOACCT) as total_accounts,
               sum(CURBAL) as total_balance
        from MISOUT.AWSA11;
    quit;
""")['LST'])
sas.endsas()
