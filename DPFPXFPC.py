Report Date: 200826 (20-08-26)
DPDATE: 200826 EQDATE: 200826
DPST records: 90326
DPST TICKETNO sample: ['N319474', 'N362347', 'N362466', 'N377670', 'N380788']
DPST INVCURRAC dtype: String
EQTN records: 532
EQTN TICKETNO sample: ['Z31222', 'Z31350', 'Z31156', 'Z31236', 'Z31336']
Common TICKETNO values: 266
DCID records after merge and filter: 266
Reading CA file...
Reading SA file...
Reading FCY file...
Reference data records: 5229286
Reference INVCURRAC2 dtype: String
After join - columns: ['TICKETNO', 'NEWIC', 'SALESID', 'CUSTCODE', 'INVCURRAC', 'ALTCURRAC', 'ROLLOVER', 'CONVERTIND', 'DEALERID', 'MANAGERID', 'CUSTNAME', 'ACCINT', 'BRANCH', 'PRODUCT', 'INVCURR', 'ALTCURR', 'CUSTICKETNO', 'INVAMT', 'ALTAMT', 'TRADEDT', 'STARTDT', 'FIXINGDT', 'MATDT', 'TENOR', 'STRIKERT', 'SPOTRT', 'DCIRT', 'MMRT', 'PREMREC', 'PREMPAID', 'UNWINDCOST', 'NEWDEAL', 'STATUSIND', 'CUSTCODE2']
After join - records: 266
Final records: 266
Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDCIA/dcid0820.parquet
Using SAS Config named: default
SAS Connection established. Subprocess id is 1183642

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS Connection terminated. Subprocess id was 1183642
Saved SAS datasets: DCI.DCID0820 and TEMP.DCID260820

Processing complete!
Output files:
  - /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDDCIA/dcid0820.parquet
