Billings Transaction Processing...
📅 Processing reporting dates...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBWTRBL_BILLING_TRANSACTION_PROCESSING.py", line 28, in <module>
    reptdate = datetime(1960, 1, 1) + datetime.timedelta(days=int(reptdate_numeric))
AttributeError: type object 'datetime.datetime' has no attribute 'timedelta'
You have mail in /var/spool/mail/sas_edw_dev
