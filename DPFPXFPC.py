================================================================================
LCR Concentration of Funding Report Generator
================================================================================
Calculating REPTDATE as (today - 1 day)...
Report Date: 31/07/2026
Report Month: 07
Reading template...
Template records: 107
Reading GL data...
GL records: 13
Reading COF data (CMM and EQU)...
Reading SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTCOF/cmm07.sas7bdat
Exists? True
Reading SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTCOF/equ07.sas7bdat
Exists? True
COF records (TAG=1): 182
Reading list files...
Reading SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTCOF/list/cof_mni_intra_group31.sas7bdat
Exists? True
Reading SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTCOF/list/cof_mni_related_party31.sas7bdat
Exists? True
Reading SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTCOF/list/cof_equ_intra_group31.sas7bdat
Exists? True
Reading SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTCOF/list/cof_equ_related_party31.sas7bdat
Exists? True
INTRAIC: 29, INTRACUS: 29
RELCUS: 33, XRELCUS: 1, RELIC: 33
INTRAEQ: 120, RELEQ: 16
Creating COF23 (TAG 2 and 3)...
Reading SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTCOF/cmm07.sas7bdat
Exists? True
PROGMAA.PBB.LCR.SASDATA.BINARYReading SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTCOF/equ07.sas7bdat
Exists? True
COF23 records: 60
Creating COF123...
COF123 records: 242
Creating COF45...
COF45 records: 182
Processing VOSTRO data...
Reading SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTCOF/vostro.sas7bdat
Exists? True
Reading SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTCOF/cisinfo.sas7bdat
Exists? True
VOSTRO records: 6
Total COF combined records: 443
Creating summaries...
COFITEM: 56, COFTOT: 27
Generating report...
Report written to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMTCOF/COF_OUTPUT.txt
Generating SFTP script...
SFTP script written to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMTCOF/SFTP_SCRIPT.txt
================================================================================
Processing completed successfully!
================================================================================
