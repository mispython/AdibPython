# =============================================================================
# PATH CONFIGURATION
#
# Layout:
#   - MFRS .sas7bdat outputs : /sas/.../XMIS/input/prod/EIMBNM01/mfrs/
#   - TEXT report            : /sas/.../XMIS/output/EIMBNM01/eimbnm01_report.txt
#   - Inputs (loan, lnwof, lnwod, dispay, btrad, lncomm, lnfee): unchanged.
# =============================================================================

# --- Inputs (unchanged) -----------------------------------------------------
BNM_LOAN_PREFIX       = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/loan{reptmon}{nowk}.sas7bdat"
BNM_LNWOF_PREFIX      = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/lnwof{reptmon}{nowk}.sas7bdat"
BNM_LNWOD_PREFIX      = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/lnwod{reptmon}{nowk}.sas7bdat"
SASD_LOAN_PREFIX      = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/loan{reptmon}.sas7bdat"
DISPAY_PREFIX         = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/dispaymth{reptmon}.sas7bdat"
LOAN_LNCOMM_SAS       = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/enrh_ln_comm_m{reptmon}.sas7bdat"
FEE_LNFEE_PREFIX      = "/stgsrcsys/host/uat/lnfee{reptmon}{nowk}.sas7bdat"
BTBNM_BTRAD_PREFIX    = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/btrad{reptmon}{nowk}{reptyear}.sas7bdat"

# --- MFRS .sas7bdat outputs (under input/prod) ------------------------------
MFRS_DIR              = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMBNM01/mfrs"
MFRS_MAST_BR_SAS      = os.path.join(MFRS_DIR, "mast_br.sas7bdat")
MFRS_ALM_CR_SAS       = os.path.join(MFRS_DIR, "alm_cr.sas7bdat")

# --- TEXT report (under output) ---------------------------------------------
REPORT_OUTPUT_DIR     = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMBNM01"
REPORT_TXT            = os.path.join(REPORT_OUTPUT_DIR, "eimbnm01_report.txt")

# --- Ensure both output directories exist -----------------------------------
os.makedirs(MFRS_DIR, exist_ok=True)
os.makedirs(REPORT_OUTPUT_DIR, exist_ok=True)
