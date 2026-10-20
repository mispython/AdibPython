import pyreadstat
import pandas as pd

# LOANTEMP product distribution
lt, _ = pyreadstat.read_sas7bdat(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOI/loantemp_iccd2.sas7bdat",
    usecols=['ACCTNO', 'PRODUCT', 'ARREAR', 'BRANCH'],
)
lt.columns = lt.columns.str.lower()

print("=== LOANTEMP PRODUCT distribution (all rows) ===")
print(lt['product'].value_counts().sort_index().to_string())
print(f"\nproduct=700 count: {(lt['product'] == 700).sum()}")

print("\n=== LOANTEMP BRANCH distribution (all rows, top 20) ===")
print(lt['branch'].value_counts().sort_index().head(20).to_string())
print(f"\nbranch=117 (KKI) count: {(lt['branch'] == 117).sum()}")

print("\n=== ARREAR >= 10 by product ===")
print(lt[lt['arrear'] >= 10]['product'].value_counts().sort_index().to_string())

print("\n=== ARREAR >= 10 by branch (top 20) ===")
print(lt[lt['arrear'] >= 10]['branch'].value_counts().sort_index().head(20).to_string())
