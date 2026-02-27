"""
EIBMNPL0 - NPL Report Runner

Purpose: Run EIBMNPL1 twice - once for PBB (conventional), once for PIBB (Islamic)

Process:
1. Clean up old output files
2. Create new output file structures
3. Run EIBMNPL1 with PBB datasets
4. Run EIBMNPL1 with PIBB datasets

Output:
- SAP.PBB.ODTLLIST (Conventional NPL report)
- SAP.PIBB.ODTLLIST (Islamic NPL report)
"""

import subprocess
import os
import sys

# Directories
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIBMNPL0 - NPL Report Runner")
print("=" * 60)

# Clean up old outputs
print("\n1. Cleaning up old files...")
for entity in ['PBB', 'PIBB']:
    for suffix in ['COLD', 'TEXT']:
        filepath = f'{OUTPUT_DIR}ODTLLIST_{entity}_{suffix}.txt'
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
                print(f"  ✓ Deleted {entity}_{suffix}")
            except Exception as e:
                print(f"  ⚠ Error deleting {entity}_{suffix}: {e}")

# Create output file structures
print("\n2. Creating output file structures...")
for entity in ['PBB', 'PIBB']:
    for suffix in ['COLD', 'TEXT']:
        filepath = f'{OUTPUT_DIR}ODTLLIST_{entity}_{suffix}.txt'
        try:
            open(filepath, 'w').close()
            print(f"  ✓ Created {entity}_{suffix}")
        except Exception as e:
            print(f"  ⚠ Error creating {entity}_{suffix}: {e}")

print("=" * 60)

# Run EIBMNPL1 for PBB (Conventional)
print("\n3. Running EIBMNPL1 for PBB (Conventional)...")
print("-" * 60)

env_pbb = os.environ.copy()
env_pbb['ENTITY'] = 'PBB'
env_pbb['BNM_DIR'] = 'data/bnm/pbb/'
env_pbb['OUTPUT_FILE'] = f'{OUTPUT_DIR}ODTLLIST_PBB_COLD.txt'

try:
    result = subprocess.run(
        ['python', 'eibmnpl1.py'],
        env=env_pbb,
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print("✓ PBB NPL Report completed successfully")
        if result.stdout:
            print(result.stdout)
    else:
        print(f"✗ PBB NPL Report failed with code {result.returncode}")
        if result.stderr:
            print(result.stderr)
        sys.exit(1)

except Exception as e:
    print(f"✗ Error running PBB report: {e}")
    # Continue to PIBB anyway in production

print("=" * 60)

# Run EIBMNPL1 for PIBB (Islamic)
print("\n4. Running EIBMNPL1 for PIBB (Islamic)...")
print("-" * 60)

env_pibb = os.environ.copy()
env_pibb['ENTITY'] = 'PIBB'
env_pibb['BNM_DIR'] = 'data/bnm/pibb/'
env_pibb['OUTPUT_FILE'] = f'{OUTPUT_DIR}ODTLLIST_PIBB_COLD.txt'

try:
    result = subprocess.run(
        ['python', 'eibmnpl1.py'],
        env=env_pibb,
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print("✓ PIBB NPL Report completed successfully")
        if result.stdout:
            print(result.stdout)
    else:
        print(f"✗ PIBB NPL Report failed with code {result.returncode}")
        if result.stderr:
            print(result.stderr)
        sys.exit(1)

except Exception as e:
    print(f"✗ Error running PIBB report: {e}")
    sys.exit(1)

print("=" * 60)
print("\n✓ EIBMNPL0 Complete!")
print("=" * 60)
print("\nBoth NPL Reports Generated:")
print("  1. PBB (Conventional) - ODTLLIST_PBB_COLD.txt")
print("  2. PIBB (Islamic) - ODTLLIST_PIBB_COLD.txt")
print("\nReports Include:")
print("  - Risk detail by branch and risk category")
print("  - NPL category summary (Overdue, Delinquent, NPL)")
print("  - Coverage: Loans, HP, O/D, and Combined")
print("\nRecipient: MR JEFFERY KOH")
print("Department: CREDIT CONTROL CENTER")
print("Location: 17TH FLOOR, MENARA PBB")

sys.exit(0)
