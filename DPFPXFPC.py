
============================================================
Bad Debt Write-Off List Generation
============================================================
Report Date: 23/11/2026
Week: 4, Previous Month: 10
*** TEST MODE: Limiting to 500000 rows ***
============================================================

STEP 1: DEBUG — BORSTAT values (ENTITY_CD='PIBB')...
  Reading: enrh_ln_note.sas7bdat - 500000 rows, 121.1s
  PIBB sample rows: 27
    '' : 27

STEP 2: Reading NPLA (ENTITY_CD='PIBB', BORSTAT='A')...
  Reading: enrh_ln_note.sas7bdat - 500000 rows, 133.4s
  ENTITY_CD='PIBB' filter: 500000 -> 27 rows
  Active (PIBB, A, LOANTYPE not excluded): 0
WARNING: No active accounts after filter. Exiting.
