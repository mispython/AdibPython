for loan, need to add filter "WHERE ENTITY_CD != 'PIBB'" (conventional)
for iloan, need to add filter of "WHERE ENTITY_CD = 'PIBB'" (islamic)
all inputs are in sas7bdat sas dataset and need to be in all lowercase.
use pyreadstat to read.
remove reptdate, use datetime timedelta - 1 instead. 
output in sas7bdat and parquet files. 
write out using saspy
