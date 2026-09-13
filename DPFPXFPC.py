acctfile :

Record format . . . : FB   
Record length . . . : 4000 
Block size  . . . . : 24000
1st extent cylinders: 1500 
Secondary cylinders : 500  


nfeefile(0):

Record format . . . : FB   
Record length . . . : 300  
Block size  . . . . : 27900
1st extent cylinders: 300  
Secondary cylinders : 300  


and also please include below line in python version

DATA MIS.LOAN&REPTDAY;
     SET LOAN;
*;
DATA PREVLN;
   SET MIS.LOAN&PREVDAY;
   RENAME BRLNAMT=PBRLNAMT;
*;

or is it not used as inputs?
