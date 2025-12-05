//LN6999NX JOB ACCOUNT-CODE,MSGCLASS=A,MSGLEVEL=(1,1),CLASS=A, 
//         REGION=64M,NOTIFY=&SYSUID,USER=OPCC                 
/*JOBPARM S=S1M1                                               
//***********************************************************  
//STEP1    EXEC PGM=IEFBR14                                    
//DEL1      DD DSN=RBP2.B033.R6999.BAL.NOTE,DISP=(MOD,DELETE), 
//             SPACE=(TRK,(1,1)),UNIT=(,5)                     
//***********************************************************  
//** EXTRACT INDIVIDUAL TOTAL FROM R-6999-001                  
//** BY COST CENTER, NOTE TYPE                                 
//***********************************************************  
//SAS609   EXEC SAS609,REGION=0M,WORK='80000,80000'            
//CONFIG    DD DISP=SHR,DSN=SYS3.SAS.V609.CNTL(BATCHXA)        
//STEPLIB   DD DISP=(SHR,PASS),DSN=&LOAD                       
//          DD DISP=SHR,DSN=SYS3.SAS.V609.LIBRARY              
//IEFRDER   DD DUMMY                                           
//INSFILE   DD DISP=SHR,DSN=RBP2.B033.R6999.BAL.ITEM           
//DFSVSAMP  DD DSN=RBP2.IB330P.CONTROL(IBAMS#00),DISP=SHR      
//SASLIST   DD SYSOUT=X                                        
//OUTFLE    DD DSN=RBP2.B033.R6999.BAL.NOTE,                   
//             DISP=(NEW,CATLG),                               
//             DCB=(DSORG=PS,RECFM=FB,LRECL=500,BLKSIZE=0),     
//             SPACE=(CYL,(20,10),RLSE),UNIT=(SYSDA,5)          
//SYSIN     DD *                                                
                                                                
OPTIONS IMSDEBUG=N YEARCUTOFF=1950 SORTDEV=3390 ERRORS=0 PS=140 
MISSING=' ' NODATE NONUMBER; TITLE;                             
DATA A;                                                         
   INFILE INSFILE;                                              
   RETAIN D_RPTSET                                              
          COSTCT                                                
          NOTETY ITEM                                           
          BAL1 BAL2  BAL3 BAL4                                  
          BAL5 BAL5A BAL6 BAL6A BAL7 BAL8 BAL9 C_PRN C_PRNNAC;  
   FORMAT BAL_2 $24.                                            
          BAL_7 $21.                                            
          BAL_R $24.                                            
          BAL1  $24.                                            
          BAL2  $24.                                            
          BAL3  $21.                                            
          BAL4  $21.                                            
          BAL5  $24.                                            
         BAL5A $24.                                               
         BAL6  $24.                                               
         BAL6A $24.                                               
         BAL7  $24.                                               
         BAL8  $24.                                               
         BAL9  $24.                                               
         C_PRN $07.                                               
         C_PRNNAC $07.;                                           
  INPUT @02 D_ITEM      $25.                                      
        @02 D_SIGN      $01.                                      
        @62 SIGN2       $01.                                      
        @67 SIGN7       $01.                                      
        @97 SIGNR       $01.                                      
        @97 D_RPTNO     $06.@;                                    
  IF D_RPTNO IN ('R-6999') THEN                                   
     INPUT @97 D_RPTSET   $10.@;                                  
     IF D_RPTSET IN ('R-6999-001') THEN  DO;                      
        INPUT @02 D_HEADER    $21.@;                              
        IF D_HEADER IN ('TOTALS  ACCRUAL DATE:') THEN DO;         
           INPUT @33 D_COSTCT    $12.                             
                 @55 D_NOTETY    $10.                             
                 @70 D_DESC      $20.@;                           
          IF D_COSTCT IN ('COST CENTER:') AND          
             D_NOTETY IN ('LOAN TYPE:')   AND          
             D_DESC   IN (' ') THEN DO;                
             INPUT @46   COSTCT     7.                 
                   @66   NOTETY     3.;                
          END;                                         
          ELSE DO;                                     
                  COSTCT = 0;                          
                  NOTETY = 0;                          
          END;                                         
       END;                                            
       ELSE                                            
       IF D_ITEM IN ('PRINCIPAL/LOAN BALANCE',         
                     'PRINCIPAL IN NON-ACCRUAL',       
                     'INTEREST ACCRUALS',              
                     'INTEREST IN NON-ACCRUAL',        
                     'UNEARNED INTEREST',              
                     'RESERVE FINANCED',               
                     'NON DDA ESCROW FUNDS',           
                     'LATE FEES',                      
                     'OTHER EARNED FEES') THEN DO;     
          INPUT  @94   D_ZERO    $04.;                          
          ITEM = D_ITEM;                                        
           IF D_ZERO IN ('ZERO') THEN DO;                       
             IF ITEM IN ('PRINCIPAL/LOAN BALANCE') THEN DO;     
                BAL1 = '0.00';                                  
                C_PRN = '0';                                    
             END;                                               
             IF ITEM IN ('PRINCIPAL IN NON-ACCRUAL') THEN DO;   
                BAL2 = '0.00';                                  
                C_PRNNAC = '0';                                 
             END;                                               
             IF ITEM IN ('INTEREST ACCRUALS') THEN              
                BAL3 = '0.0000000';                             
             IF ITEM IN ('INTEREST IN NON-ACCRUAL') THEN        
                BAL4 = '0.0000000';                             
             IF ITEM IN ('UNEARNED INTEREST') THEN DO;          
                BAL5  = '0.00';                                 
                BAL5A = '0.00';                                 
             END;                                               
             IF ITEM IN ('RESERVE FINANCED') THEN DO;           
                BAL6  = '0.00';                                 
                BAL6A = '0.00';                                 
              END;                                          
              IF ITEM IN ('NON DDA ESCROW FUNDS') THEN      
                 BAL7 = '0.00';                             
              IF ITEM IN ('LATE FEES') THEN                 
                 BAL8 = '0.00';                             
              IF ITEM IN ('OTHER EARNED FEES') THEN         
                 BAL9 = '0.00';                             
            END;                                            
        END;                                                
        ELSE IF D_SIGN IN ('=') THEN DO;                    
           INPUT @39 BALANCE2 $23.                          
                 @47 BALANCE7 $20.                          
                 @74 BALANCER $23.                          
                 @30 COUNT    $07.;                         
                                                            
           BAL_2 = SIGN2||BALANCE2;                         
           BAL_7 = SIGN7||BALANCE7;                         
           BAL_R = SIGNR||BALANCER;                         
                                                            
           IF ITEM IN ('PRINCIPAL/LOAN BALANCE') THEN DO;   
              BAL1 = BAL_2;                                 
              C_PRN = COUNT;                               
           END;                                            
           IF ITEM IN ('PRINCIPAL IN NON-ACCRUAL') THEN DO;
              BAL2 = BAL_2;                                
              C_PRNNAC = COUNT;                            
           END;                                            
           IF ITEM IN ('INTEREST ACCRUALS') THEN           
              BAL3 = BAL_7;                                
           IF ITEM IN ('INTEREST IN NON-ACCRUAL') THEN     
              BAL4 = BAL_7;                                
           IF ITEM IN ('UNEARNED INTEREST') THEN DO;       
              BAL5  = BAL_2;                               
              BAL5A = BAL_R;                               
           END;                                            
           IF ITEM IN ('RESERVE FINANCED') THEN DO;        
              BAL6  = BAL_2;                               
              BAL6A = BAL_R;                               
           END;                                            
           IF ITEM IN ('NON DDA ESCROW FUNDS') THEN        
              BAL7 = BAL_2;                                
           IF ITEM IN ('LATE FEES') THEN                   
               BAL8 = BAL_2;                              
            IF ITEM IN ('OTHER EARNED FEES') THEN         
               BAL9 = BAL_2;                              
         END;                                             
         ELSE IF D_ITEM IN ('EXTENSION FEES') AND         
            COSTCT NOT = 0 THEN DO;                       
            OUTPUT A;                                     
         END;                                             
      END;                                                
RUN;                                                      
                                                          
DATA _NULL_;                                              
SET A;                                                    
FILE OUTFLE;                                              
    PRN_BAL      = RIGHT(BAL1);                           
    PRNNACCR     = RIGHT(BAL2);                           
    INT_ACCR     = RIGHT(BAL3);                           
    INTNACCR     = RIGHT(BAL4);                           
    UNERNINT     = RIGHT(BAL5);                           
    UNERNNON     = RIGHT(BAL5A);                          
    RSRV_FIN     = RIGHT(BAL6);                           
    RSRV_DLR     = RIGHT(BAL6A);     
    NON_DDA      = RIGHT(BAL7);      
    LATEFEES     = RIGHT(BAL8);      
    OTHERFEE     = RIGHT(BAL9);      
    C_PRN        = RIGHT(C_PRN);     
    C_PRNNAC     = RIGHT(C_PRNNAC);  
    PUT @001  COSTCT             Z7. 
        @010  NOTETY             Z3. 
        @015  PRN_BAL           $24. 
        @047  PRNNACCR          $24. 
        @079  INT_ACCR          $21. 
        @111  INTNACCR          $21. 
        @143  UNERNINT          $24. 
        @175  UNERNNON          $24. 
        @207  RSRV_FIN          $24. 
        @239  RSRV_DLR          $24. 
        @271  NON_DDA           $24. 
        @303  LATEFEES          $24. 
        @335  OTHERFEE          $24. 
        @367  C_PRN             $07. 
        @377  C_PRNNAC          $07.;
RUN;
