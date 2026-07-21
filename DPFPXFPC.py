/*LNMCRSCV JOB ACCOUNT-CODE,MSGCLASS=A,MSGLEVEL=(1,1),CLASS=A,          JOB24477
//         REGION=64M,NOTIFY=&SYSUID USER=OPCC                                  
//*JOBPARM S=S1M1                                                               
//********************************************************************          
//*2020-423                                                                     
//*   (1) VALIDATE THE TOTAL COUNT BETWEEN INTERFACE FILE AND CONTROL           
//*       FILE FROM MIS                                                         
//*******************************************************************           
//SAS609   EXEC SAS609,REGION=0M,WORK='80000,80000'                             
//CONFIG    DD DISP=SHR,DSN=SYS3.SAS.V609.CNTL(BATCHXA)                         
//STEPLIB   DD DISP=(SHR,PASS),DSN=&LOAD                                        
//          DD DISP=SHR,DSN=SYS3.SAS.V609.LIBRARY                               
//IEFRDER   DD DUMMY                                                            
//MISFILE   DD DSN=RBP2.B033.LNMCRSCU.INPUT(0),DISP=SHR                         
//CTRLFLE   DD DSN=RBP2.B033.LNMCRSCV.INPUT(0),DISP=SHR                         
//OUTFILE   DD DSN=RBP2.B033.LNMCRSCU.INPUT(0),DISP=OLD        223197           
//DFSVSAMP  DD DSN=RBP2.IB330P.CONTROL(IBAMS#00),DISP=SHR                       
//SASLIST   DD SYSOUT=X                                                         
//ABENDLOG  DD SYSOUT=X                                                         
//SYSIN     DD *                                                                
                                                                                
OPTIONS IMSDEBUG=N YEARCUTOFF=1950 SORTDEV=3390 ERRORS=0 PS=140                 
MISSING=' ' NODATE NONUMBER; TITLE;    */   

  FILENAME CTRLDATE "/host_pq/loan/input/SRSCTRL1.txt";
  *FILENAME CTRLDATE "/stgsrcsys/host/uat/kam/SRSCTRL1.txt";                                                                              
 /*----------------------------------------------------------------*/           
 /* GET FIELDS FROM CTRLDATE                                       */           
 /*----------------------------------------------------------------*/           
 DATA _NULL_;                                                                   
   INFILE CTRLDATE;                                                             
   INPUT @1  DATE  8.;                                                          
   TODAYDTE=INPUT(SUBSTR(PUT(DATE, Z8.), 1, 8),YYMMDD8.);                       
   CALL SYMPUT('TODAYDTE',PUT(TODAYDTE,YYMMDD8.));                              
                                                                                
   TDY_YYMM =SUBSTR(PUT(DATE, Z8.),1,6);                                        
   CALL SYMPUT('TDY_YYMM',TDY_YYMM);   
   TODAYDTE=INPUT(PUT(DATE, Z8.),YYMMDD8.);              /*EDWSAS*/        
   CALL SYMPUT('FILEDT',PUT(TODAYDTE,YYMMDDN8.));           /*EDWSAS*/   
 
 RUN;                                                                           

 /*----------------------------------------------------------------*/           
 /*       GET PROCESSING DATE/SYSTEM DATE FROM DATEFILE            */           
 /*----------------------------------------------------------------*/           
 DATA GETDATE;                                                                  
   DT=TODAY();                                                                  
   DD=PUT(DAY(DT),Z2.);                                                         
   MM=PUT(MONTH(DT),Z2.);                                                       
   CCYY=PUT(YEAR(DT),Z4.);                                                      
   CC = SUBSTR(PUT(CCYY, 4.),1,2);                                              
   YY = SUBSTR(PUT(CCYY, 4.),3,2);                                              
   CALL SYMPUT('DAY', PUT(DD,2.));                                              
   CALL SYMPUT('MONTH', PUT(MM,2.));                                            
   CALL SYMPUT('CTRY', PUT(CC,2.));                                             
   CALL SYMPUT('YEAR', PUT(YY,2.));   
 
 RUN;                       

 /*-------------------------------------------------------------------*/        
 /* INPUT FILES                                                       */        
 /*-------------------------------------------------------------------*/ 
 FILENAME MISFILE   "/host_pq/loan/input/LNMCRSCU_INPUT_&FILEDT..txt"; 
 FILENAME CTRLFLE   "/host_pq/loan/input/LNMCRSCV_INPUT_&FILEDT..txt"; 
 *FILENAME CTRLDATE "/stgsrcsys/host/uat/kam/source/LNMCRSCU_INPUT_&FILEDT..txt";                                                                              



 /*-------------------------------------------------------------------*/        
 /* OUTPUT  FILES                                                     */        
 /*-------------------------------------------------------------------*/        
 FILENAME OUTFILE  "/host_pq/loan/input/LNMCRSCU_INPUT_&FILEDT..txt"; 
 *FILENAME OUTFLE  "/stgsrcsys/host/uat/kam/nna/output/LNMCRSCU_INPUT_&FILEDT..txt";

                                                                                
 /**************************************************************/               
 /* CHECK MIS FILE TOTAL RECORD                                */               
 /**************************************************************/               
DATA TOTREC;                                                                    
   INFILE MISFILE;                                                              
                                                                                
   INPUT   @01   ACCTNO      10.                                                
           @01   REC_DET    $80.                       /*223197*/               
           @12   NOTENO       5.                       /*223197*/               
           @18   EFF_DD      $2.                       /*223197*/               
           @21   EFF_MM      $2.                       /*223197*/               
           @24   EFF_YY      $2.;                      /*223197*/               
                                                                                
     EFFDTE_DMY = EFF_DD || EFF_MM || EFF_YY;          /*223197*/               
     EFFDMY_SAS = INPUT(SUBSTR                         /*223197*/               
                  (PUT(EFFDTE_DMY,$6.),1,6),DDMMYY8.); /*223197*/               
                                                                                
          COUNT+1;                                                              
   /* CALL SYMPUT('TOTC',PUT(COUNT,Z10.)); */                                   
      CALL SYMPUT('TOTC',PUT(COUNT,10.));                                       
                                                                                
RUN;                                                                            
 /**************************************************************/               
 /* CHECK MIS CONTROL FILE RECORD                              */               
 /**************************************************************/               
DATA TOTCTL;                                                                    
   INFILE CTRLFLE;                                                              
   FILE ABENDLOG;                                                               
                                                                                
   INPUT   @01   INF_DATE    $5.                                                
           @06   INF_CNT     $8.;                                               
                                                                                
       IF INF_CNT NOT= &TOTC THEN DO;                                           
          PUT  @1  '*************************************************';         
          PUT  @1  '* TOTAL INPUT COUNT NOT TALLY WITH CTL TABLE COUNT';        
          PUT  @1  '* PLEASE CONTACT PROGRAMMER!!!        ';                    
          PUT  @1  '*************************************************';         
          PUT  @1  '  TOTAL CNT: ' "&TOTC";                                     
          PUT  @1  '  CONTROL CNT : ' INF_CNT;                                  
          ABORT ABEND 888;                                                      
      END;                                                                      
      ELSE DO;                                                                  
          PUT  @1  '*************************************************';         
          PUT  @1  '* TOTAL COUNT RECEIVED:                          ';         
          PUT  @1  '*************************************************';         
          PUT  @1  '  TOTAL CNT: ' "&TOTC";                                     
          PUT  @1  '  CONTROL CNT : ' INF_CNT;                                  
      END;                                                                      
                                                                                
RUN;                                                                            
                                                                                
PROC SORT DATA = TOTREC; BY ACCTNO NOTENO EFFDMY_SAS;  /*223197*/               
 /**************************************************************/               
 /* OUTPUT FILE AFTER SORT ACCOUNTS BY ACCT NO., NOTE NO.,     */               
 /* & EFF DATE                                           223197*/               
 /**************************************************************/               
DATA _NULL_;                                           /*223197*/               
   SET TOTREC;                                         /*223197*/               
   FILE OUTFILE MOD;                                       /*223197*/               
                                                       /*223197*/               
     PUT   @01   REC_DET    $80.;                      /*223197*/               
                                                       /*223197*/               
RUN;                                                   /*223197*/               
