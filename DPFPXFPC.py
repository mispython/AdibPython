DATA SCOREREC SCORE_SEQ1 DEBUG;                                                
 /*INFILE SCOREFLE;                                          EDWSAS*/  
   INFILE SCOREFLE RECFM=F LRECL=800 pad;                  /*EDWSAS*/  

   FORMAT SCORE_DESC   $17.                                                     
          SC_SORT_SEQ    2.                                                     
          GRADE_X      $12.;                                                   
   SCORE_DESC  = ' ';                                                           
   SC_SORT_SEQ = 99;                                                            
   GRADE_X = ' ';                                                               
                                                                                
 /*INPUT @1   ACCTNO              PD6.                       EDWSAS*/                            
 /*      @7   NOTENO              PD3.                       EDWSAS*/                   
 /*      @10  SC_SECTION          $20.                       EDWSAS*/                   
 /*      @30  SC_TYPE             $10.                       EDWSAS*/                   
 /*      @40  SC_SRCE             $10.                       EDWSAS*/                   
 /*      @50  SC_SRCE_DATE         $8.   *FORMAT: DDMMCCYY*  EDWSAS*/                   
 /*      @58  SC_GRADE      $UPCASE12.                       EDWSAS*/                   
 /*      @70  SC_SCORE             $8.;                      EDWSAS*/

   INPUT @1   ACCTNO         S370FPD6.                     /*EDWSAS*/                            
         @7   NOTENO         S370FPD3.                     /*EDWSAS*/                   
         @10  SC_SECTION    $EBCDIC20.                     /*EDWSAS*/                   
         @30  SC_TYPE       $EBCDIC10.                     /*EDWSAS*/                   
         @40  SC_SRCE       $EBCDIC10.                     /*EDWSAS*/                   
         @50  SC_SRCE_DATE   $EBCDIC8.                     /*EDWSAS*/                   
         @58  SC_GRADE1     $EBCDIC12.                     /*EDWSAS*/                   
         @70  SC_SCORE       $EBCDIC8.;                    /*EDWSAS*/   
		 SC_GRADE2 = UPCASE(SC_GRADE1);
         SC_GRADE = TRANSLATE(SC_GRADE2, "�",  '['); 
/*		 put SC_GRADE = &hex24;*/
		 
                                                                                
   IF SC_SRCE_DATE EQ 'NA' THEN DELETE;                                         
                                                                                
   SC_DATE_SAS = INPUT(SUBSTR(PUT(SC_SRCE_DATE,$8.),1,8),DDMMYY8.);             
   SCORE_DESC  = TRIM(SC_TYPE) || ' ' || TRIM(SC_SRCE);                         
                                                                                
   SC_GRADE = LEFT(TRIM(SC_GRADE));                                             
   GRADE_X = SC_GRADE;                                                          
                                                                                
   /* FOR SCORE RMD - AFTER REMOVED LEADING SPACES */                           
   SC_GRADE2 = PUT(SUBSTR(PUT(SC_GRADE,$12.),3,2),$2.);                         
                                                                                
   /* FOR CRR - AFTER REMOVED LEADING SPACES */                                 
   SC_GRADE3 = PUT(SUBSTR(PUT(SC_GRADE,$12.),1,3),$3.);                         
                                                                                
   /* ------------------------- */                                              
   /* ASSIGN SCORE SORTING SEQ  */                                              
   /* ------------------------- */                                              
                                                                                
   SELECT (SCORE_DESC);                                                         
      WHEN ('B SCORE RMD')        SC_SORT_SEQ = 1;                              
      WHEN ('A SCORE ELDS')       SC_SORT_SEQ = 2;                              
      WHEN ('CRR ELDS')           SC_SORT_SEQ = 3;                              
      WHEN ('A SCORE ELDS/REV')   SC_SORT_SEQ = 4;                              
      WHEN ('A SCORE REV')        SC_SORT_SEQ = 5;                              
      WHEN ('L CRR REV')          SC_SORT_SEQ = 6;                              
      WHEN ('B SCORE REV')        SC_SORT_SEQ = 7;                              
      OTHERWISE                   SC_SORT_SEQ = 99;                             
   END;                                                                         
                                                                                
   IF SC_SORT_SEQ EQ 1 AND                                                      
      SC_GRADE    EQ 'NA' THEN DO;                                              
      OUTPUT DEBUG;                                                             
      DELETE;                                                                   
   END;                                                                         
                                                                                
 /*IF SC_SORT_SEQ EQ 99                                                         
      THEN DO;                                                                  
      OUTPUT DEBUG;                                                             
      DELETE;                                                                   
   END;        ENRTEST  */                                                      
                                                                                
   IF SC_SORT_SEQ = 1 THEN OUTPUT SCORE_SEQ1;                                   
   ELSE OUTPUT SCOREREC;                                                                                                                                  
 RUN;
