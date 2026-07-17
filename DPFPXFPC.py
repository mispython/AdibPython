   INPUT @1   ACCTNO         S370FPD6.                     /*EDWSAS*/                            
         @7   NOTENO         S370FPD3.                     /*EDWSAS*/                   
         @10  SC_SECTION    $EBCDIC20.                     /*EDWSAS*/                   
         @30  SC_TYPE       $EBCDIC10.                     /*EDWSAS*/                   
         @40  SC_SRCE       $EBCDIC10.                     /*EDWSAS*/                   
         @50  SC_SRCE_DATE   $EBCDIC8.                     /*EDWSAS*/                   
         @58  SC_GRADE1     $EBCDIC12.                     /*EDWSAS*/                   
         @70  SC_SCORE       $EBCDIC8.;                    /*EDWSAS*/   
		 SC_GRADE2 = UPCASE(SC_GRADE1);
         SC_GRADE = TRANSLATE(SC_GRADE2, "Ý",  '['); 
/*		 put SC_GRADE = &hex24;*/
		 
		                                                             
   IF SC_SRCE_DATE EQ 'NA' THEN DELETE; 
