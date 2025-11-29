data work.WSGTCEI / view = work.WSGTCEI ; 
   infile '/sasdata/rawdata/lookup/LKP_BRANCH'
          lrecl = 80
          firstobs = 1; 
   ; 
   attrib BRCODE length = 8; 
   attrib BRABBR length = $3; 
   attrib BRSTAT length = $29; 
   attrib BRSTATEIND length = $1; 
   attrib BRSTATUS length = $1; 
   
   input BRCODE 2-4
          BRABBR $ 6-8
          BRSTAT $ 12-40
          BRSTATEIND $ 45-45
          BRSTATUS $ 50-50; 
   
run; 
