lzopts servercp=iso8859-1,clientcp=ibm-1140,notrim,overflow=trunc    
lzopts mode=text                                                     
lzopts lrecl=134,recfm=fba,blksize=0,space=cyl.1.1                   
cd /stgsrcsys/host/uat/kam/nna/output                                
get LNFDBTAG_REPORT_20260715.txt    \                                
                      //!RBD2.B033.SW.LNFDBTAG.TEST                  
EOB                                                                  
**************************** Bottom of Data *************************
