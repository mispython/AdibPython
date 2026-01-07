DATA REPTDATE;                                                          00230003
   INFILE RPVBDATA LRECL=600 OBS=1;                                     00240003
   INPUT @03 TBDATE $8.;                                                00250003
   REPTDATE = INTNX('MONTH',INPUT(TBDATE,YYMMDD8.),-1,'E');             00260003
   PREVDATE = INTNX('MONTH',REPTDATE,-1,'E');                           00270003
   CALL SYMPUT('REPTDT', PUT(REPTDATE, MMYYN4.));                       00280003
   CALL SYMPUT('PREVDT', PUT(PREVDATE, MMYYN4.));                       00290003
RUN;                                                                    00300003
                                                                        00310003
DATA _NULL_;                                                            00320003
   INFILE SRSDATA LRECL=80 OBS=1;                                       00330003
   INPUT @01 TBDATE $8.;                                                00340003
   REPTDATE = INPUT(TBDATE,YYMMDD8.);                                   00350003
   CALL SYMPUT('SRSTDT', PUT(REPTDATE, MMYYN4.));                       00360003
RUN;                                                                    00370003
                                                                        00380003
%MACRO PROCESS;                                                         00390003
   %IF "&REPTDT" NE "&SRSTDT" %THEN %DO;                                00400003
      %PUT THE SAP.PBB.RPVB.TEXT IS NOT DATED (MMYY:&SRSTDT);           00410003
      DATA A;                                                           00420003
      ABORT 77;                                                         00430003
   %END;                                                                00440003
%MEND;                                                                  00450003
%PROCESS;                                                               00460003
                                                                        00470003
DATA RPVB1(DROP=DD1 MM1 YY1 DD2 MM2 YY2 DD3 MM3 YY3                     00480003
                DD4 MM4 YY4 DD5 MM5 YY5 DD6 MM6 YY6);                   00490003
     INFILE RPVBDATA FIRSTOBS=2;                                        00500003
     INPUT @001 RECID    1.                                             00510003
           @003 MNIACTNO 10.                                            00520003
           @014 LOANNOTE 10.                                            00530003
           @025 NAME     $UPCASE50.                                     00540003
           @076 ACCTSTA  $UPCASE1.                                      00550003
           @078 PRODTYPE $5.                                            00560003
           @084 PRSTCOND $UPCASE1.                                      00570003
           @086 REGCARD  $UPCASE1.                                      00580003
           @088 IGNTKEY  $UPCASE1.                                      00590003
           @090 REPODIST 10.                                            00600003
           @101 ACCTWOFF $UPCASE1.                                      00610003
           @103 YY1      4.                                             00620003
           @107 MM1      2.                                             00630003
           @109 DD1      2.                                             00640003
           @112 MODEREPO $UPCASE1.                                      00650003
           @114 YY2      4.                                             00660003
           @118 MM2      2.                                             00670003
           @120 DD2      2.                                             00680003
           @123 REPOPAID 10.                                            00690003
           @134 REPOSTAT $UPCASE6.                                      00700003
           @141 TKEPRICE 10.                                            00710003
           @152 MRKTVAL  10.                                            00720003
           @163 RSVPRICE 10.                                            00730003
           @174 FTHSCHLD 10.                                            00740003
           @185 YY3      4.                                             00750003
           @189 MM3      2.                                             00760003
           @191 DD3      2.                                             00770003
           @194 MODEDISP $UPCASE1.                                      00780003
           @196 APPVDISP 10.                                            00790003
           @207 YY4      4.                                             00800003
           @211 MM4      2.                                             00810003
           @213 DD4      2.                                             00820003
           @216 YY5      4.                                             00830003
           @220 MM5      2.                                             00840003
           @222 DD5      2.                                             00850003
           @225 YY6      4.                                             00860003
           @229 MM6      2.                                             00870003
           @231 DD6      2.                                             00880003
           @234 HOPRICE  10.                                            00890003
           @245 NOAUCT   $5.                                            00900003
           @251 PRIOUT   $20.                                           00910003
           ;                                                            00920003
     DATEWOFF = MDY(MM1,DD1,YY1);                                       00930003
     DATEREPO = MDY(MM2,DD2,YY2);                                       00940003
     DATE5TH  = MDY(MM3,DD3,YY3);                                       00950003
     DATEAPRV = MDY(MM4,DD4,YY4);                                       00960003
     DATESTLD = MDY(MM5,DD5,YY5);                                       00970003
     DATEHO   = MDY(MM6,DD6,YY6);                                       00980003
RUN;                                                                    00990003
                                                                        01000003
DATA RPVB2;                                                             01010003
   SET RPVB1;                                                           01020003
   IF ACCTSTA IN ('D','S','R');                                         01030003
RUN;                                                                    01040003
                                                                        01050003
DATA RPVB3;                                                             01060003
   SET RPVB2;                                                           01070003
   IF DATESTLD NE '';                                                   01080003
RUN;                                                                    01090003
                                                                        01100003
DATA REPO.REPS&REPTDT;                                                  01110003
   SET RPVB3 REPO.REPS&PREVDT;                                          01120003
RUN;                                                                    01130003
                                                                        01140003
DATA REPOWH.REPS&REPTDT;                                                01150003
   SET REPO.REPS&REPTDT;                                                01160003
RUN;                                                                    01170003
                                                                        01180003
PROC SORT NODUPKEY DATA=REPOWH.REPS&REPTDT;                             01190003
   BY MNIACTNO;                                                         01200003
RUN;                                                                    01210003
    
