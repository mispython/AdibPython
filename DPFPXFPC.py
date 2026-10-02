*   PRODUCT:   SAS
365   *   VERSION:   9.4
366   *   CREATOR:   External File Interface
367   *   DATE:      02OCT26
368   *   DESC:      Generated SAS Datastep Code
369   *   TEMPLATE SOURCE:  (None Specified.)
370   ***********************************************************************/
371      data _null_;
372      %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
373      %let _EFIREC_ = 0;     /* clear export record count macro variable */
374      file '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/l124094.txt' delimiter=';' DSD DROPOVER
374! lrecl=32767;
375      if _n_ = 1 then        /* write column names or labels */
376       do;
377         put
378            "acctno"
379         ';'
380            "noteno"
381         ';'
382            "eir_adj"
383         ';'
384            "sector"
385         ';'
386            "name"
387         ';'
388            "commno"
389         ';'
390            "custcd"
391         ';'
392            "origmt"
393         ';'
394            "prodcd"
395         ';'
396            "riskcd"
397         ';'
398            "collcd"
399         ';'
400            "origmx"
401         ';'
402            "remainmx"
403         ';'
404            "sectorcd"
405         ';'
406            "statecd"
407         ';'
408            "riskrte"
409         ';'
410            "amtind"
411         ';'
412            "cgcref"
413         ';'
414            "custcode"
415         ';'
416            "product"
417         ';'
418            "branch"
419         ';'
420            "assmdate"
421         ';'
422            "intamt"
423         ';'
424            "appvalue"
425         ';'
426            "noteterm"
427         ';'
428            "colldesc"
429         ';'
430            "flag3"
431         ';'
432            "costctr"
433         ';'
434            "census"
435         ';'
436            "billtype"
437         ';'
438            "netproc"
439         ';'
440            "intrate"
441         ';'
442            "spread"
443         ';'
444            "ntint"
445         ';'
446            "rebate"
447         ';'
448            "intearn"
449         ';'
450            "accrual"
451         ';'
452            "secure"
453         ';'
454            "liabcode"
455         ';'
456            "loanstat"
457         ';'
458            "borstat"
459         ';'
460            "marketvl"
461         ';'
462            "earnterm"
463         ';'
464            "usuryidx"
465         ';'
466            "intearn2"
467         ';'
468            "intearn3"
469         ';'
470            "intearn4"
471         ';'
472            "payamt"
473         ';'
474            "payfreq"
475         ';'
476            "biltot"
477         ';'
478            "bilpay"
479         ';'
480            "paytype"
481         ';'
482            "feeamt4"
483         ';'
484            "apprdate"
485         ';'
486            "totpdeop"
487         ';'
488            "intpdytd"
489         ';'
490            "accruytd"
491         ';'
492            "accrueop"
493         ';'
494            "ntindex"
495         ';'
496            "rebatei"
497         ';'
498            "acctyind"
499         ';'
500            "hstprin"
501         ';'
502            "costfund"
503         ';'
504            "restbalc"
505         ';'
506            "nxbildt"
507         ';'
508            "payind"
509         ';'
510            "feeamt"
511         ';'
512            "nxtbil"
513         ';'
514            "bldate"
515         ';'
516            "sectold"
517         ';'
518            "exprdate"
519         ';'
520            "closedte"
521         ';'
522            "issdte"
523         ';'
524            "sectorma"
525         ';'
526            "fisspurp"
527         ';'
528            "newsec"
529         ';'
530            "census4"
531         ';'
532            "custidno"
533         ';'
534            "rleasamt"
535         ';'
536            "apprlimt"
537         ';'
538            "undrawn"
539         ';'
540            "apprlmtacct"
541         ';'
542            "odplan"
543         ';'
544            "rate1"
545         ';'
546            "rate2"
547         ';'
548            "todrate"
549         ';'
550            "flatrate"
551         ';'
552            "baserate"
553         ';'
554            "odstat"
555         ';'
556            "orgcode"
557         ';'
558            "limit1"
559         ';'
560            "limit2"
561         ';'
562            "col1"
563         ';'
564            "col2"
565         ';'
566            "avgamt"
567         ';'
568            "odintacc"
569         ';'
570            "censust"
571         ';'
572            "acctype"
573         ';'
574            "ccricode"
575         ';'
576            "crispurp"
577         ';'
578            "sectorz"
579         ';'
580            "sectorzz"
581         ';'
582            "secvalid"
583         ';'
584            "fisspori"
585         ';'
586            "sectpori"
587         ';'
588            "seccust"
589         ';'
590            "newicind"
591         ';'
592            "bussind"
593         ';'
594            "custori"
595         ';'
596            "balmnim"
597         ';'
598            "curcode"
599         ';'
600            "bal_aft_eir"
601         ';'
602            "eirind"
603         ';'
604            "remmth"
605         ';'
606            "ccy"
607         ';'
608            "forate"
609         ';'
610            "fcybal"
611         ';'
612            "appr2fcy"
613         ';'
614            "cjfee"
615         ';'
616            "cfindex"
617         ';'
618            "write_down_bal"
619         ';'
620            "oribal_aft_eir"
621         ';'
622            "oribalance"
623         ';'
624            "oribalmni"
625         ';'
626            "oldnotebldate"
627         ';'
628            "oldnotedayarr"
629         ';'
630            "dnbfisme"
631         ';'
632            "dnbfi_ori"
633         ';'
634            "vb"
635         ';'
636            "freleas"
637         ';'
638            "escracct"
639         ';'
640            "siacctno"
641         ';'
642            "abm_hl"
643         ';'
644            "ia_lru"
645         ';'
646            "ascore_perm"
647         ';'
648            "ascore_ltst"
649         ';'
650            "unearned1"
651         ';'
652            "unearned2"
653         ';'
654            "unearned"
655         ';'
656            "fdcertno"
657         ';'
658            "fdacctno"
659         ';'
660            "ccris_instlamt"
661         ';'
662            "industrial_sector_cd"
663         ';'
664            "sectorcd_ori"
665         ';'
666            "invalid_loc"
667         ';'
668            "state"
669         ';'
670            "ascore_comm"
671         ';'
672            "apprlim2ori"
673         ';'
674            "paidind"
675         ';'
676            "remainmt"
677         ';'
678            "remainmh"
679         ';'
680            "curbal"
681         ';'
682            "balance"
683         ';'
684            "balmni"
685         ';'
686            "apprlim2"
687         ;
688       end;
689     set  WORK.L124094   end=EFIEOD;
690         format acctno best12. ;
691         format noteno best12. ;
692         format eir_adj best12. ;
693         format sector $8. ;
694         format name $8. ;
695         format commno best12. ;
696         format custcd $8. ;
697         format origmt $8. ;
698         format prodcd $8. ;
699         format riskcd $8. ;
700         format collcd $8. ;
701         format origmx $8. ;
702         format remainmx $8. ;
703         format sectorcd $8. ;
704         format statecd $8. ;
705         format riskrte best12. ;
706         format amtind $8. ;
707         format cgcref $8. ;
708         format custcode best12. ;
709         format product best12. ;
710         format branch best12. ;
711         format assmdate best12. ;
712         format intamt best12. ;
713         format appvalue best12. ;
714         format noteterm best12. ;
715         format colldesc $8. ;
716         format flag3 $8. ;
717         format costctr best12. ;
718         format census best12. ;
719         format billtype $8. ;
720         format netproc best12. ;
721         format intrate best12. ;
722         format spread best12. ;
723         format ntint $8. ;
724         format rebate best12. ;
725         format intearn best12. ;
726         format accrual best12. ;
727         format secure $8. ;
728         format liabcode $8. ;
729         format loanstat best12. ;
730         format borstat $8. ;
731         format marketvl best12. ;
732         format earnterm best12. ;
733         format usuryidx best12. ;
734         format intearn2 best12. ;
735         format intearn3 best12. ;
736         format intearn4 best12. ;
737         format payamt best12. ;
738         format payfreq $8. ;
739         format biltot best12. ;
740         format bilpay best12. ;
741         format paytype $8. ;
742         format feeamt4 best12. ;
743         format apprdate best12. ;
744         format totpdeop best12. ;
745         format intpdytd best12. ;
746         format accruytd best12. ;
747         format accrueop best12. ;
748         format ntindex best12. ;
749         format rebatei best12. ;
750         format acctyind best12. ;
751         format hstprin best12. ;
752         format costfund best12. ;
753         format restbalc best12. ;
754         format nxbildt best12. ;
755         format payind $8. ;
756         format feeamt best12. ;
757         format nxtbil best12. ;
758         format bldate best12. ;
759         format sectold $8. ;
760         format exprdate best12. ;
761         format closedte best12. ;
762         format issdte best12. ;
763         format sectorma $8. ;
764         format fisspurp $8. ;
765         format newsec $8. ;
766         format census4 $8. ;
767         format custidno $8. ;
768         format rleasamt best12. ;
769         format apprlimt best12. ;
770         format undrawn best12. ;
771         format apprlmtacct best12. ;
772         format odplan best12. ;
773         format rate1 best12. ;
774         format rate2 best12. ;
775         format todrate best12. ;
776         format flatrate best12. ;
777         format baserate best12. ;
778         format odstat $8. ;
779         format orgcode $8. ;
780         format limit1 best12. ;
781         format limit2 best12. ;
782         format col1 $8. ;
783         format col2 $8. ;
784         format avgamt best12. ;
785         format odintacc best12. ;
786         format censust best12. ;
787         format acctype $8. ;
788         format ccricode best12. ;
789         format crispurp $8. ;
790         format sectorz $8. ;
791         format sectorzz $8. ;
792         format secvalid $8. ;
793         format fisspori $8. ;
794         format sectpori $8. ;
795         format seccust $8. ;
796         format newicind $8. ;
797         format bussind $8. ;
798         format custori $8. ;
799         format balmnim best12. ;
800         format curcode $8. ;
801         format bal_aft_eir best12. ;
802         format eirind best12. ;
803         format remmth best12. ;
804         format ccy $8. ;
805         format forate $8. ;
806         format fcybal best12. ;
807         format appr2fcy best12. ;
808         format cjfee best12. ;
809         format cfindex best12. ;
810         format write_down_bal best12. ;
811         format oribal_aft_eir best12. ;
812         format oribalance best12. ;
813         format oribalmni best12. ;
814         format oldnotebldate best12. ;
815         format oldnotedayarr best12. ;
816         format dnbfisme $8. ;
817         format dnbfi_ori $8. ;
818         format vb $8. ;
819         format freleas best12. ;
820         format escracct best12. ;
821         format siacctno best12. ;
822         format abm_hl $8. ;
823         format ia_lru $8. ;
824         format ascore_perm $8. ;
825         format ascore_ltst $8. ;
826         format unearned1 best12. ;
827         format unearned2 best12. ;
828         format unearned best12. ;
829         format fdcertno best12. ;
830         format fdacctno best12. ;
831         format ccris_instlamt best12. ;
832         format industrial_sector_cd $8. ;
833         format sectorcd_ori $8. ;
834         format invalid_loc $8. ;
835         format state $8. ;
836         format ascore_comm $8. ;
837         format apprlim2ori best12. ;
838         format paidind $8. ;
839         format remainmt $8. ;
840         format remainmh best12. ;
841         format curbal best12. ;
842         format balance best12. ;
843         format balmni best12. ;
844         format apprlim2 best12. ;
845       do;
846         EFIOUT + 1;
847         put acctno @;
848         put noteno @;
849         put eir_adj @;
850         put sector $ @;
851         put name $ @;
852         put commno @;
853         put custcd $ @;
854         put origmt $ @;
855         put prodcd $ @;
856         put riskcd $ @;
857         put collcd $ @;
858         put origmx $ @;
859         put remainmx $ @;
860         put sectorcd $ @;
861         put statecd $ @;
862         put riskrte @;
863         put amtind $ @;
864         put cgcref $ @;
865         put custcode @;
866         put product @;
867         put branch @;
868         put assmdate @;
869         put intamt @;
870         put appvalue @;
871         put noteterm @;
872         put colldesc $ @;
873         put flag3 $ @;
874         put costctr @;
875         put census @;
876         put billtype $ @;
877         put netproc @;
878         put intrate @;
879         put spread @;
880         put ntint $ @;
881         put rebate @;
882         put intearn @;
883         put accrual @;
884         put secure $ @;
885         put liabcode $ @;
886         put loanstat @;
887         put borstat $ @;
888         put marketvl @;
889         put earnterm @;
890         put usuryidx @;
891         put intearn2 @;
892         put intearn3 @;
893         put intearn4 @;
894         put payamt @;
895         put payfreq $ @;
896         put biltot @;
897         put bilpay @;
898         put paytype $ @;
899         put feeamt4 @;
900         put apprdate @;
901         put totpdeop @;
902         put intpdytd @;
903         put accruytd @;
904         put accrueop @;
905         put ntindex @;
906         put rebatei @;
907         put acctyind @;
908         put hstprin @;
909         put costfund @;
910         put restbalc @;
911         put nxbildt @;
912         put payind $ @;
913         put feeamt @;
914         put nxtbil @;
915         put bldate @;
916         put sectold $ @;
917         put exprdate @;
918         put closedte @;
919         put issdte @;
920         put sectorma $ @;
921         put fisspurp $ @;
922         put newsec $ @;
923         put census4 $ @;
924         put custidno $ @;
925         put rleasamt @;
926         put apprlimt @;
927         put undrawn @;
928         put apprlmtacct @;
929         put odplan @;
930         put rate1 @;
931         put rate2 @;
932         put todrate @;
933         put flatrate @;
934         put baserate @;
935         put odstat $ @;
936         put orgcode $ @;
937         put limit1 @;
938         put limit2 @;
939         put col1 $ @;
940         put col2 $ @;
941         put avgamt @;
942         put odintacc @;
943         put censust @;
944         put acctype $ @;
945         put ccricode @;
946         put crispurp $ @;
947         put sectorz $ @;
948         put sectorzz $ @;
949         put secvalid $ @;
950         put fisspori $ @;
951         put sectpori $ @;
952         put seccust $ @;
953         put newicind $ @;
954         put bussind $ @;
955         put custori $ @;
956         put balmnim @;
957         put curcode $ @;
958         put bal_aft_eir @;
959         put eirind @;
960         put remmth @;
961         put ccy $ @;
962         put forate $ @;
963         put fcybal @;
964         put appr2fcy @;
965         put cjfee @;
966         put cfindex @;
967         put write_down_bal @;
968         put oribal_aft_eir @;
969         put oribalance @;
970         put oribalmni @;
971         put oldnotebldate @;
972         put oldnotedayarr @;
973         put dnbfisme $ @;
974         put dnbfi_ori $ @;
975         put vb $ @;
976         put freleas @;
977         put escracct @;
978         put siacctno @;
979         put abm_hl $ @;
980         put ia_lru $ @;
981         put ascore_perm $ @;
982         put ascore_ltst $ @;
983         put unearned1 @;
984         put unearned2 @;
985         put unearned @;
986         put fdcertno @;
987         put fdacctno @;
988         put ccris_instlamt @;
989         put industrial_sector_cd $ @;
990         put sectorcd_ori $ @;
991         put invalid_loc $ @;
992         put state $ @;
993         put ascore_comm $ @;
994         put apprlim2ori @;
995         put paidind $ @;
996         put remainmt $ @;
997         put remainmh @;
998         put curbal @;
999         put balance @;
1000         put balmni @;
1001         put apprlim2 ;
1002         ;
1003       end;
1004      if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
1005      if EFIEOD then call symputx('_EFIREC_',EFIOUT);
1006      run;
NOTE: The file '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/l124094.txt' is:
      Filename=/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/l124094.txt,
      Owner Name=sas_edw_dev,
      Group Name=sas_edw_dev_grp,
      Access Permission=-rw-rw-r--,
      Last Modified=02Oct2026:11:30:02

NOTE: 1 record was written to the file '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/l124094.txt'.
      The minimum record length was 1310.
      The maximum record length was 1310.
NOTE: There were 0 observations read from the data set WORK.L124094.
NOTE: DATA statement used (Total process time):
      real time           0.00 seconds
      cpu time            0.01 seconds
      
0 records created in /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/l124094.txt from WORK.L124094.
  
  
NOTE: "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/l124094.txt" file was successfully created.
NOTE: PROCEDURE EXPORT used (Total process time):
      real time           0.11 seconds
      cpu time            0.03 seconds
      
1007  
1008  
1009  ods html5 (id=saspy_internal) close;ods listing;

SAS Connection terminated. Subprocess id was 3433409
L124 written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/l124094.sas7bdat  (0 rows)
L124PBBD: reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1/uloan094.sas7bdat ...
WARNING: 'ul124094' has 0 rows — writing empty dataset.
SAS Connection established. Subprocess id is 3433466

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1839: UserWarning: Note that Indexes are not transferred over as columns. Only actual columns are transferred
  warnings.warn("Note that Indexes are not transferred over as columns. Only actual columns are transferred")
=== SAS log: LIBNAME+DATA -> ul124094.sas7bdat ===

124  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
124! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
125  
126  
127              LIBNAME _outlib "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm";
NOTE: Libref _OUTLIB was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm
128              DATA _outlib.ul124094;
129                  SET WORK.ul124094;
130              RUN;
NOTE: There were 0 observations read from the data set WORK.UL124094.
NOTE: The data set _OUTLIB.UL124094 has 0 observations and 28 variables.
NOTE: DATA statement used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
131              LIBNAME _outlib CLEAR;
NOTE: Libref _OUTLIB has been deassigned.
132  
133  
134  ods html5 (id=saspy_internal) close;ods listing;

=== SAS log: PROC EXPORT txt -> ul124094.txt ===

136  ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
136! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
137  
138  
139              PROC EXPORT DATA=WORK.ul124094
140                  OUTFILE="/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/ul124094.txt"
141                  DBMS=DLM REPLACE;
142                  DELIMITER=';';
143              RUN;
NOTE: Unable to open parameter catalog: SASUSER.PARMS.PARMS.SLIST in update mode. Temporary parameter values will be saved to 
WORK.PARMS.PARMS.SLIST.
NOTE: No observations in data set WORK.UL124094.
NOTE: Data set has 0 observations.
NOTE: Unable to open SASUSER.PROFILE. WORK.PROFILE will be opened instead.
NOTE: All profile changes will be lost at the end of the session.
144   /**********************************************************************
145   *   PRODUCT:   SAS
146   *   VERSION:   9.4
147   *   CREATOR:   External File Interface
148   *   DATE:      02OCT26
149   *   DESC:      Generated SAS Datastep Code
150   *   TEMPLATE SOURCE:  (None Specified.)
151   ***********************************************************************/
152      data _null_;
153      %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
154      %let _EFIREC_ = 0;     /* clear export record count macro variable */
155      file '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/ul124094.txt' delimiter=';' DSD DROPOVER
155! lrecl=32767;
156      if _n_ = 1 then        /* write column names or labels */
157       do;
158         put
159            "name"
160         ';'
161            "amtind"
162         ';'
163            "acctno"
164         ';'
165            "commno"
166         ';'
167            "custcode"
168         ';'
169            "crispurp"
170         ';'
171            "branch"
172         ';'
173            "custcd"
174         ';'
175            "product"
176         ';'
177            "fisspurp"
178         ';'
179            "prodcd"
180         ';'
181            "origmt"
182         ';'
183            "exprdate"
184         ';'
185            "apprdate"
186         ';'
187            "rleasamt"
188         ';'
189            "apprlimt"
190         ';'
191            "undrawn"
192         ';'
193            "curcode"
194         ';'
195            "ccricode"
196         ';'
197            "industrial_sector_cd"
198         ';'
199            "acctype"
200         ';'
201            "sectfiss"
202         ';'
203            "issdte"
204         ';'
205            "fisspori"
206         ';'
207            "sectold"
208         ';'
209            "sectorcd_ori"
210         ';'
211            "costctr"
212         ';'
213            "sector"
214         ;
215       end;
216     set  WORK.UL124094   end=EFIEOD;
217         format name $8. ;
218         format amtind $8. ;
219         format acctno best12. ;
220         format commno best12. ;
221         format custcode best12. ;
222         format crispurp $8. ;
223         format branch best12. ;
224         format custcd $8. ;
225         format product best12. ;
226         format fisspurp $8. ;
227         format prodcd $8. ;
228         format origmt $8. ;
229         format exprdate best12. ;
230         format apprdate best12. ;
231         format rleasamt best12. ;
232         format apprlimt best12. ;
233         format undrawn best12. ;
234         format curcode $8. ;
235         format ccricode best12. ;
236         format industrial_sector_cd $8. ;
237         format acctype $8. ;
238         format sectfiss $8. ;
239         format issdte best12. ;
240         format fisspori $8. ;
241         format sectold $8. ;
242         format sectorcd_ori $8. ;
243         format costctr best12. ;
244         format sector $8. ;
245       do;
246         EFIOUT + 1;
247         put name $ @;
248         put amtind $ @;
249         put acctno @;
250         put commno @;
251         put custcode @;
252         put crispurp $ @;
253         put branch @;
254         put custcd $ @;
255         put product @;
256         put fisspurp $ @;
257         put prodcd $ @;
258         put origmt $ @;
259         put exprdate @;
260         put apprdate @;
261         put rleasamt @;
262         put apprlimt @;
263         put undrawn @;
264         put curcode $ @;
265         put ccricode @;
266         put industrial_sector_cd $ @;
267         put acctype $ @;
268         put sectfiss $ @;
269         put issdte @;
270         put fisspori $ @;
271         put sectold $ @;
272         put sectorcd_ori $ @;
273         put costctr @;
274         put sector $ ;
275         ;
276       end;
277      if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
278      if EFIEOD then call symputx('_EFIREC_',EFIOUT);
279      run;
NOTE: The file '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/ul124094.txt' is:
      Filename=/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/ul124094.txt,
      Owner Name=sas_edw_dev,
      Group Name=sas_edw_dev_grp,
      Access Permission=-rw-rw-r--,
      Last Modified=02Oct2026:11:30:05

NOTE: 1 record was written to the file '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/ul124094.txt'.
      The minimum record length was 239.
      The maximum record length was 239.
NOTE: There were 0 observations read from the data set WORK.UL124094.
NOTE: DATA statement used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
0 records created in /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/ul124094.txt from WORK.UL124094.
  
  
NOTE: "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/ul124094.txt" file was successfully created.
NOTE: PROCEDURE EXPORT used (Total process time):
      real time           0.03 seconds
      cpu time            0.03 seconds
      
280  
281  
282  ods html5 (id=saspy_internal) close;ods listing;

SAS Connection terminated. Subprocess id was 3433466
UL124 written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/ul124094.sas7bdat  (0 rows)
WARNING: 'lalw094' has 0 rows — writing empty dataset.
SAS Connection established. Subprocess id is 3433551

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS Connection terminated. Subprocess id was 3433551
LALW written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/lalw094.sas7bdat  (0 rows)
SAS Connection established. Subprocess id is 3433603

=== SAS log: LIBNAME+DATA -> alw094.sas7bdat ===

73   ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
73 ! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
74   
75   
76               LIBNAME _outlib "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm";
NOTE: Libref _OUTLIB was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm
77               DATA _outlib.alw094;
78                   SET WORK.alw094;
79               RUN;
NOTE: There were 302 observations read from the data set WORK.ALW094.
NOTE: The data set _OUTLIB.ALW094 has 302 observations and 3 variables.
NOTE: DATA statement used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
80               LIBNAME _outlib CLEAR;
NOTE: Libref _OUTLIB has been deassigned.
81   
82   
83   ods html5 (id=saspy_internal) close;ods listing;

=== SAS log: PROC EXPORT txt -> alw094.txt ===

85   ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
85 ! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
86   
87   
88               PROC EXPORT DATA=WORK.alw094
89                   OUTFILE="/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/alw094.txt"
90                   DBMS=DLM REPLACE;
91                   DELIMITER=';';
92               RUN;
NOTE: Unable to open parameter catalog: SASUSER.PARMS.PARMS.SLIST in update mode. Temporary parameter values will be saved to 
WORK.PARMS.PARMS.SLIST.
NOTE: Unable to open SASUSER.PROFILE. WORK.PROFILE will be opened instead.
NOTE: All profile changes will be lost at the end of the session.
93    /**********************************************************************
94    *   PRODUCT:   SAS
95    *   VERSION:   9.4
96    *   CREATOR:   External File Interface
97    *   DATE:      02OCT26
98    *   DESC:      Generated SAS Datastep Code
99    *   TEMPLATE SOURCE:  (None Specified.)
100   ***********************************************************************/
101      data _null_;
102      %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
103      %let _EFIREC_ = 0;     /* clear export record count macro variable */
104      file '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/alw094.txt' delimiter=';' DSD DROPOVER
104! lrecl=32767;
105      if _n_ = 1 then        /* write column names or labels */
106       do;
107         put
108            "itcode"
109         ';'
110            "amtind"
111         ';'
112            "amount"
113         ;
114       end;
115     set  WORK.ALW094   end=EFIEOD;
116         format itcode $14. ;
117         format amtind $1. ;
118         format amount best12. ;
119       do;
120         EFIOUT + 1;
121         put itcode $ @;
122         put amtind $ @;
123         put amount ;
124         ;
125       end;
126      if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
127      if EFIEOD then call symputx('_EFIREC_',EFIOUT);
128      run;
NOTE: The file '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/alw094.txt' is:
      Filename=/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/alw094.txt,
      Owner Name=sas_edw_dev,
      Group Name=sas_edw_dev_grp,
      Access Permission=-rw-rw-r--,
      Last Modified=02Oct2026:11:30:10

NOTE: 303 records were written to the file '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/alw094.txt'.
      The minimum record length was 17.
      The maximum record length was 29.
NOTE: There were 302 observations read from the data set WORK.ALW094.
NOTE: DATA statement used (Total process time):
      real time           0.00 seconds
      cpu time            0.00 seconds
      
302 records created in /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/alw094.txt from WORK.ALW094.
  
  
NOTE: "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/alw094.txt" file was successfully created.
NOTE: PROCEDURE EXPORT used (Total process time):
      real time           0.09 seconds
      cpu time            0.01 seconds
      
129  
130  
131  ods html5 (id=saspy_internal) close;ods listing;

SAS Connection terminated. Subprocess id was 3433603
ALW copied from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnmx/alw094.sas7bdat to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/alw094.sas7bdat (302 rows)
P124RDAL DEBUG: REPTMON='09' NOWK='4' sfx='094'
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWP124.py", line 181, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWP124.py", line 177, in main
    run_p124rdal()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/P124RDAL.py", line 222, in main
    import PBBMRDLF  # noqa: F401
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/PBBMRDLF.py", line 104, in <module>
    build()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/PBBMRDLF.py", line 68, in build
    pyreadstat.write_sas7bdat(
AttributeError: module 'pyreadstat' has no attribute 'write_sas7bdat'
