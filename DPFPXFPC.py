/*****************************************************************/
/* TAKE NOTE : ANY AMENDMENTS TO PRODUCTS MAPPING, THESE FORMATS */
/*             APPLY TO BANK TRADE DATASET                       */
/* CREATE    : 28 JUNE 2007 (TGX)                                */
/*****************************************************************/

PROC FORMAT;
   INVALUE DAYR
     LOW -   30 =   0
      31 -   59 =   1
      60 -   89 =   2
      90 -  121 =   3
     122 -  151 =   4
     152 -  182 =   5
     183 -  213 =   6
     214 -  243 =   7
     244 -  273 =   8
     274 -  303 =   9
     304 -  333 =  10
     334 -  364 =  11
     365 -  394 =  12
     395 -  424 =  13
     425 -  456 =  14
     457 -  486 =  15
     487 -  516 =  16
     517 -  547 =  17
     548 -  577 =  18
     578 -  608 =  19
     609 -  638 =  20
     639 -  668 =  21
     669 -  698 =  22
     699 -  729 =  23
     730 -  HIGH=  24;

   VALUE $DIRCT
     'MDL','MLL','MOL','MOF','MCL','MCF','PEF','PCE','PCB',
     'TRX','TRL','TRC','BAX','BAL','FBP','FBD','FBC','IEF',
     'TFL','TML','TFC','TMC','TFO','TMO','TLL','TNL','TLC','TNC',
     'TNO','PBU','PBR','TLZ','TLQ','TLS','TLX','TLY','TLO',
     'BAP','BAI','BAS','BAE','PBA','FRL','TRF',
     'FAS','FAU','PFU','FDS','FDU','PFD','FCL','FTB','FTL','FTI',
     'DAS','DAU','PAU','DDS','DDU','DDT','PDU','PDT','ITB','PTB',
     'POS','PRO','BRM','BRN','PBO','PBD','PBQ','PBZ','PFT',
     'VAL','DIL','FIL','PRE','PCR','BRF','BRL','PUM',
     'TFI','TBI','TLI','TXI','BPI','BII','BSI','BEI',
     'PTR','PRU','PBI','PCP','MFL'                      = 'D'

     'IFS','IFD','IFU','IFO','ILS','ILB','ILU','ILL','SFC','SLC',
     'TFR','TLR','BFC','BLC','DLC','RFC','RLC','PLC','ALC',
     'SGL','SGC','APG','BGF','BGT','BGP',
     '190','200','BUF','BUL','BRA',
     'FSI','FUI','LSI','LUI','SLI','SCI','GTI','GPI',
     'GFI','UFI','UDI','BGG','GGI'                      = 'I'
     OTHER                                              = ' ';


   VALUE $LIAB
     'IFS','IFD','IFU','IFO','ILS','ILB','ILU','ILL',
     'ALC','TFR','BLC','DLC','RLC','PLC','BUF','UFI',
     'FSI','FUI','LSI','LSU','IUF','IUL','UDI',
     'BUL','BRA','190','200','BFC','RFC','LUI',
     'LUO','LSO','FUO','FSO'                      = '34810'
     'SFC'                                        = '34821'
     'SLC'                                        = '34822'
     'BRF','BRM','BRL','BRN','PBU','PBR','PUM','PFT',
     'PCR','PEF','PCE','PCP','FRL','TRF','PRU',
     'FTL','FTI'                                  = '34480'
     'TFL','TML','TFC','TMC','TFO','TMO','TLL',
     'TNL','TLC','TNC','TLO','TNO',
     'TLF',
     'TLZ','TLQ','TLS','TLX','TLY','TLW','TLV',
     'TFI','TBI','TLI','TXI','PTR'                = '34440'
     'FAS','FAU','FDS','FDU','FCL','FTB','FFS',
     'FFU','FCS','FCU','FFL'                      = '34422'
     'DAS','DAU','DDS','DDU','DDT','ITB','PAU',
     'PDU','PDT','PTB','PFU','PFD'                = '34421'
     'BAP','BAI','BAS','BAE','PBA','PBO','PBZ',
     'PBD','PBQ','BPI','BII','BSI','BEI','PBI'    = '34470'
     'MDL','MOL','MLL','MFL',
     'VAL','DIL','FIL','PRE','MOF'                = '34411'
     'POS','PRO'                                  = '34412'
     'SGL','SGC',
     'ISL','ISC','SLI','SCI'                      = '34850'
     'BGT','BGP',
     'GTI','GPI'                                  = '34840'
     'BGG','GGI'                                  = '34831'
     'BGF','APG','GFI'                            = '34832'
     'MCF','IEF'                                  = '34490'
     OTHER                                        = '99999'
     ;


   VALUE $BTFCEPT
     'TBI','TXI'                                  = '12'
     'BSI'                                        = '19'
     'FSI','FUI','LSI','LUI','UFI','UDI',
     'LUO','LSO','FUO','FSO'                      = '22'
     'SLI','SCI','GFI','GPI','GTI','GGI'          = '23'
     'BPI'                                        = '35'
     'TFI','TLI','PRU'                            = '36'
     'BII','BEI','PBI'                            = '49'
     OTHER                                        = '99'
     ;

   VALUE $PRCTYPE /* TYPE OF PRICING */
     'FSI','FUI','LSI','LUI','FSO','FUO','LSO','LUO',
     'UFI','UDI','GTI','GPI','GFI','SLI','SCI','ALC',
     'IFD','IFO','ILS','ILU','IFS','IFU','ILB','ILL',
     'SFC','SLC','BFC','BLC','DLC','RFC','RLC','FAS',
     'FAU','FDS','FDU','FCL','FFS','FFU','FCS','FCU',
     'FFL','FTB','BUF','BUL','BGT','BGP','BGF','APG',
     'SGL','SGC','BGG','GGI'                          = '00'
     'TFI','TLI','TFL','TLL','TLZ','DAS',
     'DAU','DDS','DDU','ITB','BRF','BRL','PBU','PDU',
     'PRE','PRO','PBA','PBZ'                          = '41'
     'BPI','BSI','BII','BEI','BAS','BAE','BAP','BAI'  = '53'
     'FTI','FTL'                                      = '68'
     'PBI','PRU','VAL','DIL','FIL','POS'              = '79'
     OTHER                                            = '99'
     ;

   VALUE $PRCTYPESFS /* TYPE OF PRICING - SFS */
     'FSI','FUI','LSI','LUI','FSO','FUO','LSO','LUO',
     'UFI','UDI','GTI','GPI','GFI','SLI','SCI','ALC',
     'IFD','IFO','ILS','ILU','IFS','IFU','ILB','ILL',
     'SFC','SLC','BFC','BLC','DLC','RFC','RLC','FAS',
     'FAU','FDS','FDU','FCL','FFS','FFU','FCS','FCU',
     'FFL','FTB','BUF','BUL','BGT','BGP','BGF','APG',
     'SGL','SGC','BGG','GGI'                          = '00'
     'DAS','DAU','DDS','DDU','ITB','PBU','PDU','PRE',
     'PRO'                                            = '41'
     'FTI','FTL'                                      = '68'
     'PBI','PRU','VAL','DIL','FIL','POS'              = '79'
     'TFI','TLI','BPI','BSI','BII','BEI','TFL','TLL',
     'TLZ','BRF','BRL','PBA','PBZ','BAS','BAE','BAP',
     'BAI'                                            = '59'
     OTHER                                            = '99'
     ;

   VALUE $NSRSLIAB
     'TFL','TML','TFC','TMC','TFO','TMO','TLL',
     'TNL','TLC','TNC','TLO','TNO','TLF','TLQ',
     'TLS','TLV','TLW','TLX','TLY','TLZ','TRF',
     'TFI','TBI','TLI','TXI'                      = '34440'
     'BAE','BEI'                                  = '34473'
     'BAI','BII'                                  = '34474'
     'BAP','BAS','PBA','PBO','PBD','PBQ','PBZ',
     'BPI','BSI','PBI'                            = '34476'
     'FAS','FAU','FDS','FDU','FCL','FTB','DAS',
     'DAU','DDS','DDU','DDT','ITB','IEF','MDL',
     'MOL','MLL','MFL','MOF','MCF','PDU','PCE',
     'PCP','PEF','PFD','PFU','PTB','FFS','FFU',
     'FCS','FCU','FFL'                            = '34479'
     'BRF','BRL','BRM','BRN','PCR','PBU','PBR',
     'PUM','FTI','FTL','PRU'                      = '34480'
     'VAL','DIL','FIL','POS','PRO','PRE'          = '34530'
     'IFS','IFD','IFU','IFO','ILS','ILB','ILU',
     'ILL','ALC','BFC','BLC','DLC','RFC','RLC',
     'PLC','BUF','BRA','BUL','FSI','FUI','LSI',
     'LUI','UFI','UDI','LUO','LSO','FUO','FSO'    = '34810'
     'SFC'                                        = '34821'
     'SLC'                                        = '34822'
     'BGG','GGI'                                  = '34831'
     'BGF','GFI','APG'                            = '34832'
     'BGT','BGP','GPI','GTI'                      = '34840'
     'SGL','SGC','SLI','SCI'                      = '34850'
     '190','200'                                  = '34899'
     OTHER                                        = '99999'
     ;
RUN;


