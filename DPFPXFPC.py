             
                # Print page header if first branch in category
                if first_branch_in_category:
                    pagecnt += 1
                    f.write(f"PROGRAM-ID : EIMAR201                     P U B L I C   I S L A M I C   B A N K   B E R H A D                        PAGE NO.: {pagecnt}\n")
                    cat_type = branch_group['TYPE'].iloc[0] if len(branch_group) > 0 else '          '
                    f.write(f"                                   OUTSTANDING LOANS IN ARREARS ISSUED FROM 01 JAN 1998  {cat_type}       {rdate}\n")
                    f.write("\n")
                    # Column headers with 2-3 spaces between columns
                    f.write("BRH     NO          < 1 MTH       NO     1 TO < 2 MTH       NO     2 TO < 3 MTH        NO      3 TO < 4 MTH        NO      4 TO < 5 MTH\n")
                    f.write("        NO     5 TO < 6 MTH       NO     6 TO < 7 MTH       NO     7 TO < 8 MTH        NO      8 TO < 9 MTH        NO     9 TO < 10 MTH\n")
                    f.write("        NO   10 TO < 11 MTH       NO   11 TO < 12 MTH       NO   12 TO < 18 MTH        NO    18 TO < 24 MTH        NO    24 TO < 36 MTH\n")
                    f.write("        NO         > 36 MTH       NO          DEFICIT       NO   SUBTOTAL >=3MTH       NO   SUBTOTAL >=6MTH        NO             TOTAL\n")
                    f.write("-" * 134 + "\n")
                    first_branch_in_category = False
                
                # Get BRHCODE
                brhcode = branch_group['BRHCODE'].iloc[0] if len(branch_group) > 0 else '   '
                
                # Line 1: Branch number + columns 1-5
                f.write(format_line1(branch, [noacc[0], brhamt[0], noacc[1], brhamt[1], noacc[2], brhamt[2], noacc[3], brhamt[3], noacc[4], brhamt[4]]) + "\n")
                
                # Line 2: BRHCODE + columns 6-10
                f.write(format_line2(brhcode, [noacc[5], brhamt[5], noacc[6], brhamt[6], noacc[7], brhamt[7], noacc[8], brhamt[8], noacc[9], brhamt[9]]) + "\n")
                
                # Line 3: Columns 11-15
                f.write(format_line3([noacc[10], brhamt[10], noacc[11], brhamt[11], noacc[12], brhamt[12], noacc[13], brhamt[13], noacc[14], brhamt[14]]) + "\n")
                
                # Line 4: Columns 16-17 + subtotals
                f.write(format_line4([noacc[15], brhamt[15], noacc[16], brhamt[16], subacc, subbrh, subac2, subbr2, sotacc, totbrh]) + "\n")
            
            # Calculate grand totals for category
            sgtotbrh = np.sum(totamt[3:])
            sgtotbr2 = sgtotbrh - totamt[3] - totamt[4] - totamt[5]
            sgtotacc = np.sum(totacc[3:])
            sgtotac2 = sgtotacc - totacc[3] - totacc[4] - totacc[5]
            gtotbrh = sgtotbrh + totamt[0] + totamt[1] + totamt[2]
            gtotacc = sgtotacc + totacc[0] + totacc[1] + totacc[2]
            
            # Print category totals
            f.write("-" * 134 + "\n")
            f.write(format_line1("TOT", [totacc[0], totamt[0], totacc[1], totamt[1], totacc[2], totamt[2], totacc[3], totamt[3], totacc[4], totamt[4]]) + "\n")
            f.write(format_line2("", [totacc[5], totamt[5], totacc[6], totamt[6], totacc[7], totamt[7], totacc[8], totamt[8], totacc[9], totamt[9]]) + "\n")
            f.write(format_line3([totacc[10], totamt[10], totacc[11], totamt[11], totacc[12], totamt[12], totacc[13], totamt[13], totacc[14], totamt[14]]) + "\n")
            f.write(format_line4([totacc[15], totamt[15], totacc[16], totamt[16], sgtotacc, sgtotbrh, sgtotac2, sgtotbr2, gtotacc, gtotbrh]) + "\n")
            f.write("-" * 134 + "\n")
            f.write("\n")



at this point just follow above spacing and alignment
