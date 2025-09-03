import re
import os

def extract_length(format_spec):
    """
    Extracts the length from a format string.
    Supports:
    - Numeric formats like "8.2", "11." → returns 8 or 11
    - Range formats like "19-23" → returns 5
    """
    if not format_spec:
        return 0
    
    # Remove any trailing period
    format_spec = format_spec.rstrip('.')

    # Case 1: Range format (e.g., "19-23")
    range_match = re.search(r'(\d+)-(\d+)', format_spec)
    if range_match:
        start = int(range_match.group(1))
        end = int(range_match.group(2))
        return end - start + 1

    # Case 2: Numeric format with optional decimal (e.g., "8.2", "11.")
    length_match = re.search(r'(\d+)(?:\.(\d+))?$', format_spec)
    if length_match:
        return int(length_match.group(1))

    # Case 3: Unknown format
    print(f"Warning: Could not extract length from format: {format_spec}")
    return 0

def infer_binary_format(fmt):
    """Handle binary formats like $EBCDIC and S370FPD packed decimals."""
    if fmt.startswith("$EBCDIC"):
        return "ebcdic", "default", 0

    if fmt.startswith("S370FPD"):
        match = re.search(r'S370FPD(\d+)(?:\.(\d+))?', fmt)
        decimal = int(match.group(2)) if match and match.group(2) else 0
        return "packed", "s370fpd", decimal
    
    return None
    
def infer_text_format(fmt):
    """Handle ASCII-based text format specifiers like $UPCASE, $CHAR, etc."""
    if  fmt.startswith("$EBCDIC"):
        return None
    
    if fmt.startswith("$UPCASE"):
        return "ascii", "upcase", 0
    elif fmt.startswith("$CHAR"):
        return "ascii", "char", 0
    elif fmt.startswith("$"):
        return "ascii", "default", 0
    else:
        return None

def infer_date_format(fmt):
    if fmt.startswith("DATE") or "YYMMDD" in fmt:
        return "date", "yymmdd", 0
    
def infer_numeric_format(fmt):
    """Handle simple numeric formats like 11. or 8.2"""
    if re.fullmatch(r"\d+\.\d+", fmt) or re.fullmatch(r"\d+\.", fmt):
        match = re.search(r'(\d+)(?:\.(\d+))?', fmt)
        decimal = int(match.group(2)) if match and match.group(2) else 0
        return "ascii", "numeric", decimal
    else:
        return None

def retrieve_other_metadata(fmt):
    if fmt:
        fmt = fmt.upper()
        for parser in (infer_text_format, infer_binary_format, infer_numeric_format, infer_date_format):
            result = parser(fmt)
            if result:
                return result

        # Fallback if format is missing or unknown
        return "unspecified", "default", 0  

def parse_sas_data_file(file_path):
    lrecl = 0
    column_specs = []
    column_names = []
    column_types = []
    column_subtypes = []
    column_decimals = []
    
    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()  # Remove leading/trailing whitespace
            line = line.strip(';')
            
            # Skip empty lines
            if not line:
                continue

            lrecl_match = re.search(r'lrecl\s*=\s*(\d+)', line, re.IGNORECASE)
            lrecl = int(lrecl_match.group(1)) if lrecl_match else lrecl

            line_match_type_a = re.findall(r'(@\s*\d+\s+\w+\s+[^\s.]+\.\d*)', line)
            line_match_type_b = re.findall(r'(\w+\s+\$?\s*\d+-\d+)', line)

            
            if not line_match_type_a and not line_match_type_b:
                continue
            elif line_match_type_a:
                for match in line_match_type_a:
                    # if input format is "@01  MAANO  $UPCASE13. proceed
                    # if input format is "@ 01  MAANO  $UPCASE13. remove space after @
                    line = re.sub(r'@\s+(\d+)', r'@\1', match)
                    parts = line.split()
                    
                    # Extract the start position (remove @ symbol)
                    start_position = int(parts[0][1:]) - 1 # SAS starts at 1 but python starts at 0
                    
                    # Extract column name (second element)
                    column_name = parts[1]
                    
                    # Extract length from format specifier
                    format_spec = parts[2]
                    length = extract_length(format_spec)
                    end_position = start_position + length - 1

                    column_type, column_subtype, column_decimal = retrieve_other_metadata(format_spec)
            
            # Case 2: x-y position specified
            elif line_match_type_b:
                for match in line_match_type_b:
                    # check for standalone $ and remove space
                    line = re.sub(r'\$\s+(\d+)', r'$\1', match) # DAYARR_MO $19-23

                    # search for the format x-y position
                    match_form = re.search(r'(\d+)-(\d+)', line)
                    if match:
                    
                        start_position = int(match_form.group(1)) - 1
                        end_position = int(match_form.group(2)) - 1

                        parts = line.split()
                        format_spec = parts[1]

                        # Try to extract column 
                        # If start of the line has Uppercase/Numerical/Underscore characters
                        name_match = re.match(r'^([A-Z0-9_]+)', line)
                        column_name = name_match.group(1) if name_match else f"COL_{start_position}"

                        column_type, column_subtype, column_decimal = retrieve_other_metadata(format_spec)

            # Add column information
            column_specs.append((start_position, end_position))
            column_names.append(column_name)
            column_types.append(column_type)
            column_subtypes.append(column_subtype)
            column_decimals.append(column_decimal)

    return column_names, column_specs, column_types,column_subtypes, column_decimals, lrecl

def save_output_to_file(names, specs, types, subtypes, decimals, lrecl, output_path):
    with open(output_path, 'w') as f:
        f.write("Lrecl:\n")
        f.write(f"{lrecl}")
        
        # Write column specs
        f.write("\n\nSpecs:\n")
        for i, spec in enumerate(specs):
            # Add the spec
            f.write(f"{spec}")
            
            # Add comma after all items except the last one
            if i < len(specs) - 1:
                f.write(", ")
                
            # Add line break after every 10th item (but not at the end)
            if (i + 1) % 10 == 0 and i < len(specs) - 1:
                f.write("\n")
        
        f.write("\n\nNames:\n")
        for i, name in enumerate(names):
            # Add the name
            f.write(f"\"{name}\"")
            
            # Add comma after all items except the last one
            if i < len(names) - 1:
                f.write(", ")
                
            # Add line break after every 10th item (but not at the end)
            if (i + 1) % 10 == 0 and i < len(names) - 1:
                f.write("\n")

        other_metadatas = list(zip(types, subtypes, decimals))  
        f.write("\n\nOther:\n")
        for i, (col_type, subtype, decimal)  in enumerate(other_metadatas):
            # Add the name
            f.write(f"\"{col_type}, {subtype}, {decimal}\"")
            
            # Add comma after all items except the last one
            if i < len(other_metadatas) - 1:
                f.write(", ")
                
            # Add line break after every 10th item (but not at the end)
            if (i + 1) % 10 == 0 and i < len(other_metadatas) - 1:
                f.write("\n")

def process_file(input_file, output_file):
    try:
        column_names, column_specs, column_types,column_subtypes, column_decimals, lrecl = parse_sas_data_file(input_file)
        save_output_to_file(column_names, column_specs, column_types,column_subtypes, column_decimals, lrecl, output_file)
        print(f"✅ Processed {input_file}. Output saved to {output_file}. Found {len(column_specs)} columns.")
    except Exception as e:
        print(f"❌ Error processing {input_file}: {e}")

def process_all_files(input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)

    for filename in os.listdir(input_folder):
        file_path = os.path.join(input_folder, filename)
        if os.path.isfile(file_path):
            file_root, _ = os.path.splitext(filename)
            output_file = os.path.join(output_folder, f"{file_root}_output.txt")

            # To run only one file, please uncomment line below (225 & 226) and line after process_file (230)
            # file_path = "/pythonITD/dwh_dev/Data_Warehouse/EDW/LOAN/FIELDS_LAYOUT/WOMOVE.txt"
            # output_file = "/pythonITD/dwh_dev/Data_Warehouse/EDW/LOAN/COLUMN_CONFIG/WOMOVE_output.txt"

            process_file(file_path, output_file)

            # continue

# Entry point
if __name__ == "__main__":
    input_folder = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/Outsource/fields_layout"
    output_folder = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/Outsource/column_config"
    process_all_files(input_folder, output_folder)
