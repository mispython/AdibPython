import os
import ast
import pandas as pd
import io
import pyarrow as pa
import pyarrow.parquet as pq
import mmap
import numpy as np
from numba import njit
import time
import re

# Global Paths
input_folder_path = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/Outsource/input"
output_folder_path = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/rawdata_converted"
var_path = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/Outsource/column_config"


# Get file names to export Parquet
def get_file_names(directory_path):
    file_names = []

    try:
        files = os.listdir(directory_path)

        for file in files:
            full_path = os.path.join(directory_path, file)

            # Only take file names not folders
            if os.path.isfile(full_path):
                base_name = file.rsplit('_', 1)[0]
                file_names.append((file, base_name))

        return file_names

    except FileNotFoundError:
        print(f"Error: The directory '{directory_path}' was not found")
        return []
    
    except Exception as e:
        print(f"Error: {e}")
        return []
    
# Get column details from mapping files
def load_variables(var_path, script_name):
    # Get current script name without extension
    input_path = os.path.join(f"{var_path}", f"{script_name}_output.txt")

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file '{input_path}' not found.")

    with open(input_path, 'r') as file:
        content = file.read()

     # Extract specs and names from text using simple markers
    specs = []
    names = []
    other = []
    try:

        # Extract LRECL
        lrecl_start = content.index("Lrecl:") + len("Lrecl:")
        lrecl_end =content.index("Specs:")
        lrecl_str = content[lrecl_start:lrecl_end].strip()
        lrecl = int(lrecl_str)

       # Locate positions of the metadata sections
        specs_start = content.index("Specs:") + len("Specs:")
        names_start = content.index("Names:") + len("Names:")
        other_start = content.index("Other:")

        # Extract text blocks
        specs_str = content[specs_start:names_start - + len("Names:")].strip()
        names_str = content[names_start:other_start].strip()
        other_str = content[other_start + len("Other:"):].strip()

        # Safely evaluate to Python lists
        specs = ast.literal_eval(f"[{specs_str}]")
        names = ast.literal_eval(f"[{names_str}]")
        other = ast.literal_eval(f"[{other_str}]")  # this will be list of strings like "ascii, numeric, 0"

        # If needed, split `other` entries into individual parts:
        column_types, column_subtypes, column_decimals = [], [], []
        for entry in other:
            col_type, subtype, decimal = [x.strip() for x in entry.split(",")]
            column_types.append(col_type)
            column_subtypes.append(subtype)
            column_decimals.append(int(decimal)) 

    except Exception as e:
        raise ValueError(f"Failed to parse variable file: {e}")

    return specs, names, other, column_types, column_subtypes, column_decimals, lrecl

# Check if table exists and delete if present
def check_and_delete_table(output_file):
    if os.path.exists(output_file):
        print(f"Table {output_file} exists. Deleting...")
        os.remove(output_file)
        print(f"Deleted: {output_file}")
    else:
        print(f"Table {output_file} does not exist.")

# Packed Decimal Decoder using numba for faster processing
''' remove @njit due to Pickle error
@njit
def decode_s370fpd_column(data: np.ndarray, starts: np.ndarray, ends: np.ndarray, decimals: int) -> np.ndarray:
    n = data.shape[0]
    result = np.empty(n, dtype = np.float64)

    for i in range(n):
        value = 0
        field = data[i, starts[i]:ends[i]+1]
        
        for b in field[:-1]:
            value = value * 100 + ((b >> 4) * 10 + (b & 0x0F))
        sign = field[-1] & 0x0F
        value = value * 10 + ((field[-1] >> 4) & 0x0F)
        if sign == 0x0D or sign == 0x0B:
            value = -value
        result[i] = value / (10 ** decimals)
    return result
'''
def decode_s370fpd_column(data: np.ndarray, starts: np.ndarray, ends: np.ndarray, decimals: int) -> np.ndarray:
    n = data.shape[0]
    result = np.empty(n, dtype = np.float64)

    for i in range(n):
        field = data[i, starts[i]:ends[i]+1]
        
        value = 0
        for bb in field[:-1]:
            b = int(bb)
            value = value * 100 + ((b >> 4) * 10 + (b & 0x0F))

        last = int(field[-1])
        sign = last & 0x0F
        value = value * 10 + ((last >> 4) & 0x0F)
        if sign in (0x0D,0x0B):
            value = -value
        result[i] = value / (10 ** decimals)
    return result

# Decode EBCDIC columns
def decode_ebcdic_column(data: np.ndarray, start: int, end: int) -> list:
    return [data[i, start:end+1].tobytes().decode('cp037').strip().rstrip('\x00') for i in range(data.shape[0])]

# Read binary file to array
def load_binary_to_array(input_file, lrecl):
    with open(input_file, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        num_records = mm.size() // lrecl

        # Read data into numpy array
        data = np.frombuffer(mm, dtype = np.uint8)
        data = data[:num_records * lrecl] # Trim
        data = data.reshape((num_records, lrecl))
    return data

# If input file is in .txt, read using fixed width decoder
def load_text_file(input_file, column_specs, column_names, column_decimals):

    with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
        data = f.read()
        adj_column_specs = [(start, end+1) for start, end in column_specs]

        df = pd.read_fwf(io.StringIO(data), colspecs=adj_column_specs, names=column_names)

    return df

# Decoding by columns
def process_columns(data: np.ndarray, column_specs, column_names, column_types, column_subtypes, column_decimals):
    result = {}

    for idx, (name, (start, end), ctype, subtype, decimal) in enumerate(zip(column_names, column_specs, column_types, column_subtypes, column_decimals)):
        print(f"Decoding column: {name}")

        if ctype == 'packed' and subtype == "s370fpd":
            result[name] = decode_s370fpd_column(data, np.full(data.shape[0], start), np.full(data.shape[0], end), decimal)
        elif ctype == 'ebcdic':
            result[name] = decode_ebcdic_column(data, start, end)
        else:
            col_data = []
            for i in range(data.shape[0]):
                field = data[i, start:end+1].tobytes()
                try:
                    col_data.append(field.decode('ascii').strip())
                except:
                    col_data.append(None)
            result[name] = col_data
    return pd.DataFrame(result)

# Program flow for decoding
def etl_loader_column(column_specs, column_names, column_types, column_subtypes, column_decimals, lrecl, source_file):
    match_text_format = re.search(r'.*\.txt$', source_file)
    t0 = time.time()
    if match_text_format:
        print(f"Processing Text File: {source_file}")
        df = load_text_file(source_file, column_specs, column_names, column_decimals)
    else:
        data = load_binary_to_array(source_file, lrecl)
        df = process_columns(data, column_specs, column_names, column_types, column_subtypes, column_decimals)
    t1 = time.time()
    print(f"time taken decoding: {t1 - t0}")
    return df

# Save DataFrame to Parquet file
def save_to_parquet(df, output_file):
    print(output_file)
    if not df.empty:
        try:
            table = pa.Table.from_pandas(df, preserve_index = False)
            pq.write_table(table, output_file)
            print(f"Data saved to {output_file}")
        except Exception as e:  
            print(f"Error saving data: {e}")
    else:
        print("No data to save.")


if __name__ == "__main__":

    # Retrieves full file name and the extracted base name (e.g ACCTFILE_20250427 and ACCTFILE )
   # files = get_file_names(input_folder_path)

    files = [
        #binary
         
          ('CISBNMID', 'CISBNMID'),
          ('CISSSMID', 'CISSSMID'),
        
        #text   
           ('CISADDR.txt', 'CISADDR'),    
           ('CISNAME.txt', 'CISNAME'),     
           ('CISRLCC.txt', 'CISRLCC'),     
           ('CISDP.txt', 'CISDP'),     
           ('CISTAXID.txt', 'CISTAXID'),    
           ('CISRMRK.txt', 'CISRMRK'),
           ('CISSALE.txt', 'CISSALE') ,
           ('CISEMPL.txt', 'CISEMPL') 
          ]
   

    for file_name, base_name in files:

        # input_file = f"/sasdata/rawdata/ln/{file_name}"
        input_file = f"{input_folder_path}/{file_name}"
        output_file_name = file_name.rsplit('.')[0]
        output_file = f"{output_folder_path}/{output_file_name}.parquet"

        # Get values
        column_specs, column_names, column_meta, column_types, column_subtypes, column_decimals, lrecl = load_variables(var_path, base_name)

        # Check and delete existing table
        check_and_delete_table(output_file)

        # Read the fixed-width file
        print("Reading source")
        # # df = etl_loader(column_specs, column_names, column_meta, column_types, column_subtypes, column_decimals, lrecl, input_file, output_file,num_workers=4)
        df = etl_loader_column(column_specs, column_names, column_types, column_subtypes, column_decimals, lrecl, input_file)

        print("Saving to Parquet")
        # Save to Parquet
        save_to_parquet(df, output_file)
