import re
import pprint
from collections import defaultdict
import os

FOLDER_NAME = 'DETICA'

def generate_layout_file(field_layout_file, output_dir='.'):
    if not os.path.exists(field_layout_file):
        raise FileNotFoundError(f"Field Config File not found in {field_layout_file}")
    
    try:
        with open(field_layout_file, 'r', encoding='utf-8') as f:
            sas_input_code = f.read()

        lrecl_pattern = re.compile(r'lrecl\s*=\s*(\d+)', re.IGNORECASE)
        lrecl_match = lrecl_pattern.search(sas_input_code)
        if not lrecl_match:
            raise ValueError("Could not find LRECL in the code")
        lrecl = int(lrecl_match.group(1))

        layout = {}
        input_line_pattern = re.compile(r'@\s*(\d+)\s+([A-Za-z0-9_]+)\s+([$\w\.]+)')
        print(input_line_pattern)
        range_line_pattern = re.compile(r'^\s*([A-Za-z0-9_]+)\s*(\$)?[ ]*(\d+)-(\d+)')
        input_lines = sas_input_code.split('input')[1].split(";")[0]

        for line in input_lines.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            match = input_line_pattern.match(line)
            print(match)
            if match:
                start_pos, field_name, informat_str = match.groups()
                start_pos = int(start_pos)

                spec = {}
                byte_length = None
                clean_informat = informat_str.strip('$.').upper()

                float_match = re.match(r'(\d+)\.(\d+)', clean_informat)
                pd_match = re.match(r'S370FPD(\d+)(?:\.(\d+))?', clean_informat)
                ebcdic_match = re.match(r'EBCDIC(\d+)', clean_informat)
                integer_match = re.match(r'(\d+)\.', informat_str.upper())
                string_match = re.match(r'\$(\d+)\.', informat_str.upper())
                if pd_match:
                    spec['type'] = 'packed'
                    byte_length =  int(pd_match.group(1))
                    decimals = pd_match.group(2)
                    if decimals:
                        spec['decimals'] = int(decimals)
                elif ebcdic_match:
                    spec['type'] = 'ebcdic'
                    byte_length =  int(ebcdic_match.group(1))
                elif float_match:
                    spec['type'] = 'float'
                    byte_length = int(float_match.group(1))
                    spec['decimals'] = int(float_match.group(2))
                elif integer_match:
                    spec['type'] = 'integer'
                    byte_length = int(clean_informat)
                elif string_match:
                    spec['type'] = 'string'
                    byte_length = int(clean_informat)
                elif re.match(r'YYMMDD(\d+)', clean_informat):
                    spec['type'] = 'integer'
                    byte_length = int(re.match(r'YYMMDD(\d+)', clean_informat).group(1))

                if byte_length is None:
                    print(f"Could not parse informat '{informat_str}' for field '{field_name}'. Skiping")     
                    continue

                spec['slice'] = slice(start_pos - 1, start_pos - 1 + byte_length)
                layout[field_name] = spec
            elif match := range_line_pattern.match(line):
                field_name, dollar_sign, start_pos_str, end_pos_str = match.groups()
                start_pos = int(start_pos_str)
                end_pos = int(end_pos_str)

                spec = {}
                byte_length = end_pos - start_pos - 1

                if dollar_sign:
                    spec['type'] = 'string'
                else:
                    spec['type'] = 'integer'

                spec['slice'] = slice(start_pos - 1, end_pos)
                layout[field_name] = spec
            else:
                print(f"Skipping line: {line} does not match defined INPUT format")
                

        output_filename = f"{os.path.basename(field_layout_file).rsplit('.' , 1)[0]}_layout.py"
        output_layout_path = os.path.join(output_dir, output_filename)

        formatted_layout_string = pprint.pformat(layout, sort_dicts=False)

        with open(output_layout_path, 'w', encoding='utf-8') as f:
            f.write(f"LRECL = {lrecl}\n\n")
            f.write(f"LAYOUT = {formatted_layout_string}\n")
        print(f"Successfully generated layout file to {output_layout_path}")
    except Exception as e:
        print(f"Error generating layout file: {e}")


FIELD_LAYOUT_PATH = f"/host/mis/config/{FOLDER_NAME}"
LAYOUT_OUTPUT_DIR = f"/host/mis/config/{FOLDER_NAME}/output"

if not os.path.exists(LAYOUT_OUTPUT_DIR):
    os.makedirs(LAYOUT_OUTPUT_DIR)
    
if __name__ == "__main__":
    try:
        for filename in os.listdir(FIELD_LAYOUT_PATH):
            if filename.endswith(".txt"):
                file_path = os.path.join(FIELD_LAYOUT_PATH, filename)
                try:
                    generate_layout_file(file_path, output_dir=LAYOUT_OUTPUT_DIR)
                except Exception as e:
                    print(f"Error generating layout {filename}: {e}")
    except FileNotFoundError as e:
        print(f"Error path not found: {e}")
    print(f"Completed generating layout file.")





import os
import sys
import dask.config
from dask.distributed import Client, LocalCluster
from datetime import datetime,timedelta
import dask
sys.dont_write_bytecode = True
import numpy as np
from numba import njit
import dask.bag as db
import dask.dataframe as dd
import pandas as pd
import importlib.util
import shutil
import time
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from decimal import Decimal
import json

@njit
def unpack_packed_decimal(pd_bytes_array):
    val = np.int64(0)

    if not pd_bytes_array.size:
        return val
    
    sign_nibble = pd_bytes_array[-1] & 0x0F
    is_negative = (sign_nibble == 0x0D or sign_nibble == 0x0B)

    for i in range(len(pd_bytes_array)):
        byte = pd_bytes_array[i]
        high_nibble = byte >> 4

        if i == len(pd_bytes_array) - 1:
            val = val * 10 + high_nibble
        else:
            low_nibble = byte & 0x0F
            val = val * 100 + high_nibble * 10 + low_nibble

    return -val if is_negative else val

def decode_records(record_bytes, layout):

    decoded_row = {}
    for field_name, specs in layout.items():
        field_bytes = record_bytes[specs['slice']]
        field_type = specs['type']

        try:
            if field_type == 'ebcdic':
                decoded_row[field_name] = field_bytes.decode('cp037').strip()
            elif field_type == 'packed':
                raw_val = unpack_packed_decimal(np.frombuffer(field_bytes, dtype=np.uint8))
                decimals = specs.get('decimals', 0)
                if decimals > 0:
                    decoded_row[field_name] = Decimal(raw_val) / (Decimal(10) ** decimals)
                else:
                    decoded_row[field_name] = raw_val
        except Exception:
            decoded_row[field_name] = None

    return decoded_row

def generate_records(filename, record_size, max_records):
    records_yeild = 0
    try:
        with open(filename, 'rb') as f:
            while True:
                if max_records != None and records_yeild >= max_records:
                    break
                record = f.read(record_size)
                if not record:
                    break
                if len(record) == record_size:
                    yield record
                    records_yeild += 1
    except Exception as e:
        print(f"Error file{e}:")
        return
    
def load_layout_file(layout_file_path):
    spec = importlib.util.spec_from_file_location("layout_module", layout_file_path)
    if spec is None:
        raise FileNotFoundError(f"Layout file not found at {layout_file_path}")
    
    layout_module = importlib.util.module_from_spec(spec)
    sys.modules['layout_module'] = layout_module
    spec.loader.exec_module(layout_module)
    return layout_module.LAYOUT, layout_module.LRECL

def is_text_file(filepath, chunksize=4096):
    try:
        with open(filepath, 'r', encoding='ascii') as f:
            f.read(chunksize)
            return True
    except UnicodeDecodeError:
        return False
    except Exception:
        return False

def read_decode_chunk_to_df(byte_range, filename, lrecl, layout, schema):

    start_byte, end_byte = byte_range
    try:
        with open(filename, 'rb') as f:
            f.seek(start_byte)
            while f.tell() < end_byte:
                record_byte = f.read(lrecl)
                if not record_byte or len(record_byte) < lrecl:
                    break
                yield decode_records(record_byte, layout)
    except Exception as e:
        print(f"Error file{e}:")
        return pd.Dataframe(columns=schema.keys()).astype(schema)
    
def create_status_file(all_jobs, status_file_path):
    if os.path.exists(status_file_path):
        print(f"Rerun detected. Loading job status fom {status_file_path}")
        with open(status_file_path, 'r') as f:
            status = json.load(f)
    else:
        print(f"First run detected. Creating new status file {status_file_path}")
        status = {job['input_file']: "pending" for job in all_jobs}
        with open(status_file_path, 'w') as f:
            json.dump(status, f, indent=4)
    return status

def update_status_file(status_file_path, file_path, new_status):
    with open(status_file_path, 'r+') as f:
        data = json.load(f)
        data[file_path] = new_status
        f.seek(0)
        json.dump(data, f, indent=4)
        f.truncate()

def process_file(job_spec, output_dir, applname):
    file_start_time = time.time()
    input_file = job_spec['input_file']
    layout_file_path = job_spec['layout_file']
    is_text_file = job_spec['is_text']

    file_base_name = os.path.splitext(os.path.basename(input_file))[0]
    base_name = file_base_name.rsplit('_', 1)[0]
    final_parquet_output = f"{output_dir}/STG_{applname}_{base_name}.parquet"

    print(f"\n---- START Processing for file: {input_file} ----")

    try:
        FILE_LAYOUT, INPUT_LRECL = load_layout_file(layout_file_path)

        meta_schema = {}
        for name, spec in FILE_LAYOUT.items():
            if spec['type'] == 'packed':
                meta_schema[name] = float if 'decimals' in spec else int
            elif spec['type'] == 'ebcdic':
                meta_schema[name] = str
            elif spec['type'] == 'string':
                meta_schema[name] = str
            elif spec['type'] == 'integer':
                if is_text_file:
                    meta_schema[name] = float
                else:
                    meta_schema[name] = int
            elif spec['type'] == 'float':
                meta_schema[name] = float

        if os.path.getsize(input_file) == 0:
            print(f"The data file '{input_file}' is empty, return empty Dataframe")
            df = dd.from_pandas(pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in meta_schema.items()}), npartitions=1)
        else:
            if is_text_file:
                print(f"The file {input_file} is detected as Text File / Non-Binary.")

                colspecs = [(spec['slice'].start, spec['slice'].stop) for spec in FILE_LAYOUT.values()]
                names = list(FILE_LAYOUT.keys())

                ddf = dd.read_fwf(
                    input_file,
                    colspecs=colspecs,
                    names=names,
                    dtype=str,
                    blocksize='128MB',
                    engine='python'
                )
                transformation = {}

                for name, spec in FILE_LAYOUT.items():
                    if spec['type'] == 'integer':
                        numeric_col = dd.to_numeric(ddf[name], errors='coerce')
                        transformation[name] = numeric_col
                    elif spec['type'] == 'float' and 'decimals' in spec and spec['decimals'] > 0:
                        decimals = spec['decimals']
                        numeric_col = dd.to_numeric(ddf[name], errors='coerce')
                        transformation[name] = numeric_col / (10**decimals)
                if transformation:
                    ddf = ddf.assign(**transformation)
                df = ddf
            else:
                print(f"The file {input_file} is detected as Binary File.")
                file_size = os.path.getsize(input_file)
                chunk_size = 128 * 1024 * 1024
                chunks = []
                start = 0

                while start < file_size:
                    end = min(start + chunk_size, file_size)
                    if end < file_size and end % INPUT_LRECL != 0:
                        end -= (end % INPUT_LRECL)
                    if start < end:
                        chunks.append((start, end))
                    start = end

                chunks_bag = db.from_sequence(chunks)

                df_bag = chunks_bag.map(read_decode_chunk_to_df, filename=input_file, lrecl=INPUT_LRECL, layout=FILE_LAYOUT, schema=meta_schema).flatten()

                df = df_bag.to_dataframe(meta=meta_schema)

        row_count = len(df)
        df.to_parquet(final_parquet_output)

        file_end_time = time.time()
        print(f"Successfully processed {input_file}. Saved to {final_parquet_output}")
        print(f"Time taken to complete file {input_file}: {file_end_time - file_start_time}")
        print(f"Total Row Count for {input_file}: {row_count}")
        print(f"\n---- END Processing for file: {input_file} ----")
        return {"file": input_file, "status": "completed"}
    except Exception as e:
        print(f"Failed to process {input_file}: {e}")
        print(f"\n---- END Processing for file: {input_file} ----")
        return {"file": input_file, "status": "failed"}
    
def run_file_reader(input_dir, output_dir, layout_dir, batch_dt_str, status_file_dir,applname):
    all_jobs = []
    unique_files = set()
    for layout_file_name in os.listdir(layout_dir):
        if not layout_file_name.endswith("_layout.py"):
            continue

        base_name = os.path.splitext(layout_file_name)[0].rsplit("_layout", 1)[0]

        binary_input_path = os.path.join(input_dir, f"{base_name}_{batch_dt_str}")
        text_input_path = os.path.join(input_dir, f"{base_name}_{batch_dt_str}.txt")
        ctrl_input_path = os.path.join(input_dir,f"{base_name}")


        input_file_path = None
        if os.path.exists(binary_input_path):
            input_file_path = binary_input_path
            print(f"==========================={input_file_path}=====================================")
        elif os.path.exists(text_input_path):
            input_file_path = text_input_path
            print(f"==========================={input_file_path}=====================================")
        elif os.path.exists(ctrl_input_path):
            input_file_path = ctrl_input_path
            print(f"==========================={input_file_path}=====================================")
        else:
            print(f"No data file found for file '{layout_file_name}'. Skipping")
            
        is_text_flag = is_text_file(input_file_path)
        all_jobs.append({
            "input_file": input_file_path,
            "layout_file": os.path.join(layout_dir, layout_file_name),
            "is_text": is_text_flag
        })

    if not os.path.exists(status_file_dir):
        os.makedirs(status_file_dir)

    status_file_path = os.path.join(status_file_dir, f"{applname}_status_{batch_dt_str}.json")
    job_status = create_status_file(all_jobs, status_file_path)

    jobs_to_run = [job for job in all_jobs if job_status.get(job['input_file']) in ("pending", "failed")]

    if not jobs_to_run:
        print("All the files for today's batch are completed.")
        return

    lazy_tasks = [dask.delayed(process_file)(job_spec, output_dir, applname) for job_spec in jobs_to_run]

    results = dask.compute(*lazy_tasks)
    for result in results:
        if result:
            update_status_file(status_file_path, result['file'], result['status'])

if __name__ == "__main__":
    batch_dt =  (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    APPL_KEY   = 'lookup'
    APPL_NAME  = 'DETICA'
    
    config = {
        'input_dir': f"/sasdata/rawdata/{APPL_KEY}",
        'output_dir' : f"/host/mis/parquet/control/input",
        'layout_dir' : f"/host/mis/config/{APPL_NAME}/output",
        'status_file_dir' : f"/host/mis/control/job_status",
        'batch_date' : batch_dt,
        'n_workers' : 2,
        'threads_per_worker' : 1,
        'memory_limit': '64GB',
        'applname': APPL_NAME
    }

    dask.config.set({"distributed.worker.timeouts.connect":"180s"})
    dask.config.set({"distributed.worker.timeouts.termination":"120s"})

    cluster  = LocalCluster(
        n_workers=config['n_workers'],
        threads_per_worker=config['threads_per_worker'],
        memory_limit=config['memory_limit']
    )
    client = Client(cluster)

    try:
        run_file_reader(
            input_dir=config['input_dir'], 
            output_dir=config['output_dir'], 
            layout_dir=config['layout_dir'], 
            batch_dt_str=config['batch_date'], 
            status_file_dir=config['status_file_dir'],
            applname=config['applname']        
        )
    finally:
        client.close()
        client.shutdown()
