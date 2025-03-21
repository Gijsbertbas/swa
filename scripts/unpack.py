import json
from pathlib import Path
import gzip
import shutil
import sys
import os


def unzip(file_name: str, out_path: str) -> str:
    gz_path = Path(file_name)
    original_filename = gz_path.stem

    output_path = Path(out_path)
    output_path.mkdir(parents=True, exist_ok=True)
    output_file = output_path / original_filename

    if os.path.isfile(output_file):
        print(f"file {output_file} exists, skipping...")
        return None

    with gzip.open(gz_path, 'rb') as gz_file:
        with open(output_file, 'wb') as out_file:
            shutil.copyfileobj(gz_file, out_file)
    return output_file


def parse_dynamo_data(data):
    """
    Parse DynamoDB JSON format and convert to native Python types.

    Args:
        data (dict/list): DynamoDB formatted data

    Returns:
        Converted native Python type
    """
    if not isinstance(data, (dict, list)):
        return data

    if isinstance(data, list):
        return [parse_dynamo_data(item) for item in data]

    # Handle empty dict
    if not data:
        return data

    # Get the type identifier (first and only key in the dict)
    type_key = list(data.keys())[0]
    value = data[type_key]

    # Parse based on DynamoDB type
    type_handlers = {
        'S': str,
        'N': lambda x: int(x) if x.isdigit() else float(x),
        'BOOL': bool,
        'NULL': lambda x: None,
        'L': lambda x: [parse_dynamo_data(item) for item in x],
        'M': lambda x: {k: parse_dynamo_data(v) for k, v in x.items()},
        'SS': set,
        'NS': lambda x: {int(n) if n.isdigit() else float(n) for n in x},
    }

    handler = type_handlers.get(type_key)
    if handler:
        return handler(value)

    return value


def parse(file_name: str, out_path: str):        

    output_path = Path(out_path)
    output_path.mkdir(parents=True, exist_ok=True)
    output_file = output_path / file_name.name


    lines = open(file_name, 'r').readlines()
    output = []
    for line in lines:
        read = json.loads(line)["Item"]
        converted = {k: parse_dynamo_data(v) for k, v in read.items()}
        converted = dict(sorted(converted.items()))
        output.append(json.dumps(converted))

    with open(output_file, 'w') as out_file:
        for line in output:
            out_file.write(f"{line}\n")


def unpack(root_path: str):
    root = Path(root_path)

    for folder_path in root.iterdir():
        if folder_path.is_dir() and not folder_path.name in ["unpacked", "parsed"]:
            summary_file = folder_path / 'manifest-summary.json'
            summary = json.load(open(summary_file, 'r'))
            table = summary["tableArn"].split(':')[-1].split('/')[1].split('-')[0]
            print(f"Unpacking and parsing folder {folder_path.name} - table {table}")

            data_path = folder_path / "data"
            for file in data_path.glob("*.gz"):
                unpacked = unzip(file, f"{root_path}/unpacked/{table}")
                if unpacked is not None:
                    parse(unpacked, f"{root_path}/parsed/{table}")
                

if __name__ == "__main__":
    root = "./aws"
    if len(sys.argv) > 1:
        root = sys.argv[1]
    unpack(root)

# aws s3 cp --recursive AWSDynamoDB/parsed/ s3://groengegeven-swa-v2-export/parsed/ 
