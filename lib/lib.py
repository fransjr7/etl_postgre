import os
import json

def read_file_string(path: str):
    try:
        with open(path, 'r') as file:
            data = file.read()
        return data
    except Exception as error:
        print(f"error to load file to string {error}")

def read_file_json(path: str):
    try:
        with open(path, 'r') as file:
            data = json.load(file)
        return data
    except Exception as error:
        print(f"error to load file to json {error}")

def scan_dir(path: str):
    subfolders= [f.path for f in os.scandir(path) if f.is_dir()]
    for path in list(subfolders):
        subfolders.extend(scan_dir(path))
    return subfolders