import argparse
import json
import mmap
import os
import shutil
import subprocess
import time
import uuid
from collections import defaultdict
from tqdm import tqdm
import csv
import random
import re

# if AWS_BATCH_JOB_ID exists, use it as unique id else use uuid
unique_id = os.environ.get('AWS_BATCH_JOB_ID', str(uuid.uuid4())).split(':')[0]

import s3_accessor
from tqdm import tqdm

data_dir = '/tmp/data'
temp_folder = '/tmp/pii'
content_column = "text"
filter_columns = ["identity_attack", "insult","obscene","severe_toxicity","sexual_explicit","threat","toxicity"]
filter_threshold = 0.5

def get_files(path):
    print(f'get_files({path, os.listdir(path)})')
    files = []
    # if path is a file, use it directly
    if os.path.isfile(path):
        files = [path]
        print(f'path is a file')
    # if path is a directory, use all json files in it
    elif os.path.isdir(path):
        for file in os.listdir(path):
            print(file)
            if ".json" in file:
                files.append(os.path.join(path, file))
    return files

import struct 
pre_sep = b"\xff\xff"
post_sep = b""

UID = 0
def sep():
    global UID
    UID += 1
    return pre_sep+struct.pack("<I", UID)+post_sep

def write_line(line, fout):
    next_line = line.encode('utf-8') + sep() 
    # print(next_line)
    fout.write(next_line)

# TODO: maybe think of a way to do this parallelly
def extract_lines_from_jsonl_files(files, output_file, include_newline=True, content_column="text"):
    line_count = 0
    with open(output_file, 'w') as of:
        for file in tqdm(files):
            with open(file, mode="r", encoding="utf-8") as f:
                with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mmap_in:
                    while True:
                        line = mmap_in.readline()
                        if not line:
                            break
                        json_line = json.loads(line)
                        if not any(column in json_line and float(json_line[column]) >= filter_threshold for column in filter_columns):
                            modified_line = json_line[content_column].replace('\n', ' ').replace('\r', ' ')
                            of.write(modified_line)
                            of.write('\n')
                            # write_line(json_line[content_column], of)

def recreate_dir(dir):
    if os.path.exists(dir):
        shutil.rmtree(dir)
    os.makedirs(dir, exist_ok=True)

def find_no_of_contaminated_lines_from_rust_result(rust_result):
    # aka the last non-empty line as a list
    last_line = ""
    #  find last non-empty line in val
    val_lines = rust_result.split('\n')
    for i in range(len(val_lines) - 1, -1, -1):
        if val_lines[i] != '':
            last_line = val_lines[i]
            break
    # convert the last line to a list
    last_line = json.loads(last_line)
    return last_line


def main(train_files_path, pii_local_dir, args):
    train_files = get_files(train_files_path)
    print(f'train_files {train_files}')
    
    # make temp folder
    print(f'making temp folder {temp_folder}, cache_dir {args.cache_dir} ...')
    recreate_dir(temp_folder)
    recreate_dir(args.cache_dir)
    modified_train_file = os.path.join(temp_folder, 'train.txt')
    extract_lines_from_jsonl_files(train_files, modified_train_file, include_newline=False)

    # print(f'building suffix array for {modified_train_file} ...')
    # start = time.time()
    # cmd = ['python3', './scripts/make_suffix_array.py', modified_train_file]
    # rust_result = subprocess.Popen(cmd).wait()
    # print(rust_result)
    # print(f'suffix array built in {time.time() - start} seconds')

    # TODO: load the detected PII file, read it, call contains for each, dump contaminated lines somewhere, aggregate them later.
    pii_files = get_files(pii_local_dir)
    assert len(pii_files) == 1, "expect only one pii file"
    modified_pii_file = os.path.join(temp_folder, 'pii.txt')

    with open(modified_pii_file, 'w') as of:
        with open(pii_files[0], mode="r", encoding="utf-8") as f:
            line = f.readline()
            json_line = json.loads(line)
            
    extract_lines_from_jsonl_files(pii_files, modified_pii_file, include_newline=False, content_column="detected_pii")

    # rust_result = os.popen(f"cargo run exact-lookup --data-file {modified_train_file} --query-file {modified_pii_file} --num-threads {os.cpu_count() or 1}").read()
    rust_result = os.popen(f"cargo run contains --data-file {modified_train_file} --query-file {modified_pii_file} --gram-size 8 --num-threads {os.cpu_count() or 1}").read()


    # rust_result = os.popen(f"cargo run count-occurrences-multi --data-file {modified_train_file} --query-file {modified_pii_file}").read()

    print(f"rust result {rust_result}")
    contaminated_lines = find_no_of_contaminated_lines_from_rust_result(rust_result)

    # upload to s3 
    line_indicies_file_path = os.path.join(temp_folder, f'{unique_id}-{args.batch_array_index}-contaminated_lines.txt')

    with open(line_indicies_file_path, 'w') as f:
        f.write(json.dumps(contaminated_lines))
        f.write("\n")

    s3_accessor.upload(f"{args.result_dir.strip('/')}/{unique_id}/{args.batch_array_index}-contaminated_lines.txt", line_indicies_file_path)


        


def copy_s3_to_local(files, dir):
    print(f'downloading {len(files)} files to {dir} ...')
    for file in files:
        s3_accessor.download_to_local(file, dir)
    
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--train_files', type=str, required=True, help='S3 path to train files. (Provide dir or file)')
    parser.add_argument('--pii_lookup_file', type=str, required=True, help='S3 path to PII lookup file')
    parser.add_argument('--result_dir', type=str, default="s3://anuragik-dev/pii_lookups/", help='result directory remote path')
    parser.add_argument('--batch_array_size', type=int, default=400, help='AWS Batch array size')
    parser.add_argument('--batch_array_index', type=int, default=0, help='AWS Batch array index for a job')
    parser.add_argument('--cache_dir', type=str, default="/tmp/cache", help='cache directory for saving dups and sizes files output by cmd_self_similar')
    parser.add_argument('--test_file', type=str, help='local test file, skips fetching files from AWS')

    args = parser.parse_args()

    if args.test_file:
        print(f'looking at {args.test_file}')
        extract_pii_lines_from_jsonl_files([args.test_file], 'train.txt', include_newline=False)
        exit()

    train_files_remote_path = args.train_files
    train_files_local_dir = os.path.join(data_dir, 'train')
    pii_local_dir = os.path.join(data_dir, 'pii')

    train_files = []

    train_bucket, train_key = s3_accessor.getBucketNameAndPrefix(train_files_remote_path)
    
    key_index = 0
    for key in s3_accessor.getNextKey(bucket=train_bucket, prefixes=[train_key], suffixes=['.jsonl','.json']):
        if key_index % args.batch_array_size == args.batch_array_index:
            train_files.append(f"s3://{train_bucket}/{key}")
        key_index+=1
    
    # check if the directories exist
    for d in [train_files_local_dir, pii_local_dir]:
        if os.path.exists(d):
            shutil.rmtree(d)
        # create the directories
        os.makedirs(d, exist_ok=True)

    # copy files from s3 to local
    print(f'going to get {len(train_files)} files from s3')
    copy_s3_to_local(train_files, train_files_local_dir)
    copy_s3_to_local([args.pii_lookup_file], pii_local_dir)

    # run the main function
    main(train_files_local_dir, pii_local_dir, args)