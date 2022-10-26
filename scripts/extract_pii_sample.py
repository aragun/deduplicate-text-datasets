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
val_line_map = {}

regex = "\b\d{1,8}\b[\s\S]{10,100}?\b(AK|AL|AR|AZ|CA|CO|CT|DC|DE|FL|GA|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WI|WV|WY)\b\s\d{5}\b|\b((\+|\b)[1l][\−\. ])?\(?}\b[\dOlZSB]{3,5}([\−\. ]|\) ?)[\dOlZSB]{3}[\−\.][\dOlZSB]{4}\b|[\w\.=−]+@[\w\.−]+\.[\w]{2,3}|\b(birth|birthdate|birthday|dob|born)\W+(?:\w+\W+){0,5}?(?:(\d{4}|\d{1,2})[\/\−]\d{1,2}[\/\−](\d{4}|\d{1,2}))\b|\b([0−8]\d{2}|7([0−6]\d))([−]?|\s{1})\d\d\d{4}\b|(?:5[1−5][0−9]{2}|222[1−9]|22[3−9][0−9]|2[3−6][0−9]{2}|27[01][0−9]|2720)[0−9]{12}|\b([4]\d{3}[\s]\d{4}[\s]\d{4}[\s]\d{4}|[4]\d{3}[−]\d{4}[−]\d{4}[−]\d{4}|[4]\d{3}[.]\d{4}[.]\d{4}[.]\d{4}|[4]\d{3}\d{4}\d{4}\d{4})\b|3[47][0−9]{13}|\d{3}−\d{2}−\d{4}|(?:(\d{1,5}( 1\/[234])?(\x20[A−Z]([a−z])+)+ )|(P\.O\. Box \d{1,5}))\s{1,2}(?i:(?:(((APT|B LDG|DEPT|FL|HNGR|LOT|PIER|RM|S(LIP|PC|T(E|OP))|TRLR|UNIT)\x20\w{1,5})|(BSMT|FRNT|LBBY|LOWR|OFC|PH|REAR|SIDE|UPPR)\.?)\s{1,2})?)(?:[A−Z]([a−z])+(\.?)(\x20[A−Z]([a−z])+){0,2})\, \x20(?:A[LKSZRAP]|C[AOT]|D[EC]|F[LM]|G[AU]|HI|I[ADL N]|K[SY]|LA|M[ADEHINOPST]|N[CDEHJMVY]|O[HKR]|P[ARW]|RI|S[CD]|T[NX]|UT|V[AIT]|W[AIVY])\x20(?:\d{5}(−\d {4})?)|(?:(\d{1,5}( 1\/[234])?(\x20[A−Z]([a−z])+)+ )|(P\.O\. Box \d{1,5}))|[A−Z0−9<]{9}[0−9]{1}[A−Z]{3}[0−9]{7}[A−Z]{1}[0−9]{7}[A−Z0−9<]{14}[0−9]{2}|[A−Z9]{5}[0−9]([05][1−9]|[16][0−2])(0[1−9]|[12][0−9]|3[01])[0−9][A−Z9][0−9][A−Z0−9]([0−9]{2}?)"

def get_line_seperator():
    return b"\xff\xff"

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
    next_line = sep() + line.encode()
    # print(next_line)
    fout.write(next_line)


# TODO: maybe think of a way to do this parallelly
def extract_pii_lines_from_jsonl_files(files, output_file, include_newline=True):
    line_count, total_lines = 0, 0
    matches = []
    with open(output_file, 'wb') as of:
        for file in tqdm(files):
            with open(file, mode="r", encoding="utf-8") as f:
                with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mmap_in:
                    while True:
                        line = mmap_in.readline()
                        if not line:
                            break
                        json_line = json.loads(line)
                        sample = {}
                        if not any(column in json_line and float(json_line[column]) >= filter_threshold for column in filter_columns):
                            # look up regex here 
                            # print(f'{line_count} reading {json_line.keys()}')
                            total_lines += 1
                            all_matches = re.findall(regex, json_line[content_column])
                            # filter empty results
                            if all_matches:
                                # get all matches
                                allm = []
                                for m in all_matches:
                                    tmp = list(filter(None, m))
                                    if len(tmp) > 0:
                                        allm.append(tmp)
                                
                                if len(allm) > 0:
                                    # print(f'found match {json_line[content_column]}')
                                    print(f'all matches {allm}')
                                    line_count += len(allm)
                                    # matches[line_count] = match[0]
                                    # write_line(json_line[content_column], of)
                                    
                                    sample['id'] = line_count
                                    sample[content_column] = json_line[content_column]
                                    sample['match'] = allm

                                    matches.append(sample)

                                    if line_count > 1000:
                                        break
    
    print(f'{len(matches), line_count} lines of potential PII extracted from {total_lines} lines ...')
    for m in matches:
        print(f"id {m['id']}, match {m['match']}, len(doc) {len(m[content_column])}")

    with open('data.json', 'w') as f:
        json.dump(matches, f)

def recreate_dir(dir):
    if os.path.exists(dir):
        shutil.rmtree(dir)
    os.makedirs(dir, exist_ok=True)

def main(train_files_path, args):
    train_files = get_files(train_files_path)
    print(f'train_files {train_files}')
    
    # make temp folder
    print(f'making temp folder {temp_folder}, cache_dir {args.cache_dir} ...')
    recreate_dir(temp_folder)
    recreate_dir(args.cache_dir)
    modified_train_file = os.path.join(temp_folder, 'train.txt')
    extract_pii_lines_from_jsonl_files(train_files, modified_train_file, include_newline=False)

    # print(f'looking at combined {modified_train_file} ...')
    # #first make the suffix array
    # cmd = ['python3', './scripts/make_suffix_array.py', modified_train_file]
    # rust_result = subprocess.Popen(cmd).wait()
    # print(rust_result)

    # rust_result = os.popen(f"cargo run memorization-sample --data-file {modified_train_file} --length-threshold {args.length_threshold} --cache-dir {args.cache_dir} --num-threads {os.cpu_count() or 1} --frequency-threshold {args.frequency_threshold}").read()
    # print(f'rust_result for memorization_sample {rust_result}')

    # save_mem_sample_json(f'{args.cache_dir}/mem_sample_ranges_train.txt', modified_train_file)
    # s3_accessor.upload(f"{args.result_dir.strip('/')}/{unique_id}/{args.batch_array_index}-mem_sample.csv", "mem_sample.csv")


def copy_s3_to_local(files, dir):
    print(f'downloading {len(files)} files to {dir} ...')
    for file in files:
        s3_accessor.download_to_local(file, dir)
    
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--train_files', type=str, required=True, help='S3 path to train files. (Provide dir or file)')
    parser.add_argument('--data_dir', type=str, default='./data', help='Local data directory')
    parser.add_argument('--result_dir', type=str, default="s3://anuragik-dev/batch-jobs/", help='result directory remote path')
    parser.add_argument('--batch_array_size', type=int, default=400, help='AWS Batch array size')
    parser.add_argument('--batch_array_index', type=int, default=0, help='AWS Batch array index for a job')
    parser.add_argument('--cache_dir', type=str, default="/tmp/cache", help='cache directory for saving dups and sizes files output by cmd_self_similar')

    args = parser.parse_args()
    train_files_remote_path = args.train_files
    train_files_local_dir = os.path.join(data_dir, 'train')

    train_files = []

    train_bucket, train_key = s3_accessor.getBucketNameAndPrefix(train_files_remote_path)
    
    key_index = 0
    for key in s3_accessor.getNextKey(bucket=train_bucket, prefixes=[train_key], suffixes=['.jsonl','.json']):
        if key_index % args.batch_array_size == args.batch_array_index:
            train_files.append(f"s3://{train_bucket}/{key}")
        key_index+=1
    
    # check if the directories exist
    if os.path.exists(train_files_local_dir):
        shutil.rmtree(train_files_local_dir)

    # create the directories
    os.makedirs(train_files_local_dir, exist_ok=True)

    # copy files from s3 to local
    print(f'going to get {len(train_files)} files from s3')
    copy_s3_to_local(train_files, train_files_local_dir)

    # run the main function
    main(train_files_local_dir, args)