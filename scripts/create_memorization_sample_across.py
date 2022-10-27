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

# if AWS_BATCH_JOB_ID exists, use it as unique id else use uuid
unique_id = os.environ.get('AWS_BATCH_JOB_ID', str(uuid.uuid4())).split(':')[0]

import s3_accessor
from tqdm import tqdm

data_dir = '/tmp/data'
temp_folder = '/tmp/memorization'
content_column = "text"
filter_columns = ["identity_attack", "insult","obscene","severe_toxicity","sexual_explicit","threat","toxicity"]
filter_threshold = 0.5
val_line_map = {}

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
def extract_lines_from_jsonl_files(files, output_file, include_newline=True):
    line_count = 0
    with open(output_file, 'wb') as of:
        for file in tqdm(files):
            with open(file, mode="r", encoding="utf-8") as f:
                with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mmap_in:
                    while True:
                        line = mmap_in.readline()
                        if not line:
                            break
                        json_line = json.loads(line)
                        if not any(column in json_line and float(json_line[column]) >= filter_threshold for column in filter_columns):
                            write_line(json_line[content_column], of)

def recreate_dir(dir):
    if os.path.exists(dir):
        shutil.rmtree(dir)
    os.makedirs(dir, exist_ok=True)

def save_mem_sample_json(byterange, filename):
    if not os.path.isfile(byterange):
        print(f'{byterange} does not exist, cannot create a sample!')
        return

    branges = open(byterange, "rb").read()
    data=open(filename,"rb").read()

    def get_range(l, r):
        try:
            ans = data[l:r].decode()
        except UnicodeDecodeError as ex:
            # print(f"{ex}")
            return None
        return ans

    samples = defaultdict(int)

    with open(byterange, "r") as file:
        for line in tqdm(file):
            line = line.rstrip().split()
            l, r = int(line[0]), int(line[1])
            # bytes [l, l+length_threshold] are duplicated 
            sample = get_range(l, r)
            if sample is not None:
                samples[sample] += 1
            
    
    print(f'total samples {len(list(samples))}, writing to mem_sample.csv')
    keys = random.sample(list(samples), min(len(list(samples)), 200))

    with open('mem_sample.csv', 'w', newline='') as csvfile:
        fieldnames = ['frequency', 'sample']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for k in keys:
            writer.writerow({'frequency': samples[k], 'sample': k})


def main(train_files_path, val_files_path, args):
    train_files = get_files(train_files_path)
    print(f'train_files {train_files}')

    val_files = get_files(val_files_path)
    
    #TODO TEST HACK - only look at 10 val files
    val_files = val_files[:100]

    modified_train_file = os.path.join(temp_folder, 'train.txt')
    extract_lines_from_jsonl_files(train_files, modified_train_file, include_newline=False)

    modified_val_file = os.path.join(temp_folder, 'val.txt')
    extract_lines_from_jsonl_files(val_files, modified_val_file)

    print(f'building suffix array for {modified_train_file} ...')
    start = time.time()
    cmd = ['python3', './scripts/make_suffix_array.py', modified_train_file]
    rust_result = subprocess.Popen(cmd).wait()
    print(rust_result)
    print(f'suffix array built in {time.time() - start} seconds')
    # TODO: also output size of the suffix array

    print(f'building suffix array for {modified_val_file} ...')
    start = time.time()
    cmd = ['python3', './scripts/make_suffix_array.py', modified_val_file]
    rust_result = subprocess.Popen(cmd).wait()
    print(rust_result)
    print(f'suffix array built in {time.time() - start} seconds')
    # TODO: also output size of the suffix array

    rust_result = os.popen(f"cargo run memorization-sample-across --data-file-1 {modified_train_file} --data-file-2 {modified_val_file} --length-threshold {args.length_threshold} --cache-dir {args.cache_dir} --num-threads {os.cpu_count() or 1} --frequency-threshold {args.frequency_threshold}").read()
    print(f'rust_result for memorization_sample {rust_result}')

    save_mem_sample_json(f'{args.cache_dir}/mem_sample_ranges_val.txt', modified_val_file)
    # s3_accessor.upload(f"{args.result_dir.strip('/')}/{unique_id}/{args.batch_array_index}-mem_sample.csv", "mem_sample.csv")


def copy_s3_to_local(files, dir):
    print(f'downloading {len(files)} files to {dir} ...')
    for file in files:
        s3_accessor.download_to_local(file, dir)
    
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--train_files', type=str, required=True, help='S3 path to train files. (Provide dir or file)')
    parser.add_argument('--val_files', type=str, required=True, help='S3 path to val files. (Provide dir or file)')
    parser.add_argument('--data_dir', type=str, default='./data', help='Local data directory')
    parser.add_argument('--result_dir', type=str, default="s3://anuragik-dev/batch-jobs/", help='result directory remote path')
    parser.add_argument('--batch_array_size', type=int, default=100, help='AWS Batch array size')
    parser.add_argument('--batch_array_index', type=int, default=0, help='AWS Batch array index for a job')
    parser.add_argument('--length_threshold', type=int, default=200, help='Minimum length threshold in bytes for sampling')
    parser.add_argument('--cache_dir', type=str, default="/tmp/cache", help='cache directory for saving dups and sizes files output by cmd_self_similar')
    parser.add_argument('--frequency_threshold', type=int, default=5, help='Maximum frequency threshold for sampling')    

    args = parser.parse_args()
    train_files_remote_path = args.train_files
    train_files_local_dir = os.path.join(data_dir, 'train')
    val_files_remote_path = args.val_files
    val_files_local_dir = os.path.join(data_dir, 'val')

    train_files = []
    val_files = [] 

    train_bucket, train_key = s3_accessor.getBucketNameAndPrefix(train_files_remote_path)
    val_bucket, val_key = s3_accessor.getBucketNameAndPrefix(val_files_remote_path)
    val_files =[f"s3://{val_bucket}/{key}" for key in s3_accessor.getNextKey(bucket=val_bucket, prefixes=[val_key], suffixes=['.jsonl','.json'])]
    
    key_index = 0
    for key in s3_accessor.getNextKey(bucket=train_bucket, prefixes=[train_key], suffixes=['.jsonl','.json']):
        if key_index % args.batch_array_size == args.batch_array_index:
            train_files.append(f"s3://{train_bucket}/{key}")
        key_index+=1
    
   
    # make temp folder
    print(f'recreating {temp_folder, args.cache_dir, train_files_local_dir, val_files_local_dir}')
    for d in [temp_folder, args.cache_dir, train_files_local_dir, val_files_local_dir]:
        recreate_dir(d)

    # copy files from s3 to local
    print(f'going to get {len(train_files)} train files, {len(val_files)} val files from s3')
    copy_s3_to_local(train_files, train_files_local_dir)
    # copy_s3_to_local(val_files, val_files_local_dir)

    # run the main function
    main(train_files_local_dir, '/home/anuragik/data/pile-c4-hq', args)