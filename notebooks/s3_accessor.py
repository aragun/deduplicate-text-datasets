import boto3
import json
from s3urls import parse_url
import logging
import sys

LOG = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

s3 = boto3.resource('s3')

def read_json(s3_url_string):
    LOG.info("Reading manifest from " + s3_url_string)
    s3_url = parse_url(s3_url_string);    
    content_object = s3.Object(s3_url['bucket'], s3_url['key'])
    file_content = content_object.get()['Body'].read().decode('utf-8')
    return json.loads(file_content)

def getLocalNameFromS3(s3_key):
    print(s3_key)
    file_chunks = s3_key.split("/")
    return file_chunks[-1]

def download_to_local(s3_url_string, local_dir, file_prefix=""):
    s3_url = parse_url(s3_url_string)
    file_name = getLocalNameFromS3(s3_url['key'])
    local_file_path = local_dir + "/" + file_prefix + file_name
    LOG.info("Dowloading " + s3_url_string + " to " + local_file_path)
    s3.Bucket(s3_url['bucket']).download_file(s3_url['key'], local_file_path)
    return local_file_path

def upload(s3_url_string, local_file):
    LOG.info("Uploading " + local_file + " to " + s3_url_string)
    s3_url = parse_url(s3_url_string)
    s3.meta.client.upload_file(local_file, s3_url['bucket'], s3_url['key'], ExtraArgs={"ACL": "bucket-owner-full-control"})
    #bucket = s3.Bucket(s3_url['bucket'])
    #with open(local_file, 'rb') as f:
        #bucket.put_object(Key=s3_url['key'], ACL='bucket-owner-full-control', Body = f)

def write_file_changes(debug_path, file_changes, local_to_S3_map):
    LOG.info("Writing file changes to s3")
    s3_file_changes = {}
    for local_path, s3_path in local_to_S3_map.items():
        if local_path in file_changes:
            s3_file_changes[s3_path] = list(file_changes[local_path])
    write_debug_json_file(debug_path, s3_file_changes, "file_change.json")

def write_debug_json_file(debug_path, data, file_name):
    s3_path = "/".join([debug_path, file_name])
    local_path = "/tmp/" + file_name
    with open(local_path, 'w') as outfile:
        json.dump(data, outfile, indent=1)
    upload(s3_path, local_path)

def write_debugfiles(s3_debug_path, local_path_files):
    for local_path_file in local_path_files:
        s3_file_name = local_path_file.split("/")[-1]
        s3_path = "/".join([s3_debug_path, s3_file_name])
        upload(s3_path, local_path_file)

def _nextS3Object(bucket, prefixes = None, suffixes = None):
    s3 = boto3.client("s3")

    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {'Bucket': bucket}

    if prefixes is None:
        prefixes = ('', )
    elif isinstance(prefixes, str):
        prefixes = (prefixes, )
    else:
        prefixes = prefixes

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix
        for page in paginator.paginate(**kwargs):
            contents = []
            try:
                contents = page["Contents"]
            except KeyError:
                break
            for obj in contents:
                key = obj["Key"]
                if suffixes is None:
                    yield obj
                else:
                    for suffix in suffixes:
                        if key.endswith(suffix):
                            yield obj

def getNextKey(bucket, prefixes = None, suffixes = None):
    for obj in _nextS3Object(bucket, prefixes, suffixes):
        yield obj["Key"]

def getBucketNameAndPrefix(s3_url_string):
    s3_url = parse_url(s3_url_string)
    return s3_url['bucket'], s3_url['key']