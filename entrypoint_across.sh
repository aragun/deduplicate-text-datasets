#!/bin/bash
# if AWS_BATCH_JOB_ARRAY_INDEX is not set, set it to 0
if [ -z "$AWS_BATCH_JOB_ARRAY_INDEX" ]; then
    export AWS_BATCH_JOB_ARRAY_INDEX=0
fi

# ./entrypoint_across.sh s3://mstar-bedrock-dev/staging/package-10-04-22-v2/train1/ s3://xuerui-dev/pile-c4-book-hq-2/ 200 /tmp/memorization_cache 5 400 0 50
python3 scripts/create_memorization_sample_across.py --train_files $1 --val_files $2 --length_threshold $3 --cache_dir $4 --frequency_threshold $5 --batch_array_size $6 --val_range_start $7 --val_range_end $8 --batch_array_index $AWS_BATCH_JOB_ARRAY_INDEX