#!/bin/bash
# if AWS_BATCH_JOB_ARRAY_INDEX is not set, set it to 0
if [ -z "$AWS_BATCH_JOB_ARRAY_INDEX" ]; then
    export AWS_BATCH_JOB_ARRAY_INDEX=0
fi

python3 scripts/create_memorization_sample.py --train_files $1 --length_threshold $2 --cache_dir $3 --frequency_threshold $4 --batch_array_size $5 --batch_array_index $AWS_BATCH_JOB_ARRAY_INDEX