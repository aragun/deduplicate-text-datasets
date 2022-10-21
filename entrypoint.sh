#!/bin/bash
# if AWS_BATCH_JOB_ARRAY_INDEX is not set, set it to 0
if [ -z "$AWS_BATCH_JOB_ARRAY_INDEX" ]; then
    export AWS_BATCH_JOB_ARRAY_INDEX=0
fi

python3 scripts/create_memorization_sample.py --train_files s3://mstar-data/datasets/pile-c4-filtered-by-toxicity-with-url-hq-en-books-data/ --length_threshold 400 --cache_dir /tmp/memorization_cache --frequency_threshold 5 --batch_array_index $AWS_BATCH_JOB_ARRAY_INDEX
# python3 scripts/contamination_rate.py --train_files $1 --val_files $2 --result_dir $3 --array_size $4 --array_index $AWS_BATCH_JOB_ARRAY_INDEX