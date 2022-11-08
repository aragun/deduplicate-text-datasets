#!/bin/bash
# if AWS_BATCH_JOB_ARRAY_INDEX is not set, set it to 0
if [ -z "$AWS_BATCH_JOB_ARRAY_INDEX" ]; then
    export AWS_BATCH_JOB_ARRAY_INDEX=0
fi

python3 scripts/lookup_pii_sample.py --train_files $1 --pii_lookup_file $2 --batch_array_size $3 --batch_array_index $AWS_BATCH_JOB_ARRAY_INDEX