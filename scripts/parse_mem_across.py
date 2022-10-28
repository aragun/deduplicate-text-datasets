import s3_accessor
import argparse
import pandas as pd
from tqdm import tqdm
from collections import defaultdict
import numpy as np

def get_count_from_key(k):
    # key is of the form batch-jobs/across_similar/0e901c98-00fc-4f13-83ec-8280a0f63cd8/9-mem_sample_f_4.csv
    count = int(k.split('.')[0].split('/')[-1].split('_')[-1])
    print(f'key {k}, count {count}')
    return count

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--result_dir', type=str, required=True, help='result directory with all the mem_samples')
    parser.add_argument('--experiment_name', type=str, required=True, help='name of the experiment')
    args = parser.parse_args()

    bucket, key = s3_accessor.getBucketNameAndPrefix(args.result_dir)
    # print(f'bucket {bucket}, key {key}')

    sample_counts = defaultdict(int)

    all_data = pd.DataFrame(columns=['frequency', 'sample', 'count'])
    files_read = 0
    candidates = 0
    for key in tqdm(s3_accessor.getNextKey(bucket=bucket, prefixes=[key], suffixes=['.csv'])):
        print(f'key {key}')
        df = pd.read_csv(f's3://{bucket}/{key}')
        files_read += 1
        count = get_count_from_key(key)
        for _, row in df.iterrows():
            candidates += 1
            sample_counts[row['sample']] += count
        
        # all_data = all_data.append(df)
    
    print(f'files read {files_read}')
    print(f'{len(sample_counts)} unique samples from {candidates} candidates')
    print(f'count values {set(sample_counts.values())}')

    valid_sample = []
    for sample, count in sample_counts.items():
        if count >= 2 and count <= 5:
            valid_sample.append(sample)

    wl = [len(s.split()) for s in valid_sample]
    print(f'word lens {wl}, average = {np.mean(wl)}')

    print(f'{len(valid_sample)} valid samples!') 

    with open('f5samples.txt', 'w') as f:
        for line in valid_sample:
            f.write(f"{line}\n")
    
    # print(f'dtypes {all_data.dtypes}')

    # all_data = all_data.convert_dtypes()
    # print(f'dtypes {all_data.dtypes}')

    # all_data['sample_len'] = all_data['sample'].str.len()
    
    # print(f'{all_data.shape[0]} samples from {files_read} files')
    # print(f'{all_data.head(5)}')
    # print(f'{all_data.sample_len.describe()}')

    # all_data.drop_duplicates(subset=['sample'], inplace=True)

    # print(f'{all_data.shape[0]} samples after dropping duplicates')
   
    # sample_size = 5000
    # filename = f'{args.experiment_name}_{sample_size}_mem_sample.csv'
    # print(f'sampling {sample_size} into {filename}')

    # all_data.sample(n=sample_size).to_csv(filename)
    # s3_accessor.upload(f"s3://anuragik-dev/mem_samples/{filename}", filename)
