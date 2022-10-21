import s3_accessor
import argparse
import pandas as pd
from tqdm import tqdm

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--result_dir', type=str, required=True, help='result directory with all the mem_samples')
    args = parser.parse_args()

    bucket, key = s3_accessor.getBucketNameAndPrefix(args.result_dir)
    print(f'bucket {bucket}, key {key}')
    all_data = pd.DataFrame(columns=['frequency', 'sample'])
    files_read = 0
    for key in tqdm(s3_accessor.getNextKey(bucket=bucket, prefixes=[key], suffixes=['.csv'])):
        df = pd.read_csv(f's3://{bucket}/{key}')
        files_read += 1
        # print('samples,\n{df.head(2)}')
        all_data = all_data.append(df)
    
    print(f'{all_data.shape[0]} samples from {files_read} files')
    print(f'{all_data.head(5)}')

    all_data.drop_duplicates(subset=['sample'], inplace=True)

    print(f'{all_data.shape[0]} samples after dropping duplicates')
    print(f'{all_data.head(5)}')

    all_data.to_csv('mem_sample_all.csv')