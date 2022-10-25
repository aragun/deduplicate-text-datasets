import s3_accessor
import argparse
import pandas as pd
from tqdm import tqdm

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--result_dir', type=str, required=True, help='result directory with all the mem_samples')
    parser.add_argument('--experiment_name', type=str, required=True, help='name of the experiment')
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
    
    print(f'dtypes {all_data.dtypes}')

    all_data = all_data.convert_dtypes()
    print(f'dtypes {all_data.dtypes}')

    all_data['sample_len'] = all_data['sample'].str.len()
    
    print(f'{all_data.shape[0]} samples from {files_read} files')
    print(f'{all_data.head(5)}')
    print(f'{all_data.sample_len.describe()}')

    all_data.drop_duplicates(subset=['sample'], inplace=True)

    print(f'{all_data.shape[0]} samples after dropping duplicates')
   
    sample_size = 5000
    filename = f'{args.experiment_name}_{sample_size}_mem_sample.csv'
    print(f'sampling {sample_size} into {filename}')

    all_data.sample(n=sample_size).to_csv(filename)
    s3_accessor.upload(f"s3://anuragik-dev/mem_samples/{filename}", filename)
