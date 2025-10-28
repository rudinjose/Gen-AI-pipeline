# transform_data.py
# Reads CSV files from S3 raw/ prefix, performs simple transformations, writes CSV to processed/ prefix.
import os, sys, csv, tempfile
import pandas as pd
import boto3
from io import BytesIO, StringIO

# Safe helpers (short versions to match your style)
def sanitize_filename(filename):
    import re
    return re.sub(r'[^a-zA-Z0-9_\.\-]', '_', filename)

if __name__ == '__main__':
    # args: bucket raw_prefix processed_prefix
    if len(sys.argv) < 4:
        print('Usage: transform_data.py <bucket> <raw_prefix> <processed_prefix>')
        sys.exit(1)

    bucket = sys.argv[1]
    raw_prefix = sys.argv[2].rstrip('/')
    processed_prefix = sys.argv[3].rstrip('/')

    s3 = boto3.client('s3')

    # list objects under raw_prefix
    prefix = f"{raw_prefix}/"
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' not in resp:
        print('No files found in raw prefix:', prefix)
        sys.exit(0)

    csv_objects = [obj['Key'] for obj in resp['Contents'] if obj['Key'].lower().endswith('.csv')]
    if not csv_objects:
        print('No CSV files found in raw prefix. Keys present:', [k['Key'] for k in resp.get('Contents', [])])
        sys.exit(0)

    all_df = []
    for key in csv_objects:
        print('Reading s3://{}/{}'.format(bucket, key))
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        try:
            df = pd.read_csv(StringIO(body.decode('utf-8')))
        except Exception as e:
            # try ISO-8859-1
            df = pd.read_csv(StringIO(body.decode('ISO-8859-1')))
        all_df.append(df)

    df = pd.concat(all_df, ignore_index=True)

    # Simple transformations requested:
    # 1) drop duplicates
    # 2) group by manufacturer -> avg price, avg mileage
    df = df.drop_duplicates()

    # Ensure numeric columns are floats
    for col in ['mileage','price']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    agg = df.groupby('manufacturer', as_index=False).agg({
        'price': 'mean',
        'mileage': 'mean',
        'model': 'count'
    }).rename(columns={'model': 'count'})

    # round numeric columns
    agg['price'] = agg['price'].round(2)
    agg['mileage'] = agg['mileage'].round(2)

    out_csv = agg.to_csv(index=False)

    # write to processed prefix as processed_summary_<ts>.csv
    ts = pd.Timestamp.utcnow().strftime('%Y%m%dT%H%M%SZ')
    out_key = f"{processed_prefix}/processed_summary_{ts}.csv"
    print('Writing to s3://{}/{}'.format(bucket, out_key))
    s3.put_object(Bucket=bucket, Key=out_key, Body=out_csv.encode('utf-8'))

    print('Transform complete. Wrote:', out_key)
