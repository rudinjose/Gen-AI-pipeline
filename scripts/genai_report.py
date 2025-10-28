# genai_report.py
# Reads processed summary CSV from S3, generates short text reports per manufacturer using a Hugging Face model,
# and writes text files to report/ prefix.
import os, sys, boto3
import pandas as pd
from io import StringIO
from datetime import datetime

# optional: transformers based generation
# The code below expects transformers and torch to be installed.
# It uses a simple pipeline which should work with cpu, but may be slow.
try:
    from transformers import pipeline
    HF_AVAILABLE = True
except Exception as e:
    print('transformers not available, will fallback to template text. Error:', e)
    HF_AVAILABLE = False

def simple_template(row):
    return ("Manufacturer: {manufacturer}. Average price: ${price:.2f}. Average mileage: {mileage:.2f} miles. " 
            "Count of records: {count}.").format(**row)

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print('Usage: genai_report.py <bucket> <processed_prefix> <report_prefix>')
        sys.exit(1)

    bucket = sys.argv[1]
    processed_prefix = sys.argv[2].rstrip('/')
    report_prefix = sys.argv[3].rstrip('/')

    s3 = boto3.client('s3')

    # find latest processed csv
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=f"{processed_prefix}/")
    if 'Contents' not in resp:
        print('No processed files found under', processed_prefix)
        sys.exit(0)

    csv_keys = [o['Key'] for o in resp['Contents'] if o['Key'].lower().endswith('.csv')]
    if not csv_keys:
        print('No CSVs in processed prefix.')
        sys.exit(0)

    # choose the latest by key (timestamped name)
    csv_keys.sort(reverse=True)
    latest = csv_keys[0]
    print('Using processed file:', latest)
    obj = s3.get_object(Bucket=bucket, Key=latest)
    body = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(body))

    # ensure columns
    for col in ['manufacturer','price','mileage','count']:
        if col not in df.columns:
            print('Missing expected column:', col)
            sys.exit(1)

    reports = []
    if HF_AVAILABLE:
        # using a small model; change as desired. This will attempt CPU inference.
        gen = pipeline('text-generation', model='google/flan-t5-base', device=-1)

    for _, row in df.iterrows():
        context = simple_template(row.to_dict())
        if HF_AVAILABLE:
            # prompt the model to expand into a short analyst-style summary
            prompt = f"Summarize the following manufacturer summary into 1-2 sentences for a business report: {context}"
            out = gen(prompt, max_length=128, do_sample=False)
            text = out[0]['generated_text'] if isinstance(out, list) else str(out)
        else:
            text = context + ' (HF not available locally; install transformers to enable AI summaries)'

        key = f"{report_prefix}/{row['manufacturer'].replace(' ','_')}_report_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.txt"
        print('Writing report to s3://{}/{}'.format(bucket, key))
        s3.put_object(Bucket=bucket, Key=key, Body=text.encode('utf-8'))
        reports.append(key)

    print('Generated reports:', reports)
