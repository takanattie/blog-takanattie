import sys

import boto3


if len(sys.argv) not in (1, 2):
    print("Usage: python3 listbuckets.py [endpoint_url]")
    sys.exit(1)

endpoint_url = sys.argv[1] if len(sys.argv) == 2 else None

s3 = boto3.client('s3', endpoint_url=endpoint_url)

response = s3.list_buckets()

if 'Buckets' in response:
    for bucket in response['Buckets']:
        print(bucket['Name'])
