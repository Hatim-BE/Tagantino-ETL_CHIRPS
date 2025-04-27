import os
import sys
import argparse
import logging
import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from utils import setup_logging
from config import AWS_S3_BUCKET, AWS_S3_REGION

logger = setup_logging()

def upload_file_to_s3(file_path, bucket=None, key=None):
    if bucket is None:
        bucket = AWS_S3_BUCKET
        
    if key is None:
        # By default, use filename as the key
        key = os.path.basename(file_path)
    import re
    # Create an S3 client
    s3_client = boto3.client('s3', region_name=AWS_S3_REGION)
    rel_path = os.path.relpath(file_path, start=os.getcwd())
    file_path_chunks = re.split(r'[\\\/]', rel_path)
    try:
        logger.info(f"Uploading {file_path} to S3 bucket {bucket}, key: {key}")
        if file_path_chunks[1] == "daily":
            s3_client.upload_file(file_path, bucket, f"{file_path_chunks[1]}/{file_path_chunks[3]}/{file_path_chunks[4]}")
        else:
            s3_client.upload_file(file_path, bucket, f"{file_path_chunks[1]}/{file_path_chunks[3]}")
        logger.info(f"Successfully uploaded {file_path} to S3")
        return True
    except ClientError as e:
        logger.error(f"Error uploading file to S3: {e}")
        return False

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Upload CSV files to AWS S3'
    )
    parser.add_argument('files', nargs='*', help='CSV files to upload to S3')
    parser.add_argument('--s3-bucket', help='AWS S3 bucket name')
    
    args = parser.parse_args()
    
    successful_uploads = 0
    
    for file_path in args.files:
        s3_key = os.path.basename(file_path)
            
        if upload_file_to_s3(file_path, args.s3_bucket, s3_key):
            successful_uploads += 1
    
    if successful_uploads == len(args.files):
        logger.info(f"Successfully uploaded {successful_uploads} files to S3")
        sys.exit(0)
    else:
        logger.error(f"Failed to upload some files. {successful_uploads}/{len(args.files)} successful")
        sys.exit(1)
