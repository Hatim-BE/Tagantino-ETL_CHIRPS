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
    """
    Upload a file to an S3 bucket
    
    Args:
        file_path (str): Path to the file to upload
        bucket (str): Bucket name. If None, uses default from config
        key (str): S3 object key. If None, uses filename
        
    Returns:
        bool: True if file was uploaded, else False
    """
    if bucket is None:
        bucket = AWS_S3_BUCKET
        
    if key is None:
        # By default, use filename as the key
        key = os.path.basename(file_path)
    
    # Create an S3 client
    s3_client = boto3.client('s3', region_name=AWS_S3_REGION)
    
    try:
        logger.info(f"Uploading {file_path} to S3 bucket {bucket}, key: {key}")
        s3_client.upload_file(file_path, bucket, key)
        logger.info(f"Successfully uploaded {file_path} to S3")
        return True
    except ClientError as e:
        logger.error(f"Error uploading file to S3: {e}")
        return False
        
def upload_dir_to_s3(directory, bucket=None, prefix=None, pattern=None):
    """
    Upload all files in a directory to S3
    
    Args:
        directory (str): Directory to upload
        bucket (str): Bucket name. If None, uses default from config
        prefix (str): S3 prefix to add to object keys
        pattern (str): File pattern to match (e.g., ".csv")
        
    Returns:
        int: Number of successfully uploaded files
    """
    if bucket is None:
        bucket = AWS_S3_BUCKET
        
    if prefix is None:
        prefix = ""
    
    uploaded_count = 0
    failed_count = 0
    
    # Walk through the directory
    for root, dirs, files in os.walk(directory):
        for file in files:
            # Skip if pattern is provided and file doesn't match
            if pattern and not file.endswith(pattern):
                continue
                
            file_path = os.path.join(root, file)
            
            # Construct the S3 key by preserving directory structure relative to input directory
            rel_path = os.path.relpath(file_path, directory)
            s3_key = os.path.join(prefix, rel_path).replace("\\", "/")
            
            if upload_file_to_s3(file_path, bucket, s3_key):
                uploaded_count += 1
            else:
                failed_count += 1
    
    logger.info(f"Uploaded {uploaded_count} files to S3, {failed_count} failed")
    return uploaded_count


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Upload CSV files to AWS S3'
    )
    parser.add_argument('files', nargs='*', help='CSV files to upload to S3')
    parser.add_argument('--upload-dir', help='Upload an entire directory to S3')
    parser.add_argument('--s3-bucket', help='AWS S3 bucket name')
    parser.add_argument('--s3-prefix', help='AWS S3 key prefix')
    parser.add_argument('--pattern', default='.csv', help='File pattern to match (default: .csv)')
    
    args = parser.parse_args()
    
    # If upload_dir is specified, upload the entire directory to S3
    if args.upload_dir:
        upload_count = upload_dir_to_s3(
            args.upload_dir,
            bucket=args.s3_bucket,
            prefix=args.s3_prefix,
            pattern=args.pattern
        )
        
        if upload_count > 0:
            logger.info(f"Successfully uploaded {upload_count} files to S3")
            sys.exit(0)
        else:
            logger.error("Failed to upload any files to S3")
            sys.exit(1)
    
    # Otherwise process individual files
    elif args.files:
        successful_uploads = 0
        
        for file_path in args.files:
            # Preserve directory structure for S3 key if prefix is provided
            if args.s3_prefix:
                rel_path = os.path.basename(file_path)
                s3_key = os.path.join(args.s3_prefix, rel_path)
            else:
                s3_key = os.path.basename(file_path)
                
            if upload_file_to_s3(file_path, args.s3_bucket, s3_key):
                successful_uploads += 1
        
        if successful_uploads == len(args.files):
            logger.info(f"Successfully uploaded {successful_uploads} files to S3")
            sys.exit(0)
        else:
            logger.error(f"Failed to upload some files. {successful_uploads}/{len(args.files)} successful")
            sys.exit(1)
    else:
        logger.error("No files or directory specified for upload")
        parser.print_help()
        sys.exit(1)
