"""
CHIRPS ETL Runner Script

This script demonstrates the complete ETL pipeline for CHIRPS data:
1. Extract: Download CHIRPS data
2. Transform: Decompress, clip the data, and convert to CSV
3. Load: Upload processed CSV data to AWS S3

Usage:
  python run_etl.py --start-date 2023-01-01 --end-date 2023-01-31 --data-type daily
"""
import argparse
import logging
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from config import setup_logging, DEFAULT_DATA_DIR, DEFAULT_MAX_FAILS, DEFAULT_CSV_DIR
from src import download_chirps_data, process_files, upload_file_to_s3
from utils import validate_date

logger = setup_logging()

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Run the CHIRPS ETL Pipeline'
    )
    parser.add_argument('--start-date', required=True, 
                        help='Start date (YYYY-MM-DD for daily, YYYY-MM for monthly)')
    parser.add_argument('--end-date', 
                        help='End date (optional)')
    parser.add_argument('--data-type', choices=['daily', 'monthly'], default='daily',
                        help='Type of data to download (daily or monthly)')
    parser.add_argument('--raw-dir',
                        help='Directory for raw downloaded files')
    parser.add_argument('--csv-dir',
                        help='Directory for CSV files')
    parser.add_argument('--count', type=int,
                        help='Number of days/months to download after start-date')
    parser.add_argument('--max-fails', type=int, default=DEFAULT_MAX_FAILS,
                        help="Number of consecutive failures before stopping the indefinite download")
    parser.add_argument('--skip-extract', action='store_true',
                        help='Skip extraction step (download)')
    parser.add_argument('--skip-transform', action='store_true',
                        help='Skip transformation step (decompress, clip, CSV conversion)')
    parser.add_argument('--skip-load', action='store_true',
                        help='Skip loading step (S3 upload)')
    parser.add_argument('--no-clip', dest='clip', action='store_false',
                        help='Skip clipping to Morocco')
    parser.add_argument('--no-csv', dest='convert_to_csv', action='store_false',
                        help='Skip conversion to CSV')
    parser.add_argument('--input-files', nargs='*',
                        help='Specify input files for transform/load steps (skips extract)')
    # S3 options
    parser.add_argument('--s3-bucket',
                        help='AWS S3 bucket name')
    parser.add_argument('--s3-prefix',
                        help='AWS S3 key prefix')
    
    return parser.parse_args()

def main():
    args = parse_arguments()
    
    # Set up directories
    if not args.raw_dir:
        args.raw_dir = f'{DEFAULT_DATA_DIR}/{args.data_type}'
    
    if not args.csv_dir:
        args.csv_dir = f'{DEFAULT_CSV_DIR}/{args.data_type}'
    
    # Handle dates
    date_start = validate_date(args.start_date, args.data_type)
    
    if args.end_date:
        date_end = validate_date(args.end_date, args.data_type)
        indefinite_mode = False
        logger.info(f"Processing data from {args.start_date} to {args.end_date}")
    elif args.count:
        if args.data_type == 'daily':
            date_end = date_start + timedelta(days=args.count - 1)
        else:
            date_end = date_start + relativedelta(months=args.count - 1)
            indefinite_mode = False
        logger.info(f"Processing {args.count} {args.data_type} from {args.start_date}")
    else:
        logger.info(f"Processing data starting from {args.start_date} (current date)")
        date_end = None
        indefinite_mode = True

    files_to_process = args.input_files or []
    
    # EXTRACT: Download CHIRPS data
    if not args.skip_extract and not args.input_files:
        logger.info("Starting extraction step (download)")
        try:
            downloaded_files = download_chirps_data(
                start_date=date_start,
                end_date=date_end,
                data_type=args.data_type,
                output_dir=f"data/{args.data_type}/raw",
                indefinite_mode=indefinite_mode,
                max_fails=args.max_fails
            )
            files_to_process = downloaded_files
            logger.info(f"Extraction completed. {len(downloaded_files)} files downloaded/found.")
        except Exception as e:
            logger.error(f"Extraction step failed: {e}")
            return
    elif args.skip_extract:
        logger.info("Skipping extraction step (download)")
    
    if not files_to_process:
        logger.error("No files to process. Extraction step didn't provide any files.")
        return
    
    # TRANSFORM: Decompress, clip data, convert to CSV
    transformed_files = []
    if not args.skip_transform:
        logger.info("Starting transformation step (decompress, clip, CSV conversion)")
        try:
            transformed_files = process_files(
                files_to_process,
                decompress=True,
                clip=args.clip,
                convert_to_csv=args.convert_to_csv
            )
            logger.info(f"Transformation completed. {len(transformed_files)} files processed.")
        except Exception as e:
            logger.error(f"Transformation step failed: {e}")
            return
    else:
        logger.info("Skipping transformation step")
        transformed_files = files_to_process
    
    if not transformed_files:
        logger.error("No files after transformation step.")
        return
    
    # LOAD: Upload to S3
    if not args.skip_load:
        logger.info("Starting loading step (S3 upload)")
        try:
            # Filter only CSV files for upload
            csv_files = [f for f in transformed_files if f.endswith('.csv')]
            
            if csv_files:
                # Otherwise upload files individually
                success_count = 0
                for file_path in csv_files:
                    # Preserve directory structure for S3 key if prefix is provided
                    if args.s3_prefix:
                        rel_path = os.path.basename(file_path)
                        s3_key = os.path.join(args.s3_prefix, rel_path)
                    else:
                        s3_key = os.path.basename(file_path)
                        
                    if upload_file_to_s3(file_path, args.s3_bucket, s3_key):
                        success_count += 1
                
                if success_count == len(csv_files):
                    logger.info(f"Successfully uploaded {success_count} files to S3")
                else:
                    logger.error(f"Failed to upload some files. {success_count}/{len(csv_files)} successful")
            else:
                logger.warning("No CSV files found for S3 upload")
        except Exception as e:
            logger.error(f"Loading step failed: {e}")
    else:
        logger.info("Skipping loading step")
    
    logger.info("ETL pipeline execution completed.")

if __name__ == '__main__':
    main() 