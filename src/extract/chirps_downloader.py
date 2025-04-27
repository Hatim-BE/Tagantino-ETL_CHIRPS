import json
import os
import sys
import argparse
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from config import (
    CHIRPS_BASE_URLS,
    DEFAULT_MAX_FAILS,
    MAX_FUTURE_DAYS,
    DEFAULT_DATA_DIR,
    setup_logging
)

from utils import (
    validate_date,
    generate_paths,
    download_with_retry
)

from src.load.loader import upload_file_to_s3

logger = setup_logging()


def download_chirps_data(start_date, end_date, data_type, output_dir, 
                         indefinite_mode=False, max_fails=DEFAULT_MAX_FAILS):
    """
    Download CHIRPS data within a date range.
    
    Args:
        start_date (datetime): Start date for download
        end_date (datetime): End date for download (None if indefinite_mode is True)
        data_type (str): Type of data ('daily' or 'monthly')
        output_dir (str): Output directory for downloaded files
        indefinite_mode (bool): If True, download until consecutive failures
        max_fails (int): Maximum consecutive failures before stopping in indefinite mode
        
    Returns:
        list: List of paths to downloaded files
    """
    current_date = start_date
    consecutive_fails = 0
    downloaded_files = []
    
    # Define a limit date to avoid infinite loops
    max_future_date = datetime.now() + timedelta(days=MAX_FUTURE_DAYS)
    
    while True:
        if not indefinite_mode and current_date > end_date:
            break
            
        # Check that we don't exceed an excessive future date
        if current_date > max_future_date:
            logger.warning(f"Download stopped: excessive future date reached ({current_date.strftime('%Y-%m-%d')})")
            break
            
        dir_path, file_name = generate_paths(current_date, data_type)
        full_url = f"{CHIRPS_BASE_URLS[data_type]}{dir_path}{file_name}"
        dest_dir = os.path.join(output_dir, dir_path)
        os.makedirs(dest_dir, exist_ok=True)
        tif_file = os.path.join(dest_dir, file_name)
        print(tif_file.removesuffix(".gz"))
        if not os.path.exists(tif_file.removesuffix(".gz")):
            success = download_with_retry(full_url, tif_file)
            
            if success:
                consecutive_fails = 0
                downloaded_files.append(tif_file)
            else:
                consecutive_fails += 1
                logger.warning(f"Download failed {consecutive_fails}/{max_fails}")
                
                if indefinite_mode and consecutive_fails >= max_fails:
                    logger.info(f"Download stopped after {max_fails} consecutive failures")
                    break
        else:
            logger.info(f"File already exists, skipping download: {tif_file}")
            downloaded_files.append(tif_file)

        if data_type == 'daily':
            current_date += timedelta(days=1)
        else:
            current_date += relativedelta(months=1)
            
    return downloaded_files


def parse_arguments():
    
    parser = argparse.ArgumentParser(
        prog='CHIRPS Downloader',
        description='Download CHIRPS data from the UC Santa Barbara Climate Hazards Center'
    )
    parser.add_argument('--start-date', required=True, 
                        help='Start date (YYYY-MM-DD for daily, YYYY-MM for monthly)')
    parser.add_argument('--end-date', 
                        help='End date (optional)')
    parser.add_argument('--data-type', choices=['daily', 'monthly'], default='daily',
                        help='Type of data to download (daily or monthly)')
    parser.add_argument('--output-dir',
                        help='Destination directory')
    parser.add_argument('--count', type=int,
                        help='Number of days/months to download after start-date')
    parser.add_argument('--max-fails', type=int, default=DEFAULT_MAX_FAILS,
                        help="Number of consecutive failures before stopping the indefinite download")
    parser.add_argument('--transform', action='store_true',
                        help='Pass downloaded files to the transformation step')
    parser.add_argument('--s3-upload', action='store_true', 
                        help='Upload processed files to S3')
    parser.add_argument('--s3-bucket',
                        help='AWS S3 bucket name')
    parser.add_argument('--s3-prefix',
                        help='AWS S3 key prefix')
    
    return parser.parse_args()


def main():
    args = parse_arguments()
    
    if not args.output_dir:
        args.output_dir = f'{DEFAULT_DATA_DIR}/{args.data_type}'
    
    date_start = validate_date(args.start_date, args.data_type)
    
    if args.end_date:
        date_end = validate_date(args.end_date, args.data_type)
        indefinite_mode = False
        logger.info(f"Downloading from {args.start_date} to {args.end_date}")
    elif args.count:
        if args.data_type == 'daily':
            date_end = date_start + timedelta(days=args.count - 1)
        else:
            date_end = date_start + relativedelta(months=args.count - 1)
        indefinite_mode = False
        logger.info(f"Downloading {args.count} {args.data_type} from {args.start_date}")
    else:
        logger.info(f"Indefinite download starting from {args.start_date}")
        date_end = None
        indefinite_mode = True

    
    downloaded_files = download_chirps_data(
        start_date=date_start,
        end_date=date_end,
        data_type=args.data_type,
        output_dir=args.output_dir,
        indefinite_mode=indefinite_mode,
        max_fails=args.max_fails
    )
    
    logger.info(f"Download completed. {len(downloaded_files)} files downloaded/already existed.")
    
    # If transform flag is set, pass files to transformation stage
    if args.transform and downloaded_files:
        try:
            from src.transform.processor import process_files
            transformed_files = process_files(downloaded_files, clip=True, decompress=True, convert_to_csv=True)
            logger.info(f"Transformation completed. {len(transformed_files)} files processed.")
            
            # If s3-upload flag is set, upload the processed CSV files
            if args.s3_upload and transformed_files:
                try:
                    # Filter only CSV files for upload
                    csv_files = [f for f in transformed_files if f.endswith('.csv')]
                    
                    if csv_files:
                        # Check if files are in a common directory
                        dirs = set(os.path.dirname(f) for f in csv_files)
                        
                        if len(dirs) == 1 and all(os.path.isfile(f) for f in csv_files):
                            # If all files are in the same directory, use upload_dir_to_s3
                            csv_dir = dirs.pop()
                            upload_count = upload_dir_to_s3(
                                csv_dir,
                                bucket=args.s3_bucket,
                                prefix=args.s3_prefix,
                                pattern='.csv'
                            )
                            if upload_count > 0:
                                logger.info(f"Successfully uploaded {upload_count} files to S3")
                            else:
                                logger.error("Failed to upload any files to S3")
                        else:
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
                except ImportError:
                    logger.error("S3 upload module not found or failed to import")
        except ImportError:
            logger.error("Transformation module not found or failed.")


if __name__ == '__main__':
    main() 