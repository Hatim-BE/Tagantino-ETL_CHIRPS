import os
import sys
import argparse
import logging
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import gzip
import shutil
from datetime import timedelta
from dateutil.relativedelta import relativedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import setup_logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s][%(asctime)s][%(name)s] %(message)s',
    handlers=[
        logging.FileHandler('chirps_download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('CHIRPSLoader')

CHIRPS_BASE_URLS = {
    'daily': "https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_daily/tifs/p05/",
    'monthly': "https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_monthly/tifs/"
}


def decompress_gz_file(gz_path, output_path):
    """
    Decompress gzip files
    
    Args:
        gz_path (str)
        output_path (str)
        
    Returns:
        bool
    """
    try:
        with gzip.open(gz_path, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                
        logger.info(f"File decompressed: {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error during decompression {gz_path}: {e}")
        return False
    

def validate_date(date_str, data_type):
    try:
        if data_type == 'daily':
            return datetime.strptime(date_str, '%Y-%m-%d')
        return datetime.strptime(date_str, '%Y-%m')
    except ValueError as e:
        logger.critical(f"Date invalide: {date_str} ({e})")
        sys.exit(1)

def generate_paths(date_obj, data_type):
    """path generator"""
    if data_type == 'daily':
        return (
            f"{date_obj.year}/",
            f"chirps-v2.0.{date_obj.year}.{date_obj.month:02d}.{date_obj.day:02d}.tif.gz"
        )
    return (
        "",
        f"chirps-v2.0.{date_obj.year}.{date_obj.month:02d}.tif.gz"
    )

def download_with_retry(url, dest_path, max_retries=3):
    for attempt in range(max_retries):
        try:
            with requests.get(url, stream=True, timeout=30) as r:
                if r.status_code == 404:
                    return False
                r.raise_for_status()
                with open(dest_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info(f"Upload successful: {os.path.basename(dest_path)}")
                return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.info(f"File not found: {os.path.basename(dest_path)}")
                return False
            logger.warning(f"Try {attempt+1}/{max_retries} failed: {e}")
        except Exception as e:
            logger.warning(f"Try {attempt+1}/{max_retries} failed: {e}")
    return False

def main():
    parser = argparse.ArgumentParser(prog='CHIRPS Downloader')
    parser.add_argument('--start-date', required=True)
    parser.add_argument('--end-date', help='Optional for a date range')
    parser.add_argument('--data-type', choices=['daily', 'monthly'], default='daily')
    parser.add_argument('--output-dir')
    parser.add_argument('--decompress', action='store_true')
    parser.add_argument('--count', type=int, help='Number of days/months after start-date')
    parser.add_argument('--max-fails', type=int, default=3)
    args = parser.parse_args()

    if not args.output_dir:
        args.output_dir = f'data/{args.data_type}'

    date_start = validate_date(args.start_date, args.data_type)
    
    if args.end_date:
        date_end = validate_date(args.end_date, args.data_type)
        indefinite_mode = False
    elif args.count:
        if args.data_type == 'daily':
            date_end = date_start + timedelta(days=args.count - 1)
        else:
            date_end = date_start + relativedelta(months=args.count - 1)
        logger.info(f"Downloadinf {args.count} {args.data_type} from {args.start_date}")
        indefinite_mode = False
    else:
        logger.info(f"No end date mentionned, starting from {args.start_date} untill stoppage")
        date_end = None  # Will be determined dynamically
        indefinite_mode = True

    current_date = date_start
    consecutive_fails = 0
    
    max_future_date = datetime.now() + timedelta(days=30)
    
    while True:
        if not indefinite_mode and current_date > date_end:
            break
            
        if current_date > max_future_date:
            logger.warning(f"stopping download : future date achieved: ({current_date.strftime('%Y-%m-%d')})")
            break
            
        dir_path, file_name = generate_paths(current_date, args.data_type)
        full_url = f"{CHIRPS_BASE_URLS[args.data_type]}{dir_path}{file_name}"
        dest_dir = os.path.join(args.output_dir, dir_path)
        os.makedirs(dest_dir, exist_ok=True)

        success = download_with_retry(full_url, os.path.join(dest_dir, file_name))
        
        if success:
            consecutive_fails = 0
            if args.decompress:
                try:
                    compressed_file = os.path.join(dest_dir, file_name)
                    decompressed_file = compressed_file[:-3]
                    
                    with gzip.open(compressed_file, 'rb') as f_in:
                        with open(decompressed_file, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    
                    if os.path.exists(decompressed_file) and os.path.getsize(decompressed_file) > 0:
                        logger.info(f"Decompression done: {os.path.basename(decompressed_file)}")
                        
                        # Optional: Deleting the .gz
                        os.remove(compressed_file)
                    else:
                        logger.error(f"Decompression error: file not found - {os.path.basename(decompressed_file)}")
                except Exception as e:
                    logger.error(f"Decompression error of {file_name}: {e}")
        else:
            consecutive_fails += 1
            logger.warning(f"Download error {consecutive_fails}/{args.max_fails}")
            
            if indefinite_mode and consecutive_fails >= args.max_fails:
                logger.info(f"Download stopped after {args.max_fails} fails")
                break
        
        if args.data_type == 'daily':
            current_date += timedelta(days=1)
        else:
            current_date += relativedelta(months=1)

if __name__ == '__main__':
    main()