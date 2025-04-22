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
    DEFAULT_CSV_DIR,
    setup_logging
)

from utils import (
    decompress_gz_file,
    validate_date,
    generate_paths,
    download_with_retry,
    clip_raster,
    raster_to_csv
)

logger = setup_logging()


def download_chirps_data(start_date, end_date, data_type, output_dir, clip, decompress=False, 
                         indefinite_mode=False, max_fails=DEFAULT_MAX_FAILS):
    """
    Download CHIRPS data within a date range.
    
    """
    current_date = start_date
    consecutive_fails = 0
    
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

        if not os.path.exists(tif_file.removesuffix(".gz")):
            success = download_with_retry(full_url, os.path.join(dest_dir, file_name))
            
            if success:
                consecutive_fails = 0
                if decompress:
                    compressed_file = os.path.join(dest_dir, file_name)
                    decompressed_file = compressed_file[:-3]  # Remove the .gz prefix
                    
                    if decompress_gz_file(compressed_file, decompressed_file):
                        os.remove(compressed_file)
                    
                        if clip:
                            clip_raster(decompressed_file)
                        
                        csv_dir = os.path.join(f"{DEFAULT_CSV_DIR}/{data_type}", dir_path)
                        os.makedirs(csv_dir, exist_ok=True)
                        csv_filename = file_name.removesuffix(".tif.gz") + ".csv"
                        csv_file = os.path.join(csv_dir, csv_filename)
                        raster_to_csv(decompressed_file, csv_file)
            else:
                consecutive_fails += 1
                logger.warning(f"Download failed {consecutive_fails}/{max_fails}")
                
                if indefinite_mode and consecutive_fails >= max_fails:
                    logger.info(f"Download stopped after {max_fails} consecutive failures")
                    break
        else:
            logger.info(f"File already exists, skipping download: {tif_file}")

        if data_type == 'daily':
            current_date += timedelta(days=1)
        else:
            current_date += relativedelta(months=1)


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
    parser.add_argument('--decompress', action='store_true',
                        help='Decompress files after download')
    parser.add_argument('--count', type=int,
                        help='Number of days/months to download after start-date')
    parser.add_argument('--max-fails', type=int, default=DEFAULT_MAX_FAILS,
                        help="Number of consecutive failures before stopping the indefinite download")
    parser.add_argument('--clip', action='store_true', help="clipping tif data to morocco")
    
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

    
    download_chirps_data(
        start_date=date_start,
        end_date=date_end,
        data_type=args.data_type,
        output_dir=args.output_dir,
        clip=args.clip,
        decompress=args.decompress,
        indefinite_mode=indefinite_mode,
        max_fails=args.max_fails
    )
    
    logger.info("Download completed")


if __name__ == '__main__':
    main() 