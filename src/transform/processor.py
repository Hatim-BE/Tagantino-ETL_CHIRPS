import os
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import rasterio
import numpy as np
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from utils import (
    decompress_gz_file,
    clip_raster,
    setup_logging,
    parse_chirps_filename
)

logger = setup_logging()

def raster_to_csv(input_tif, output_csv):
    """
    Convert a raster TIF file to CSV format.
    
    Args:
        input_tif (str): Path to the input TIF file
        output_csv (str): Path to the output CSV file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Open the clipped raster
        with rasterio.open(input_tif) as src:
            # Read raster data
            data = src.read(1)

            # Get coordinates for each pixel
            height, width = data.shape
            cols, rows = np.meshgrid(np.arange(width), np.arange(height))
            xs, ys = rasterio.transform.xy(src.transform, rows, cols)

            # Get date from filename
            filename = os.path.basename(input_tif)
            date_info = parse_chirps_filename(filename)
            date = date_info['date']

            # Create DataFrame
            df = pd.DataFrame({
                'date': date,
                'latitude': np.array(ys).flatten(),
                'longitude': np.array(xs).flatten(),
                'precipitation': data.flatten()
            })

            # Remove NoData values
            df = df[df['precipitation'] != src.nodata]

            # Create directories if they don't exist
            os.makedirs(os.path.dirname(output_csv), exist_ok=True)
            
            # Save to CSV
            df.to_csv(output_csv, index=False)
            logger.info(f"Saved {len(df)} records to {output_csv}")
            return True
    except Exception as e:
        logger.error(f"Error converting {input_tif} to CSV: {e}")
        return False

def process_file(file_path, decompress=True, clip=False, convert_to_csv=True):
    """
    Process a single CHIRPS file by decompressing, optionally clipping, and converting to CSV.
    
    Args:
        file_path (str): Path to the compressed file
        decompress (bool): Whether to decompress the file
        clip (bool): Whether to clip the file to Morocco
        convert_to_csv (bool): Whether to convert the TIF to CSV
        
    Returns:
        str: Path to the processed file, or None if processing failed
    """
    processed_file = None
    
    # Skip processing if the file is not a .gz file and we're asked to decompress
    if decompress and not file_path.endswith('.gz'):
        logger.info(f"File already decompressed, skipping: {file_path}")
        processed_file = file_path
    
    # Decompress .gz files
    elif decompress and file_path.endswith('.gz'):
        decompressed_file = file_path[:-3]  # Remove .gz extension
        
        if os.path.exists(decompressed_file):
            logger.info(f"Decompressed file already exists: {decompressed_file}")
            processed_file = decompressed_file
        else:
            if decompress_gz_file(file_path, decompressed_file):
                # Delete compressed file after successful decompression
                os.remove(file_path)
                processed_file = decompressed_file
            else:
                return None
    else:
        processed_file = file_path
    
    if clip and processed_file and processed_file.endswith('.tif'):
        clip_raster(processed_file)
    
    # Convert TIF to CSV if requested
    if convert_to_csv and processed_file and processed_file.endswith('.tif'):
        # Create CSV path with same directory structure but in processed/ directory
        base_dir = os.path.dirname(processed_file)
        filename = os.path.basename(processed_file)
        csv_filename = os.path.splitext(filename)[0] + '.csv'
        
        # Replace 'raw' with 'processed' in the path
        if 'raw' in base_dir:
            csv_dir = base_dir.replace('raw', 'csv')
        else:
            csv_dir = os.path.join('csv', base_dir)
            
        csv_path = os.path.join(csv_dir, csv_filename)
        
        # Convert to CSV
        if raster_to_csv(processed_file, csv_path):
            return csv_path
    
    return processed_file


def process_files(file_paths, decompress=True, clip=False, convert_to_csv=True, max_workers=4):
    """
    Process multiple CHIRPS files in parallel.
    
    Args:
        file_paths (list): List of file paths to process
        decompress (bool): Whether to decompress files
        clip (bool): Whether to clip files to Morocco
        convert_to_csv (bool): Whether to convert TIFs to CSVs
        max_workers (int): Maximum number of worker threads
        
    Returns:
        list: List of paths to processed files
    """
    processed_files = []
    
    logger.info(f"Processing {len(file_paths)} files (decompress={decompress}, clip={clip}, convert_to_csv={convert_to_csv})")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(process_file, file_path, decompress, clip, convert_to_csv): file_path
            for file_path in file_paths
        }
        
        for future in as_completed(future_to_file):
            original_file = future_to_file[future]
            try:
                processed_file = future.result()
                if processed_file:
                    processed_files.append(processed_file)
            except Exception as e:
                logger.error(f"Error processing {original_file}: {e}")
    
    return processed_files


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Process CHIRPS files by decompressing, clipping, and converting to CSV'
    )
    parser.add_argument('files', nargs='+', help='Files to process')
    parser.add_argument('--no-decompress', action='store_false', dest='decompress',
                       help='Skip decompression of .gz files')
    parser.add_argument('--clip', action='store_true',
                       help='Clip rasters to Morocco boundaries')
    parser.add_argument('--no-csv', action='store_false', dest='convert_to_csv',
                       help='Skip conversion to CSV')
    parser.add_argument('--workers', type=int, default=4,
                       help='Number of worker threads')
    parser.add_argument('--s3-upload', action='store_true',
                       help='Upload processed files to S3')
    parser.add_argument('--s3-bucket', help='AWS S3 bucket name')
    parser.add_argument('--s3-prefix', help='AWS S3 key prefix')
    
    args = parser.parse_args()
    
    processed_files = process_files(
        args.files,
        decompress=args.decompress,
        clip=args.clip,
        convert_to_csv=args.convert_to_csv,
        max_workers=args.workers
    )
    
    logger.info(f"Processed {len(processed_files)} files")
    
    # If s3-upload flag is set, upload the processed files
    if args.s3_upload and processed_files:
        try:
            from src.load.loader import upload_file_to_s3
            
            success_count = 0
            for file_path in processed_files:
                if file_path.endswith('.csv'):
                    # Preserve directory structure for S3 key if prefix is provided
                    if args.s3_prefix:
                        rel_path = os.path.basename(file_path)
                        s3_key = os.path.join(args.s3_prefix, rel_path)
                    else:
                        s3_key = os.path.basename(file_path)
                    
                    if upload_file_to_s3(file_path, args.s3_bucket, s3_key):
                        success_count += 1
            
            logger.info(f"Uploaded {success_count}/{len(processed_files)} files to S3")
        except ImportError:
            logger.error("S3 loader module not found or failed to import") 