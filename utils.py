#!/usr/bin/env python
"""
Utility functions for the CHIRPS ETL pipeline.

This module contains various helper functions used throughout the pipeline,
particularly for the extraction process of CHIRPS precipitation data.
"""
import os
import gzip
import shutil
import requests
from datetime import datetime
import re
from dateutil.relativedelta import relativedelta

# Import configuration if the file is used directly
try:
    from config import (
        DEFAULT_MAX_RETRIES,
        DEFAULT_TIMEOUT,
        DOWNLOAD_CHUNK_SIZE,
        FILE_FORMAT_DAILY,
        FILE_FORMAT_MONTHLY,
        DIR_FORMAT_DAILY,
        DIR_FORMAT_MONTHLY,
        setup_logging
    )
    logger = setup_logging()
except ImportError:
    # If imported from elsewhere, assume logger is configured
    import logging
    logger = logging.getLogger('CHIRPSLoader')
    # Default values
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_TIMEOUT = 30
    DOWNLOAD_CHUNK_SIZE = 8192


def decompress_gz_file(gz_path, output_path):
    """
    Decompress a gzip file.
    
    Args:
        gz_path (str): Path to the compressed file.
        output_path (str): Path to save the decompressed file.
        
    Returns:
        bool: True if decompression was successful, False otherwise.
    """
    try:
        with gzip.open(gz_path, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                
        logger.info(f"File successfully decompressed: {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error decompressing {gz_path}: {e}")
        return False


def validate_date(date_str, data_type):
    """
    Validate and parse a date according to the appropriate format.
    
    Args:
        date_str (str): Date in format 'YYYY-MM-DD' (daily) or 'YYYY-MM' (monthly)
        data_type (str): Data type ('daily' or 'monthly')
    
    Returns:
        datetime: Datetime object representing the date
    
    Raises:
        ValueError: If the date is invalid
    """
    try:
        if data_type == 'daily':
            return datetime.strptime(date_str, '%Y-%m-%d')
        return datetime.strptime(date_str, '%Y-%m')
    except ValueError as e:
        logger.critical(f"Invalid date: {date_str} ({e})")
        raise ValueError(f"Invalid date: {date_str} ({e})")


def generate_paths(date_obj, data_type):
    """
    Generate URL and file paths for a given date.
    
    Args:
        date_obj (datetime): Date for which to generate paths
        data_type (str): Data type ('daily' or 'monthly')
    
    Returns:
        tuple: (directory_path, file_name)
    """
    if data_type == 'daily':
        return (
            DIR_FORMAT_DAILY.format(year=date_obj.year),
            FILE_FORMAT_DAILY.format(year=date_obj.year, month=date_obj.month, day=date_obj.day)
        )
    return (
        DIR_FORMAT_MONTHLY,
        FILE_FORMAT_MONTHLY.format(year=date_obj.year, month=date_obj.month)
    )


def download_with_retry(url, dest_path, max_retries=DEFAULT_MAX_RETRIES):
    """
    Download a file with retry system in case of failure.
    
    Args:
        url (str): URL of the file to download
        dest_path (str): Local path to save the file
        max_retries (int): Maximum number of attempts in case of failure
    
    Returns:
        bool: True if the download was successful, False otherwise
    """
    for attempt in range(max_retries):
        try:
            with requests.get(url, stream=True, timeout=DEFAULT_TIMEOUT) as r:
                if r.status_code == 404:
                    return False  # File not found
                r.raise_for_status()
                with open(dest_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                        f.write(chunk)
                logger.info(f"Download successful: {os.path.basename(dest_path)}")
                return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.info(f"File not found: {os.path.basename(dest_path)}")
                return False
            logger.warning(f"Attempt {attempt+1}/{max_retries} failed: {e}")
        except Exception as e:
            logger.warning(f"Attempt {attempt+1}/{max_retries} failed: {e}")
    return False


def parse_chirps_filename(filename):
    """
    Parse date information from a CHIRPS filename.
    
    Args:
        filename (str): The CHIRPS filename to parse
        
    Returns:
        dict: Dictionary containing parsed date information (year, month, day if available)
        
    Raises:
        ValueError: If the filename doesn't match the expected pattern
    """
    # Example formats:
    # Daily: chirps-v2.0.2023.01.15.tif.gz
    # Monthly: chirps-v2.0.2023.01.tif.gz
    
    # Try daily format first
    daily_pattern = r'chirps-v2\.0\.(\d{4})\.(\d{2})\.(\d{2})\.tif(?:\.gz)?'
    daily_match = re.match(daily_pattern, filename)
    
    if daily_match:
        year, month, day = map(int, daily_match.groups())
        return {
            'year': year,
            'month': month,
            'day': day,
            'is_daily': True,
            'date': datetime(year, month, day)
        }
    
    # Try monthly format
    monthly_pattern = r'chirps-v2\.0\.(\d{4})\.(\d{2})\.tif(?:\.gz)?'
    monthly_match = re.match(monthly_pattern, filename)
    
    if monthly_match:
        year, month = map(int, monthly_match.groups())
        return {
            'year': year,
            'month': month,
            'is_daily': False,
            'date': datetime(year, month, 1)
        }
    
    raise ValueError(f"Could not parse date from CHIRPS filename: {filename}")
