"""
Configuration settings for the CHIRPS ETL Pipeline.
"""
import logging
import os

# Base URLs for CHIRPS products
CHIRPS_BASE_URLS = {
    'daily': "https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_daily/tifs/p05/",
    'monthly': "https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_monthly/tifs/"
}

# Logging configuration
LOG_FILE = 'chirps_download.log'
LOG_FORMAT = '[%(levelname)s][%(asctime)s][%(name)s] %(message)s'
LOG_LEVEL = logging.INFO

# Download parameters
DOWNLOAD_CHUNK_SIZE = 8192  # Chunk size for downloads (bytes)
DEFAULT_MAX_RETRIES = 3     # retry attempts
DEFAULT_TIMEOUT = 30        # seconds
DEFAULT_MAX_FAILS = 3       

# Default paths
DEFAULT_DATA_DIR = 'data/raw'

# Security configurations
MAX_FUTURE_DAYS = 30        # Maximum number of days in the future to avoid infinite loops

# File formats
FILE_FORMAT_DAILY = "chirps-v2.0.{year}.{month:02d}.{day:02d}.tif.gz"
FILE_FORMAT_MONTHLY = "chirps-v2.0.{year}.{month:02d}.tif.gz"
DIR_FORMAT_DAILY = "{year}/"
DIR_FORMAT_MONTHLY = ""
MOROCCO_CLIP = "morocco.geojson"

# AWS S3 Configuration
AWS_S3_BUCKET = os.environ.get('AWS_S3_BUCKET', 'chirps-data')
AWS_S3_REGION = os.environ.get('AWS_S3_REGION', 'us-east-1')

# Initialize logging configuration
def setup_logging():
    """
    Configure the logging system for the application.
    """
    logging.basicConfig(
        level=LOG_LEVEL,
        format=LOG_FORMAT,
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger('CHIRPSLoader')
