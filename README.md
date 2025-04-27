# Tagantino-ETL_CHIRPS

Extract, Transform, and Load pipeline for CHIRPS (Climate Hazards Group InfraRed Precipitation with Station data) precipitation datasets, with Airflow automation support.

---

## 🌍 Overview

This project is a lightweight, modular ETL pipeline designed for acquiring and processing satellite-based rainfall data (CHIRPS). It is especially tailored for regional climate analysis, particularly drought monitoring in Morocco or other specified areas.

Main features:
- 📥 Download CHIRPS rainfall data (daily/monthly)
- 🗂️ Automatically organize and preprocess raster data
- 🧮 Process and clip raster files to geospatial boundaries
- 🔄 Convert to CSV format for analytics
- ☁️ Upload processed data to AWS S3
- 🔄 Airflow integration for scheduled and automated workflows

CHIRPS is a 30+ year quasi-global rainfall dataset (1981–present) that combines satellite imagery with in-situ data, supporting high-resolution climate research.

---

## 🚀 Installation

### Requirements
- Python 3.8 or newer
- GDAL-compatible raster stack (rasterio, geopandas, numpy, etc.)
- AWS CLI and `boto3` (for cloud storage modules)
- Apache Airflow (for workflow automation)

### Steps
```bash
# Clone the repo
git clone https://github.com/Hatim-BE/Tagantino-ETL_CHIRPS.git
cd Tagantino-ETL_CHIRPS

# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# For Airflow setup with Docker
docker-compose up -d
```

### AWS CLI Installation and Setup

Follow the official AWS CLI installation instructions for your operating system:

#### Linux
```bash
# Download the AWS CLI installer
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip

# Install the AWS CLI
sudo ./aws/install

# Verify the installation
aws --version
```

#### macOS
```bash
# Download the AWS CLI installer
curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"

# Install the AWS CLI
sudo installer -pkg ./AWSCLIV2.pkg -target /

# Verify the installation
aws --version
```

#### Windows
1. Download and run the AWS CLI MSI installer for Windows (64-bit):
   https://awscli.amazonaws.com/AWSCLIV2.msi

2. Follow the on-screen instructions to complete the installation.

3. Verify the installation by opening a command prompt and running:
   ```
   aws --version
   ```

After installing the AWS CLI, configure your AWS credentials:
```bash
aws configure
```

You'll be prompted to enter:
- AWS Access Key ID
- AWS Secret Access Key
- Default region name (e.g., us-east-1)
- Default output format (json recommended)

For more detailed installation instructions, see the [AWS CLI User Guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

---

## 📁 Project Structure
```
Tagantino-ETL_CHIRPS/
├── LICENSE
├── airflow/                 # Airflow configuration
│   ├── dags/
│   │   └── chirps_etl_dag.py  # Main Airflow DAG for ETL pipeline
├── test/
│   └── test.py              # Simple unit tests
├── config.py                # Customizable runtime config (S3, GeoJSON path, etc.)
├── docker-compose.yml       # Docker setup for Airflow
├── requirements.txt         # Python package dependencies
├── utils.py                 # General utility functions
├── src/
│   ├── extract/
│   │   └── chirps_downloader.py  # Handles downloading CHIRPS data
│   ├── transform/
│   │   └── processor.py     # Raster operations (decompress, clip, convert)
│   └── load/
│       └── loader.py        # Handles upload to AWS S3
├── data/                    # Will be auto-created
│   ├── daily/raw/           # Raw daily CHIRPS data
│   ├── monthly/raw/         # Raw monthly CHIRPS data
│   └── csv/                 # Processed CSV data
├── morocco.geojson          # Boundary file used for clipping
├── run_etl.py               # Command-line ETL runner
├── terraform/               # Terraform infrastructure as code
│   └── s3.tf                # S3 bucket configuration
├── .gitignore               # Excludes large files and credentials
└── README.md                # Project guide and usage
```

---

## ⚙️ Usage

### Command Line ETL Runner
```bash
# Run complete ETL pipeline
python run_etl.py --start-date 2023-01-01 --end-date 2023-01-31 --data-type daily

# Download only
python run_etl.py --start-date 2023-01-01 --end-date 2023-01-31 --data-type daily --skip-transform --skip-load
```

### S3 Bucket Setup

#### Option 1: Using AWS Console
1. Create an S3 bucket:
   - Go to the [AWS S3 Console](https://console.aws.amazon.com/s3/)
   - Click "Create bucket"
   - Enter a globally unique bucket name (e.g., "my-chirps-data")
   - Select the region closest to you
   - Configure options (public access, versioning, encryption) as needed
   - Create the bucket

#### Option 2: Using Terraform (Infrastructure as Code)
1. Create a `name.tf` file with the following content:
   ```hcl
   provider "aws" {
     region = "us-east-1"  # Change to your preferred region
   }
   
   resource "aws_s3_bucket" "chirps_data" {
     bucket = "my-chirps-data"  # Change to your preferred bucket name
     
     tags = {
       Name        = "CHIRPS Data Bucket"
     }
   }
   ------- OPTIONAL -------
   # Configure bucket access control
   resource "aws_s3_bucket_public_access_block" "chirps_data_access" {
     bucket = aws_s3_bucket.chirps_data.id
     
     block_public_acls       = true
     block_public_policy     = true
     ignore_public_acls      = true
     restrict_public_buckets = true
   }
   
   # Enable versioning (optional)
   resource "aws_s3_bucket_versioning" "chirps_data_versioning" {
     bucket = aws_s3_bucket.chirps_data.id
     versioning_configuration {
       status = "Enabled"
     }
   }
   
   # Create folder structure (optional)
   resource "aws_s3_object" "processed_daily_folder" {
     bucket  = aws_s3_bucket.chirps_data.id
     key     = "processed/daily/"
     content = ""
   }
   
   resource "aws_s3_object" "processed_monthly_folder" {
     bucket  = aws_s3_bucket.chirps_data.id
     key     = "processed/monthly/"
     content = ""
   }
   
   # Output the bucket name
   output "bucket_name" {
     value = aws_s3_bucket.chirps_data.bucket
   }
   ```

3. Initialize Terraform and apply the configuration:
   ```bash
   terraform init
   terraform plan
   terraform apply
   ```

4. After confirmation, Terraform will create the S3 bucket and output its name.

5. To destroy the infrastructure when no longer needed:
   ```bash
   terraform destroy
   ```

### Configure the Project to Use Your Bucket

After creating your S3 bucket using either method, configure the project to use it:

- modify the `config.py` file:
  ```python
  # Update this line
  AWS_S3_BUCKET = os.environ.get('AWS_S3_BUCKET', 'my-chirps-data')
  ```

### S3 Data Organization
The project organizes data in S3 with the following structure:
```
my-chirps-data/
├── daily/           
└── monthly/          
```

### Using Airflow DAGs
The project includes an Airflow DAG that automates extract and transform steps of the pipeline:

1. Set Airflow variables:
   - `data_type`: 'daily' or 'monthly'
   - `start_date`: Start date in format 'YYYY-MM-DD' (daily) or 'YYYY-MM' (monthly)
   - `end_date`: End date in same format

2. Run the DAG through Airflow UI:
   - Navigate to http://localhost:8080 after starting Docker
   - Trigger the 'chirps_etl_by_file_grouped' DAG manually or set up scheduling

3. Loading to S3:
   - Currently performed using the command-line interface
   - Future enhancement to add S3 loading step to the Airflow DAG

---

## 🔄 ETL Pipeline Details

### 1. Extract
Downloads CHIRPS data files from the UCSB Climate Hazards Group server:
- Daily or monthly precipitation raster files
- Compressed GeoTIFF format (.tif.gz)
- Africa regional data at 0.05° resolution

### 2. Transform
Processes the downloaded raster files:
- Decompresses the GZ files
- Clips to Morocco (or other region defined by GeoJSON)
- Converts to CSV format for analytics

### 3. Load
Uploads the processed data to AWS S3 (via command-line interface):
- Preserves directory structure
- Options for single file or directory upload
- Configurable S3 bucket and prefix

---

## 🤖 Airflow Integration

The project uses Apache Airflow to automate parts of the ETL workflow:

1. **Current DAG Structure**:
   - Task groups for each date to process
   - Sequential extract→transform for each file
   - Parallel processing across multiple dates

2. **Configuration**:
   - Configuration via Airflow variables
   - Dynamic task creation based on date ranges
   - Error handling and retries

3. **Planned Enhancements**:
   - Adding the load step to complete the ETL pipeline in Airflow
   - More complex workflows with branching and dependencies
   - Automating data quality checks

---

## 📖 Documentation

- **CHIRPS**: https://www.chc.ucsb.edu/data/chirps
- **Apache Airflow**: https://airflow.apache.org/docs/
- **GDAL**: https://gdal.org/
- **Rasterio**: https://rasterio.readthedocs.io/
- **Boto3 for S3**: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
- **AWS CLI**: https://docs.aws.amazon.com/cli/latest/userguide/
- **Terraform**: https://developer.hashicorp.com/terraform/docs

---

## 📄 License

MIT License. See the [LICENSE](./LICENSE) file for full terms.

---

## 🙏 Acknowledgements

- [Climate Hazards Group (UCSB)](https://www.chc.ucsb.edu/)
- GDAL, rasterio, and other geospatial Python tools
- AWS S3 for scalable data storage
- Apache Airflow for workflow automation

---

## 🤝 Contributing

Feel free to fork and submit PRs for bugfixes, enhancements, or new modules. Please include tests and update the README accordingly.

---

## 🧠 Future Enhancements
- Add S3 loading step to Airflow DAG to complete the ETL pipeline
- Add rainfall anomaly maps
- Implement SPI (Standardized Precipitation Index) calculation
- Integrate temperature datasets (e.g. MODIS)
- Generate drought bulletins as PDF reports
- Support country-level SPI maps via dashboards (e.g. Streamlit)
- Add more complex Airflow workflows with branching and dependencies

---
