# Tagantino-ETL_CHIRPS

Extract, Transform, and Load pipeline for CHIRPS (Climate Hazards Group InfraRed Precipitation with Station data) precipitation datasets.

---

## 🌍 Overview

This project is a lightweight, modular ETL pipeline designed for acquiring and processing satellite-based rainfall data (CHIRPS). It is especially tailored for regional climate analysis, particularly drought monitoring in Morocco or other specified areas.

Main features:
- 📥 Download CHIRPS rainfall data (daily/monthly)
- 🗂️ Automatically organize and preprocess raster data
- 🧮 Calculate SPI (Standardized Precipitation Index)
- 🔍 Identify and analyze drought events
- ☁️ Upload and store processed data to AWS S3 (in progress)

CHIRPS is a 30+ year quasi-global rainfall dataset (1981–present) that combines satellite imagery with in-situ data, supporting high-resolution climate research.

---

## 🚀 Installation

### Requirements
- Python 3.8 or newer
- GDAL-compatible raster stack (rasterio, geopandas, numpy, etc.)
- AWS CLI and `boto3` (for cloud storage modules)

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
```

---

## 📁 Project Structure
```
Tagantino-ETL_CHIRPS/
├── LICENSE
├── test/
│   └── test.py               # Simple unit tests
├── config.py                 # Customizable runtime config (S3, GeoJSON path, etc.)
├── requirements.txt          # Python package dependencies
├── utils.py                  # General utility functions
├── src/
│   ├── extract/
│   │   ├── chirps_downloader.py  # Handles downloading, decompressing, clipping
│   │   └── utils.py              # Helper methods
│   ├── transform/
│   │   ├── raster_processor.py  # Raster operations (clip, merge, reproject) [WIP]
│   │   └── spi_calculator.py    # SPI computation and drought detection [WIP]
│   └── load/
│       └── s3_uploader.py       # Handles upload to AWS S3 [WIP]
├── data/                    # Will be auto-created
│   └── raw/                 # Raw CHIRPS data
├── morocco.geojson          # Boundary file used for clipping
├── .gitignore               # Excludes large files and credentials
└── README.md                # Project guide and usage
```

---

## ⚙️ Usage

### Download CHIRPS Data
```bash
# Download single daily file
python src/extract/chirps_downloader.py --start-date 2025-01-01 --end-date 2025-01-02 --data-type daily

# Download full monthly range
python src/extract/chirps_downloader.py --start-date 2025-01 --end-date 2025-12 --data-type monthly

# Download & preprocess 10 days
python src/extract/chirps_downloader.py --start-date 2025-01-01 --count 10 --data-type daily --decompress --clip

# Continuous download (until unavailable)
python src/extract/chirps_downloader.py --start-date 2020-01-01 --data-type daily --decompress --output-dir data/custom_path
```

### Transform: Raster Processing (Planned)
The `raster_processor.py` module will offer:
- 💾 Reading/writing GeoTIFFs
- ✂️ Clipping rasters to boundaries
- 📐 Reprojecting rasters
- 🧩 Merging multiple tiles
- 📊 Computing mean, sum, or custom stats

### Analyze: SPI & Droughts (Planned)
The `spi_calculator.py` will:
- Load raster data into `xarray`
- Compute SPI (1, 3, 6-months)
- Extract severity classes (mild, moderate, severe)
- Flag drought events based on SPI threshold

---

## 🧪 Example Notebooks (Planned)
Add your own notebooks or testing scripts in `notebooks/` to:
- Visualize raw and clipped rasters
- Plot SPI time series
- Map severe drought periods

---

## 📖 Documentation

- **CHIRPS**: https://www.chc.ucsb.edu/data/chirps
- **SPI (WMO Guide)**: https://library.wmo.int/index.php?lvl=notice_display&id=13682
- **GDAL**: https://gdal.org/
- **Rasterio**: https://rasterio.readthedocs.io/
- **Boto3 for S3**: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html

---

## 📄 License

MIT License. See the [LICENSE](./LICENSE) file for full terms.

---

## 🙏 Acknowledgements

- [Climate Hazards Group (UCSB)](https://www.chc.ucsb.edu/)
- GDAL, rasterio, xarray, and other geospatial Python tools
- AWS S3 for scalable data storage

---

## 🤝 Contributing

Feel free to fork and submit PRs for bugfixes, enhancements, or new modules. Please include tests and update the README accordingly.

---

## 🧠 Ideas for Expansion
- Add rainfall anomaly maps
- Integrate temperature datasets (e.g. MODIS)
- Enable scheduled runs with Airflow - a must for production
- Generate drought bulletins as PDF reports
- Support country-level SPI maps via dashboards (e.g. Streamlit)

---
