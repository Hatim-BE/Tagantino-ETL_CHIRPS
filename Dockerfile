FROM apache/airflow:2.7.3-python3.10

USER root

# Install system dependencies for GDAL and rasterio
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libgdal-dev \
        gdal-bin \
        python3-dev \
        gcc \
        g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Set environment variables for GDAL
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal
ENV GDAL_DATA=/usr/share/gdal

# Install Python packages
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements.txt