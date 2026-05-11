FROM apache/spark:3.5.3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gdal-bin \
    libgdal-dev \
    libhdf4-alt-dev \
    libhdf5-dev \
    python3-dev \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir \
    pyyaml==6.0.1 \
    xarray==2023.1.0 \
    netCDF4==1.6.5 \
    h5netcdf==1.1.0 \
    h5py==3.10.0 \
    pyhdf==0.11.4 \
    rasterio==1.3.9 \
    "GDAL==$(gdal-config --version)" \
    pyproj==3.5.0 \
    shapely==2.0.2 \
    numpy==1.24.4 \
    pandas==1.5.3 \
    scikit-learn==1.3.2 \
    lightgbm==4.3.0 \
    xgboost==2.0.3

USER root
