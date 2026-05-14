FROM apache/spark:3.5.3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gfortran \
    gdal-bin \
    libgdal-dev \
    libeccodes-data \
    libhdf4-alt-dev \
    libhdf5-dev \
    libnetcdff-dev \
    python3-dev \
    python3-pip \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Optional HYSPLIT install:
# Place the official NOAA Linux HYSPLIT archive under spark/hysplit/ before
# building. The public NOAA download requires accepting their use agreement.
COPY hysplit/ /tmp/hysplit/
RUN set -eux; \
    mkdir -p /opt/hysplit; \
    archive="$(find /tmp/hysplit -maxdepth 1 -type f \( -name '*.tar.gz' -o -name '*.tgz' -o -name '*.tar' -o -name '*.zip' \) | head -n 1)"; \
    if [ -n "$archive" ]; then \
      case "$archive" in \
        *.zip) unzip -q "$archive" -d /opt/hysplit ;; \
        *.tar|*.tar.gz|*.tgz) tar -xf "$archive" -C /opt/hysplit ;; \
      esac; \
      nested="$(find /opt/hysplit -mindepth 1 -maxdepth 2 -type d -iname 'hysplit*' | head -n 1)"; \
      if [ -n "$nested" ] && [ "$nested" != "/opt/hysplit" ]; then \
        cp -a "$nested"/. /opt/hysplit/; \
      fi; \
      chmod -R a+rx /opt/hysplit; \
      if [ ! -x /opt/hysplit/exec/era5_2arl ] && [ -x /opt/hysplit/exec/era52arl ]; then \
        ln -sf /opt/hysplit/exec/era52arl /opt/hysplit/exec/era5_2arl; \
      fi; \
      test -x /opt/hysplit/exec/era5_2arl; \
    else \
      echo "No HYSPLIT archive found in spark/hysplit; skipping HYSPLIT binary install."; \
    fi; \
    rm -rf /tmp/hysplit

ENV ECCODES_DEFINITION_PATH=/usr/share/eccodes/definitions

RUN rm -rf /root/.cache/pip && \
    python3 -m pip install --no-cache-dir --only-binary=:all: \
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
    lightgbm==3.3.5 && \
    python3 -m pip install --no-cache-dir --only-binary=:all: \
    xgboost==2.0.3

USER root
