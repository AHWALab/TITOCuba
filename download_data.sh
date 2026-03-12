#!/bin/bash

echo "Downloading data files from Zenodo..."

# Download the zip archive from Zenodo (this is a 1GB file, will take some time)
ZENODO_URL="https://zenodo.org/api/records/18663123/files-archive"
ARCHIVE_FILE="18663123.zip"

echo "Downloading archive from $ZENODO_URL..."
echo "This is a 1GB file and may take several minutes..."

# Remove any existing archive to ensure a fresh download
if [ -f "$ARCHIVE_FILE" ]; then
    echo "Removing existing archive..."
    rm -f "$ARCHIVE_FILE"
fi

curl -L --progress-bar -o "$ARCHIVE_FILE" "$ZENODO_URL"

if [ ! -f "$ARCHIVE_FILE" ]; then
    echo "Error: Failed to download the archive."
    exit 1
fi

echo "Download complete. Extracting archives..."

# Extract the main archive to get individual zip files
unzip -q "$ARCHIVE_FILE"

# Extract basic.zip to basic/ folder
if [ -f "basic.zip" ]; then
    echo "Replacing basic/ folder..."
    rm -rf basic/
    unzip -q basic.zip -d basic/
else
    echo "Warning: basic.zip not found in the archive."
fi

# Extract DA_Climatology.zip to DA_Climatology/ folder
if [ -f "DA_Climatology.zip" ]; then
    echo "Replacing DA_Climatology/ folder..."
    rm -rf DA_Climatology/
    unzip -q DA_Climatology.zip -d DA_Climatology/
else
    echo "Warning: DA_Climatology.zip not found in the archive."
fi

# Extract parameters.zip to parameters/ folder
if [ -f "parameters.zip" ]; then
    echo "Replacing parameters/ folder..."
    rm -rf parameters/
    unzip -q parameters.zip -d parameters/
else
    echo "Warning: parameters.zip not found in the archive."
fi

echo "Cleaning up downloaded zip files..."
rm -f "$ARCHIVE_FILE"
rm -f basic.zip
rm -f DA_Climatology.zip
rm -f parameters.zip

echo "Data download and extraction complete!"
