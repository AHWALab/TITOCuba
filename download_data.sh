#!/bin/bash

echo "Downloading data files from Zenodo..."

# Download the zip archive from Zenodo (this is a 1GB file, will take some time)
ZENODO_URL="https://zenodo.org/api/records/17716930/files-archive"
ARCHIVE_FILE="17716930.zip"

echo "Downloading archive from $ZENODO_URL..."
echo "This is a 1GB file and may take several minutes..."
curl -L -C - --progress-bar -o "$ARCHIVE_FILE" "$ZENODO_URL"

if [ ! -f "$ARCHIVE_FILE" ]; then
    echo "Error: Failed to download the archive."
    exit 1
fi

echo "Download complete. Extracting archives..."

# Extract the main archive to get individual zip files
unzip -q "$ARCHIVE_FILE"

# Extract basic.zip to basic/ folder
if [ -f "basic.zip" ]; then
    echo "Extracting basic.zip to basic/ folder..."
    unzip -o -q basic.zip -d basic/
else
    echo "Warning: basic.zip not found in the archive."
fi

# Extract DA_Climatology.zip to DA_Climatology/ folder
if [ -f "DA_Climatology.zip" ]; then
    echo "Extracting DA_Climatology.zip to DA_Climatology/ folder..."
    unzip -o -q DA_Climatology.zip -d DA_Climatology/
else
    echo "Warning: DA_Climatology.zip not found in the archive."
fi

# Extract parameters.zip to parameters/ folder
if [ -f "parameters.zip" ]; then
    echo "Extracting parameters.zip to parameters/ folder..."
    unzip -o -q parameters.zip -d parameters/
else
    echo "Warning: parameters.zip not found in the archive."
fi

echo "Cleaning up downloaded zip files..."
rm -f "$ARCHIVE_FILE"
rm -f basic.zip
rm -f DA_Climatology.zip
rm -f parameters.zip

echo "Data download and extraction complete!"
