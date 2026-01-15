## Set up config file
# Create conda environment from the tito_env.yml file.
echo "Creating conda environment from tito_env.yml..."
conda env create -f tito_env.yml 

# Initialize conda for bash to enable conda activate
eval "$(conda shell.bash hook)"

# Activate the conda environment
conda activate tito_env

echo "Installing ML libraries..."
cd Nowcast/nowcasting/
pip install -e . 
conda install -y requests
cd ../../

chmod +x pipeline.sh

# mkdir precip/
# mkdir precipEF5/

echo "Environment installed successfully..."

# Download and extract data from Zenodo
echo "Downloading required data files..."
chmod +x download_data.sh
./download_data.sh

echo "Configuring EF5 path..."
chmod +x setup_ef5_path.sh
./setup_ef5_path.sh
