#!/bin/bash

## Run the nowcasting pipeline

# 1. Prepare the Environment for the run
source /Users/vrobledodelgado/miniconda3/etc/profile.d/conda.sh
conda activate /Dedicated/Humberto/TITO/env/tito_env

# 2. Run orchestrator

python3 orchestrator.py Cuba_config.py

conda deactivate

