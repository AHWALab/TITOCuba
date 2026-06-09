#!/bin/bash
# =============================================================================
# TiTiler API — One-time Setup & Test
# =============================================================================
# Creates the conda environment, installs dependencies, and runs a quick
# verification that all product directories are accessible.
#
# Usage:
#   bash setup_and_test.sh
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "============================================"
echo " AHWA TiTiler API — Setup & Test"
echo "============================================"

# ── 1. Create conda environment ──────────────────────────────────────────
echo ""
echo "📦 Step 1: Creating conda environment 'titiler-ahwa'..."
if conda env list | grep -q "titiler-ahwa"; then
    echo "   Environment already exists. To recreate: conda env remove -n titiler-ahwa"
else
    conda env create -f environment.yml
    echo "   ✅ Environment created."
fi

# ── 2. Verify Python dependencies ────────────────────────────────────────
echo ""
echo "🔍 Step 2: Verifying Python dependencies..."
CONDA_PYTHON="$(conda run -n titiler-ahwa which python)"
echo "   Python: $CONDA_PYTHON"

conda run -n titiler-ahwa python -c "
import rasterio; print(f'   rasterio {rasterio.__version__} ✅')
import numpy; print(f'   numpy {numpy.__version__} ✅')
from PIL import Image; print(f'   Pillow ✅')
import fastapi; print(f'   fastapi {fastapi.__version__} ✅')
"

# ── 3. Test config loading ───────────────────────────────────────────────
echo ""
echo "⚙️  Step 3: Testing config loading..."
conda run -n titiler-ahwa python -c "
from config import PRODUCTS, PRODUCT_BY_ID
print(f'   Products defined: {len(PRODUCTS)}')
for p in PRODUCTS:
    import os
    exists = '✅' if os.path.isdir(p.path) else '⚠️  MISSING'
    print(f'   {p.id}: {exists} ({p.path})')
"

# ── 4. Test file scanning ────────────────────────────────────────────────
echo ""
echo "📁 Step 4: Testing file scanning (dry run)..."
conda run -n titiler-ahwa python -c "
import re, os
from datetime import datetime
from config import PRODUCTS

for p in PRODUCTS:
    if not os.path.isdir(p.path):
        print(f'   {p.id}: SKIP (directory not found)')
        continue
    tifs = [f for f in os.listdir(p.path) if f.endswith('.tif')]
    parsed = 0
    for f in tifs[:5]:  # test first 5
        m = re.search(p.filename_regex, f)
        if m:
            parsed += 1
    print(f'   {p.id}: {len(tifs)} tifs, regex matched {parsed}/{min(5, len(tifs))} samples')
"

# ── 5. Summary ───────────────────────────────────────────────────────────
echo ""
echo "============================================"
echo " Setup complete!"
echo ""
echo " To start the server:"
echo "   conda activate titiler-ahwa"
echo "   cd ${SCRIPT_DIR}"
echo "   bash start.sh"
echo ""
echo " Then open: http://localhost:8000/docs"
echo "============================================"
