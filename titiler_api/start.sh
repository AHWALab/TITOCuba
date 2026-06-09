#!/bin/bash
# =============================================================================
# TiTiler API — Startup Script
# =============================================================================
# Starts the TiTiler FastAPI server for serving GeoTIFF rasters as XYZ/WMS.
#
# Usage:
#   ./start.sh              # Start on default port 8000
#   ./start.sh 8001         # Start on custom port
#   ./start.sh --reload     # Start with hot-reload for development
#
# First-time setup:
#   conda env create -f environment.yml
#   conda activate titiler-ahwa
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT="${1:-8000}"
RELOAD=""

if [[ "${1:-}" == "--reload" ]] || [[ "${2:-}" == "--reload" ]]; then
    RELOAD="--reload"
    PORT="${1:-8000}"
    [[ "$1" == "--reload" ]] && PORT=8000
fi

# Check conda environment
if [[ "${CONDA_DEFAULT_ENV:-}" != "titiler-ahwa" ]]; then
    echo "⚠️  Conda env 'titiler-ahwa' not active."
    if conda env list | grep -q "titiler-ahwa"; then
        echo "   Activating with: conda activate titiler-ahwa"
    else
        echo "   Creating environment first: conda env create -f ${SCRIPT_DIR}/environment.yml"
        conda env create -f "${SCRIPT_DIR}/environment.yml"
    fi
    echo "   Then run: conda activate titiler-ahwa && bash start.sh"
    exit 1
fi

echo "============================================"
echo "  AHWA TiTiler API Server"
echo "  Port: ${PORT}"
echo "  Config: ${SCRIPT_DIR}/config.py"
echo "  Docs:  http://localhost:${PORT}/docs"
echo "============================================"

cd "${SCRIPT_DIR}"

# Set environment variable for data root (override if needed)
export TITILER_DATA_ROOT="${TITILER_DATA_ROOT:-/home/nammehta/labWork/geoServer/data}"

exec uvicorn main:app \
    --host 0.0.0.0 \
    --port "${PORT}" \
    ${RELOAD} \
    --log-level info
