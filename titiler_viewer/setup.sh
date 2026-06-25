#!/bin/bash
# =============================================================================
# TiTiler Viewer — Setup Script
# =============================================================================
# Installs npm dependencies and verifies the environment.
# Run:  bash setup.sh
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "════════════════════════════════════════════"
echo " AHWA TiTiler Viewer — Setup"
echo "════════════════════════════════════════════"

# ── Check Node.js / npm ───────────────────────────────────────────────────
if ! command -v node &>/dev/null; then
    echo "❌ ERROR: Node.js is not installed."
    echo "   Install Node.js 18+ from https://nodejs.org or via your package manager."
    echo "   e.g.:  curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -"
    echo "          sudo apt-get install -y nodejs"
    exit 1
fi

if ! command -v npm &>/dev/null; then
    echo "❌ ERROR: npm is not installed (usually ships with Node.js)."
    exit 1
fi

NODE_VER=$(node --version | sed 's/v//' | cut -d'.' -f1)
echo "✅ Node.js $(node --version) detected"

if [ "$NODE_VER" -lt 18 ]; then
    echo "⚠️  WARNING: Node.js 18+ recommended. You have $(node --version)."
fi

# ── Install dependencies ───────────────────────────────────────────────────
echo ""
echo "📦 Installing npm dependencies..."
npm install

# ── Verify key files ───────────────────────────────────────────────────────
echo ""
echo "🔍 Verifying project structure..."

REQUIRED_FILES=(
    "package.json"
    "vite.config.js"
    "index.html"
    "src/main.js"
    "src/App.svelte"
    "src/lib/config.js"
    "src/lib/titiler.js"
    "src/lib/gaugePoints.js"
    "src/lib/utils.js"
    "src/components/Map.svelte"
    "src/components/ProductSelector.svelte"
    "src/components/TimestepControls.svelte"
    "src/components/LayerToggle.svelte"
    "src/components/StatusBar.svelte"
    "src/components/GaugePopup.svelte"
    "public/gaugePoints.txt"
)

ALL_OK=true
for f in "${REQUIRED_FILES[@]}"; do
    if [ -f "$f" ]; then
        echo "   ✅ $f"
    else
        echo "   ❌ MISSING: $f"
        ALL_OK=false
    fi
done

if [ "$ALL_OK" = false ]; then
    echo "❌ Some required files are missing. Please check the project structure."
    exit 1
fi

# ── Check TiTiler API ──────────────────────────────────────────────────────
echo ""
echo "🔌 Checking TiTiler API (http://localhost:2000/health)..."
if curl -sf http://localhost:2000/health > /dev/null 2>&1; then
    echo "   ✅ TiTiler API is running on port 2000"
else
    echo "   ⚠️  TiTiler API is NOT reachable at http://localhost:2000"
    echo "   Start it first:  cd ../titiler_api && bash start.sh"
fi

# ── Done ───────────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════"
echo " Setup Complete!"
echo ""
echo " To start the viewer:"
echo "   cd $(pwd)"
echo "   npm run dev"
echo ""
echo " Then open: http://localhost:3000"
echo "════════════════════════════════════════════"
