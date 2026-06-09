#!/bin/bash
# =============================================================================
# TiTiler Refresh Script — Stage & COG-convert new GeoTIFFs for TiTiler
# =============================================================================
#
# Reads raw TITO pipeline output from:
#   /var/ef5/TITOCuba/outputs/tmp_output_crest  (standard)
#   /var/ef5/TITOCuba/outputs_25m               (high-res depth)
#
# Renames to match existing GeoServer naming (param_YYYYMMDDTHHMMSS.tif),
# converts to Cloud-Optimized GeoTIFF (COG), and places them directly in
# the TiTiler data directories under /var/ef5/geoServer/.
#
# This is a SOFT refresh — no store/layer teardown. Only file operations.
# Skips files that have already been processed (idempotent).
#
# Cron usage:
#   ./manage_cron.sh install titiler_api/refresh_titiler.sh
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="${SCRIPT_DIR}/refresh_titiler_log.txt"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "════════════════════════════════════════════"
echo " TiTiler Refresh — $(date '+%Y-%m-%d %H:%M:%S')"
echo "════════════════════════════════════════════"

# ── Path Configuration (hardcoded) ───────────────────────────────────────
# Source: TITO pipeline raw output directories
SRC_CREST="/var/ef5/TITOCuba/outputs/tmp_output_crest"
SRC_DEPTH="/var/ef5/TITOCuba/outputs_25m"

# Target: TiTiler serves directly from these GeoServer data directories
DATA_ROOT="/var/ef5/geoServer"

# ── Product Mapping ──────────────────────────────────────────────────────
# param → target subdirectory (just the param name, flat under DATA_ROOT)
# e.g. maxq files go to /var/ef5/geoServer/maxq/
declare -A PRODUCTS
PRODUCTS["maxunitq"]=1
PRODUCTS["maxq"]=1
PRODUCTS["qpfaccum"]=1
PRODUCTS["qpeaccum"]=1
PRODUCTS["maxsm"]=1
PRODUCTS["maxdepth"]=1

# ── Helper: COG-convert in-place ─────────────────────────────────────────
convert_to_cog() {
    local tif_file="$1"
    local cog_tmp="${tif_file}.cog_tmp"

    if gdal_translate \
        -of COG \
        -co BLOCKSIZE=512 \
        -co COMPRESS=DEFLATE \
        -co LEVEL=6 \
        -co NUM_THREADS=2 \
        -q \
        "$tif_file" "$cog_tmp" 2>/dev/null; then
        mv "$cog_tmp" "$tif_file"
        return 0
    else
        rm -f "$cog_tmp"
        return 1
    fi
}

# ── Process a source directory ───────────────────────────────────────────
process_source() {
    local src_dir="$1"
    local new_count=0 cog_ok=0 cog_fail=0

    if [[ ! -d "$src_dir" ]]; then
        echo "   ⚠️  Source not found: $src_dir"
        return
    fi

    while IFS= read -r -d '' src_file; do
        local fname param f_date f_time
        fname=$(basename "$src_file")

        # ── Parse TITO pipeline filename ──────────────────────────────
        # Two formats:
        #   Standard: param.date.time.tif        (maxq.20250608.120000.tif)
        #   High-res: param.25m.date.time.tif    (maxdepth.25m.20250608.120000.tif)
        if [[ "$fname" == *".25m."* ]]; then
            param=$(echo "$fname" | cut -d'.' -f1)
            f_date=$(echo "$fname" | cut -d'.' -f3)
            f_time=$(echo "$fname" | cut -d'.' -f4)
        else
            param=$(echo "$fname" | cut -d'.' -f1)
            f_date=$(echo "$fname" | cut -d'.' -f2)
            f_time=$(echo "$fname" | cut -d'.' -f3)
        fi

        # ── Only process known products ───────────────────────────────
        [[ -z "${PRODUCTS[$param]:-}" ]] && continue

        # ── Build target filename with underscore (matches existing files) ──
        # Existing format: maxq_20250608T120000.tif
        local new_name="${param}_${f_date}T${f_time}.tif"
        local dest_dir="${DATA_ROOT}/${param}"
        local dest_file="${dest_dir}/${new_name}"

        mkdir -p "$dest_dir"

        # ── Skip if already processed ─────────────────────────────────
        [[ -f "$dest_file" ]] && continue

        # ── Copy → COG ────────────────────────────────────────────────
        echo "   📋 ${fname} → ${param}/${new_name}"
        cp "$src_file" "$dest_file"
        new_count=$((new_count + 1))

        echo "   🔄 COG: ${new_name}"
        if convert_to_cog "$dest_file"; then
            echo "   ✅ ${new_name}"
            cog_ok=$((cog_ok + 1))
        else
            echo "   ❌ ${new_name} — COG FAILED"
            cog_fail=$((cog_fail + 1))
        fi

    done < <(find "$src_dir" -maxdepth 1 -type f \( -name "*.tif" -o -name "*.tiff" \) -print0 2>/dev/null || true)

    if [[ $new_count -gt 0 ]]; then
        echo "   📊 ${src_dir}: ${new_count} new, ${cog_ok} COG, ${cog_fail} failed"
    fi
}

# ── Main ─────────────────────────────────────────────────────────────────
echo "📂 Scanning TITO output directories..."

process_source "$SRC_CREST"
process_source "$SRC_DEPTH"

# ── Fix permissions ──────────────────────────────────────────────────────
echo "🔐 Fixing permissions on ${DATA_ROOT}..."
chmod -R a+rX "$DATA_ROOT" 2>/dev/null || true

# ── Summary ──────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════"
echo " TiTiler Refresh Complete — $(date '+%Y-%m-%d %H:%M:%S')"
echo "════════════════════════════════════════════"
for param in "${!PRODUCTS[@]}"; do
    dir="${DATA_ROOT}/${param}"
    if [[ -d "$dir" ]]; then
        count=$(find "$dir" -maxdepth 1 -type f -name "*.tif" | wc -l)
        echo "   ${param}: ${count} granules"
    fi
done
