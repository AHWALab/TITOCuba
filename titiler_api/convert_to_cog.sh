#!/bin/bash
# =============================================================================
# COG Converter — Fast Parallel GeoTIFF → Cloud-Optimized GeoTIFF
# =============================================================================
# Uses background-job pool for parallel conversion with nice/ionice
# to avoid being killed by the OOM killer.
#
# Current files: block=(1, N) row-by-row — terrible for tile serving.
# COG files:    block=(512,512) tiled + overviews — 10-100x faster.
#
# Usage:
#   bash convert_to_cog.sh                          # All products, 8 parallel
#   bash convert_to_cog.sh --dry-run                # Preview only
#   bash convert_to_cog.sh --product highResMaxInu  # Single product
#   bash convert_to_cog.sh --parallel 4             # 4 parallel jobs
#   bash convert_to_cog.sh --path /data/Cuba/highResMaxInu  # Specific path
#   bash convert_to_cog.sh --replace                # Replace originals (risky!)
#   bash convert_to_cog.sh --no-resume              # Reconvert everything

# delete the .bak files if they exist (cleanup from previous --replace runs)
#   find /home/nammehta/labWork/geoServer/data/WestAfrica/maxUnitStreamFlow -name '*.bak' -delete
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_ROOT="${TITILER_DATA_ROOT:-/home/nammehta/labWork/geoServer/data}"

# ── Configuration ────────────────────────────────────────────────────────
DRY_RUN=false
RESUME=true
REPLACE=false
TARGET_PRODUCT=""
CUSTOM_PATH=""
NCPU=$(nproc 2>/dev/null || echo 8)
PARALLEL=$(( NCPU > 2 ? NCPU - 2 : 1 ))   # leave 2 cores free for other work
TIMEOUT_PER_FILE=300

# ── Parse args ───────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=true ;;
        --no-resume) RESUME=false ;;
        --replace) REPLACE=true ;;
        --parallel) PARALLEL="${2:-4}"; shift ;;
        --product) TARGET_PRODUCT="${2:-}"; shift ;;
        --path) CUSTOM_PATH="${2:-}"; shift ;;
        --timeout) TIMEOUT_PER_FILE="${2:-300}"; shift ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
    shift
done

# ── Product directories ──────────────────────────────────────────────────
PRODUCT_DIRS=(
    "Cuba/maxUnitStreamFlow"          "Cuba/maxStreamFlow"
    "Cuba/precipAccum"                "Cuba/precipAccumImerg"
    "Cuba/maxSoilSat"                 "Cuba/highResMaxInu"
    "Cuba/highResMaxStreamFlow"       "Cuba/highResMaxUnitStreamFlow"
    "CubaIMERGE/cubaImergeMaxUnitStreamFlow"  "CubaIMERGE/cubaImergeMaxStreamFlow"
    "CubaIMERGE/cubaImergePrecipAccum"        "CubaIMERGE/cubaImergeMaxSoilSat"
    "CubaIMERGE/cubaImergeInu"
    "WestAfrica/maxUnitStreamFlow"    "WestAfrica/maxStreamFlow"
    "WestAfrica/qpeAccum"             "WestAfrica/qpfAccum"
)

# ── State tracking (temp dir cleaned on exit) ────────────────────────────
STATEDIR="$(mktemp -d /tmp/cog_convert_XXXXXX)"
trap 'rm -rf "$STATEDIR"' EXIT

# Counters in files (subshell-safe)
echo "0" > "$STATEDIR/done"
echo "0" > "$STATEDIR/skipped"
echo "0" > "$STATEDIR/failed"
echo "0" > "$STATEDIR/started"

_inc() {
    # Atomic counter increment using a subshell with set -C (noclobber)
    local f="$STATEDIR/$1"
    (
        local val=0
        [[ -f "$f" ]] && read -r val < "$f" 2>/dev/null || true
        echo $((val + 1))
    ) > "$f.tmp" && mv "$f.tmp" "$f" 2>/dev/null || true
}

_counter() { cat "$STATEDIR/$1" 2>/dev/null || echo 0; }

# ── Single-file converter (runs in background subshell) ──────────────────
convert_one() {
    local tif_file="$1" cog_file="$2" orig_size="$3"
    local fname; fname=$(basename "$tif_file" .tif)
    local t0; t0=$(date +%s)

    # nice -n 19: lowest CPU priority
    # ionice -c 3: idle I/O class (only uses disk when nothing else needs it)
    # timeout: kill if hung
    if nice -n 19 ionice -c 3 timeout "$TIMEOUT_PER_FILE" \
        gdal_translate \
            -of COG \
            -co BLOCKSIZE=512 \
            -co COMPRESS=DEFLATE \
            -co LEVEL=6 \
            -co NUM_THREADS=1 \
            -q \
            "$tif_file" "$cog_file" 2>/dev/null; then

        local elapsed; elapsed=$(($(date +%s) - t0))
        local cog_size; cog_size=$(stat -c%s "$cog_file" 2>/dev/null || echo 0)
        local pct="?"
        [[ "$orig_size" -gt 0 && "$cog_size" -gt 0 ]] && \
            pct=$(awk "BEGIN {printf \"%.0f\", $cog_size * 100 / $orig_size}")

        printf "   ✅ %-45s %5ss  %s%%\n" "${fname}" "${elapsed}" "${pct}"
        _inc "done"

        if $REPLACE; then
            mv "$tif_file" "${tif_file}.bak" 2>/dev/null || true
            mv "$cog_file" "$tif_file" 2>/dev/null || true
        fi
    else
        printf "   ❌ %-45s FAILED\n" "${fname}"
        _inc "failed"
        rm -f "$cog_file"
    fi
}

# ═════════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════════

echo "============================================"
echo " COG Converter — GeoTIFF → Cloud-Optimized"
echo "============================================"
echo " Data root:  ${DATA_ROOT}"
echo " Parallel:   ${PARALLEL} concurrent jobs"
echo " Timeout:    ${TIMEOUT_PER_FILE}s per file"
echo " Nice:       yes (CPU + I/O idle priority)"
if $DRY_RUN;  then echo " MODE:       DRY RUN (preview only)"; fi
if $RESUME;   then echo " Resume:     skip existing _cog.tif files"; fi
if $REPLACE;  then echo " Replace:    WILL SWAP originals with COGs"; fi
echo "============================================"
if [[ -n "$CUSTOM_PATH" ]]; then echo " Custom path: ${CUSTOM_PATH}"; fi
echo "============================================"

# ── Replace already-converted files when --replace is set ────────────────
if $REPLACE; then
    echo ""
    echo "🔁 --replace: swapping existing _cog.tif → .tif..."
    swapped=0
    if [[ -n "$CUSTOM_PATH" ]]; then
        dirs_to_check=("$CUSTOM_PATH")
    else
        dirs_to_check=()
        for dir in "${PRODUCT_DIRS[@]}"; do
            [[ -n "$TARGET_PRODUCT" && "$(basename "$dir")" != "$TARGET_PRODUCT" ]] && continue
            [[ -d "${DATA_ROOT}/${dir}" ]] && dirs_to_check+=("${DATA_ROOT}/${dir}")
        done
    fi
    for full_dir in "${dirs_to_check[@]}"; do
        for cog in "$full_dir"/*_cog.tif; do
            [[ ! -f "$cog" ]] && continue
            tif="${full_dir}/$(basename "$cog" _cog.tif).tif"
            if [[ -f "$tif" ]]; then
                mv "$tif" "${tif}.bak" 2>/dev/null || true
                mv "$cog" "$tif" 2>/dev/null || true
                swapped=$((swapped + 1))
            fi
        done
    done
    echo "   Swapped: ${swapped} files (originals → .bak)"
fi

# ── Scan for files ──────────────────────────────────────────────────────
FILE_LIST="$STATEDIR/filelist"
> "$FILE_LIST"

# If custom path provided, use it directly
if [[ -n "$CUSTOM_PATH" ]]; then
    full_dir="$CUSTOM_PATH"
    if [[ ! -d "$full_dir" ]]; then
        echo "❌ Path not found: ${full_dir}"; exit 1
    fi
    dir_count=0
    for tif in "$full_dir"/*.tif; do
        [[ ! -f "$tif" ]] && continue
        [[ "$tif" == *_cog.tif ]] && continue
        cog="${full_dir}/$(basename "$tif" .tif)_cog.tif"
        if $RESUME && [[ -f "$cog" ]]; then
            _inc "skipped"
            continue
        fi
        orig=$(stat -c%s "$tif" 2>/dev/null || echo 0)
        echo "${tif}|${cog}|${orig}" >> "$FILE_LIST"
        dir_count=$((dir_count + 1))
    done
    echo "   ${full_dir}: ${dir_count} files to convert"
else
    for dir in "${PRODUCT_DIRS[@]}"; do
    [[ -n "$TARGET_PRODUCT" && "$(basename "$dir")" != "$TARGET_PRODUCT" ]] && continue
    full_dir="${DATA_ROOT}/${dir}"
    [[ ! -d "$full_dir" ]] && continue

    dir_count=0
    for tif in "$full_dir"/*.tif; do
        [[ ! -f "$tif" ]] && continue
        [[ "$tif" == *_cog.tif ]] && continue

        cog="${full_dir}/$(basename "$tif" .tif)_cog.tif"
        if $RESUME && [[ -f "$cog" ]]; then
            _inc "skipped"
            continue
        fi
        orig=$(stat -c%s "$tif" 2>/dev/null || echo 0)
        echo "${tif}|${cog}|${orig}" >> "$FILE_LIST"
        dir_count=$((dir_count + 1))
    done
    [[ $dir_count -gt 0 ]] && echo "   ${dir}: ${dir_count} files to convert"
done
fi  # end custom-path if/else

TOTAL=$(wc -l < "$FILE_LIST")
SKIPPED=$(_counter "skipped")

# ── Replace already-converted files when --replace is set ────────────────
if $REPLACE && [[ "$SKIPPED" -gt 0 ]]; then
    echo ""
    echo "🔁 Swapping ${SKIPPED} already-converted files (_cog.tif → .tif)..."
    swapped=0
    # Iterate over the same directories
    if [[ -n "$CUSTOM_PATH" ]]; then
        dirs_to_check=("$CUSTOM_PATH")
    else
        dirs_to_check=()
        for dir in "${PRODUCT_DIRS[@]}"; do
            [[ -n "$TARGET_PRODUCT" && "$(basename "$dir")" != "$TARGET_PRODUCT" ]] && continue
            dirs_to_check+=("${DATA_ROOT}/${dir}")
        done
    fi
    for full_dir in "${dirs_to_check[@]}"; do
        [[ ! -d "$full_dir" ]] && continue
        for cog in "$full_dir"/*_cog.tif; do
            [[ ! -f "$cog" ]] && continue
            tif="${full_dir}/$(basename "$cog" _cog.tif).tif"
            if [[ -f "$tif" ]] && [[ ! -f "${tif}.bak" ]]; then
                mv "$tif" "${tif}.bak" && mv "$cog" "$tif" && swapped=$((swapped + 1))
            fi
        done
    done
    echo "   Swapped: ${swapped} files (originals → .bak)"
    # Re-scan after swap so counts are accurate
    > "$FILE_LIST"
    echo "0" > "$STATEDIR/skipped"
fi

echo ""
echo "📊 ${TOTAL} files to convert, ${SKIPPED} already done"

[[ "$TOTAL" -eq 0 ]] && { echo "Nothing to do!"; exit 0; }

if $DRY_RUN; then
    echo ""
    echo "🏃 Dry run — would convert (first 10):"
    head -10 "$FILE_LIST" | while IFS='|' read -r tif cog orig; do
        echo "   $(basename "$tif")"
    done
    exit 0
fi

# ── Parallel conversion with job pool ────────────────────────────────────
echo ""
echo "🚀 Starting ${PARALLEL} parallel jobs..."
echo ""
START_TIME=$(date +%s)

active=0
while IFS='|' read -r tif cog orig; do
    # Wait if pool is full
    while [[ $active -ge $PARALLEL ]]; do
        wait -n 2>/dev/null || true
        active=$((active - 1))
    done
    convert_one "$tif" "$cog" "$orig" &
    active=$((active + 1))
    _inc "started"
done < "$FILE_LIST"

# Wait for remaining
wait 2>/dev/null || true

# ── Summary ─────────────────────────────────────────────────────────────
ELAPSED=$(($(date +%s) - START_TIME))
DONE=$(_counter "done")
FAILED=$(_counter "failed")

echo ""
echo "============================================"
echo " ✅ Complete in ${ELAPSED}s (${PARALLEL} parallel)"
echo "    Converted: ${DONE}"
echo "    Skipped:   ${SKIPPED}"
echo "    Failed:    ${FAILED}"
[[ "$DONE" -gt 0 ]] && echo "    Rate:      ~$(awk "BEGIN {printf \"%.0f\", $DONE / ($ELAPSED / 60)}") files/min"
echo "============================================"

if $REPLACE && [[ "$DONE" -gt 0 ]]; then
    echo ""
    echo "⚠️  Originals saved as .bak — verify rendering, then:"
    echo "   find ${DATA_ROOT} -name '*.bak' -delete"
elif [[ "$DONE" -gt 0 ]]; then
    echo ""
    echo "📝 COGs created as _cog.tif alongside originals."
    echo "   To use them, either:"
    echo "   1. Update config.py paths (*.tif → *_cog.tif)"
    echo "   2. Or run with --replace to swap them in-place"
fi
