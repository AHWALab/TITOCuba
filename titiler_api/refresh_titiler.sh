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

# SRC_CREST="/home/nammehta/TITOCubaMainTest/TITOCuba/outputs/tmp_output_crest"
# SRC_DEPTH="/home/nammehta/TITOCubaMainTest/TITOCuba/outputs_25m/tmp_output_crest_25m"

# Target: TiTiler serves directly from these GeoServer data directories
DATA_ROOT="/var/ef5/geoServer"
# DATA_ROOT="/home/nammehta/TITOCubaMainTest/titilerTest"

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
        -co OVERVIEW_RESAMPLING=NEAREST \
        -q \
        "$tif_file" "$cog_tmp" 2>/dev/null; then
        mv "$cog_tmp" "$tif_file"
        return 0
    else
        rm -f "$cog_tmp"
        return 1
    fi
}

# ── Metadata helpers ─────────────────────────────────────────────────────
# Cache parsed CU_Regional_crest.txt values keyed by timestep
declare -A META_TBEGIN META_TEND META_TBEGIN_LR

parse_crest_meta() {
    local ts="$1"
    # Already cached?
    [[ -n "${META_TBEGIN[$ts]:-}" ]] && return 0

    local crest_file="${DATA_ROOT}/logs/${ts}/CU_Regional_crest.txt"
    [[ -f "$crest_file" ]] || return 1

    while IFS='=' read -r key val; do
        case "$key" in
            TIME_BEGIN)     META_TBEGIN[$ts]="$val" ;;
            TIME_END)       META_TEND[$ts]="$val" ;;
            TIME_BEGIN_LR)  META_TBEGIN_LR[$ts]="$val" ;;
        esac
    done < "$crest_file"
    return 0
}

# Write TIME_BEGIN / TIME_END metadata to a single GeoTIFF based on product
write_tiff_meta() {
    local tif="$1"
    local param="$2"
    local ts="$3"

    parse_crest_meta "$ts" || return 0  # no crest file = skip metadata silently

    local tbegin tend
    case "$param" in
        qpeaccum)
            tbegin="${META_TBEGIN[$ts]}"
            tend="${META_TBEGIN_LR[$ts]}" ;;
        qpfaccum)
            tbegin="${META_TBEGIN_LR[$ts]}"
            tend="${META_TEND[$ts]}" ;;
        *)  # maxunitq, maxq, maxsm, maxdepth
            tbegin="${META_TBEGIN[$ts]}"
            tend="${META_TEND[$ts]}" ;;
    esac

    [[ -z "$tbegin" || -z "$tend" ]] && return 0

    gdal_edit.py -mo "TIME_BEGIN=${tbegin}" -mo "TIME_END=${tend}" "$tif" 2>/dev/null || true
    echo "      🏷️  meta: TIME_BEGIN=${tbegin} TIME_END=${tend}"
}

# ── Process a source directory (iterates timestamp subfolders) ────────────
# Usage: process_source <src_dir> [log_subdir]
#   log_subdir: optional subfolder under logs/ (e.g. "" for crest, "25m" for depth)
process_source() {
    local src_dir="$1"
    local log_subdir="${2:-}"
    local total_tiffs=0 total_cog_ok=0 total_cog_fail=0 total_csvs=0 total_logs=0

    if [[ ! -d "$src_dir" ]]; then
        echo "   ⚠️  Source not found: $src_dir"
        return
    fi

    # Iterate over timestamp subdirectories (e.g., 202606091400/)
    while IFS= read -r -d '' ts_dir; do
        local ts_name
        ts_name=$(basename "$ts_dir")
        echo ""
        echo "   📁 Processing timestep: ${ts_name}"

        local tiff_count=0 cog_ok=0 cog_fail=0 csv_count=0 log_count=0

        # ── Process TIFFs ────────────────────────────────────────────
        while IFS= read -r -d '' src_file; do
            local fname param f_date f_time
            fname=$(basename "$src_file")

            # Parse TITO pipeline filename
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

            # Only process known products
            [[ -z "${PRODUCTS[$param]:-}" ]] && continue

            # Build target filename with underscore (matches existing files)
            # Existing format: maxq_20250608T120000.tif
            local new_name="${param}_${f_date}T${f_time}.tif"
            local dest_dir="${DATA_ROOT}/${param}"
            local dest_file="${dest_dir}/${new_name}"

            mkdir -p "$dest_dir"

            # Skip if already processed, but retroactively add metadata if missing
            if [[ -f "$dest_file" ]]; then
                # Check if metadata already exists
                if ! gdalinfo "$dest_file" 2>/dev/null | grep -q "TIME_BEGIN"; then
                    write_tiff_meta "$dest_file" "$param" "$ts_name"
                fi
                continue
            fi

            echo "      📋 ${fname} → ${param}/${new_name}"
            mv "$src_file" "$dest_file"
            tiff_count=$((tiff_count + 1))

            echo "      🔄 COG: ${new_name}"
            if convert_to_cog "$dest_file"; then
                echo "      ✅ ${new_name}"
                cog_ok=$((cog_ok + 1))
                write_tiff_meta "$dest_file" "$param" "$ts_name"
            else
                echo "      ❌ ${new_name} — COG FAILED"
                cog_fail=$((cog_fail + 1))
            fi
        done < <(find "$ts_dir" -maxdepth 1 -type f \( -name "*.tif" -o -name "*.tiff" \) -print0 2>/dev/null || true)

        # ── Process CSVs (timeseries discharge data) ──────────────────
        local discharge_dir="${DATA_ROOT}/discharge/${ts_name}"
        while IFS= read -r -d '' csv_file; do
            local csv_fname csv_dest
            csv_fname=$(basename "$csv_file")
            csv_dest="${discharge_dir}/${csv_fname}"

            mkdir -p "$discharge_dir"

            # Skip if already processed
            [[ -f "$csv_dest" ]] && continue

            echo "      📋 ${csv_fname} → discharge/${ts_name}/${csv_fname}"
            mv "$csv_file" "$csv_dest"
            csv_count=$((csv_count + 1))
        done < <(find "$ts_dir" -maxdepth 1 -type f -name "*.csv" -print0 2>/dev/null || true)

        # ── Process logs, txt, json (pipeline run artifacts) ──────────
        local logs_dir
        if [[ -n "$log_subdir" ]]; then
            logs_dir="${DATA_ROOT}/logs/${log_subdir}/${ts_name}"
        else
            logs_dir="${DATA_ROOT}/logs/${ts_name}"
        fi
        while IFS= read -r -d '' log_file; do
            local log_fname log_dest
            log_fname=$(basename "$log_file")
            log_dest="${logs_dir}/${log_fname}"

            mkdir -p "$logs_dir"

            # Skip if already processed
            [[ -f "$log_dest" ]] && continue

            local log_label
            if [[ -n "$log_subdir" ]]; then
                log_label="logs/${log_subdir}/${ts_name}/${log_fname}"
            else
                log_label="logs/${ts_name}/${log_fname}"
            fi
            echo "      📋 ${log_fname} → ${log_label}"
            mv "$log_file" "$log_dest"
            log_count=$((log_count + 1))
        done < <(find "$ts_dir" -maxdepth 1 -type f \( -name "*.txt" -o -name "*.log" -o -name "*.json" \) -print0 2>/dev/null || true)

        # ── Cleanup: remove source files already present at destination ──
        # (handles leftovers from previous cp-based runs)
        for leftover in "$ts_dir"/*; do
            [[ -f "$leftover" ]] || continue
            local lf_name="${leftover##*/}"
            # TIFFs: destination uses underscore naming (param_YYYYMMDDTHHMMSS.tif)
            if [[ "$lf_name" == *.tif ]] || [[ "$lf_name" == *.tiff ]]; then
                local lf_param lf_date lf_time
                if [[ "$lf_name" == *".25m."* ]]; then
                    lf_param="${lf_name%%.*}"
                    lf_date="$(echo "$lf_name" | cut -d'.' -f3)"
                    lf_time="$(echo "$lf_name" | cut -d'.' -f4)"
                else
                    lf_param="${lf_name%%.*}"
                    lf_date="$(echo "$lf_name" | cut -d'.' -f2)"
                    lf_time="$(echo "$lf_name" | cut -d'.' -f3)"
                fi
                local lf_dest="${DATA_ROOT}/${lf_param}/${lf_param}_${lf_date}T${lf_time}.tif"
                [[ -f "$lf_dest" ]] && rm -f "$leftover"
            # CSVs → discharge/{timestep}/
            elif [[ "$lf_name" == *.csv ]]; then
                [[ -f "${discharge_dir}/${lf_name}" ]] && rm -f "$leftover"
            # Logs/txt/json → logs/{timestep}/
            elif [[ "$lf_name" == *.txt ]] || [[ "$lf_name" == *.log ]] || [[ "$lf_name" == *.json ]]; then
                [[ -f "${logs_dir}/${lf_name}" ]] && rm -f "$leftover"
            fi
        done

        # Remove timestep dir if empty after cleanup
        rmdir "$ts_dir" 2>/dev/null || true

        echo "      📊 TIFFs: ${tiff_count} (${cog_ok} COG, ${cog_fail} fail) | CSVs: ${csv_count} | Logs: ${log_count}"

        total_tiffs=$((total_tiffs + tiff_count))
        total_cog_ok=$((total_cog_ok + cog_ok))
        total_cog_fail=$((total_cog_fail + cog_fail))
        total_csvs=$((total_csvs + csv_count))
        total_logs=$((total_logs + log_count))

    done < <(find "$src_dir" -mindepth 1 -maxdepth 1 -type d -print0 2>/dev/null | sort -z || true)

    echo ""
    echo "   📊 ${src_dir} TOTAL: ${total_tiffs} TIFFs, ${total_cog_ok} COG OK, ${total_cog_fail} COG fail, ${total_csvs} CSVs, ${total_logs} logs"
}

# ── Main ─────────────────────────────────────────────────────────────────
echo "📂 Scanning TITO output directories..."

process_source "$SRC_CREST"
process_source "$SRC_DEPTH" "25m"

# ── Retroactive metadata for existing TIFFs ──────────────────────────────
retro_metadata() {
    echo ""
    echo "🏷️  Checking existing TIFFs for missing metadata..."
    local meta_added=0
    for param in "${!PRODUCTS[@]}"; do
        local tif_dir="${DATA_ROOT}/${param}"
        [[ -d "$tif_dir" ]] || continue
        while IFS= read -r -d '' tif_file; do
            # Already has TIME_BEGIN? skip
            gdalinfo "$tif_file" 2>/dev/null | grep -q "TIME_BEGIN" && continue
            # Extract timestep from filename: param_YYYYMMDDTHHMMSS.tif
            local tif_name="${tif_file##*/}"
            local ts_raw="${tif_name##*_}"          # YYYYMMDDTHHMMSS.tif
            ts_raw="${ts_raw%.tif}"                  # YYYYMMDDTHHMMSS
            local ts="${ts_raw:0:8}${ts_raw:9:4}"   # YYYYMMDDHHMM
            write_tiff_meta "$tif_file" "$param" "$ts" && meta_added=$((meta_added + 1))
        done < <(find "$tif_dir" -maxdepth 1 -type f \( -name "*.tif" -o -name "*.tiff" \) -print0 2>/dev/null || true)
    done
    echo "   🏷️  Metadata added to ${meta_added} existing TIFFs"
}
retro_metadata

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

# ── Discharge summary ────────────────────────────────────────────────────
discharge_root="${DATA_ROOT}/discharge"
if [[ -d "$discharge_root" ]]; then
    ts_count=$(find "$discharge_root" -mindepth 1 -maxdepth 1 -type d | wc -l)
    csv_total=$(find "$discharge_root" -type f -name "*.csv" | wc -l)
    echo "   discharge: ${ts_count} timesteps, ${csv_total} CSV files"
fi

# ── Logs summary ─────────────────────────────────────────────────────────
logs_root="${DATA_ROOT}/logs"
if [[ -d "$logs_root" ]]; then
    log_ts_count=$(find "$logs_root" -mindepth 1 -maxdepth 1 -type d ! -name "25m" | wc -l)
    log_total=$(find "$logs_root" -maxdepth 1 -type f 2>/dev/null | wc -l)
    echo "   logs: ${log_ts_count} timesteps, ${log_total} files"
    # 25m subfolder
    logs_25m="${logs_root}/25m"
    if [[ -d "$logs_25m" ]]; then
        ts25_count=$(find "$logs_25m" -mindepth 1 -maxdepth 1 -type d | wc -l)
        f25_total=$(find "$logs_25m" -type f | wc -l)
        echo "   logs/25m: ${ts25_count} timesteps, ${f25_total} files"
    fi
fi
