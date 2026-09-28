#!/bin/bash
# =============================================================================
# TiTiler Refresh Script — Stage & COG-convert new GeoTIFFs for TiTiler
# =============================================================================
#
# Reads raw TITO pipeline output from:
#   /var/EF5/TITOCuba/outputs/tmp_output_crest              (standard 1km)
#   /var/EF5/TITOCuba/outputs_25m/tmp_output_crest_25m      (high-res depth)
#
# Renames to match existing GeoServer naming (param_YYYYMMDDTHHMMSS.tif),
# converts to Cloud-Optimized GeoTIFF (COG), and places them directly in
# the TiTiler data directories.
#
# Snapshot products (maxq, maxunitq, ...) go flat under DATA_ROOT/<param>/.
# Time-series products (precip, q, unitq, sm) go under
# DATA_ROOT/<dest>/<run_timestamp>/geotiff/  (1km crest output only).
# CSV pixel extracts (ID,value) go to .../<run_timestamp>/csv/ for time-series,
# and next to the GeoTIFF (DATA_ROOT/<param>/<name>.csv) for 1km snapshot products.
# Grid lookup DATA_ROOT/pixel_id_map.csv maps ID → lat,lon once for the 1km domain.
# Snapshot CSVs start from DATA_ROOT/csv_export_since.txt (set on first run to the
# newest existing snapshot), so an existing archive is not backfilled.
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
SRC_CREST="/var/EF5/TITOCuba/outputs/tmp_output_crest"
SRC_DEPTH="/var/EF5/TITOCuba/outputs_25m/tmp_output_crest_25m"

# SRC_CREST="/home/nammehta/TITOCubaMainTest/TITOCuba/outputs/tmp_output_crest"
# SRC_DEPTH="/home/nammehta/TITOCubaMainTest/TITOCuba/outputs_25m/tmp_output_crest_25m"

# Target: TiTiler serves directly from these GeoServer data directories
DATA_ROOT="/var/EF5/geoServer"
# DATA_ROOT="/Dedicated/Humberto/Naman/TITO_Cuba_Titiler_outputs"

# ── Product Mapping ──────────────────────────────────────────────────────
# Snapshot products: param → flat dest under DATA_ROOT
# e.g. maxq files go to DATA_ROOT/maxq/maxq_YYYYMMDDTHHMMSS.tif
declare -A PRODUCTS
PRODUCTS["maxunitq"]=1
PRODUCTS["maxq"]=1
PRODUCTS["qpfaccum"]=1
PRODUCTS["qpeaccum"]=1
PRODUCTS["maxsm"]=1
PRODUCTS["maxdepth"]=1

# Time-series products from 1km crest output only (not 25m).
# Source param → dest subdirectory; files go to DATA_ROOT/<dest>/<run_timestamp>/
# e.g. q.20260908_0900.crest.tif → streamflow/202609071900/q_20260908T090000.tif
declare -A SERIES_PRODUCTS
SERIES_PRODUCTS["precip"]="precip"
SERIES_PRODUCTS["q"]="streamflow"
SERIES_PRODUCTS["unitq"]="unitq"
SERIES_PRODUCTS["sm"]="soilmoisture"

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

# Parse TITO pipeline TIFF names into _param, _f_date, _f_time (HHMMSS).
# Formats:
#   Standard:     param.date.time.tif              (maxq.20250608.120000.tif)
#   High-res:     param.25m.date.time.tif          (maxdepth.25m.20250608.120000.tif)
#   Time-series:  param.YYYYMMDD_HHMM.crest.tif    (precip.20260907_1530.crest.tif)
parse_tito_filename() {
    local fname="$1"
    _param="${fname%%.*}"
    if [[ "$fname" == *".25m."* ]]; then
        _f_date=$(echo "$fname" | cut -d'.' -f3)
        _f_time=$(echo "$fname" | cut -d'.' -f4)
    elif [[ "$fname" == *.crest.tif || "$fname" == *.crest.tiff ]]; then
        local dt
        dt=$(echo "$fname" | cut -d'.' -f2)
        _f_date="${dt%%_*}"
        _f_time="${dt##*_}"
    else
        _f_date=$(echo "$fname" | cut -d'.' -f2)
        _f_time=$(echo "$fname" | cut -d'.' -f3)
    fi
    _f_time="${_f_time%%.*}"
    if [[ ${#_f_time} -eq 4 ]]; then
        _f_time="${_f_time}00"
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
            local fname param f_date f_time dest_param dest_dir dest_file new_name
            local is_series=0
            fname=$(basename "$src_file")
            parse_tito_filename "$fname"
            param="$_param"
            f_date="$_f_date"
            f_time="$_f_time"

            # Time-series layers (1km only): precip/q/unitq/sm nested by run timestamp
            if [[ -n "${SERIES_PRODUCTS[$param]:-}" && -z "$log_subdir" ]]; then
                dest_param="${SERIES_PRODUCTS[$param]}"
                dest_dir="${DATA_ROOT}/${dest_param}/${ts_name}/geotiff"
                new_name="${param}_${f_date}T${f_time}.tif"
                dest_file="${dest_dir}/${new_name}"
                is_series=1
            elif [[ -n "${PRODUCTS[$param]:-}" ]]; then
                dest_param="$param"
                dest_dir="${DATA_ROOT}/${param}"
                new_name="${param}_${f_date}T${f_time}.tif"
                dest_file="${dest_dir}/${new_name}"
            else
                continue
            fi

            mkdir -p "$dest_dir"

            # Skip if already processed, but retroactively add metadata if missing
            if [[ -f "$dest_file" ]]; then
                if [[ "$is_series" -eq 0 ]] && ! gdalinfo "$dest_file" 2>/dev/null | grep -q "TIME_BEGIN"; then
                    write_tiff_meta "$dest_file" "$param" "$ts_name"
                fi
                continue
            fi

            if [[ "$is_series" -eq 1 ]]; then
                echo "      📋 ${fname} → ${dest_param}/${ts_name}/geotiff/${new_name}"
            else
                echo "      📋 ${fname} → ${param}/${new_name}"
            fi
            mv "$src_file" "$dest_file"
            tiff_count=$((tiff_count + 1))

            echo "      🔄 COG: ${new_name}"
            if convert_to_cog "$dest_file"; then
                echo "      ✅ ${new_name}"
                cog_ok=$((cog_ok + 1))
                if [[ "$is_series" -eq 0 ]]; then
                    write_tiff_meta "$dest_file" "$param" "$ts_name"
                fi
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
                parse_tito_filename "$lf_name"
                local lf_dest
                if [[ -n "${SERIES_PRODUCTS[$_param]:-}" && -z "$log_subdir" ]]; then
                    local lf_dest_param="${SERIES_PRODUCTS[$_param]}"
                    lf_dest="${DATA_ROOT}/${lf_dest_param}/${ts_name}/geotiff/${_param}_${_f_date}T${_f_time}.tif"
                    local lf_dest_old="${DATA_ROOT}/${lf_dest_param}/${ts_name}/${_param}_${_f_date}T${_f_time}.tif"
                    [[ -f "$lf_dest" || -f "$lf_dest_old" ]] && rm -f "$leftover"
                    continue
                else
                    lf_dest="${DATA_ROOT}/${_param}/${_param}_${_f_date}T${_f_time}.tif"
                fi
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
# Snapshot CSV cutoff: recorded once, before staging, as the newest snapshot
# already archived. Only granules staged after it get CSVs. Delete the file to
# backfill everything.
CSV_SINCE_FILE="${DATA_ROOT}/csv_export_since.txt"
if [[ ! -f "$CSV_SINCE_FILE" ]]; then
    mkdir -p "$DATA_ROOT"
    csv_since=$(for p in maxq maxunitq maxsm qpeaccum qpfaccum; do
            [[ -d "${DATA_ROOT}/${p}" ]] && find "${DATA_ROOT}/${p}" -maxdepth 1 -type f -name "${p}_*T*.tif" -printf '%f\n'
        done | sed -nE 's/.*_([0-9]{8}T[0-9]{6})\.tif$/\1/p' | sort | tail -n 1)
    echo "${csv_since:-00000000T000000}" > "$CSV_SINCE_FILE"
    echo "📌 Snapshot CSV export starts after ${csv_since:-00000000T000000} (${CSV_SINCE_FILE})"
fi
CSV_SINCE=$(head -n 1 "$CSV_SINCE_FILE" | tr -d '[:space:]')

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

# ── GeoTIFF → CSV (timeseries + 1km snapshots) ─────────────────────────
echo ""
echo "📄 Converting GeoTIFFs to CSV..."
# Same conda locations as pipeline.sh; needs numpy + rasterio (tito_env has both)
TITO_PY=""
for base in "${HOME}/miniconda3" "${HOME}/anaconda3" "${HOME}/mambaforge" "/opt/conda"; do
    for env in tito_env2 tito_env; do
        if [[ -x "${base}/envs/${env}/bin/python" ]]; then
            TITO_PY="${base}/envs/${env}/bin/python"
            break 2
        fi
    done
done
if [[ -z "$TITO_PY" ]]; then
    echo "   ⚠️  tito_env/tito_env2 python not found — skipping CSV export"
elif ! "$TITO_PY" "${SCRIPT_DIR}/timeseries_to_csv.py" --data-root "$DATA_ROOT" \
        --snapshot-since "$CSV_SINCE"; then
    # Non-fatal: GeoTIFF staging above already succeeded; retried next run
    echo "   ⚠️  CSV export reported errors — continuing"
fi

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

for src_param in "${!SERIES_PRODUCTS[@]}"; do
    dest_param="${SERIES_PRODUCTS[$src_param]}"
    dir="${DATA_ROOT}/${dest_param}"
    if [[ -d "$dir" ]]; then
        ts_count=$(find "$dir" -mindepth 1 -maxdepth 1 -type d | wc -l)
        count=$(find "$dir" -type f -name "*.tif" | wc -l)
        echo "   ${dest_param}: ${ts_count} timesteps, ${count} granules"
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
