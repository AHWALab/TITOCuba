#!/usr/bin/env python3
"""Export TiTiler GeoTIFFs to compact CSV.

Writes once:
  DATA_ROOT/pixel_id_map.csv  — ID,lat,lon for the full 1km grid

Timeseries products (precip, streamflow, unitq, soilmoisture):
  .../<run_timestamp>/geotiff/<name>.tif
  .../<run_timestamp>/csv/<name>.csv   — ID,value only

Snapshot products (maxq, maxunitq, maxsm, qpeaccum, qpfaccum; 1km only):
  DATA_ROOT/<param>/<name>.tif
  DATA_ROOT/<param>/<name>.csv         — ID,value, same folder as the GeoTIFF
  --snapshot-since YYYYMMDDTHHMMSS limits this to GeoTIFFs whose filename
  timestamp is newer, so an existing archive is not backfilled.

Timestamp is in the CSV filename (param_YYYYMMDDTHHMMSS.csv).
Nodata (-9999) and zeros are dropped. Pixel ID = row * width + col.
Idempotent: existing CSVs newer than their GeoTIFF are skipped.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import get_context
from typing import List, Optional, Tuple

import numpy as np
import rasterio
from rasterio.windows import Window

PRODUCTS = ("precip", "streamflow", "unitq", "soilmoisture")
SNAPSHOT_PRODUCTS = ("maxq", "maxunitq", "maxsm", "qpeaccum", "qpfaccum")
NODATA = -9999.0
LOOKUP_NAME = "pixel_id_map.csv"
CHUNK_ROWS = 1024  # rows read per window
TS_RE = re.compile(r"_(\d{8}T\d{6})\.tiff?$", re.IGNORECASE)


def _pixel_centers(height: int, width: int, transform) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    cols = np.arange(width, dtype=np.float64)
    rows = np.arange(height, dtype=np.float64)
    col_grid, row_grid = np.meshgrid(cols, rows)
    lons = transform.c + transform.a * (col_grid + 0.5) + transform.b * (row_grid + 0.5)
    lats = transform.f + transform.d * (col_grid + 0.5) + transform.e * (row_grid + 0.5)
    ids = (row_grid * width + col_grid).astype(np.int32)
    return ids, lats, lons


def write_pixel_id_map(tif_path: str, map_path: str) -> int:
    with rasterio.open(tif_path) as src:
        ids, lats, lons = _pixel_centers(src.height, src.width, src.transform)
    tmp = map_path + ".tmp"
    np.savetxt(
        tmp,
        np.column_stack((ids.ravel(), lats.ravel(), lons.ravel())),
        fmt=["%d", "%.8f", "%.8f"],
        delimiter=",",
        header="ID,lat,lon",
        comments="",
    )
    os.replace(tmp, map_path)
    return int(ids.size)


def tif_to_csv(tif_path: str, csv_path: str) -> int:
    """Write valid pixels as ID,value. Reads in row chunks."""
    tmp = csv_path + ".tmp"
    n = 0
    with rasterio.open(tif_path) as src, open(tmp, "w", encoding="utf-8", newline="\n") as f:
        nodata = src.nodata
        width = src.width
        f.write("ID,value\n")
        for row0 in range(0, src.height, CHUNK_ROWS):
            nrows = min(CHUNK_ROWS, src.height - row0)
            data = src.read(1, window=Window(0, row0, width, nrows))
            mask = np.isfinite(data) & (data != np.float32(NODATA)) & (data != 0)
            if nodata is not None:
                mask &= data != nodata
            # np.nonzero is row-major, so IDs come out already sorted
            rows_i, cols_i = np.nonzero(mask)
            if rows_i.size == 0:
                continue
            ids = (rows_i.astype(np.int64) + row0) * width + cols_i
            np.savetxt(
                f,
                np.column_stack((ids, data[rows_i, cols_i])),
                fmt=["%d", "%.6g"],
                delimiter=",",
            )
            n += rows_i.size
    os.replace(tmp, csv_path)
    return n


def _csv_is_fresh(tif_path: str, csv_path: str) -> bool:
    return (
        os.path.isfile(csv_path)
        and os.path.getsize(csv_path) > 0
        and os.path.getmtime(csv_path) >= os.path.getmtime(tif_path)
    )


def _stage_geotiffs(ts_dir: str, geotiff_dir: str) -> None:
    os.makedirs(geotiff_dir, exist_ok=True)
    with os.scandir(ts_dir) as it:
        for entry in it:
            if not entry.is_file():
                continue
            name = entry.name
            if not name.lower().endswith((".tif", ".tiff")):
                continue
            dest = os.path.join(geotiff_dir, name)
            if os.path.isfile(dest):
                os.remove(entry.path)
            else:
                os.rename(entry.path, dest)


def process_timestamp_dir(ts_dir: str) -> Tuple[str, int, int, int, Optional[str]]:
    geotiff_dir = os.path.join(ts_dir, "geotiff")
    csv_dir = os.path.join(ts_dir, "csv")
    os.makedirs(csv_dir, exist_ok=True)
    _stage_geotiffs(ts_dir, geotiff_dir)

    converted = 0
    skipped = 0
    rows = 0
    tifs = sorted(
        n for n in os.listdir(geotiff_dir)
        if n.lower().endswith((".tif", ".tiff"))
    )
    for name in tifs:
        tif_path = os.path.join(geotiff_dir, name)
        csv_path = os.path.join(csv_dir, os.path.splitext(name)[0] + ".csv")
        if _csv_is_fresh(tif_path, csv_path):
            skipped += 1
            continue
        try:
            rows += tif_to_csv(tif_path, csv_path)
            converted += 1
        except Exception as exc:
            return ts_dir, converted, skipped, rows, f"{name}: {exc}"
    return ts_dir, converted, skipped, rows, None


def process_snapshot_file(tif_path: str) -> Tuple[str, int, int, int, Optional[str]]:
    csv_path = os.path.splitext(tif_path)[0] + ".csv"
    if _csv_is_fresh(tif_path, csv_path):
        return tif_path, 0, 1, 0, None
    try:
        return tif_path, 1, 0, tif_to_csv(tif_path, csv_path), None
    except Exception as exc:
        return tif_path, 0, 0, 0, str(exc)


def iter_timestamp_dirs(data_root: str, products: List[str]) -> List[str]:
    dirs: List[str] = []
    for product in products:
        root = os.path.join(data_root, product)
        if not os.path.isdir(root):
            continue
        with os.scandir(root) as it:
            for entry in it:
                if entry.is_dir() and entry.name not in ("geotiff", "csv"):
                    dirs.append(entry.path)
    dirs.sort()
    return dirs


def iter_snapshot_tifs(data_root: str, products: List[str], since: Optional[str] = None) -> List[str]:
    tifs: List[str] = []
    for product in products:
        root = os.path.join(data_root, product)
        if not os.path.isdir(root):
            continue
        with os.scandir(root) as it:
            for entry in it:
                if not entry.is_file():
                    continue
                m = TS_RE.search(entry.name)
                if not m or (since and m.group(1) <= since):
                    continue
                tifs.append(entry.path)
    tifs.sort()
    return tifs


def _first_tif(ts_dirs: List[str]) -> Optional[str]:
    for ts_dir in ts_dirs:
        geotiff_dir = os.path.join(ts_dir, "geotiff")
        search = [geotiff_dir, ts_dir]
        for folder in search:
            if not os.path.isdir(folder):
                continue
            for name in sorted(os.listdir(folder)):
                if name.lower().endswith((".tif", ".tiff")):
                    return os.path.join(folder, name)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Export TiTiler GeoTIFFs to CSV")
    parser.add_argument("--data-root", required=True, help="TiTiler DATA_ROOT")
    parser.add_argument("--workers", type=int, default=min(8, os.cpu_count() or 4))
    parser.add_argument("--products", nargs="*", default=list(PRODUCTS))
    parser.add_argument("--snapshot-products", nargs="*", default=list(SNAPSHOT_PRODUCTS))
    parser.add_argument("--snapshot-since", default=None,
                        help="only export snapshot GeoTIFFs with filename timestamp > YYYYMMDDTHHMMSS")
    args = parser.parse_args()

    ts_dirs = iter_timestamp_dirs(args.data_root, args.products)
    snap_tifs = iter_snapshot_tifs(args.data_root, args.snapshot_products, args.snapshot_since)
    if not ts_dirs and not snap_tifs:
        print("   No GeoTIFFs found to export")
        return 0

    map_path = os.path.join(args.data_root, LOOKUP_NAME)
    # Any 1km GeoTIFF defines the grid; fall back to a snapshot before series exist
    sample = _first_tif(ts_dirs) or (snap_tifs[0] if snap_tifs else None)
    if sample and not os.path.isfile(map_path):
        n = write_pixel_id_map(sample, map_path)
        print(f"   wrote {LOOKUP_NAME} ({n} pixels) from {os.path.basename(sample)}")
    elif os.path.isfile(map_path):
        print(f"   using existing {LOOKUP_NAME}")

    workers = max(1, args.workers)
    converted = skipped = rows = errors = 0
    print(f"   {len(ts_dirs)} timestamp folders, {len(snap_tifs)} snapshot GeoTIFFs, {workers} workers")

    with ProcessPoolExecutor(max_workers=workers, mp_context=get_context("spawn")) as pool:
        futs = [pool.submit(process_timestamp_dir, d) for d in ts_dirs]
        futs += [pool.submit(process_snapshot_file, p) for p in snap_tifs]
        for fut in as_completed(futs):
            path, c, s, r, err = fut.result()
            converted += c
            skipped += s
            rows += r
            rel = os.path.relpath(path, args.data_root)
            if err:
                errors += 1
                print(f"   FAIL {rel}: {err}")
            elif c:
                print(f"   {rel}: {c} csv written, {s} skipped, {r} pixels")

    print(f"   CSV export done: {converted} written, {skipped} skipped, {rows} pixels, {errors} errors")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
