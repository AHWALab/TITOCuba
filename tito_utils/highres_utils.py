import os
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set

import numpy as np

try:
    import rasterio
    from rasterio.windows import Window, bounds as window_bounds, from_bounds
except ImportError as exc:  # pragma: no cover - handled at runtime
    rasterio = None
    _RASTERIO_IMPORT_ERROR = exc


GAUGE_BLOCK_PATTERN = re.compile(
    r"#---Start Gauge-Basin Block.*?#---End Gauge-Basin Block", re.DOTALL
)


@dataclass
class HighResSelection:
    """Container for the gauges selected for a 25 m rerun."""

    gauge_ids: List[int]
    gauge_lookup: Dict[int, str]
    gauge_name_prefix: str
    da_gauge_ids: Optional[List[str]] = None  # List of DA gauge IDs (e.g., ['EMB2800002', ...])
    da_gauge_lookup: Optional[Dict[str, str]] = None  # Mapping of DA gauge ID to gauge line

    @property
    def count(self) -> int:
        return len(self.gauge_ids)


def _require_rasterio() -> None:
    if rasterio is None:
        raise RuntimeError(
            "rasterio is required for the high-resolution workflow but is not installed"
        ) from _RASTERIO_IMPORT_ERROR


def _load_maxunitq(maxunitq_path: str):
    if not os.path.exists(maxunitq_path):
        raise FileNotFoundError(f"maxunitq raster not found: {maxunitq_path}")

    with rasterio.open(maxunitq_path) as src:
        band = src.read(1, masked=True)
        meta = {
            "transform": src.transform,
            "crs": src.crs,
            "width": src.width,
            "height": src.height,
            "dtype": src.dtypes[0],
        }
    return band, meta


def _collect_gauges_from_mask(
    mask_path: str, rows: np.ndarray, cols: np.ndarray, target_transform
) -> List[int]:
    """Return all gauge IDs from the mask that overlap the requested coarse pixels."""
    if not len(rows):
        return []

    if not os.path.exists(mask_path):
        raise FileNotFoundError(f"Mask grid not found: {mask_path}")

    gauge_ids: Set[int] = set()
    with rasterio.open(mask_path) as mask_ds:
        nodata = mask_ds.nodata
        for row, col in zip(rows, cols):
            cell_window = Window(col, row, 1, 1)
            cell_bounds = window_bounds(cell_window, target_transform)
            mask_window = from_bounds(
                *cell_bounds,
                transform=mask_ds.transform,
            )
            if mask_window.width <= 0 or mask_window.height <= 0:
                continue

            data = mask_ds.read(
                1,
                window=mask_window,
                boundless=True,
                masked=True,
                fill_value=nodata,
            )
            if data.size == 0:
                continue

            if np.ma.isMaskedArray(data):
                values = data.compressed()
            else:
                values = data.ravel()

            for val in np.unique(values):
                ivalue = int(round(float(val)))
                if ivalue >= 0:
                    gauge_ids.add(ivalue)

    return sorted(gauge_ids)


def _extract_hot_gauges(
    maxunitq_band: np.ma.MaskedArray,
    mask_grid_path: str,
    target_meta: dict,
    threshold: float,
) -> List[int]:
    if np.ma.is_masked(maxunitq_band):
        valid_mask = ~maxunitq_band.mask
        values = maxunitq_band.filled(np.nan)
    else:
        valid_mask = np.ones(maxunitq_band.shape, dtype=bool)
        values = maxunitq_band

    exceed_mask = valid_mask & np.isfinite(values) & (values >= threshold)
    if not np.any(exceed_mask):
        return []

    rows, cols = np.where(exceed_mask)
    return _collect_gauges_from_mask(mask_grid_path, rows, cols, target_meta["transform"])


def _load_gauge_lookup(gauge_list_path: str) -> Dict[int, str]:
    if not os.path.exists(gauge_list_path):
        raise FileNotFoundError(f"Gauge list file not found: {gauge_list_path}")

    lookup: Dict[int, str] = {}
    with open(gauge_list_path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or not line.startswith("[Gauge"):
                continue
            match = re.match(r"\[Gauge\s+(\d+)\](.*)", line)
            if not match:
                continue
            gauge_id = int(match.group(1))
            lookup[gauge_id] = line
    return lookup


def _load_da_gauge_lookup(gauge_list_path: str) -> Dict[str, str]:
    """Load DA gauges (EMBxxxxxx format) from the 25m gauge list file."""
    if not os.path.exists(gauge_list_path):
        return {}

    lookup: Dict[str, str] = {}
    with open(gauge_list_path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or not line.startswith("[gauge EMB"):
                continue
            # Match [gauge EMBxxxxxxx] pattern
            match = re.match(r"\[gauge\s+(EMB\d+)\]", line)
            if not match:
                continue
            gauge_id = match.group(1)
            lookup[gauge_id] = line
    return lookup


def _reindex_gauge_line(raw_line: str, new_index: int) -> str:
    return re.sub(r"\[Gauge\s+\d+\]", f"[Gauge {new_index}]", raw_line, count=1)


def _render_block_text(
    gauge_ids: Sequence[int],
    gauge_lookup: Dict[int, str],
    gauge_name_prefix: str,
) -> str:
    lines: List[str] = ["#---Start Gauge-Basin Block", ""]

    reindexed_lines: List[str] = []
    missing: List[int] = []
    for new_idx, gauge_id in enumerate(gauge_ids):
        raw_line = gauge_lookup.get(gauge_id)
        if raw_line is None:
            missing.append(gauge_id)
            continue
        reindexed_lines.append(_reindex_gauge_line(raw_line, new_idx))

    if missing:
        print(
            f"Warning: skipped {len(missing)} gauge(s) absent from the 25m list: {missing}"
        )

    if reindexed_lines:
        lines.extend(reindexed_lines)
        lines.append("")
        lines.append("[Basin 0]")
        gauge_names = " ".join(
            f"gauge={gauge_name_prefix}_{gid}" for gid in gauge_ids
        )
        if gauge_names:
            lines.append(f"# {gauge_names}")
        gauge_indices = " ".join(f"gauge={idx}" for idx in range(len(reindexed_lines)))
        lines.append(gauge_indices)
        lines.append("")

    lines.append("#---End Gauge-Basin Block")
    lines.append("")
    return "\n".join(lines)


def _extract_lat_lon_from_gauge_line(gauge_line: str) -> Optional[tuple[float, float]]:
    """Extract latitude and longitude from a gauge definition line.
    
    Returns (lat, lon) or None if not found.
    """
    lat_match = re.search(r"lat=([-+]?[0-9]*\.?[0-9]+)", gauge_line, re.IGNORECASE)
    lon_match = re.search(r"lon=([-+]?[0-9]*\.?[0-9]+)", gauge_line, re.IGNORECASE)
    
    if lat_match and lon_match:
        return float(lat_match.group(1)), float(lon_match.group(1))
    return None


def _filter_da_gauges_by_highres_selection(
    selected_gauge_ids: List[int],
    gauge_lookup: Dict[int, str],
    da_gauge_lookup: Dict[str, str],
    mask_grid_path: str,
) -> tuple[List[str], Dict[str, str]]:
    """Filter DA gauges to only include those falling within selected high-res gauges.
    
    This checks if DA gauge coordinates fall within the drainage area of any selected
    high-res gauge by using the maskgrid raster.
    
    Args:
        selected_gauge_ids: List of selected high-res gauge IDs
        gauge_lookup: Lookup dict for high-res gauges (not used but kept for consistency)
        da_gauge_lookup: Dict mapping DA gauge ID to gauge line
        mask_grid_path: Path to the maskgrid raster file
        
    Returns:
        Tuple of (filtered_da_gauge_ids, filtered_da_gauge_lookup)
    """
    if not selected_gauge_ids or not da_gauge_lookup:
        return [], {}
    
    _require_rasterio()
    
    if not os.path.exists(mask_grid_path):
        print(f"    Warning: maskgrid not found at {mask_grid_path}, including all DA gauges")
        return list(da_gauge_lookup.keys()), da_gauge_lookup.copy()
    
    # Load the maskgrid
    try:
        with rasterio.open(mask_grid_path) as mask_ds:
            transform = mask_ds.transform
            nodata = mask_ds.nodata
            mask_array = mask_ds.read(1, masked=True)
            
            selected_da_ids = []
            selected_da_lookup = {}
            
            # Convert selected gauge IDs to a set for faster lookup
            selected_set = set(selected_gauge_ids)
            
            # Check each DA gauge
            for da_gauge_id, da_line in da_gauge_lookup.items():
                coords = _extract_lat_lon_from_gauge_line(da_line)
                if coords is None:
                    print(f"    Warning: Could not extract coordinates from DA gauge {da_gauge_id}")
                    continue
                
                lat, lon = coords
                
                # Convert lat/lon to raster row/col using the inverse transform
                try:
                    col, row = ~transform * (lon, lat)
                    col, row = int(col), int(row)
                    
                    # Check if coordinates are within raster bounds
                    if 0 <= row < mask_array.shape[0] and 0 <= col < mask_array.shape[1]:
                        # Get the gauge ID at this location from the maskgrid
                        mask_value = mask_array[row, col]
                        
                        # Check if this pixel drains to one of our selected gauges
                        if not np.ma.is_masked(mask_value):
                            gauge_id_at_location = int(round(float(mask_value)))
                            if gauge_id_at_location in selected_set:
                                selected_da_ids.append(da_gauge_id)
                                selected_da_lookup[da_gauge_id] = da_line
                                
                except Exception as e:
                    print(f"    Warning: Error processing DA gauge {da_gauge_id}: {e}")
                    continue
            
            return selected_da_ids, selected_da_lookup
            
    except Exception as e:
        print(f"    Warning: Error reading maskgrid ({e}), including all DA gauges")
        return list(da_gauge_lookup.keys()), da_gauge_lookup.copy()


def prepare_highres_control(
    maxunitq_path: str,
    mask_grid_path: str,
    gauge_list_path: str,
    threshold: float,
    gauge_name_prefix: Optional[str] = None,
    enable_da: bool = False,
) -> HighResSelection:
    """Identify gauges exceeding the threshold for high-res rerun.
    
    Returns a HighResSelection with gauge IDs, lookup data, and name prefix.
    The actual control file modification is handled by write_control_file.
    
    Args:
        maxunitq_path: Path to maxunitq raster file
        mask_grid_path: Path to mask grid file  
        gauge_list_path: Path to 25m gauge list file
        threshold: Threshold value for gauge selection
        gauge_name_prefix: Prefix for gauge names
        enable_da: Whether DA is enabled (if True, will include DA gauges)
    """

    _require_rasterio()

    if threshold is None:
        raise ValueError("High-res threshold is not defined in the configuration file")

    gauge_name_prefix = gauge_name_prefix or "HighResGauge"

    try:
        maxunitq_band, meta = _load_maxunitq(maxunitq_path)
    except FileNotFoundError as exc:
        print(f"High-res rerun skipped: {exc}")
        return HighResSelection([], {}, gauge_name_prefix, None, None)

    hot_gauges = _extract_hot_gauges(
        maxunitq_band,
        mask_grid_path,
        meta,
        float(threshold),
    )

    lookup = _load_gauge_lookup(gauge_list_path)

    # Handle DA gauges if enabled
    da_gauge_ids = None
    da_gauge_lookup = None
    if enable_da and hot_gauges:
        da_gauge_lookup_full = _load_da_gauge_lookup(gauge_list_path)
        if da_gauge_lookup_full:
            da_gauge_ids, da_gauge_lookup = _filter_da_gauges_by_highres_selection(
                hot_gauges, lookup, da_gauge_lookup_full, mask_grid_path
            )
            if da_gauge_ids:
                print(f"Selected {len(da_gauge_ids)} DA gauge(s) for high-res run (spatially filtered).")
            else:
                print("No DA gauges found within selected high-res drainage areas.")

    print(f"Selected {len(hot_gauges)} gauge(s) for high-res run.")
    return HighResSelection(hot_gauges, lookup, gauge_name_prefix, da_gauge_ids, da_gauge_lookup)


__all__ = ["HighResSelection", "prepare_highres_control"]


