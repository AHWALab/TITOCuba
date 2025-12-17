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


def _update_template_block(template_path: str, block_text: str) -> None:
    with open(template_path, "r", encoding="utf-8") as handle:
        template = handle.read()

    if "#---Start Gauge-Basin Block" not in template:
        raise ValueError(
            f"Gauge-Basin marker not found in high-res template: {template_path}"
        )

    updated = GAUGE_BLOCK_PATTERN.sub(block_text, template, count=1)
    with open(template_path, "w", encoding="utf-8") as handle:
        handle.write(updated)


def prepare_highres_control(
    maxunitq_path: str,
    mask_grid_path: str,
    gauge_list_path: str,
    template_path: str,
    threshold: float,
    gauge_name_prefix: Optional[str] = None,
) -> HighResSelection:
    """Update the high-res template with gauges exceeding the requested threshold."""

    _require_rasterio()

    if threshold is None:
        raise ValueError("High-res threshold is not defined in the configuration file")

    gauge_name_prefix = gauge_name_prefix or "HighResGauge"

    try:
        maxunitq_band, meta = _load_maxunitq(maxunitq_path)
    except FileNotFoundError as exc:
        print(f"High-res rerun skipped: {exc}")
        return HighResSelection([])

    hot_gauges = _extract_hot_gauges(
        maxunitq_band,
        mask_grid_path,
        meta,
        float(threshold),
    )

    lookup = _load_gauge_lookup(gauge_list_path)
    block_text = _render_block_text(hot_gauges, lookup, gauge_name_prefix)
    _update_template_block(template_path, block_text)

    print(f"High-res template updated with {len(hot_gauges)} gauge(s).")
    return HighResSelection(hot_gauges)


__all__ = ["HighResSelection", "prepare_highres_control"]


