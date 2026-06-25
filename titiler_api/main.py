"""
TiTiler-based Dynamic Raster Tile Server
=========================================
Serves GeoTIFF time-series mosaics as XYZ tiles and WMS.
Drop-in replacement for GeoServer ImageMosaic layers.

Endpoints:
  GET /products                              — list all products
  GET /products/{product_id}                 — product details + available timesteps
  GET /tiles/{product_id}/{z}/{x}/{y}.png    — XYZ tile (time via query param)
  GET /wms                                   — WMS GetMap
  GET /wms?request=GetCapabilities           — WMS GetCapabilities
  GET /legend/{product_id}                   — colormap legend PNG
  GET /health                                — health check
"""

import os
import re
import time as time_module
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from math import pi, log, tan

import numpy as np
import rasterio
from rasterio.warp import transform_bounds
from rasterio.windows import from_bounds
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, Response
from PIL import Image

from config import PRODUCTS, PRODUCT_BY_ID

# ── PROJ Database Fix ───────────────────────────────────────────────────────
# The base conda environment has an incompatible proj.db; use the env's copy.
import sys
_PROJ_ENV_DIR = os.path.join(os.path.dirname(os.path.dirname(sys.executable)), 'share', 'proj')
if os.path.isdir(_PROJ_ENV_DIR):
    os.environ['PROJ_DATA'] = _PROJ_ENV_DIR
    os.environ['PROJ_LIB'] = _PROJ_ENV_DIR

# ── GDAL Performance Tuning ─────────────────────────────────────────────────
# Set BEFORE any rasterio operations. These dramatically improve performance
# for non-COG GeoTIFFs and add caching for repeated reads.

os.environ.setdefault("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")  # skip dir scan
os.environ.setdefault("GDAL_CACHEMAX", "512")        # 512 MB GDAL block cache
os.environ.setdefault("GDAL_SWATH_SIZE", "256")       # larger I/O chunks
os.environ.setdefault("GDAL_NUM_THREADS", "ALL_CPUS") # parallel decompression
os.environ.setdefault("VSI_CACHE", "TRUE")            # virtual file system cache
os.environ.setdefault("VSI_CACHE_SIZE", "256000000")  # 256 MB VSI cache

# ── In-Memory Tile Cache ────────────────────────────────────────────────────
# Simple LRU cache for rendered tiles to avoid re-reading + re-projecting
# the same GeoTIFF for repeated requests (panning, zooming).
from collections import OrderedDict

class TileCache:
    """LRU cache for rendered RGBA tiles, keyed by (filepath, z, x, y)."""
    def __init__(self, max_bytes: int = 256 * 1024 * 1024):  # 256 MB
        self._cache: OrderedDict = OrderedDict()
        self._max_bytes = max_bytes
        self._current_bytes = 0
        self._hits = 0
        self._misses = 0

    def _make_key(self, filepath: str, z: int, x: int, y: int) -> str:
        return f"{filepath}|{z}|{x}|{y}"

    def get(self, filepath: str, z: int, x: int, y: int) -> Optional[np.ndarray]:
        key = self._make_key(filepath, z, x, y)
        if key in self._cache:
            self._cache.move_to_end(key)
            self._hits += 1
            return self._cache[key].copy()  # return copy for safety
        self._misses += 1
        return None

    def put(self, filepath: str, z: int, x: int, y: int, data: np.ndarray):
        key = self._make_key(filepath, z, x, y)
        size = data.nbytes
        # Evict old entries if needed
        while self._current_bytes + size > self._max_bytes and self._cache:
            _, old_data = self._cache.popitem(last=False)
            self._current_bytes -= old_data.nbytes
        self._cache[key] = data.copy()
        self._current_bytes += size

    @property
    def stats(self) -> dict:
        total = self._hits + self._misses
        return {
            "size": len(self._cache),
            "bytes": self._current_bytes,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": f"{100 * self._hits / max(total, 1):.1f}%",
        }

_tile_cache = TileCache()

# ── App setup ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="AHWA TiTiler API",
    description="Dynamic raster tile server for AHWA hydrometeorological products",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Global file-index cache ─────────────────────────────────────────────────
_file_index: Dict[str, List[Tuple[datetime, str]]] = {}
_index_built_at: float = 0.0


def _parse_datetime(filename: str, pattern: str) -> Optional[datetime]:
    """Extract datetime from a filename using the product's regex.
    
    Returns a timezone-aware UTC datetime for consistent comparison.
    """
    m = re.search(pattern, filename)
    if not m:
        return None
    dt_str = m.group("datetime")
    try:
        return datetime.strptime(dt_str, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _normalize_time(time_str: str) -> datetime:
    """Parse an ISO8601 time string and return a UTC-aware datetime.
    
    Handles formats like:
      - 2026-06-08T13:00:00
      - 2026-06-08T13:00:00.000Z
      - 2026-06-08T13:00:00Z
      - 2026-06-08T13:00:00+00:00
    """
    dt = datetime.fromisoformat(time_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def build_file_index(force: bool = False) -> None:
    """Scan all product directories and build the file → time index.
    
    Prefers *_cog.tif (Cloud-Optimized) over .tif when both exist
    for the same timestamp. COGs are 10-100x faster for tile access.
    """
    global _file_index, _index_built_at

    now = time_module.monotonic()
    if not force and _file_index and (now - _index_built_at) < 300:
        return

    new_index: Dict[str, List[Tuple[datetime, str]]] = {}

    for product in PRODUCTS:
        path = Path(product.path)
        if not path.is_dir():
            print(f"[WARN] Product directory not found: {product.path}")
            new_index[product.id] = []
            continue

        # Build dict: datetime → filepath, preferring COG over regular
        dt_to_file: Dict[datetime, str] = {}

        # Pass 1: scan *_cog.tif (preferred)
        for f in sorted(path.glob("*_cog.tif")):
            dt = _parse_datetime(f.name, product.filename_regex)
            if dt:
                dt_to_file[dt] = str(f)

        # Pass 2: scan *.tif, only adding if no COG exists for that time
        for f in sorted(path.glob("*.tif")):
            # Skip files that are themselves COGs (already handled above)
            if f.name.endswith("_cog.tif"):
                continue
            dt = _parse_datetime(f.name, product.filename_regex)
            if dt and dt not in dt_to_file:
                dt_to_file[dt] = str(f)

        entries = sorted(dt_to_file.items(), key=lambda x: x[0])
        new_index[product.id] = entries
        
        cog_count = sum(1 for _, fp in entries if fp.endswith("_cog.tif"))
        print(f"[INDEX] {product.id}: {len(entries)} granules ({cog_count} COG)")

    _file_index = new_index
    _index_built_at = now


@app.on_event("startup")
async def startup():
    build_file_index(force=True)
    print(f"[STARTUP] Indexed {len(_file_index)} products")


def get_file_for_time(product_id: str, target_time: datetime) -> Optional[str]:
    """Find the GeoTIFF file for the EXACT requested time.
    
    Returns None if no file exists at that exact timestamp — the caller
    should return a fully transparent tile. No nearest-match fallback.
    """
    entries = _file_index.get(product_id, [])
    if not entries:
        return None
    # Build dict for O(1) exact lookup
    time_to_file = dict(entries)
    return time_to_file.get(target_time)


def _transparent_png() -> Response:
    """Return a minimal fully-transparent 1x1 PNG."""
    buf = BytesIO()
    # Pre-computed 1x1 transparent PNG (67 bytes)
    Image.new("RGBA", (1, 1), (0, 0, 0, 0)).save(buf, format="PNG")
    buf.seek(0)
    return Response(content=buf.read(), media_type="image/png")


def get_available_times(product_id: str) -> List[datetime]:
    """Return sorted list of available timesteps for a product."""
    entries = _file_index.get(product_id, [])
    return [e[0] for e in entries]


# ── Web Mercator helpers ────────────────────────────────────────────────────

WEB_MERCATOR_ORIGIN = 20037508.342789244
WEB_MERCATOR_EXTENT = 40075016.68557849


def _tile_bounds(x: int, y: int, z: int) -> Tuple[float, float, float, float]:
    """Convert tile x/y/z to bbox in EPSG:3857 (Web Mercator) meters."""
    n = 2.0 ** z
    min_x = x / n * WEB_MERCATOR_EXTENT - WEB_MERCATOR_ORIGIN
    max_x = (x + 1) / n * WEB_MERCATOR_EXTENT - WEB_MERCATOR_ORIGIN
    min_y = WEB_MERCATOR_ORIGIN - (y + 1) / n * WEB_MERCATOR_EXTENT
    max_y = WEB_MERCATOR_ORIGIN - y / n * WEB_MERCATOR_EXTENT
    return (min_x, min_y, max_x, max_y)


# ── Core rendering ──────────────────────────────────────────────────────────

def _apply_colormap(data: np.ndarray, mask: np.ndarray, colormap: dict) -> np.ndarray:
    """
    Apply a colormap to each pixel's EXACT value — no interpolation.

    Each pixel gets the pure color of the interval its value falls into,
    matching GeoServer SLD type="intervals" behavior exactly.
    Uses searchsorted(side="left") for nearest-upper-bound mapping.
    """
    stops = sorted(colormap.items(), key=lambda x: x[0])
    stop_vals = np.array([s[0] for s in stops], dtype=np.float64)
    stop_colors = np.array(
        [[c[0], c[1], c[2], c[3] if len(c) > 3 else 255] for _, c in stops],
        dtype=np.uint8,
    )

    height, width = data.shape
    rgba = np.zeros((height, width, 4), dtype=np.uint8)

    valid = mask & (~np.isnan(data))

    if not valid.any():
        return rgba

    d = data.astype(np.float64)

    # Find the first stop where value <= stop_val → pure color, no blending
    indices = np.searchsorted(stop_vals, d, side="left")
    indices = np.clip(indices, 0, len(stop_vals) - 1)

    for c in range(4):
        rgba[:, :, c] = np.where(valid, stop_colors[indices, c], 0)

    return rgba


def render_raster(
    filepath: str,
    colormap: dict,
    bbox: Tuple[float, float, float, float],
    width: int,
    height: int,
    dst_crs: str = "EPSG:3857",
    nodata: float = -9999.0,
) -> np.ndarray:
    """
    Read a GeoTIFF, reproject/clip to target bbox, and produce an RGBA uint8 image.

    Uses rasterio's reproject() for correct on-the-fly warping between CRS.
    Returns RGBA numpy array (height, width, 4), uint8.
    """
    with rasterio.open(filepath) as src:
        # Normalize source CRS (EF5 GeoTIFFs have non-standard WKT)
        src_crs = src.crs
        if src_crs.to_epsg() is None:
            # Fallback: assume WGS84 for geographic CRS with degree units
            if src_crs.is_geographic:
                src_crs = rasterio.crs.CRS.from_epsg(4326)
            else:
                raise ValueError(f"Unknown source CRS: {src.crs}")

        # --- Step 1: Determine source window that overlaps target bbox ---
        try:
            # Transform target bbox (dst_crs) → source CRS
            src_bbox = transform_bounds(dst_crs, src_crs, *bbox, densify_pts=21)
            # Get pixel window for that extent in source raster
            window = from_bounds(*src_bbox, src.transform).round_lengths().round_offsets()
            # Clamp window to valid raster extent
            window = window.intersection(
                rasterio.windows.Window(0, 0, src.width, src.height)
            )
            if window.width < 1 or window.height < 1:
                # No overlap — return fully transparent image
                return np.zeros((height, width, 4), dtype=np.uint8)
        except Exception:
            # If bounds transform fails, read the whole raster (small rasters only)
            window = rasterio.windows.Window(0, 0, src.width, src.height)

        # --- Step 2: Read source data ---
        src_data = src.read(1, window=window, boundless=True, fill_value=nodata)

        # --- Step 3: Reproject + resize in one step ---
        # Calculate source transform for the read window
        src_transform = src.window_transform(window)

        # Destination: target bbox in dst_crs at requested dimensions
        dst_transform = rasterio.transform.from_bounds(*bbox, width=width, height=height)

        dst_data = np.full((height, width), nodata, dtype=src_data.dtype)

        rasterio.warp.reproject(
            source=src_data,
            destination=dst_data,
            src_transform=src_transform,
            src_crs=src_crs,
            dst_transform=dst_transform,
            dst_crs=dst_crs,
            resampling=rasterio.warp.Resampling.nearest,
            src_nodata=nodata,
            dst_nodata=nodata,
        )

        # --- Step 4: Build mask & apply colormap ---
        mask = (dst_data != nodata) & (~np.isnan(dst_data))

        rgba = _apply_colormap(dst_data, mask, colormap)
        return rgba


# ═════════════════════════════════════════════════════════════════════════════
# API Endpoints
# ═════════════════════════════════════════════════════════════════════════════


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "products_indexed": len(_file_index),
        "cache": _tile_cache.stats,
    }


@app.get("/products")
async def list_products():
    build_file_index()
    result = []
    for p in PRODUCTS:
        times = get_available_times(p.id)
        result.append(
            {
                "id": p.id,
                "name": p.name,
                "region": p.region,
                "source": p.source,
                "units": p.units,
                "numGranules": len(times),
                "timeMin": times[0].isoformat() if times else None,
                "timeMax": times[-1].isoformat() if times else None,
            }
        )
    return result


@app.get("/products/{product_id}")
async def get_product(product_id: str):
    build_file_index()
    product = PRODUCT_BY_ID.get(product_id)
    if not product:
        raise HTTPException(404, f"Unknown product: {product_id}")

    times = get_available_times(product_id)
    return {
        "id": product.id,
        "name": product.name,
        "region": product.region,
        "source": product.source,
        "units": product.units,
        "numGranules": len(times),
        "timesteps": [t.isoformat() for t in times],
        "timeMin": times[0].isoformat() if times else None,
        "timeMax": times[-1].isoformat() if times else None,
    }


# ── XYZ Tile endpoint ──────────────────────────────────────────────────────

@app.get("/tiles/{product_id}/{z}/{x}/{y}.png")
async def xyz_tile(
    product_id: str,
    z: int,
    x: int,
    y: int,
    time: Optional[str] = Query(
        None, description="ISO8601 datetime, e.g. 2025-06-08T12:00:00"
    ),
):
    """Serve a single XYZ tile as PNG with colormap applied (EPSG:3857, 256px)."""
    build_file_index()
    product = PRODUCT_BY_ID.get(product_id)
    if not product:
        raise HTTPException(404, f"Unknown product: {product_id}")

    times = get_available_times(product_id)
    if not times:
        raise HTTPException(404, f"No data for product '{product_id}'")

    if time:
        try:
            target = _normalize_time(time)
        except ValueError:
            raise HTTPException(400, f"Invalid ISO datetime: {time}")
    else:
        target = times[-1]

    filepath = get_file_for_time(product_id, target)
    if filepath is None:
        return _transparent_png()
    bbox = _tile_bounds(x, y, z)

    # Check tile cache first
    cached = _tile_cache.get(filepath, z, x, y)
    if cached is not None:
        buf = BytesIO()
        Image.fromarray(cached, "RGBA").save(buf, format="PNG", optimize=True)
        buf.seek(0)
        return Response(
            content=buf.read(),
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=300"},
        )

    try:
        rgba = render_raster(
            filepath,
            colormap=product.colormap,
            bbox=bbox,
            width=256,
            height=256,
            dst_crs="EPSG:3857",
            nodata=product.nodata,
        )
    except Exception as e:
        raise HTTPException(500, f"Render error: {e}")

    # Store in cache
    _tile_cache.put(filepath, z, x, y, rgba)

    buf = BytesIO()
    Image.fromarray(rgba, "RGBA").save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return Response(
        content=buf.read(),
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=300"},
    )


# ── WMS Endpoint ───────────────────────────────────────────────────────────

@app.get("/wms")
async def wms(
    product_id: str = Query(..., description="Product ID"),
    request: str = Query("GetMap", alias="REQUEST", description="WMS request type"),
    service: str = Query("WMS", alias="SERVICE", description="Service type"),
    version: str = Query("1.3.0", alias="VERSION", description="WMS version"),
    layers: Optional[str] = Query(None, alias="LAYERS", description="Layer alias for product_id"),
    time: Optional[str] = Query(None, alias="TIME", description="ISO8601 datetime"),
    bbox: Optional[str] = Query(None, alias="BBOX", description="BBOX: minx,miny,maxx,maxy"),
    width: Optional[int] = Query(256, alias="WIDTH", description="Image width"),
    height: Optional[int] = Query(256, alias="HEIGHT", description="Image height"),
    crs: Optional[str] = Query(None, alias="CRS", description="CRS (WMS 1.3.0)"),
    srs: Optional[str] = Query(None, alias="SRS", description="SRS (WMS 1.1.1)"),
    fmt: Optional[str] = Query("image/png", alias="FORMAT", description="Output format"),
    transparent: Optional[str] = Query("true", alias="TRANSPARENT", description="Transparent"),
    styles: Optional[str] = Query("", alias="STYLES", description="Style"),
):
    """WMS GetMap — compatible with GeoServer/Leflet WMS clients."""
    build_file_index()

    pid = layers if layers else product_id
    product = PRODUCT_BY_ID.get(pid)
    if not product:
        raise HTTPException(404, f"Unknown product: {pid}")

    times = get_available_times(pid)
    if not times:
        raise HTTPException(404, f"No data for product '{pid}'")

    if time:
        try:
            target = _normalize_time(time)
        except ValueError:
            raise HTTPException(400, f"Invalid ISO datetime: {time}")
    else:
        target = times[-1]

    filepath = get_file_for_time(pid, target)
    if filepath is None:
        return _transparent_png()
    out_crs = crs or srs or "EPSG:3857"

    if not bbox:
        raise HTTPException(400, "BBOX parameter is required")
    try:
        parts = [float(x) for x in bbox.split(",")]
        if len(parts) != 4:
            raise ValueError
        bbox_tuple = (parts[0], parts[1], parts[2], parts[3])
    except (ValueError, IndexError):
        raise HTTPException(400, f"Invalid BBOX: {bbox}")

    w = max(width or 256, 1)
    h = max(height or 256, 1)

    try:
        rgba = render_raster(
            filepath,
            colormap=product.colormap,
            bbox=bbox_tuple,
            width=w,
            height=h,
            dst_crs=out_crs,
            nodata=product.nodata,
        )
    except Exception as e:
        raise HTTPException(500, f"Render error: {e}")

    buf = BytesIO()
    Image.fromarray(rgba, "RGBA").save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return Response(content=buf.read(), media_type="image/png")


# ── WMS GetCapabilities ────────────────────────────────────────────────────

@app.get("/wms/capabilities")
async def wms_capabilities():
    """Minimal WMS GetCapabilities XML."""
    build_file_index()
    layers_xml = []
    for p in PRODUCTS:
        times = get_available_times(p.id)
        t_min = times[0].strftime("%Y-%m-%dT%H:%M:%SZ") if times else ""
        t_max = times[-1].strftime("%Y-%m-%dT%H:%M:%SZ") if times else ""
        layers_xml.append(
            f"""    <Layer queryable="0" opaque="0">
        <Name>{p.id}</Name>
        <Title>{p.name}</Title>
        <Abstract>{p.name} — {p.source} {p.units}</Abstract>
        <CRS>EPSG:4326</CRS>
        <CRS>EPSG:3857</CRS>
        <EX_GeographicBoundingBox>
            <westBoundLongitude>-180</westBoundLongitude>
            <eastBoundLongitude>180</eastBoundLongitude>
            <southBoundLatitude>-90</southBoundLatitude>
            <northBoundLatitude>90</northBoundLatitude>
        </EX_GeographicBoundingBox>
        <Dimension name="time" units="ISO8601" default="{t_max}">
            {t_min}/{t_max}/PT1H</Dimension>
    </Layer>"""
        )

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<WMS_Capabilities xmlns="http://www.opengis.net/wms" version="1.3.0">
    <Service>
        <Name>WMS</Name>
        <Title>AHWA TiTiler WMS</Title>
        <Abstract>Dynamic raster tile server for hydrometeorological products</Abstract>
    </Service>
    <Capability>
        <Request>
            <GetCapabilities><Format>text/xml</Format></GetCapabilities>
            <GetMap><Format>image/png</Format></GetMap>
        </Request>
        <Layer>
            <Title>AHWA Products</Title>
            <CRS>EPSG:4326</CRS>
            <CRS>EPSG:3857</CRS>
{chr(10).join(layers_xml)}
        </Layer>
    </Capability>
</WMS_Capabilities>"""
    return Response(content=xml, media_type="application/xml")


# ── Legend endpoint ────────────────────────────────────────────────────────

@app.get("/legend/{product_id}.png")
async def legend(product_id: str, width: int = 24, height: int = 256):
    """Generate a vertical color ramp legend PNG."""
    product = PRODUCT_BY_ID.get(product_id)
    if not product:
        raise HTTPException(404, f"Unknown product: {product_id}")

    stops = sorted(product.colormap.items(), key=lambda x: x[0])
    gradient = np.zeros((height, width, 4), dtype=np.uint8)

    for row in range(height):
        frac = 1.0 - (row / max(height - 1, 1))
        val = stops[0][0] + frac * (stops[-1][0] - stops[0][0])

        idx = np.searchsorted([s[0] for s in stops], val, side="right")
        idx = max(1, min(idx, len(stops) - 1))

        lo_val, lo_col = stops[idx - 1]
        hi_val, hi_col = stops[idx]
        denom = hi_val - lo_val if hi_val != lo_val else 1.0
        t = (val - lo_val) / denom

        r = int(lo_col[0] + t * (hi_col[0] - lo_col[0]))
        g = int(lo_col[1] + t * (hi_col[1] - lo_col[1]))
        b = int(lo_col[2] + t * (hi_col[2] - lo_col[2]))
        a = 255
        if len(lo_col) > 3 and len(hi_col) > 3:
            a = int(lo_col[3] + t * (hi_col[3] - lo_col[3]))
        gradient[row, :] = (r, g, b, a)

    # Make canvas wider for labels
    label_w = 60
    canvas = Image.new("RGBA", (width + label_w, height), (0, 0, 0, 0))
    canvas.paste(Image.fromarray(gradient, "RGBA"), (0, 0))

    from PIL import ImageDraw, ImageFont

    draw = ImageDraw.Draw(canvas)
    try:
        font = ImageFont.load_default()
    except Exception:
        font = None

    num_labels = min(8, len(stops))
    step = max(1, len(stops) // num_labels)
    for i in range(0, len(stops), step):
        stop_val, _ = stops[i]
        frac = (
            (stop_val - stops[0][0]) / (stops[-1][0] - stops[0][0])
            if stops[-1][0] != stops[0][0]
            else 0
        )
        y_pos = int((1.0 - frac) * (height - 1))
        y_pos = max(0, min(height - 1, y_pos))

        draw.line(
            [(width - 2, y_pos), (width + 6, y_pos)], fill=(100, 100, 100, 255)
        )
        if isinstance(stop_val, float):
            if stop_val < 1:
                label = f"{stop_val:.2f}"
            elif stop_val < 10:
                label = f"{stop_val:.1f}"
            else:
                label = f"{stop_val:.0f}"
        else:
            label = str(stop_val)
        draw.text((width + 8, y_pos - 6), label, fill=(80, 80, 80, 255), font=font)

    buf = BytesIO()
    canvas.save(buf, format="PNG")
    buf.seek(0)
    return Response(content=buf.read(), media_type="image/png")


# ── Admin ──────────────────────────────────────────────────────────────────

@app.post("/admin/refresh")
async def refresh_index():
    """Force rebuild the file index."""
    build_file_index(force=True)
    total = sum(len(v) for v in _file_index.values())
    return {"status": "ok", "total_granules": total}


# ── Discharge CSV Endpoint ──────────────────────────────────────────────────
# Serves timeseries CSV files from DATA_ROOT/discharge/{timestep}/
# Used by the AHWA TiTiler Viewer web app for gauge popup charts.

from fastapi.responses import PlainTextResponse

try:
    from config import DATA_ROOT as CONFIG_DATA_ROOT
except ImportError:
    CONFIG_DATA_ROOT = "/home/nammehta/TITOCubaMainTest/titilerTest"

DISCHARGE_ROOT = os.path.join(CONFIG_DATA_ROOT, "discharge")


@app.get("/discharge/{timestep}")
async def list_discharge_files(timestep: str):
    """List available CSV files for a timestep (e.g., 202606091400)."""
    ts_dir = os.path.join(DISCHARGE_ROOT, timestep)
    if not os.path.isdir(ts_dir):
        return []
    try:
        files = sorted([
            f for f in os.listdir(ts_dir)
            if f.endswith('.csv')
        ])
        return files
    except Exception:
        return []


@app.get("/discharge/{timestep}/{filename}")
async def get_discharge_csv(timestep: str, filename: str):
    """Serve a specific discharge CSV file for a timestep."""
    filepath = os.path.join(DISCHARGE_ROOT, timestep, filename)
    if not os.path.isfile(filepath):
        raise HTTPException(status_code=404, detail="CSV file not found")
    try:
        with open(filepath, 'r') as f:
            content = f.read()
        return PlainTextResponse(content=content, media_type="text/csv")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/cron")
async def cron_status():
    """Return crontab contents for the status dashboard."""
    import subprocess
    try:
        result = subprocess.run(
            ["crontab", "-l"], capture_output=True, text=True, timeout=5
        )
        lines = [
            line.strip()
            for line in result.stdout.split("\n")
            if line.strip() and not line.strip().startswith("#")
        ]
        return {"jobs": lines}
    except Exception as e:
        return {"jobs": [f"Error reading crontab: {e}"]}


@app.get("/admin/check-path")
async def check_path(path: str = Query("/Dedicated/Humberto")):
    """Check if a filesystem path is accessible."""
    import os
    ok = os.path.isdir(path)
    return {"path": path, "accessible": ok}


# ── Root ───────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return RedirectResponse(url="/docs")


# ── Run (development) ──────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
