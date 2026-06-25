"""
TiTiler API Configuration — Product definitions, paths, and colormaps.

All GeoTIFF directories and styling are defined here so the API server
does not need any other configuration source.

Cuba GFS + IMERG products (6 layers). Served from /var/ef5/geoServer/.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional

# ── Base data root ──────────────────────────────────────────────────────────
# TiTiler serves directly from the GeoServer data tree.
# Must match DATA_ROOT in refresh_titiler.sh.
# DATA_ROOT = "/var/ef5/geoServer"

DATA_ROOT = "/home/nammehta/TITOCubaMainTest/titilerTest"



# ── Colormap definitions (exact GeoServer SLD styles, discrete intervals) ────
# Each stop defines the UPPER BOUND of a color class.
# Values ≤ stop get that color. Uses searchsorted(side="left") — no blending.

# "usaflash:Streamflow Color Ramp" — max streamflow (m³/s)
STREAMFLOW_COLORMAP = {
    0:      (0x40, 0x40, 0x40, 255),   # ≤ 0
    1:      (0xA0, 0xA0, 0xA0, 255),   # 0–1
    10:     (0xD5, 0xD5, 0x00, 255),   # 1–10
    100:    (0xFC, 0x8A, 0x23, 255),   # 10–100
    1000:   (0xBC, 0x33, 0x33, 255),   # 100–1,000
    10000:  (0xE6, 0x3D, 0xDD, 255),   # 1,000–10,000
    999999: (0x00, 0x0B, 0xD5, 255),   # >10,000
}

# "usaflash:crest_max_unit_q_ramp" — max unit streamflow (m³/s/km²)
UNIT_STREAMFLOW_COLORMAP = {
    0:      (0x59, 0x5A, 0x5A, 255),   # ≤ 0
    0.5:    (0xB2, 0xB2, 0xB3, 255),   # 0–0.5
    1:      (0xE6, 0xE6, 0xE8, 255),   # 0.5–1
    1.5:    (0xFF, 0xFD, 0x60, 255),   # 1–1.5
    1.75:   (0xFF, 0xF7, 0x01, 255),   # 1.5–1.75
    2:      (0xFF, 0xEB, 0x00, 255),   # 1.75–2
    2.5:    (0xFF, 0x91, 0x01, 255),   # 2–2.5
    3:      (0xFF, 0xA2, 0x01, 255),   # 2.5–3
    4:      (0xFF, 0xB3, 0x01, 255),   # 3–4
    4.5:    (0xEF, 0x29, 0x01, 255),   # 4–4.5
    5:      (0xDF, 0x2A, 0x01, 255),   # 4.5–5
    6:      (0xBE, 0x22, 0x00, 255),   # 5–6
    7:      (0xC4, 0x00, 0x90, 255),   # 6–7
    8:      (0xD3, 0x00, 0xBE, 255),   # 7–8
    10:     (0xD5, 0x00, 0xFF, 255),   # 8–10
    13:     (0x00, 0x08, 0xFF, 255),   # 10–13
    15:     (0x00, 0x00, 0xDF, 255),   # 13–15
    20:     (0x0E, 0x01, 0x9F, 255),   # 15–20
    999999: (0x0E, 0x01, 0x9F, 255),   # >20
}

# "usaflash:Precipitation (mm) — Discrete Ranges"
PRECIP_COLORMAP = {
    0:       (0xFF, 0xFF, 0xFF, 255),   # 0 mm
    2.54:    (0xE8, 0xF7, 0xE3, 255),   # 0–2.54
    6.35:    (0xCB, 0xEC, 0xC2, 255),   # 2.54–6.35
    12.70:   (0xA4, 0xE1, 0xA8, 255),   # 6.35–12.70
    25.40:   (0x6C, 0xC4, 0x7F, 255),   # 12.70–25.40
    38.10:   (0xB9, 0xE2, 0x6B, 255),   # 25.40–38.10
    50.80:   (0xFF, 0xE5, 0x6B, 255),   # 38.10–50.80
    76.20:   (0xFF, 0xC8, 0x4A, 255),   # 50.80–76.20
    101.60:  (0xFF, 0xA4, 0x3A, 255),   # 76.20–101.60
    152.40:  (0xFF, 0x7A, 0x3A, 255),   # 101.60–152.40
    203.20:  (0xFF, 0x4A, 0x3A, 255),   # 152.40–203.20
    254.00:  (0xD9, 0x25, 0x25, 255),   # 203.20–254.00
    381.00:  (0xC0, 0x4C, 0xBF, 255),   # 254–381
    508.00:  (0x8C, 0x3F, 0xB2, 255),   # 381–508
    762.00:  (0x57, 0x39, 0xB0, 255),   # 508–762
    9999999: (0xE0, 0xC9, 0xFF, 255),   # >762
}

# "usaflash:soil_saturation_ramp" — soil saturation (0–100%)
SOIL_SATURATION_COLORMAP = {
    0:      (0xFF, 0xFF, 0xFF, 255),   # ≤ 0%
    50:     (0xA0, 0xA0, 0xA0, 255),   # 0–50%
    75:     (0x00, 0xDA, 0x00, 255),   # 50–75%
    85:     (0xFF, 0xD6, 0x00, 255),   # 75–85%
    95:     (0xD5, 0x00, 0x00, 255),   # 85–95%
    100:    (0xDA, 0x00, 0xDA, 255),   # 95–100%
    999999: (0xDA, 0x00, 0xDA, 255),   # >100%
}

# "usaflash:Inu" — inundation (meters), binary blue above 1m
INUNDATION_COLORMAP = {
    0:      (0xFF, 0xFF, 0xFF, 0),     # ≤ 0: transparent
    1:      (0xFF, 0xFF, 0xFF, 0),     # 0–1: transparent
    999999: (0x15, 0x81, 0xBF, 255),   # >1: blue #1581BF
}


# ── Product definition ──────────────────────────────────────────────────────

@dataclass
class Product:
    """Definition of a single raster product served by TiTiler."""

    # Unique product ID used in API URLs (e.g., "cuba_gfs_maxStreamFlow")
    id: str

    # Human-readable name
    name: str

    # Region key: "cuba", "cuba_imerg", or "westafrica"
    region: str

    # Filesystem path to the directory containing GeoTIFF granules
    path: str

    # Filename pattern for parsing datetimes.
    # Use Python regex with named group "datetime" that captures YYYYMMDDTHHMMSS.
    # Example: r"maxq(?P<datetime>\d{8}T\d{6})\.tif"
    filename_regex: str

    # rio-tiler colormap dict
    colormap: dict

    # Nodata value
    nodata: float = -9999.0

    # Min/max for rescaling (None = auto from data)
    min_val: Optional[float] = None
    max_val: Optional[float] = None

    # Legend label
    units: str = ""

    # Data source label (GFS, IMERG, etc.)
    source: str = "GFS"


# ── All products ────────────────────────────────────────────────────────────
# Cuba GFS + IMERG precipitation.  Directories are flat under DATA_ROOT.

PRODUCTS: List[Product] = [
    Product(
        id="cuba_gfs_maxUnitStreamFlow",
        name="Cuba Max Unit Streamflow (GFS)",
        region="cuba",
        path=os.path.join(DATA_ROOT, "maxunitq"),
        filename_regex=r"maxunitq_(?P<datetime>\d{8}T\d{6})\.tif",
        colormap=UNIT_STREAMFLOW_COLORMAP,
        units="m³/s/km²",
        source="GFS",
    ),
    Product(
        id="cuba_gfs_maxStreamFlow",
        name="Cuba Max Streamflow (GFS)",
        region="cuba",
        path=os.path.join(DATA_ROOT, "maxq"),
        filename_regex=r"maxq_(?P<datetime>\d{8}T\d{6})\.tif",
        colormap=STREAMFLOW_COLORMAP,
        units="m³/s",
        source="GFS",
    ),
    Product(
        id="cuba_gfs_precipAccum",
        name="Cuba Precipitation Accumulation (GFS)",
        region="cuba",
        path=os.path.join(DATA_ROOT, "qpfaccum"),
        filename_regex=r"qpfaccum_(?P<datetime>\d{8}T\d{6})\.tif",
        colormap=PRECIP_COLORMAP,
        units="mm",
        source="GFS",
    ),
    Product(
        id="cuba_gfs_precipAccumImerg",
        name="Cuba Precipitation Accumulation (IMERG)",
        region="cuba",
        path=os.path.join(DATA_ROOT, "qpeaccum"),
        filename_regex=r"qpeaccum_(?P<datetime>\d{8}T\d{6})\.tif",
        colormap=PRECIP_COLORMAP,
        units="mm",
        source="IMERG",
    ),
    Product(
        id="cuba_gfs_maxSoilSat",
        name="Cuba Max Soil Saturation (GFS)",
        region="cuba",
        path=os.path.join(DATA_ROOT, "maxsm"),
        filename_regex=r"maxsm_(?P<datetime>\d{8}T\d{6})\.tif",
        colormap=SOIL_SATURATION_COLORMAP,
        units="fraction (0-1)",
        source="GFS",
    ),
    Product(
        id="cuba_gfs_highResMaxInu",
        name="Cuba High-Res Max Inundation (GFS)",
        region="cuba",
        path=os.path.join(DATA_ROOT, "maxdepth"),
        filename_regex=r"maxdepth_(?P<datetime>\d{8}T\d{6})\.tif",
        colormap=INUNDATION_COLORMAP,
        units="meters",
        source="GFS",
    ),
]

# Build lookup dict
PRODUCT_BY_ID: Dict[str, Product] = {p.id: p for p in PRODUCTS}
