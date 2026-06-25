# AHWA TiTiler API

Dynamic raster tile server for AHWA hydrometeorological products — drop-in replacement for GeoServer ImageMosaic layers.

Built on [TiTiler](https://developmentseed.org/titiler/) / [rio-tiler](https://github.com/cogeotiff/rio-tiler).

## Why TiTiler?

| Problem with GeoServer | TiTiler Solution |
|---|---|
| Docker crashes with large mosaics | Single Python process, no JVM |
| Must recreate layers to refresh index | Auto-scans directories on 5-min cache |
| Chessboard loading effect with WMS | XYZ tiles load progressively |
| Slow WMS rendering | rio-tiler renders on the fly from GeoTIFF |
| Complex styling via SLD XML | Colormaps defined in Python config |

## Products Served (6 layers)

All products serve Cuba regional GFS forecast and IMERG observation data.
Data is refreshed every pipeline run via `refresh_titiler.sh`.

### Base Products (5)

| # | Product ID | Name | Source | Short Name |
|---|-----------|------|--------|------------|
| 1 | `cuba_gfs_maxUnitStreamFlow` | Max Unit Streamflow 
| 2 | `cuba_gfs_maxStreamFlow` | Max Streamflow 
| 3 | `cuba_gfs_precipAccum` | QPF Accumulation | GFS | qpfaccum |
| 4 | `cuba_gfs_precipAccumImerg` | QPE Accumulation | IMERG | qpeaccum |
| 5 | `cuba_gfs_maxSoilSat` | Max Soil Saturation

### High-Res Product (1)

| # | Product ID | Name | Source | Short Name |
|---|-----------|------|--------|------------|
| 6 | `cuba_gfs_highResMaxInu` | Max Inundation Depth (25m) | GFS | maxdepth |

---

## Product URLs

Replace `{timestep}` with an ISO 8601 datetime, e.g. `2026-06-09T14:00:00`.
Available timesteps: `GET /products/{product_id}` → `timesteps` array.

### 1. Max Unit Streamflow — `cuba_gfs_maxUnitStreamFlow`

**XYZ:**
```
/tiles/cuba_gfs_maxUnitStreamFlow/{z}/{x}/{y}.png?time={timestep}
```
**WMS:**
```
/wms?product_id=cuba_gfs_maxUnitStreamFlow&REQUEST=GetMap&SERVICE=WMS&VERSION=1.3.0&LAYERS=cuba_gfs_maxUnitStreamFlow&STYLES=&CRS=EPSG:4326&BBOX=-85,19,-73,24&WIDTH=512&HEIGHT=512&FORMAT=image/png&TRANSPARENT=true&TIME={timestep}
```

### 2. Max Streamflow — `cuba_gfs_maxStreamFlow`

**XYZ:**
```
/tiles/cuba_gfs_maxStreamFlow/{z}/{x}/{y}.png?time={timestep}
```
**WMS:**
```
/wms?product_id=cuba_gfs_maxStreamFlow&REQUEST=GetMap&SERVICE=WMS&VERSION=1.3.0&LAYERS=cuba_gfs_maxStreamFlow&STYLES=&CRS=EPSG:4326&BBOX=-85,19,-73,24&WIDTH=512&HEIGHT=512&FORMAT=image/png&TRANSPARENT=true&TIME={timestep}
```

### 3. QPF Accumulation (GFS Forecast) — `cuba_gfs_precipAccum`

**XYZ:**
```
/tiles/cuba_gfs_precipAccum/{z}/{x}/{y}.png?time={timestep}
```
**WMS:**
```
/wms?product_id=cuba_gfs_precipAccum&REQUEST=GetMap&SERVICE=WMS&VERSION=1.3.0&LAYERS=cuba_gfs_precipAccum&STYLES=&CRS=EPSG:4326&BBOX=-85,19,-73,24&WIDTH=512&HEIGHT=512&FORMAT=image/png&TRANSPARENT=true&TIME={timestep}
```

### 4. QPE Accumulation (IMERG Observed) — `cuba_gfs_precipAccumImerg`

**XYZ:**
```
/tiles/cuba_gfs_precipAccumImerg/{z}/{x}/{y}.png?time={timestep}
```
**WMS:**
```
/wms?product_id=cuba_gfs_precipAccumImerg&REQUEST=GetMap&SERVICE=WMS&VERSION=1.3.0&LAYERS=cuba_gfs_precipAccumImerg&STYLES=&CRS=EPSG:4326&BBOX=-85,19,-73,24&WIDTH=512&HEIGHT=512&FORMAT=image/png&TRANSPARENT=true&TIME={timestep}
```

### 5. Max Soil Saturation — `cuba_gfs_maxSoilSat`

**XYZ:**
```
/tiles/cuba_gfs_maxSoilSat/{z}/{x}/{y}.png?time={timestep}
```
**WMS:**
```
/wms?product_id=cuba_gfs_maxSoilSat&REQUEST=GetMap&SERVICE=WMS&VERSION=1.3.0&LAYERS=cuba_gfs_maxSoilSat&STYLES=&CRS=EPSG:4326&BBOX=-85,19,-73,24&WIDTH=512&HEIGHT=512&FORMAT=image/png&TRANSPARENT=true&TIME={timestep}
```

### 6. Max Inundation Depth (25m High-Res) — `cuba_gfs_highResMaxInu`

**XYZ:**
```
/tiles/cuba_gfs_highResMaxInu/{z}/{x}/{y}.png?time={timestep}
```
**WMS:**
```
/wms?product_id=cuba_gfs_highResMaxInu&REQUEST=GetMap&SERVICE=WMS&VERSION=1.3.0&LAYERS=cuba_gfs_highResMaxInu&STYLES=&CRS=EPSG:4326&BBOX=-85,19,-73,24&WIDTH=512&HEIGHT=512&FORMAT=image/png&TRANSPARENT=true&TIME={timestep}
```

---

## Setup

### 1. Create Conda Environment

```bash
cd titiler_api
conda env create -f environment.yml
conda activate titiler-ahwa
```

### 2. Start the Server

```bash
bash start.sh 2000      # Production port
bash start.sh 8000      # Alternative port
```

### 3. Refresh Data

Data is staged from the TITO pipeline output into TiTiler's directory tree:

```bash
bash refresh_titiler.sh
```

This moves GeoTIFFs, CSVs, and logs from `outputs/tmp_output_crest/` → `DATA_ROOT/`.

### 4. Verify

Open http://localhost:2000/docs for interactive API docs, or http://localhost:2000/health for a JSON health check.

---

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check + product count |
| `/products` | GET | List all products with granule counts |
| `/products/{id}` | GET | Product details + available timesteps |
| `/tiles/{id}/{z}/{x}/{y}.png?time=...` | GET | XYZ tiles (Leaflet-compatible) |
| `/wms?product_id=...&REQUEST=GetMap&...` | GET | WMS GetMap + GetCapabilities |
| `/legend/{id}.png` | GET | Color ramp legend PNG |
| `/discharge/{timestep}` | GET | List CSV files for a timestep |
| `/discharge/{timestep}/{filename}` | GET | Raw CSV timeseries data |
| `/admin/refresh` | POST | Force rebuild file index |

### XYZ Tile Example

```bash
curl "http://localhost:2000/tiles/cuba_gfs_maxStreamFlow/7/30/45.png?time=2026-06-09T14:00:00"
```

### WMS GetMap Example

```bash
curl "http://localhost:2000/wms?product_id=cuba_gfs_maxStreamFlow\
&REQUEST=GetMap&SERVICE=WMS&VERSION=1.3.0\
&LAYERS=cuba_gfs_maxStreamFlow&STYLES=\
&CRS=EPSG:4326\
&BBOX=-85,19,-73,24&WIDTH=512&HEIGHT=512\
&FORMAT=image/png&TRANSPARENT=true\
&TIME=2026-06-09T14:00:00"
```

---

## Companion: AHWA TiTiler Viewer

A Svelte + Leaflet web app ships alongside this API in `../titiler_viewer/`.

```bash
cd ../titiler_viewer
bash setup.sh     # install npm dependencies
npm run dev       # start dev server → http://localhost:5173
```

Features:
- Product selector (4 main layers)
- XYZ / WMS toggle
- ±1h, ±6h, ±24h timestep navigation (UTC, no future)
- 50+ gauge markers with popup sparkline charts (discharge, precip, SM, PET)
- API health status bar

---

## Data Pipeline

```
TITO Pipeline (orchestrator.py)
    │
    └── outputs/tmp_output_crest/{timestep}/
        ├── maxq.YYYYMMDD.HHMMSS.tif
        ├── qpfaccum.YYYYMMDD.HHMMSS.tif
        ├── ts.{station}.crest.YYYYMMDD.HHMMSS.csv
        ├── CU_Regional_crest.txt
        ├── ef5.YYYYMMDD.HHMMSS.log
        └── results.json
                │
    ┌───────────▼────────────────────┐
    │  refresh_titiler.sh            │
    │  - Moves TIFFs → DATA_ROOT/    │
    │  - COG-converts TIFFs          │
    │  - Moves CSVs → discharge/     │
    │  - Moves logs → logs/          │
    └───────────┬────────────────────┘
                │
    ┌───────────▼────────────────────┐
    │  titiler_api (FastAPI)         │
    │  - Scans TIFF directories      │
    │  - Serves XYZ + WMS tiles      │
    │  - Serves discharge CSV data   │
    └───────────┬────────────────────┘
                │
    ┌───────────▼────────────────────┐
    │  titiler_viewer (Svelte)       │
    │  - Leaflet map overlay         │
    │  - Gauge popup charts          │
    └────────────────────────────────┘
```

## Configuration

Edit `config.py` to modify:
- `DATA_ROOT` — base path for raster data
- Products list — add/remove layers
- Colormaps — adjust styling per product

## Nginx (Production)

```nginx
location /titiler/ {
    proxy_pass http://127.0.0.1:2000/;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_read_timeout 30s;
}
```
```
