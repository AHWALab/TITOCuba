# AHWA TiTiler API

Dynamic raster tile server for AHWA hydrometeorological products — a drop-in replacement for GeoServer ImageMosaic layers.

Built on [TiTiler](https://developmentseed.org/titiler/) / [rio-tiler](https://github.com/cogeotiff/rio-tiler).

## Why TiTiler?

| Problem with GeoServer | TiTiler Solution |
|---|---|
| Docker crashes with large mosaics | Single Python process, no JVM |
| Must recreate layers to refresh index | Auto-scans directories on 5-min cache |
| Chessboard loading effect with WMS | XYZ tiles load progressively |
| Slow WMS rendering | rio-tiler renders on the fly from GeoTIFF |
| Complex styling via SLD XML | Colormaps defined in Python config |

## Products Served

### Cuba GFS (8 layers)
- `cuba_gfs_maxUnitStreamFlow` — Unit Streamflow
- `cuba_gfs_maxStreamFlow` — Streamflow
- `cuba_gfs_precipAccum` — Precip Accumulation (QPF)
- `cuba_gfs_precipAccumImerg` — Precip Accumulation (IMERG)
- `cuba_gfs_maxSoilSat` — Soil Saturation
- `cuba_gfs_highResMaxInu` — High-Res Inundation (25m)
- `cuba_gfs_highResMaxStreamFlow` — High-Res Streamflow
- `cuba_gfs_highResMaxUnitStreamFlow` — High-Res Unit Streamflow

### Cuba IMERG (5 layers)
- `cuba_imerg_maxUnitStreamFlow`
- `cuba_imerg_maxStreamFlow`
- `cuba_imerg_precipAccum`
- `cuba_imerg_maxSoilSat`
- `cuba_imerg_highResMaxInu`

### West Africa (4 layers)
- `westafrica_maxUnitStreamFlow`
- `westafrica_maxStreamFlow`
- `westafrica_qpeAccum` — QPE (HSAF observed)
- `westafrica_qpfAccum` — QPF (GFS forecast)

## Setup

### 1. Create Conda Environment

```bash
cd titiler_api
conda env create -f environment.yml
conda activate titiler-ahwa
```

### 2. Start the Server

```bash
bash start.sh          # Default port 8000
bash start.sh 8001     # Custom port
bash start.sh --reload # Development with hot-reload
```

### 3. Verify

Open http://localhost:8000/docs for interactive API documentation.

## API Endpoints

### XYZ Tiles (Slippy Map)
```
GET /tiles/{product_id}/{z}/{x}/{y}.png?time=2025-06-08T12:00:00
```
Leaflet-compatible XYZ tiles in Web Mercator. Omit `time` for latest data.

### WMS GetMap
```
GET /wms?product_id=cuba_gfs_maxStreamFlow
    &SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap
    &LAYERS=cuba_gfs_maxStreamFlow
    &TIME=2025-06-08T12:00:00
    &BBOX=-10000000,2000000,-9000000,3000000
    &SRS=EPSG:3857
    &WIDTH=512&HEIGHT=512
    &FORMAT=image/png&TRANSPARENT=true
```

### Product Listing
```
GET /products                          # All products with granule counts
GET /products/{product_id}            # Product details + all timesteps
```

### Legend
```
GET /legend/{product_id}.png          # Color ramp legend
```

### Admin
```
POST /admin/refresh                   # Force rebuild file index
```

## Frontend Integration

### SvelteKit / Leaflet (XYZ Tiles — Recommended)

```typescript
// Faster than WMS — tiles load progressively, no chessboard effect
const tileUrl = `/titiler/tiles/cuba_gfs_maxStreamFlow/{z}/{x}/{y}.png?time=${timestep}`;
L.tileLayer(tileUrl, { opacity: 0.7 }).addTo(map);
```

### SvelteKit / Leaflet (WMS — Backward Compatible)

```typescript
// Works with existing WMS overlay infrastructure
const wmsUrl = `/titiler/wms?product_id=cuba_gfs_maxStreamFlow`
    + `&SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap`
    + `&TIME=${timestep}`
    + `&SRS=EPSG:3857&FORMAT=image/png&TRANSPARENT=true`;
L.tileLayer.wms(wmsUrl, { opacity: 0.7 }).addTo(map);
```

## Nginx Configuration

Add to the existing nginx config to proxy `/titiler/` → the TiTiler API:

```nginx
location /titiler/ {
    proxy_pass http://172.17.0.1:8000/;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_read_timeout 30s;
}
```

## Configuration

Edit `config.py` to:
- Add new products
- Modify colormaps (style ramps)
- Change data paths (or set `TITILER_DATA_ROOT` env var)

## Architecture

```
Client (Leaflet/SvelteKit)
    │
    ├── /titiler/tiles/{product}/{z}/{x}/{y}.png?time=...  (XYZ tiles)
    └── /titiler/wms?product_id=...&TIME=...&BBOX=...       (WMS fallback)
            │
    ┌───────▼──────────────────────────────────────┐
    │  FastAPI + rio-tiler                          │
    │  - Scans GeoTIFF directories for time index   │
    │  - Reads raster data on demand                │
    │  - Reprojects to EPSG:3857                    │
    │  - Applies colormap from config               │
    │  - Returns PNG tiles/images                   │
    └───────┬──────────────────────────────────────┘
            │
    ┌───────▼──────────────────────────────────────┐
    │  GeoTIFF directories on disk                  │
    │  /data/Cuba/{maxUnitStreamFlow, ...}/*.tif    │
    │  /data/CubaIMERGE/{...}/*.tif                 │
    │  /data/WestAfrica/{...}/*.tif                 │
    └──────────────────────────────────────────────┘
```
