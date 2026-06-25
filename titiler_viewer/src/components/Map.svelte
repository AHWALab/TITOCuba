<script>
  import { onMount, onDestroy } from "svelte";
  import L from "leaflet";
  import { DEFAULT_CENTER, DEFAULT_ZOOM, MAX_BOUNDS } from "../lib/config.js";
  import { xyzTileURL, wmsURL } from "../lib/titiler.js";
  import { toTiTilerTime, toTimestepStr } from "../lib/utils.js";

  let { productId = "", selectedTime = null, layerMode = "xyz", gauges = [], overlayOpacity = 0.85, onGaugeClick } = $props();

  let mapContainer;
  let map = null;
  let tileLayer = null;
  let gaugeLayer = null;

  // ── Init Leaflet ──────────────────────────────────────────────────────
  onMount(() => {
    map = L.map(mapContainer, {
      center: DEFAULT_CENTER,
      zoom: DEFAULT_ZOOM,
      maxBounds: L.latLngBounds(MAX_BOUNDS[0], MAX_BOUNDS[1]),
      maxBoundsViscosity: 0.8,
      zoomControl: true,
      attributionControl: true,
    });

    // Base tile layer — ESRI light gray (seam-free)
    L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}", {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://www.esri.com/">ESRI</a>',
      maxZoom: 16,
      updateWhenZooming: false, // don't show white placeholders during zoom
      keepBuffer: 4, // keep extra tiles around the viewport
    }).addTo(map);

    // Initialize gauge markers layer
    gaugeLayer = L.layerGroup().addTo(map);

    updateTileLayer();
    updateGauges(gauges);

    return () => {
      map?.remove();
    };
  });

  // ── Reactive: rebuild tile layer when product/time/mode/opacity changes ─
  $effect(() => {
    if (productId && selectedTime && map) {
      updateTileLayer();
    }
  });
  $effect(() => {
    if (tileLayer) tileLayer.setOpacity(overlayOpacity);
  });

  function updateTileLayer() {
    if (!map || !productId || !selectedTime) return;
    if (tileLayer) map.removeLayer(tileLayer);

    const timeIso = toTiTilerTime(selectedTime);

    const op = overlayOpacity;
    if (layerMode === "xyz") {
      tileLayer = L.tileLayer("", {
        opacity: op,
        updateWhenZooming: false,
        keepBuffer: 4,
        className: "raster-overlay",
      });
      tileLayer.getTileUrl = function (coords) {
        return xyzTileURL(productId, coords.z, coords.x, coords.y, timeIso);
      };
    } else {
      tileLayer = L.tileLayer("", {
        opacity: op,
        tileSize: 256,
        updateWhenZooming: false,
        keepBuffer: 4,
        className: "raster-overlay",
      });
      tileLayer.getTileUrl = function (coords) {
        const bbox = tile2bbox(coords.x, coords.y, coords.z);
        return wmsURL(productId, bbox, 256, 256, timeIso);
      };
    }

    tileLayer.addTo(map);
  }

  // Convert XYZ tile coords to WGS84 bbox [west, south, east, north]
  function tile2bbox(x, y, z) {
    const nw = tile2lonlat(x, y, z);
    const se = tile2lonlat(x + 1, y + 1, z);
    return [nw[0], se[1], se[0], nw[1]]; // west, south, east, north
  }

  function tile2lonlat(x, y, z) {
    const n = Math.PI - (2 * Math.PI * y) / Math.pow(2, z);
    return [(x / Math.pow(2, z)) * 360 - 180, (180 / Math.PI) * Math.atan(Math.sinh(n))];
  }

  // ── Reactive: update gauge markers ────────────────────────────────────
  $effect(() => {
    if (gauges && map) updateGauges(gauges);
  });

  function updateGauges(gaugeList) {
    if (!gaugeLayer || !map) return;
    gaugeLayer.clearLayers();

    for (const g of gaugeList) {
      const marker = L.circleMarker([g.lat, g.lon], {
        radius: 6,
        fillColor: "#1a73e8",
        color: "#ffffff",
        weight: 2,
        opacity: 1,
        fillOpacity: 0.85,
      });

      marker.bindTooltip(g.name, {
        direction: "top",
        offset: [0, -10],
        className: "gauge-tooltip",
      });

      marker.on("click", () => {
        if (onGaugeClick) onGaugeClick(g);
      });

      marker.addTo(gaugeLayer);
    }
  }
</script>

<div class="map-container">
  <div bind:this={mapContainer} class="map"></div>
  {#if !productId || !selectedTime}
    <div class="map-overlay">
      <div class="overlay-card">
        <span class="icon">🗺️</span>
        <p>{!productId ? "Select a product layer to begin" : "Select a timestep to load data"}</p>
      </div>
    </div>
  {/if}
</div>

<style>
  .map-container {
    position: relative;
    width: 100%;
    height: 100%;
    min-height: 400px;
  }
  .map {
    width: 100%;
    height: 100%;
    min-height: 400px;
  }
  .map-overlay {
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    pointer-events: none;
    z-index: 1000;
  }
  .overlay-card {
    background: rgba(255, 255, 255, 0.92);
    backdrop-filter: blur(8px);
    border-radius: 12px;
    padding: 1.5rem 2rem;
    text-align: center;
    box-shadow: 0 4px 24px rgba(0, 0, 0, 0.12);
  }
  .overlay-card .icon {
    font-size: 2rem;
    display: block;
    margin-bottom: 0.5rem;
  }
  .overlay-card p {
    margin: 0;
    color: #555;
    font-size: 0.95rem;
  }
</style>
