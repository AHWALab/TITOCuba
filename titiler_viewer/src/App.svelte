<script>
  import { onMount } from "svelte";
  import Map from "./components/Map.svelte";
  import ProductSelector from "./components/ProductSelector.svelte";
  import TimestepControls from "./components/TimestepControls.svelte";
  import LayerToggle from "./components/LayerToggle.svelte";
  import StatusBar from "./components/StatusBar.svelte";
  import GaugePopup from "./components/GaugePopup.svelte";
  import { checkHealth } from "./lib/titiler.js";
  import { loadGaugePoints } from "./lib/gaugePoints.js";

  let selectedProductId = $state("");
  let selectedTime = $state(null);
  let layerMode = $state("xyz");
  let overlayOpacity = $state(0.85);
  let apiStatus = $state("checking");
  let apiError = $state("");
  let productCount = $state(0);
  let gauges = $state([]);
  let selectedGauge = $state(null);

  onMount(async () => {
    // Check API health
    const health = await checkHealth();
    if (health.ok) {
      apiStatus = "ok";
      productCount = health.products_indexed || health.product_count || 0;
    } else {
      apiStatus = "error";
      apiError = health.error;
    }

    // Load gauge points
    try {
      gauges = await loadGaugePoints();
    } catch (e) {
      console.warn("Failed to load gauge points:", e);
    }
  });

  function handleGaugeClick(gauge) {
    selectedGauge = gauge;
  }

  function closeGauge() {
    selectedGauge = null;
  }
</script>

<div class="app-shell">
  <header class="app-header">
    <div class="header-left">
      <h1 class="app-title">AHWA TiTiler Viewer</h1>
      <StatusBar status={apiStatus} errorMsg={apiError} {productCount} />
    </div>
    <div class="header-controls">
      <ProductSelector bind:selectedProductId disabled={apiStatus !== "ok"} />
      <LayerToggle bind:layerMode disabled={apiStatus !== "ok"} />
      <div class="opacity-slider" title="Overlay opacity">
        <span class="opacity-label">Opacity</span>
        <input type="range" min="0.1" max="1" step="0.05" bind:value={overlayOpacity} disabled={apiStatus !== "ok"} />
        <span class="opacity-val">{Math.round(overlayOpacity * 100)}%</span>
      </div>
      <TimestepControls bind:selectedTime disabled={apiStatus !== "ok"} />
    </div>
  </header>

  <main class="app-main">
    <Map productId={selectedProductId} {selectedTime} {layerMode} {overlayOpacity} {gauges} onGaugeClick={handleGaugeClick} />
  </main>

  {#if selectedGauge}
    <GaugePopup gauge={selectedGauge} {selectedTime} onClose={closeGauge} />
  {/if}
</div>

<style>
  :global(*) {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
  }
  :global(body) {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    background: #f5f5f5;
    color: #1a1a1a;
  }
  :global(.leaflet-tile) {
    border: 0 !important;
    background: transparent !important;
  }
  :global(.raster-overlay .leaflet-tile) {
    clip-path: inset(-1px);
  }
  :global(.leaflet-container) {
    background: #e8e8e8;
  }
  .app-shell {
    display: flex;
    flex-direction: column;
    height: 100vh;
    width: 100vw;
    overflow: hidden;
  }
  .app-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0.75rem 1.25rem;
    background: #fff;
    border-bottom: 1px solid #e5e7eb;
    gap: 1rem;
    flex-wrap: wrap;
    z-index: 500;
    box-shadow: 0 1px 4px rgba(0, 0, 0, 0.04);
  }
  .header-left {
    display: flex;
    align-items: center;
    gap: 1.5rem;
  }
  .app-title {
    font-size: 1.15rem;
    font-weight: 700;
    color: #1a1a1a;
    white-space: nowrap;
  }
  .header-controls {
    display: flex;
    align-items: center;
    gap: 1rem;
    flex-wrap: wrap;
  }
  .opacity-slider {
    display: flex;
    align-items: center;
    gap: 0.4rem;
  }
  .opacity-label {
    font-size: 0.8rem;
    font-weight: 600;
    color: #666;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }
  .opacity-slider input[type="range"] {
    width: 72px;
    accent-color: #1a73e8;
    cursor: pointer;
  }
  .opacity-val {
    font-size: 0.75rem;
    font-weight: 600;
    color: #1a73e8;
    min-width: 30px;
    text-align: right;
  }
  .app-main {
    flex: 1;
    overflow: hidden;
    position: relative;
  }
</style>
