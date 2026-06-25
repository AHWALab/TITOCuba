<script>
  let { layerMode = $bindable(), disabled = false } = $props();
</script>

<div class="layer-toggle">
  <span class="label">Tile Mode</span>
  <div class="toggle-group">
    <button
      class="toggle-btn"
      class:active={layerMode === 'xyz'}
      onclick={() => layerMode = 'xyz'}
      disabled={disabled}
    >XYZ</button>
    <button
      class="toggle-btn"
      class:active={layerMode === 'wms'}
      onclick={() => layerMode = 'wms'}
      disabled={disabled}
    >WMS</button>
  </div>
</div>

<style>
  .layer-toggle {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }
  .label {
    font-size: 0.8rem;
    font-weight: 600;
    color: #666;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }
  .toggle-group {
    display: flex;
    gap: 0;
  }
  .toggle-btn {
    padding: 0.4rem 0.9rem;
    border: 1px solid #d0d5dd;
    background: #fff;
    font-size: 0.8rem;
    font-weight: 600;
    color: #666;
    cursor: pointer;
    transition: all 0.15s;
  }
  .toggle-btn:first-child {
    border-radius: 8px 0 0 8px;
  }
  .toggle-btn:last-child {
    border-radius: 0 8px 8px 0;
    margin-left: -1px;
  }
  .toggle-btn.active {
    background: #1a73e8;
    border-color: #1a73e8;
    color: #fff;
    z-index: 1;
  }
  .toggle-btn:hover:not(:disabled):not(.active) {
    background: #e8f0fe;
    border-color: #1a73e8;
    color: #1a73e8;
    z-index: 1;
  }
  .toggle-btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
</style>
