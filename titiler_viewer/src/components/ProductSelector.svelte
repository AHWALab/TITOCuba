<script>
  import { PRODUCTS } from '../lib/config.js';

  let { selectedProductId = $bindable(), disabled = false } = $props();
</script>

<div class="product-selector">
  <label for="product-select">Layer</label>
  <select
    id="product-select"
    bind:value={selectedProductId}
    disabled={disabled}
  >
    <option value="">— Select product —</option>
    {#each PRODUCTS as p}
      <option value={p.id}>{p.label}</option>
    {/each}
  </select>
</div>

<style>
  .product-selector {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }
  .product-selector label {
    font-size: 0.8rem;
    font-weight: 600;
    color: #666;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    white-space: nowrap;
  }
  .product-selector select {
    padding: 0.45rem 0.75rem;
    border: 1px solid #d0d5dd;
    border-radius: 8px;
    background: #fff;
    font-size: 0.9rem;
    color: #1a1a1a;
    cursor: pointer;
    min-width: 220px;
    transition: border-color 0.15s;
  }
  .product-selector select:focus {
    outline: none;
    border-color: #1a73e8;
    box-shadow: 0 0 0 3px rgba(26,115,232,0.12);
  }
  .product-selector select:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
</style>
