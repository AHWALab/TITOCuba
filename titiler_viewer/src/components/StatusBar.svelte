<script>
  let { status = 'checking', errorMsg = '', productCount = 0 } = $props();
  // status: 'checking' | 'ok' | 'error'

  let collapsed = $state(false);
</script>

<div class="status-bar" class:ok={status === 'ok'} class:error={status === 'error'} class:checking={status === 'checking'}>
  <button class="collapse-btn" onclick={() => collapsed = !collapsed}>
    {#if status === 'checking'}
      <span class="dot pulse"></span> Checking API...
    {:else if status === 'ok'}
      <span class="dot green"></span> API Online — {productCount} products
    {:else}
      <span class="dot red"></span> API Offline
    {/if}
  </button>

  {#if !collapsed && status === 'error' && errorMsg}
    <span class="error-detail">{errorMsg}</span>
  {/if}
</div>

<style>
  .status-bar {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 0.4rem 0.75rem;
    border-radius: 8px;
    font-size: 0.8rem;
    transition: background 0.3s;
  }
  .status-bar.ok {
    background: #e6f4ea;
  }
  .status-bar.error {
    background: #fce8e6;
  }
  .status-bar.checking {
    background: #fef7e0;
  }
  .collapse-btn {
    background: none;
    border: none;
    cursor: pointer;
    display: flex;
    align-items: center;
    gap: 0.4rem;
    font-size: 0.8rem;
    color: #444;
    padding: 0;
    font-weight: 500;
  }
  .dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    display: inline-block;
  }
  .dot.green { background: #1e8e3e; }
  .dot.red { background: #d93025; }
  .dot.pulse {
    background: #f9ab00;
    animation: pulse 1s ease-in-out infinite;
  }
  @keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.3; }
  }
  .error-detail {
    color: #c5221f;
    font-family: monospace;
    font-size: 0.75rem;
  }
</style>
