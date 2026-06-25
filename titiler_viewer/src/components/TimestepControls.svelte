<script>
  import { TIMESTEP_OFFSETS } from '../lib/config.js';
  import { formatDisplay, snapToHour, utcNowHour } from '../lib/utils.js';

  let { selectedTime = $bindable(null), disabled = false } = $props();

  // Initialize to current UTC hour if not set
  if (!selectedTime) selectedTime = utcNowHour();

  let nowHour = $state(utcNowHour());

  // Update now every 60s
  const nowInterval = setInterval(() => { nowHour = utcNowHour(); }, 60000);

  function shift(hours) {
    const next = new Date(selectedTime);
    next.setUTCHours(next.getUTCHours() + hours);
    // Don't allow going into the future
    if (next > nowHour) return;
    selectedTime = snapToHour(next);
  }

  function setNow() {
    selectedTime = nowHour;
  }

  // Clamp selectedTime if it's somehow in the future
  $effect(() => {
    if (selectedTime > nowHour) selectedTime = new Date(nowHour);
  });
</script>

<div class="timestep-controls">
  <span class="label">Timestep</span>

  <div class="btn-group">
    {#each TIMESTEP_OFFSETS.filter(o => o.hours < 0) as off}
      <button
        class="btn-offset"
        onclick={() => shift(off.hours)}
        disabled={disabled}
        title={`Shift ${off.label}`}
      >{off.label}</button>
    {/each}
  </div>

  <button class="btn-now" onclick={setNow} disabled={disabled}>
    {formatDisplay(selectedTime)}
  </button>

  <div class="btn-group">
    {#each TIMESTEP_OFFSETS.filter(o => o.hours > 0) as off}
      <button
        class="btn-offset"
        onclick={() => shift(off.hours)}
        disabled={disabled || new Date(selectedTime.getTime() + off.hours * 3600000) > nowHour}
        title={`Shift ${off.label}`}
      >{off.label}</button>
    {/each}
  </div>
</div>

<style>
  .timestep-controls {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    flex-wrap: wrap;
  }
  .label {
    font-size: 0.8rem;
    font-weight: 600;
    color: #666;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }
  .btn-group {
    display: flex;
    gap: 0;
  }
  .btn-offset {
    padding: 0.4rem 0.6rem;
    border: 1px solid #d0d5dd;
    background: #fff;
    font-size: 0.8rem;
    font-weight: 500;
    color: #333;
    cursor: pointer;
    transition: all 0.15s;
  }
  .btn-offset:first-child {
    border-radius: 8px 0 0 8px;
  }
  .btn-offset:last-child {
    border-radius: 0 8px 8px 0;
  }
  .btn-offset:not(:first-child) {
    margin-left: -1px;
  }
  .btn-offset:hover:not(:disabled) {
    background: #e8f0fe;
    border-color: #1a73e8;
    color: #1a73e8;
    z-index: 1;
  }
  .btn-offset:disabled {
    opacity: 0.35;
    cursor: not-allowed;
  }
  .btn-now {
    padding: 0.4rem 1rem;
    border: 1px solid #1a73e8;
    border-radius: 8px;
    background: #1a73e8;
    color: #fff;
    font-size: 0.85rem;
    font-weight: 600;
    font-family: monospace;
    cursor: pointer;
    white-space: nowrap;
    transition: all 0.15s;
  }
  .btn-now:hover:not(:disabled) {
    background: #1557b0;
  }
  .btn-now:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
</style>
