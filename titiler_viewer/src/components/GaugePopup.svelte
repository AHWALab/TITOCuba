<script>
  import { onMount } from "svelte";
  import { fetchDischargeCSV } from "../lib/titiler.js";
  import { toTimestepStr, formatDisplay } from "../lib/utils.js";

  let { gauge = null, selectedTime = null, onClose } = $props();

  let loading = $state(false);
  let csvData = $state(null);
  let error = $state(null);
  let chartData = $state([]);
  let selectedVar = $state("Discharge(m^3 s^-1)");

  // Available variables from CSV header
  const VAR_OPTIONS = [
    { value: "Discharge(m^3 s^-1)", label: "Discharge (m³/s)" },
    { value: "Precip(mm h^-1)", label: "Precip (mm/h)" },
    { value: "SM(%)", label: "Soil Moisture (%)" },
    { value: "PET(mm h^-1)", label: "PET (mm/h)" },
  ];

  $effect(() => {
    if (gauge && selectedTime) loadCSV();
  });

  async function loadCSV() {
    if (!gauge || !selectedTime) return;
    loading = true;
    error = null;
    csvData = null;
    chartData = [];

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);

    try {
      const tsStr = toTimestepStr(selectedTime);
      const fileName = gauge.csvFileName(tsStr);
      const res = await fetch(`/api/discharge/${tsStr}/${fileName}`, { signal: controller.signal });
      clearTimeout(timeout);
      if (!res.ok) throw new Error(`CSV not found (HTTP ${res.status})`);
      const text = await res.text();
      csvData = text;
      chartData = parseCSV(text);
    } catch (e) {
      error = e.name === "AbortError" ? "Request timed out — no data at this timestep" : e.message;
    } finally {
      clearTimeout(timeout);
      loading = false;
    }
  }

  function parseCSV(text) {
    const lines = text.trim().split("\n");
    if (lines.length < 2) return [];
    const headers = lines[0].split(",").map((h) => h.trim());
    const data = [];
    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i].split(",");
      if (cols.length < 2) continue;
      const row = {};
      headers.forEach((h, idx) => {
        const raw = (cols[idx] || "").trim();
        // Time column stays as string; others try numeric parse
        if (h === "Time") {
          row[h] = raw;
        } else {
          const val = parseFloat(raw);
          row[h] = isNaN(val) ? raw : val;
        }
      });
      data.push(row);
    }
    return data;
  }

  function getLatestValue() {
    if (!chartData.length) return null;
    const key = selectedVar;
    const last = chartData[chartData.length - 1];
    return last[key];
  }

  function getSparklinePoints() {
    if (!chartData.length) return "";
    const key = selectedVar;
    const vals = chartData.map((r) => r[key]).filter((v) => typeof v === "number");
    if (vals.length < 2) return "";

    const max = Math.max(...vals);
    const min = Math.min(...vals);
    const range = max - min || 1;
    const w = 200;
    const h = 40;
    const padding = 2;

    const points = vals.map((v, i) => {
      const x = padding + (i / (vals.length - 1)) * (w - padding * 2);
      const y = h - padding - ((v - min) / range) * (h - padding * 2);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    });

    return points.join(" ");
  }

  function getStats() {
    if (!chartData.length) return null;
    const key = selectedVar;
    const vals = chartData.map((r) => r[key]).filter((v) => typeof v === "number");
    if (!vals.length) return null;
    const sum = vals.reduce((a, b) => a + b, 0);
    return {
      min: Math.min(...vals),
      max: Math.max(...vals),
      mean: sum / vals.length,
      count: vals.length,
      firstTime: chartData[0]?.Time || "",
      lastTime: chartData[chartData.length - 1]?.Time || "",
    };
  }

  function statUnit() {
    const m = selectedVar.match(/\((.+)\)/);
    return m ? m[1] : "";
  }

  /** Extract HH:MM from "2026-06-09 09:30" or similar datetime string */
  function fmtTime(raw) {
    if (raw === undefined || raw === null) return "";
    const s = String(raw);
    const m = s.match(/(\d{2}:\d{2})(?::\d{2})?$/);
    return m ? m[1] : s;
  }

  /** Format SM value — already in %, just ensure proper display */
  function fmtSM(val) {
    if (val === undefined || val === null || isNaN(val)) return "—";
    return val.toFixed(1);
  }
</script>

<div class="popup-backdrop" onclick={onClose} role="dialog" onkeydown={(e) => e.key === "Escape" && onClose()} tabindex="-1">
  <div class="popup-card" onclick={(e) => e.stopPropagation()} role="document">
    <div class="popup-header">
      <h3>{gauge?.name || "Gauge"}</h3>
      <button class="close-btn" onclick={onClose}>&times;</button>
    </div>

    <div class="popup-meta">
      <span>{gauge?.lat?.toFixed(4)}°N, {gauge?.lon?.toFixed(4)}°W</span>
      <span>{selectedTime ? formatDisplay(selectedTime) : ""}</span>
    </div>

    {#if loading}
      <div class="popup-loading">Loading discharge data...</div>
    {:else if error}
      <div class="popup-error">⚠️ {error}</div>
    {:else if chartData.length > 0}
      <div class="var-selector">
        <label for="var-select">Variable</label>
        <select id="var-select" bind:value={selectedVar}>
          {#each VAR_OPTIONS as opt}
            <option value={opt.value}>{opt.label}</option>
          {/each}
        </select>
      </div>

      <div class="sparkline-container">
        <svg viewBox="0 0 200 40" class="sparkline">
          <polyline points={getSparklinePoints()} fill="none" stroke="#1a73e8" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" />
        </svg>
      </div>

      {@const stats = getStats()}
      <div class="latest-value">
        <span class="latest-num">{getLatestValue()?.toFixed(3) ?? "—"}</span>
        <span class="latest-unit">{statUnit()}</span>
      </div>

      <div class="data-table-wrap">
        <table class="data-table">
          <thead>
            <tr>
              <th>Time (UTC)</th>
              <th class:col-active={selectedVar === "Discharge(m^3 s^-1)"}>Q m³/s</th>
              <th class:col-active={selectedVar === "Precip(mm h^-1)"}>Precip mm/h</th>
              <th class:col-active={selectedVar === "SM(%)"}>SM %</th>
              <th class:col-active={selectedVar === "PET(mm h^-1)"}>PET mm/h</th>
            </tr>
          </thead>
          <tbody>
            {#each chartData as row}
              <tr>
                <td class="td-time">{fmtTime(row["Time"])}</td>
                <td class:col-active={selectedVar === "Discharge(m^3 s^-1)"}>{row["Discharge(m^3 s^-1)"]?.toFixed(3) ?? "—"}</td>
                <td class:col-active={selectedVar === "Precip(mm h^-1)"}>{row["Precip(mm h^-1)"]?.toFixed(2) ?? "—"}</td>
                <td class:col-active={selectedVar === "SM(%)"}>{fmtSM(row["SM(%)"])}</td>
                <td class:col-active={selectedVar === "PET(mm h^-1)"}>{row["PET(mm h^-1)"]?.toFixed(2) ?? "—"}</td>
                <td class:col-active={selectedVar === "PET(mm h^-1)"}>{row["PET(mm h^-1)"]?.toFixed(2) ?? "—"}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {:else}
      <div class="popup-empty">No data available</div>
    {/if}
  </div>
</div>

<style>
  .popup-backdrop {
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: rgba(0, 0, 0, 0.35);
    backdrop-filter: blur(4px);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 9999;
  }
  .popup-card {
    background: #fff;
    border-radius: 14px;
    padding: 1.25rem;
    min-width: 380px;
    max-width: 480px;
    max-height: 85vh;
    display: flex;
    flex-direction: column;
    box-shadow: 0 8px 40px rgba(0, 0, 0, 0.18);
    animation: popup-in 0.2s ease-out;
  }
  @keyframes popup-in {
    from {
      opacity: 0;
      transform: scale(0.95) translateY(8px);
    }
    to {
      opacity: 1;
      transform: scale(1) translateY(0);
    }
  }
  .popup-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 0.4rem;
  }
  .popup-header h3 {
    margin: 0;
    font-size: 1.1rem;
    font-weight: 700;
    color: #1a1a1a;
  }
  .close-btn {
    background: none;
    border: none;
    font-size: 1.5rem;
    cursor: pointer;
    color: #999;
    padding: 0;
    line-height: 1;
  }
  .close-btn:hover {
    color: #333;
  }
  .popup-meta {
    display: flex;
    gap: 1rem;
    font-size: 0.75rem;
    color: #888;
    margin-bottom: 1rem;
  }
  .popup-loading,
  .popup-error,
  .popup-empty {
    text-align: center;
    padding: 1.5rem;
    color: #888;
    font-size: 0.9rem;
  }
  .popup-error {
    color: #d93025;
  }
  .var-selector {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-bottom: 0.75rem;
  }
  .var-selector label {
    font-size: 0.75rem;
    font-weight: 600;
    color: #666;
    text-transform: uppercase;
  }
  .var-selector select {
    padding: 0.35rem 0.5rem;
    border: 1px solid #d0d5dd;
    border-radius: 6px;
    font-size: 0.85rem;
    flex: 1;
  }
  .sparkline-container {
    background: #f8f9fa;
    border-radius: 8px;
    padding: 0.5rem;
    margin-bottom: 0.5rem;
  }
  .sparkline {
    width: 100%;
    height: auto;
  }
  .latest-value {
    text-align: center;
  }
  .latest-num {
    font-size: 1.8rem;
    font-weight: 700;
    color: #1a73e8;
    font-family: monospace;
  }
  .latest-unit {
    font-size: 0.85rem;
    color: #666;
    margin-left: 0.25rem;
  }
  .data-table-wrap {
    max-height: 320px;
    overflow-y: auto;
    margin-top: 0.5rem;
    border: 1px solid #e5e7eb;
    border-radius: 8px;
  }
  .data-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.75rem;
    font-family: monospace;
  }
  .data-table thead {
    position: sticky;
    top: 0;
    z-index: 2;
  }
  .data-table th {
    background: #f1f3f4;
    color: #555;
    font-weight: 600;
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    padding: 0.35rem 0.4rem;
    text-align: right;
    border-bottom: 2px solid #d0d5dd;
    white-space: nowrap;
  }
  .data-table th:first-child {
    text-align: left;
    min-width: 120px;
  }
  .data-table td {
    padding: 0.25rem 0.4rem;
    text-align: right;
    border-bottom: 1px solid #f0f0f0;
    color: #444;
  }
  .data-table td.td-time {
    text-align: left;
    color: #888;
    font-size: 0.7rem;
  }
  .data-table tr:hover td {
    background: #f8f9ff;
  }
  .col-active {
    background: #e8f0fe !important;
    color: #1a73e8 !important;
    font-weight: 700;
  }
  th.col-active {
    background: #d2e3fc !important;
  }
</style>
