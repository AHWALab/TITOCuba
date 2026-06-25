// ── TiTiler API Helpers ────────────────────────────────────────────────────

import { TITILER_BASE } from './config.js';

/**
 * Check if TiTiler API is running and healthy.
 * Returns { ok: true, products: [...] } or { ok: false, error: string }.
 */
export async function checkHealth() {
    try {
        const res = await fetch(`${TITILER_BASE}/health`, { signal: AbortSignal.timeout(5000) });
        if (!res.ok) return { ok: false, error: `HTTP ${res.status}` };
        const data = await res.json();
        return { ok: true, ...data };
    } catch (e) {
        return { ok: false, error: e.message || 'Connection refused' };
    }
}

/**
 * List available products from TiTiler.
 */
export async function listProducts() {
    const res = await fetch(`${TITILER_BASE}/products`);
    return res.json();
}

/**
 * Get available timesteps for a product.
 * Returns array of ISO datetime strings.
 */
export async function getAvailableTimes(productId) {
    const res = await fetch(`${TITILER_BASE}/products/${productId}`);
    const data = await res.json();
    return data.available_times || [];
}

/**
 * Build XYZ tile URL.
 */
export function xyzTileURL(productId, z, x, y, timeIso) {
    const params = new URLSearchParams();
    if (timeIso) params.set('time', timeIso);
    const qs = params.toString();
    return `${TITILER_BASE}/tiles/${productId}/${z}/${x}/${y}.png${qs ? '?' + qs : ''}`;
}

/**
 * Build WMS GetMap URL.
 */
export function wmsURL(productId, bbox, width, height, timeIso) {
    const [minx, miny, maxx, maxy] = bbox;
    const params = new URLSearchParams({
        product_id: productId,
        REQUEST: 'GetMap',
        SERVICE: 'WMS',
        VERSION: '1.3.0',
        LAYERS: productId,
        STYLES: '',
        CRS: 'EPSG:4326',
        BBOX: `${minx},${miny},${maxx},${maxy}`,
        WIDTH: String(width),
        HEIGHT: String(height),
        FORMAT: 'image/png',
        TRANSPARENT: 'true'
    });
    if (timeIso) params.set('TIME', timeIso);
    return `${TITILER_BASE}/wms?${params.toString()}`;
}

/**
 * Fetch discharge CSV for a gauge at a timestep.
 * The titiler API exposes /discharge/{timestep_str}/{gauge_csv_name}
 */
export async function fetchDischargeCSV(timestepStr, gaugeFileName) {
    const res = await fetch(`${TITILER_BASE}/discharge/${timestepStr}/${gaugeFileName}`);
    if (!res.ok) throw new Error(`CSV not found: ${res.status}`);
    return res.text();
}

/**
 * Fetch list of available gauge CSVs for a timestep.
 */
export async function listDischargeGauges(timestepStr) {
    const res = await fetch(`${TITILER_BASE}/discharge/${timestepStr}`);
    if (!res.ok) return [];
    return res.json();
}
