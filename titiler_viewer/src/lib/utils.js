// ── Time Utilities ─────────────────────────────────────────────────────────

/**
 * Format a Date to ISO string for TiTiler time param: "2026-06-09T14:00:00"
 */
export function toTiTilerTime(date) {
    const y = date.getUTCFullYear();
    const m = String(date.getUTCMonth() + 1).padStart(2, '0');
    const d = String(date.getUTCDate()).padStart(2, '0');
    const h = String(date.getUTCHours()).padStart(2, '0');
    const min = String(date.getUTCMinutes()).padStart(2, '0');
    const s = String(date.getUTCSeconds()).padStart(2, '0');
    return `${y}-${m}-${d}T${h}:${min}:${s}`;
}

/**
 * Format a Date to timestep folder string: "202606091400"
 */
export function toTimestepStr(date) {
    const y = date.getUTCFullYear();
    const m = String(date.getUTCMonth() + 1).padStart(2, '0');
    const d = String(date.getUTCDate()).padStart(2, '0');
    const h = String(date.getUTCHours()).padStart(2, '0');
    const min = String(date.getUTCMinutes()).padStart(2, '0');
    return `${y}${m}${d}${h}${min}`;
}

/**
 * Format display time: "2026-06-09 14:00 UTC"
 */
export function formatDisplay(date) {
    return date.toISOString().replace('T', ' ').slice(0, 16) + ' UTC';
}

/**
 * Snap a Date to the nearest hour in UTC.
 */
export function snapToHour(date) {
    const d = new Date(date);
    d.setUTCMinutes(0, 0, 0);
    return d;
}

/**
 * Create a UTC Date now, snapped to hour.
 */
export function utcNowHour() {
    return snapToHour(new Date());
}
