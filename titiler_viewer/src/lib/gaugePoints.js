// ── Gauge Points Parser ────────────────────────────────────────────────────
// Parses gaugePoints.txt into structured array.
// Format:
//   [Gauge Name]
//   LON=-82.34
//   LAT=23.14
//   OUTPUTTS=TRUE

const GAUGE_POINTS_PATH = '/gaugePoints.txt';

/**
 * Parse gaugePoints.txt content and return array of gauge objects.
 * Each gauge: { name, nameLower, lon, lat, csvFileName(timestepStr) }
 */
export function parseGaugePoints(text) {
    const gauges = [];
    const blocks = text.split(/\[Gauge\s+/).filter(b => b.trim());

    for (const block of blocks) {
        const lines = block.split('\n').filter(l => l.trim());
        const name = lines[0].replace(']', '').trim();
        const lonLine = lines.find(l => l.startsWith('LON='));
        const latLine = lines.find(l => l.startsWith('LAT='));
        const tsLine = lines.find(l => l.startsWith('OUTPUTTS='));

        if (!lonLine || !latLine) continue;

        const lon = parseFloat(lonLine.split('=')[1]);
        const lat = parseFloat(latLine.split('=')[1]);
        const outputTS = tsLine ? tsLine.split('=')[1].trim().toUpperCase() === 'TRUE' : false;

        if (isNaN(lon) || isNaN(lat)) continue;

        // CSV filename: ts.{name_lower}.crest.{YYYYMMDD}.{HHMMSS}.csv
        const nameLower = name.toLowerCase().replace(/\s+/g, '_');

        gauges.push({
            name,
            nameLower,
            lon,
            lat,
            outputTS,
            /** Build CSV filename for a given timestep string: YYYYMMDDHHMM */
            csvFileName(timestepStr) {
                // timestepStr = "202606091400"
                const date = timestepStr.slice(0, 8);  // 20260609
                const time = timestepStr.slice(8, 12) + '00'; // 140000
                return `ts.${nameLower}.crest.${date}.${time}.csv`;
            }
        });
    }

    return gauges;
}

/**
 * Load gauge points from the bundled text file.
 */
export async function loadGaugePoints() {
    const res = await fetch(GAUGE_POINTS_PATH);
    const text = await res.text();
    return parseGaugePoints(text);
}
