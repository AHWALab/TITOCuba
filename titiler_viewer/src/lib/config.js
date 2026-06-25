// ── Hardcoded Configuration ───────────────────────────────────────────────
// TiTiler API base URL (proxied through Vite in dev, direct in production)
export const TITILER_BASE = '/api';

// DATA_ROOT path for discharge CSV access (served by titiler /discharge endpoint)
// Same as DATA_ROOT in titiler_api/config.py
export const DATA_ROOT = '/home/nammehta/TITOCubaMainTest/titilerTest';

// ── Products ──────────────────────────────────────────────────────────────
export const PRODUCTS = [
    {
        id: 'cuba_gfs_maxUnitStreamFlow',
        label: 'Max Unit Streamflow',
        shortLabel: 'maxunitq',
        units: 'm³/s/km²'
    },
    {
        id: 'cuba_gfs_maxStreamFlow',
        label: 'Max Streamflow',
        shortLabel: 'maxq',
        units: 'm³/s'
    },
    {
        id: 'cuba_gfs_precipAccumImerg',
        label: 'QPE Accum (IMERG)',
        shortLabel: 'qpeaccum',
        units: 'mm'
    },
    {
        id: 'cuba_gfs_precipAccum',
        label: 'QPF Accum (GFS)',
        shortLabel: 'qpfaccum',
        units: 'mm'
    },
    {
        id: 'cuba_gfs_maxSoilSat',
        label: 'Max Soil Saturation',
        shortLabel: 'maxsm',
        units: '%'
    },
    {
        id: 'cuba_gfs_highResMaxInu',
        label: 'Max Inundation Depth',
        shortLabel: 'maxdepth',
        units: 'm'
    }
];

// ── Default view (Cuba) ───────────────────────────────────────────────────
export const DEFAULT_CENTER = [21.8, -79.5];
export const DEFAULT_ZOOM = 7;
export const MAX_BOUNDS = [[19.0, -85.5], [23.5, -73.5]];

// ── Timestep presets ──────────────────────────────────────────────────────
export const TIMESTEP_OFFSETS = [
    { label: '-24h', hours: -24 },
    { label: '-6h', hours: -6 },
    { label: '-1h', hours: -1 },
    { label: '+1h', hours: 1 },
    { label: '+6h', hours: 6 },
    { label: '+24h', hours: 24 }
];
