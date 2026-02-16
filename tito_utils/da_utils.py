"""
Data Assimilation (DA) utilities for TITO Cuba system.

This module handles the preparation and management of reservoir observation data
for data assimilation in the EF5 model.

For each gauge at each timestep, manual data is preferred over climatology.
If manual data is not available for a given timestep, climatology data is used.

Functions:
    - read_reservoir_list: Read the list of reservoirs to process
    - create_simulation_csv_files: Create individual CSV files for each reservoir in DA_Simulation
    - create_consolidated_da_csv: Create consolidated CSV with all reservoir data
    - process_da_for_simulation: Main function to orchestrate DA preparation
"""

import os
import glob
import shutil
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import pandas as pd


def read_reservoir_list(da_list_path: str) -> List[str]:
    """
    Read the list of reservoirs from the DA list file.
    
    Args:
        da_list_path: Path to the text file containing reservoir IDs (one per line)
        
    Returns:
        List of reservoir IDs (e.g., ['EMB2100002', 'EMB2100004', ...])
    """
    if not os.path.exists(da_list_path):
        raise FileNotFoundError(f"DA list file not found: {da_list_path}")
    
    reservoirs = []
    with open(da_list_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):  # Skip empty lines and comments
                reservoirs.append(line)
    
    print(f"    Loaded {len(reservoirs)} reservoirs from DA list")
    return reservoirs


def _parse_da_csv(file_path: str) -> Optional[pd.DataFrame]:
    """
    Read and parse a DA CSV file into a DataFrame with a 'datetime' column.

    Args:
        file_path: Path to the CSV file (two columns: timestamp, value)

    Returns:
        DataFrame with columns ['timestamp', 'value', 'datetime'] or None on error.
    """
    if not os.path.exists(file_path):
        return None
    try:
        df = pd.read_csv(file_path, header=None, names=['timestamp', 'value'])
        # Try common date formats, then fall back to automatic parsing
        for fmt in ('%m/%d/%Y %H:%M', '%d/%m/%Y %H:%M'):
            try:
                df['datetime'] = pd.to_datetime(df['timestamp'], format=fmt)
                return df
            except Exception:
                continue
        df['datetime'] = pd.to_datetime(df['timestamp'])
        return df
    except Exception as e:
        print(f"      Warning: Error reading {file_path}: {e}")
        return None


def _merge_manual_climatology(
    reservoir_id: str,
    da_manual_path: str,
    da_climatology_path: str,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> Tuple[Optional[pd.DataFrame], int, int]:
    """
    Build a merged timeseries for one reservoir.

    At every timestep within [start_ts, end_ts]:
        - if manual data exists for that timestep, use it
        - otherwise fall back to climatology

    Args:
        reservoir_id: e.g. 'EMB2100002'
        da_manual_path: folder containing manual CSVs
        da_climatology_path: folder containing climatology CSVs
        start_ts, end_ts: timezone-naive pandas Timestamps

    Returns:
        (merged_df, manual_steps, climatology_steps)
        merged_df has columns ['timestamp', 'value'] ready to write.
        Returns (None, 0, 0) when no data is available at all.
    """
    filename = f"{reservoir_id}_Vertimiento_Serie.csv"

    # ---- read climatology (baseline) ----
    clim_path = os.path.join(da_climatology_path, filename)
    df_clim = _parse_da_csv(clim_path)

    # ---- read manual (override) ----
    manual_path = os.path.join(da_manual_path, filename)
    df_manual = _parse_da_csv(manual_path)

    # ---- filter each to the simulation window ----
    def _filter(df):
        if df is None or df.empty:
            return pd.DataFrame(columns=['datetime', 'value'])
        mask = (df['datetime'] >= start_ts) & (df['datetime'] <= end_ts)
        return df.loc[mask, ['datetime', 'value']].copy()

    clim_filtered = _filter(df_clim)
    manual_filtered = _filter(df_manual)

    # If neither source has data, nothing to do
    if clim_filtered.empty and manual_filtered.empty:
        return None, 0, 0

    # ---- merge: manual takes priority ----
    # Index both by datetime for easy override
    if not manual_filtered.empty:
        manual_filtered = manual_filtered.set_index('datetime')
        manual_filtered['source'] = 'manual'
    else:
        manual_filtered = pd.DataFrame(columns=['value', 'source'])
        manual_filtered.index.name = 'datetime'

    if not clim_filtered.empty:
        clim_filtered = clim_filtered.set_index('datetime')
        clim_filtered['source'] = 'climatology'
    else:
        clim_filtered = pd.DataFrame(columns=['value', 'source'])
        clim_filtered.index.name = 'datetime'

    # Combine: manual overwrites climatology at matching timestamps
    merged = clim_filtered.combine_first(manual_filtered)
    # But we want manual to WIN, so re-overlay manual on top
    merged.update(manual_filtered)

    merged = merged.sort_index()

    manual_steps = int((merged['source'] == 'manual').sum())
    climatology_steps = int((merged['source'] == 'climatology').sum())

    # Format timestamp back to MM/DD/YYYY HH:MM for output
    merged = merged.reset_index()
    merged['timestamp'] = merged['datetime'].dt.strftime('%m/%d/%Y %H:%M')
    result = merged[['timestamp', 'value']]

    return result, manual_steps, climatology_steps


def create_simulation_csv_files(
    reservoirs: List[str],
    da_manual_path: str,
    da_climatology_path: str,
    da_simulation_path: str,
    start_time: datetime,
    end_time: datetime,
) -> Tuple[int, int, int]:
    """
    Create individual CSV files for each reservoir in DA_Simulation folder.

    For each reservoir at each timestep, manual data is preferred.
    If manual data is absent for that step, climatology data is used.

    Args:
        reservoirs: List of reservoir IDs
        da_manual_path: Path to manual DA data folder
        da_climatology_path: Path to climatology DA data folder
        da_simulation_path: Path to DA_Simulation output folder
        start_time: Simulation start time
        end_time: Simulation end time (systemEndTime, includes dry run)

    Returns:
        Tuple of (total_manual_steps, total_climatology_steps, gauges_processed)
    """
    print("    Creating individual CSV files for each reservoir in DA_Simulation:")

    # Clean DA_Simulation folder
    if os.path.exists(da_simulation_path):
        shutil.rmtree(da_simulation_path)
    os.makedirs(da_simulation_path, exist_ok=True)
    print(f"      Cleaned and created DA_Simulation folder")

    # Convert to timezone-naive pandas Timestamps
    start_ts = pd.Timestamp(start_time).tz_localize(None) if pd.Timestamp(start_time).tz else pd.Timestamp(start_time)
    end_ts = pd.Timestamp(end_time).tz_localize(None) if pd.Timestamp(end_time).tz else pd.Timestamp(end_time)

    total_manual = 0
    total_climatology = 0
    gauges_processed = 0

    for reservoir_id in reservoirs:
        result_df, m_steps, c_steps = _merge_manual_climatology(
            reservoir_id, da_manual_path, da_climatology_path, start_ts, end_ts
        )

        if result_df is None or result_df.empty:
            print(f"      {reservoir_id}: WARNING - no data in range")
            continue

        output_path = os.path.join(da_simulation_path, f"{reservoir_id}_Vertimiento_Serie.csv")
        result_df.to_csv(output_path, index=False, header=False)

        total_manual += m_steps
        total_climatology += c_steps
        gauges_processed += 1

        # Per-gauge log
        src_label = f"{m_steps} manual, {c_steps} climatology"
        print(f"      {reservoir_id}: {len(result_df)} records ({src_label})")

    print(f"    CSV file creation complete:")
    print(f"      - Gauges processed: {gauges_processed}")
    print(f"      - Total manual steps:      {total_manual}")
    print(f"      - Total climatology steps:  {total_climatology}")

    return total_manual, total_climatology, gauges_processed


def create_consolidated_da_csv(
    reservoirs: List[str],
    da_simulation_path: str,
    start_time: datetime,
    end_time: datetime,
    output_path: str,
    timestamp_str: str
) -> Optional[str]:
    """
    Create a consolidated CSV with all reservoir data for the simulation period.

    Reads from the already-merged DA_Simulation CSVs.

    Args:
        reservoirs: List of reservoir IDs
        da_simulation_path: Path to DA_Simulation folder (merged CSVs)
        start_time: Simulation start time
        end_time: Simulation end time
        output_path: Path to DA_Consolidated folder
        timestamp_str: Timestamp string for output filename

    Returns:
        Path to the created consolidated CSV file, or None if no data.
    """
    print("    Creating consolidated DA CSV file:")

    # Clear previous consolidated CSVs
    pattern = os.path.join(output_path, "da.observations.*.csv")
    old_files = glob.glob(pattern)
    for old_file in old_files:
        try:
            os.remove(old_file)
            print(f"      Removed old consolidated file: {os.path.basename(old_file)}")
        except Exception as e:
            print(f"      Warning: Could not remove {old_file}: {e}")

    os.makedirs(output_path, exist_ok=True)

    consolidated_filename = f"da.observations.{timestamp_str}.csv"
    consolidated_path = os.path.join(output_path, consolidated_filename)

    start_ts = pd.Timestamp(start_time).tz_localize(None) if pd.Timestamp(start_time).tz else pd.Timestamp(start_time)
    end_ts = pd.Timestamp(end_time).tz_localize(None) if pd.Timestamp(end_time).tz else pd.Timestamp(end_time)

    all_data = []

    for reservoir_id in reservoirs:
        obs_file = os.path.join(da_simulation_path, f"{reservoir_id}_Vertimiento_Serie.csv")

        if not os.path.exists(obs_file):
            continue

        try:
            df = pd.read_csv(obs_file, header=None, names=['timestamp', 'value'])

            # Parse timestamps
            for fmt in ('%m/%d/%Y %H:%M', '%d/%m/%Y %H:%M'):
                try:
                    df['datetime'] = pd.to_datetime(df['timestamp'], format=fmt)
                    break
                except Exception:
                    continue
            else:
                df['datetime'] = pd.to_datetime(df['timestamp'])

            # Filter to simulation period
            mask = (df['datetime'] >= start_ts) & (df['datetime'] <= end_ts)
            df_filtered = df[mask].copy()

            if df_filtered.empty:
                continue

            # Reformat timestamp to MM/DD/YYYY HH:MM
            df_filtered['timestamp_formatted'] = df_filtered['datetime'].dt.strftime('%m/%d/%Y %H:%M')
            df_filtered['reservoir_id'] = reservoir_id

            all_data.append(df_filtered[['reservoir_id', 'timestamp_formatted', 'value']])
        except Exception as e:
            print(f"      Warning: Error processing {reservoir_id}: {e}")
            continue

    if all_data:
        consolidated_df = pd.concat(all_data, ignore_index=True)
        consolidated_df.to_csv(consolidated_path, index=False, header=False)
        print(f"      Created consolidated CSV: {consolidated_filename}")
        print(f"      Total records: {len(consolidated_df)}")
        return consolidated_path
    else:
        print("      Warning: No data to consolidate")
        return None


def process_da_for_simulation(
    da_list_path: str,
    da_manual_path: str,
    da_climatology_path: str,
    da_consolidated_path: str,
    da_simulation_path: str,
    start_time: datetime,
    end_time: datetime,
    timestamp_str: str
) -> Tuple[Optional[str], Optional[str]]:
    """
    Main function to orchestrate DA data preparation for a simulation.

    For each gauge at each timestep, manual data is preferred over climatology.
    DA covers the full simulation period (start_time to end_time) including
    the dry-run tail.

    Args:
        da_list_path: Path to reservoir list file
        da_manual_path: Path to manual DA data folder
        da_climatology_path: Path to climatology DA data folder
        da_consolidated_path: Path to consolidated output folder
        da_simulation_path: Path to DA_Simulation folder for individual CSVs
        start_time: Simulation start time (systemStartTime, may include warmup)
        end_time: Simulation end time (systemEndTime, includes dry run)
        timestamp_str: Timestamp string for output filename

    Returns:
        Tuple of (da_simulation_path, consolidated_csv_path)
    """
    print("***_________Processing Data Assimilation (DA) data_________***")

    # Read reservoir list
    reservoirs = read_reservoir_list(da_list_path)

    # Create merged individual CSV files in DA_Simulation
    total_manual, total_climatology, gauges_ok = create_simulation_csv_files(
        reservoirs, da_manual_path, da_climatology_path,
        da_simulation_path, start_time, end_time
    )

    # Create consolidated CSV from the already-merged simulation files
    consolidated_csv_path = create_consolidated_da_csv(
        reservoirs, da_simulation_path, start_time, end_time,
        da_consolidated_path, timestamp_str
    )

    print("***_________DA data processing complete_________***")
    print("")

    return da_simulation_path, consolidated_csv_path


__all__ = [
    'read_reservoir_list',
    'create_simulation_csv_files',
    'create_consolidated_da_csv',
    'process_da_for_simulation',
]
