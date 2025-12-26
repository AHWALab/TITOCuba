"""
Data Assimilation (DA) utilities for TITO Cuba system.

This module handles the preparation and management of reservoir observation data
for data assimilation in the EF5 model.

Functions:
    - read_reservoir_list: Read the list of reservoirs to process
    - check_manual_da_availability: Check if manual DA data exists and covers simulation period
    - prepare_da_paths: Determine which DA path to use for each reservoir
    - create_consolidated_da_csv: Create consolidated CSV with all reservoir data
    - process_da_for_simulation: Main function to orchestrate DA preparation
"""

import os
import glob
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


def check_manual_da_availability(
    reservoir_id: str,
    da_manual_path: str,
    start_time: datetime,
    end_time: datetime
) -> Tuple[bool, Optional[str]]:
    """
    Check if manual DA data exists for a reservoir and covers the simulation period.
    
    Args:
        reservoir_id: Reservoir ID (e.g., 'EMB2100002')
        da_manual_path: Path to the DA_Manual folder
        start_time: Simulation start time
        end_time: Simulation end time
        
    Returns:
        Tuple of (data_available: bool, file_path: Optional[str])
    """
    expected_filename = f"{reservoir_id}_Vertimiento_Serie.csv"
    file_path = os.path.join(da_manual_path, expected_filename)
    
    if not os.path.exists(file_path):
        return False, None
    
    try:
        # Read the CSV file
        df = pd.read_csv(file_path, header=None, names=['timestamp', 'value'])
        
        # Parse timestamps - handle both MM/DD/YYYY and DD/MM/YYYY formats
        try:
            df['datetime'] = pd.to_datetime(df['timestamp'], format='%m/%d/%Y %H:%M')
        except:
            try:
                df['datetime'] = pd.to_datetime(df['timestamp'], format='%d/%m/%Y %H:%M')
            except:
                # Try automatic parsing as last resort
                df['datetime'] = pd.to_datetime(df['timestamp'])
        
        # Check if data covers the simulation period
        data_start = df['datetime'].min()
        data_end = df['datetime'].max()
        
        # Convert Python datetime to pandas Timestamp for comparison
        # Remove timezone info to match timezone-naive CSV data
        start_ts = pd.Timestamp(start_time).tz_localize(None) if pd.Timestamp(start_time).tz else pd.Timestamp(start_time)
        end_ts = pd.Timestamp(end_time).tz_localize(None) if pd.Timestamp(end_time).tz else pd.Timestamp(end_time)
        
        # Data should start at or before simulation start and end at or after simulation end
        if data_start <= start_ts and data_end >= end_ts:
            return True, file_path
        else:
            return False, None
            
    except Exception as e:
        print(f"    Warning: Error reading {file_path}: {e}")
        return False, None


def prepare_da_paths(
    reservoirs: List[str],
    da_manual_path: str,
    da_climatology_path: str,
    start_time: datetime,
    end_time: datetime
) -> Dict[str, str]:
    """
    Determine which DA path to use for each reservoir.
    
    Args:
        reservoirs: List of reservoir IDs
        da_manual_path: Path to manual DA data folder
        da_climatology_path: Path to climatology DA data folder
        start_time: Simulation start time
        end_time: Simulation end time
        
    Returns:
        Dictionary mapping reservoir_id -> obs_path
    """
    da_path_map = {}
    manual_count = 0
    climatology_count = 0
    
    print("    Checking DA data availability for each reservoir:")
    
    for reservoir_id in reservoirs:
        is_available, manual_file = check_manual_da_availability(
            reservoir_id, da_manual_path, start_time, end_time
        )
        
        if is_available:
            # Use manual data
            relative_path = os.path.join(da_manual_path, f"{reservoir_id}_Vertimiento_Serie.csv")
            da_path_map[reservoir_id] = relative_path
            manual_count += 1
        else:
            # Fall back to climatology data
            relative_path = os.path.join(da_climatology_path, f"{reservoir_id}_Vertimiento_Serie.csv")
            da_path_map[reservoir_id] = relative_path
            climatology_count += 1
    
    print(f"    DA path selection complete:")
    print(f"      - Using manual data: {manual_count} reservoirs")
    print(f"      - Using climatology data: {climatology_count} reservoirs")
    
    return da_path_map


def create_consolidated_da_csv(
    reservoirs: List[str],
    da_path_map: Dict[str, str],
    start_time: datetime,
    end_time: datetime,
    output_path: str,
    timestamp_str: str
) -> str:
    """
    Create a consolidated CSV with all reservoir data for the simulation period.
    
    The consolidated CSV format is:
        reservoir_id,timestamp,value
        EMB2100002,01/01/2023 00:00,1.5
        EMB2100002,01/01/2023 00:30,1.6
        ...
    
    Args:
        reservoirs: List of reservoir IDs
        da_path_map: Dictionary mapping reservoir_id -> obs_path
        start_time: Simulation start time
        end_time: Simulation end time
        output_path: Path to DA_Consolidated folder
        timestamp_str: Timestamp string for output filename (e.g., '20230609_0000')
        
    Returns:
        Path to the created consolidated CSV file
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
    
    # Create new consolidated CSV
    consolidated_filename = f"da.observations.{timestamp_str}.csv"
    consolidated_path = os.path.join(output_path, consolidated_filename)
    
    all_data = []
    
    for reservoir_id in reservoirs:
        obs_file = da_path_map[reservoir_id]
        
        if not os.path.exists(obs_file):
            print(f"      Warning: File not found for {reservoir_id}: {obs_file}")
            continue
        
        try:
            # Read the reservoir data
            df = pd.read_csv(obs_file, header=None, names=['timestamp', 'value'])
            
            # Parse timestamps
            try:
                df['datetime'] = pd.to_datetime(df['timestamp'], format='%m/%d/%Y %H:%M')
            except:
                try:
                    df['datetime'] = pd.to_datetime(df['timestamp'], format='%d/%m/%Y %H:%M')
                except:
                    df['datetime'] = pd.to_datetime(df['timestamp'])
            
            # Convert Python datetime to pandas Timestamp for comparison
            # Remove timezone info to match timezone-naive CSV data
            start_ts = pd.Timestamp(start_time).tz_localize(None) if pd.Timestamp(start_time).tz else pd.Timestamp(start_time)
            end_ts = pd.Timestamp(end_time).tz_localize(None) if pd.Timestamp(end_time).tz else pd.Timestamp(end_time)
            
            # Filter to simulation period
            mask = (df['datetime'] >= start_ts) & (df['datetime'] <= end_ts)
            df_filtered = df[mask].copy()
            
            # Add reservoir ID column
            df_filtered['reservoir_id'] = reservoir_id
            
            # Reformat timestamp to MM/DD/YYYY HH:MM
            df_filtered['timestamp_formatted'] = df_filtered['datetime'].dt.strftime('%m/%d/%Y %H:%M')
            
            # Select columns in correct order: reservoir_id, timestamp, value
            df_output = df_filtered[['reservoir_id', 'timestamp_formatted', 'value']]
            
            all_data.append(df_output)
            
        except Exception as e:
            print(f"      Warning: Error processing {reservoir_id}: {e}")
            continue
    
    # Combine all data and write to file
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
    start_time: datetime,
    end_time: datetime,
    timestamp_str: str
) -> Tuple[Dict[str, str], Optional[str]]:
    """
    Main function to orchestrate DA data preparation for a simulation.
    
    Args:
        da_list_path: Path to reservoir list file
        da_manual_path: Path to manual DA data folder
        da_climatology_path: Path to climatology DA data folder
        da_consolidated_path: Path to consolidated output folder
        start_time: Simulation start time
        end_time: Simulation end time
        timestamp_str: Timestamp string for output filename
        
    Returns:
        Tuple of (da_path_map, consolidated_csv_path)
        - da_path_map: Dict mapping reservoir_id -> obs_path for control file
        - consolidated_csv_path: Path to consolidated CSV file
    """
    print("***_________Processing Data Assimilation (DA) data_________***")
    
    # Read reservoir list
    reservoirs = read_reservoir_list(da_list_path)
    
    # Determine which path to use for each reservoir
    da_path_map = prepare_da_paths(
        reservoirs, da_manual_path, da_climatology_path, start_time, end_time
    )
    
    # Create consolidated CSV
    consolidated_csv_path = create_consolidated_da_csv(
        reservoirs, da_path_map, start_time, end_time,
        da_consolidated_path, timestamp_str
    )
    
    print("***_________DA data processing complete_________***")
    print("")
    
    return da_path_map, consolidated_csv_path


__all__ = [
    'read_reservoir_list',
    'check_manual_da_availability',
    'prepare_da_paths',
    'create_consolidated_da_csv',
    'process_da_for_simulation',
]
