"""
Real-time model/subdomain execution script

This is an IMERG-based operational system that integrates either Ml routines or 
NWP outputs from public available sources to produce a flash flood forecast in real time. 


Contributors:
Vanessa Robledo - vrobledodelgado@uiowa.edu
Humberto Vergara - humberto-vergaraarrieta@uiowa.edu
V.2.0 - October 01, 2025

Please use this script and a configuration file as follows:

    $> python orchestrator.py <configuration_file.py>

"""

from shutil import rmtree, copy
import os
from os import makedirs, listdir, rename, remove
import glob
from datetime import datetime as dt
from datetime import timedelta, timezone
import numpy as np
import re
import subprocess
import sys
from tito_utils.file_utils import cleanup_precip, newline
from tito_utils.qpe_utils import get_new_precip
from tito_utils.qpf_utils import run_convlstm, download_GFS, GFS_searcher, WRF_searcher 
from tito_utils.ef5 import prepare_ef5, run_ef5_simulation
from tito_utils.highres_utils import prepare_highres_control
print(">>> Modules imported")

"""
Setup Environment Variables for Linux Shared Libraries and OpenMP Threads (PARA USAR ML de AGRHYMET)

"""

def _sync_highres_states(source_path, target_path, state_names):
    """Copy the subset of low-res state files needed by the 25 m rerun."""
    if not state_names:
        return 0
    os.makedirs(target_path, exist_ok=True)
    copied = 0
    for state in state_names:
        pattern = os.path.join(source_path, f"{state}_*.tif")
        for src_file in glob.glob(pattern):
            dest_file = os.path.join(target_path, os.path.basename(src_file))
            if os.path.abspath(src_file) == os.path.abspath(dest_file):
                continue
            try:
                if (not os.path.exists(dest_file) or
                        os.path.getmtime(src_file) > os.path.getmtime(dest_file)):
                    copy(src_file, dest_file)
                    copied += 1
            except Exception as exc:
                print(f"    Warning: unable to copy state {src_file} -> {dest_file}: {exc}")
    if copied:
        print(f"    Synced {copied} high-res state file(s) into {target_path}")
    else:
        print("    No new high-res state files needed for this cycle.")
    return copied


def main(args):
    """Main function of the script.
    
    This function reads the real-time configuration script, makes sure the necessary files to run EF5 exist and are in the right place, runs the model(s), writes the outputs and states, and reports vie email if an error occurs during execution.
    
    Arguments:
        args {list} -- the first argument ([1]) corresponds to a real-time configuration file.
    """
    ###-------------------------- SETTING SECTION --------------------------------
    #set true of False to fill 4h imerg latency and create +2h hours (nowcast)
    NOWCAST = True 
    
    # Read the configuration file User should change this line if the configuration file has a different name
    import Cuba_config as config_file
    print(">>> Config file loaded")

    #Configuration file
    domain = config_file.domain
    subdomain = config_file.subdomain
    xmin = config_file.xmin
    ymin = config_file.ymin
    xmax = config_file.xmax
    ymax = config_file.ymax
    systemModel = config_file.systemModel
    systemName = config_file.systemName
    systemTimestep = config_file.systemTimestep
    ef5Path = config_file.ef5Path
    precipFolder = config_file.precipFolder
    statesPath = config_file.statesPath
    statesHighResPath = getattr(config_file, "statesHighResPath", statesPath)
    precipEF5Folder = config_file.precipEF5Folder
    modelStates = config_file.modelStates
    highres_state_models = getattr(config_file, "highres_state_models", ["crest_SM", "kwr_IR"])
    templatePath = config_file.templatePath
    template = config_file.templates
    nowcast_model_name = config_file.nowcast_model_name
    dataPath = config_file.dataPath
    qpf_store_path = config_file.qpf_store_path
    tmpOutput = config_file.tmpOutput
    run_highres = getattr(config_file, "run_highres", False)
    highres_threshold = getattr(config_file, "highres_threshold", None)
    highres_template = getattr(config_file, "highres_template", template)
    highres_maskgrid = getattr(config_file, "highres_maskgrid", None)
    highres_gauge_list = getattr(config_file, "highres_gauge_list", None)
    highres_resolution_tag = getattr(config_file, "highres_resolution_tag", "25m")
    highres_min_gauges = getattr(config_file, "highres_min_gauges", 1)
    highres_dataPath = getattr(config_file, "highres_dataPath", dataPath)
    highres_tmpOutput = getattr(config_file, "highres_tmpOutput", tmpOutput)
    SEND_ALERTS = config_file.SEND_ALERTS
    alert_recipients = config_file.alert_recipients
    HindCastMode = config_file.HindCastMode
    HindCastDate = config_file.HindCastDate
    LR_run = config_file.run_LR
    LR_TimeStep = config_file.LR_timestep
    GFS_archive_path = config_file.QPF_archive_path
    email_gpm = config_file.email_gpm
    server = config_file.server
    smtp_config = {
        'smtp_server': config_file.smtp_server,
        'smtp_port': config_file.smtp_port,
        'account_address': config_file.account_address,
        'account_password': config_file.account_password,
        'alert_sender': config_file.alert_sender}
    
    newline(2)
    
    # Real-time mode or Hindcast mode
    # Figure out the timing for running the current timestep
    if HindCastMode == True:
        currentTime = dt.strptime(HindCastDate, "%Y-%m-%d %H:%M")
    else:
        currentTime = dt.now(timezone.utc)
    
    # Round down the current minutess to the nearest 30min increment in the past (for 30 forecast)
    if systemTimestep == 30:
        minutes = int(np.floor(currentTime.minute / 30.0) * 30)
    if systemTimestep == 60: #for 60 min forecast
        minutes = 0 
    # Use the rounded down minutes as the timestamp for the current time step
    currentTime = currentTime.replace(minute=minutes, second=0, microsecond=0)
    
    if HindCastMode == True:
        print(f"*** Starting hindcast run cycle at {currentTime.strftime('%Y-%m-%d_%H:%M')} UTC ***")
        newline(2)
    else:
        print(f"*** Starting real-time run cycle at {currentTime.strftime('%Y-%m-%d_%H:%M')} UTC ***")
        newline(2) 
        
    # Configure the system to run once every hour
    # Start the simulation using QPEs from 4-6 hours ago
    systemStartTime = currentTime - timedelta(hours=4.5) 
    # Save states for the current run with the current time step's timestamp
    systemStateEndTime = currentTime - timedelta(hours=4) #change to 4
    # Run warm up using the last hour of data until the current time step
    systemWarmEndTime = currentTime - timedelta(hours=4)
    # Only check for states as far as we have QPs (6 hours)
    failTime = currentTime - timedelta(hours=6)
    
    systemStartLRTime = dt.strptime(config_file.StartLRtime,"%Y-%m-%d %H:%M")
    EndLRTime = dt.strptime(config_file.EndLRTime,"%Y-%m-%d %H:%M")
    
    if HindCastMode and LR_run:
        systemEndTime = EndLRTime + timedelta(hours=6) #4 hours dry
    if HindCastMode and not LR_run:
        systemEndTime = currentTime + timedelta(hours=6) #4 hours dry after ml
    #operational options
    if not HindCastMode and LR_run:
        systemStartLRTime = currentTime #change as desired [removed + timedelta(hours=2) as we have GFS so LR can be same as current time ]
        EndLRTime = currentTime + timedelta(hours=24) #4 hours of qpf
        systemEndTime = EndLRTime + timedelta(hours=6) #4 hours dry after gfs
    if not HindCastMode and not LR_run:
        systemEndTime = currentTime + timedelta(hours=6) #si no corro gfs y hindcast no 
        
    ###-------------------------- START ROUTINES --------------------------------
    try:
        # Clean up old QPE files from GeoTIFF archive (older than 6 hours)
        # Keep latest QPFs
        print("***_________Cleaning old QPE files from the precip folder_________***")
        cleanup_precip(currentTime, precipFolder, qpf_store_path)
        newline(1)
        print("***_________Precip folder cleaning completed_________***")
        newline(2)
        
        # Get the necessary QPEs and QPFs for the current time step into the GeoTIFF precip folder store whether there's a QPE gap or the QPEs for the current time step is missing
        print("***_________Retrieving IMERG files_________***")
        get_new_precip(currentTime, server, precipFolder, email_gpm, HindCastMode, qpf_store_path, xmin, ymin, xmax, ymax)
        newline(1)
        print("***_________IMERG files are complete in precip folder_________***")
        newline(2)
    except Exception as e:
        import traceback
        print(f"There was a problem with the QPE routines: {e}. Ignoring errors and continuing with execution")
        traceback.print_exc()
        
    ###-------------------------- START NOWCAST SECTION --------------------------------      
    if NOWCAST:
        try:
            #if true, will create a nowcast filling the last 4 hours of imerge latency + 2hours of nowcast 
            print(f"***_________Generating the nowcast from {currentTime - timedelta(hours=3.5)} to {currentTime}_________***")
            run_convlstm(currentTime, precipFolder, nowcast_model_name, xmin, ymin, xmax, ymax)
            newline(1)
            print("***_________Nowcast/ML files are complete in precip folder_________***")
            newline(2)
        except:
            print("There was a problem with the ML routines. Ignoring errors and continuing with execution")
            
    ###-------------------------- START LR-QPF SECTION --------------------------------
    if LR_run:
        # When in LR mode, use GFS for the 24-hour forecast period only
        # The 4-hour gap is filled by nowcast above
        print(f"***_________Preparing GFS QPF for 24-hour forecast from {systemStartLRTime} to {EndLRTime}_________***")
        print(f"    Gap-filling (4 hours ago to current time) is handled by nowcast above")
        print(f"    GFS provides 24-hour forecast from current time onwards")
        try:
            # GFS download for the 24-hour forecast period
            GFS_searcher(GFS_archive_path, qpf_store_path, systemStartLRTime, EndLRTime, xmin, xmax, ymin, ymax)
            newline(1)
            print("***_________GFS forecast files are complete_________***")
        except Exception as e:
            print(f"There was a problem with the GFS routines: {e}. Ignoring errors and continuing with execution")
        
        newline(1)
        print("***_________All QPE + QPF files are ready in local folder_________***")
    newline(2)
    
    ###-------------------------- START EF5 SECTION --------------------------------
    print("***_________Preparing the EF5 run_________***")
    realSystemStartTime, controlFile = prepare_ef5(precipEF5Folder, precipFolder, statesPath, modelStates, 
        systemStartTime, failTime, currentTime, systemName, SEND_ALERTS, 
        alert_recipients, smtp_config, tmpOutput, dataPath, 
        subdomain, systemModel, templatePath, template, systemStartLRTime, 
        systemWarmEndTime, systemStateEndTime, systemEndTime, LR_TimeStep, LR_run)
    
    print(f"    Running simulation system for: {currentTime.strftime('%Y%m%d_%H%M')}")
    print(f"    Simulations start at: {realSystemStartTime.strftime('%Y%m%d_%H%M')} and ends at: {systemEndTime.strftime('%Y%m%d_%H%M')} while state update ends at: {systemStateEndTime.strftime('%Y%m%d_%H%M')}")
    
    print("***_________EF5 is ready to be run_________***")
    
    # Use orchestrator's currentTime to timestamp outputs/logs
    output_timestamp_str = currentTime.strftime("%Y%m%d.%H%M%S")
    run_ef5_simulation(ef5Path, tmpOutput, controlFile, output_timestamp_str)
    newline(2)
    print("******** EF5 Outputs are ready!!! ********")

    if run_highres:
        maxunitq_path = os.path.join(tmpOutput, f"maxunitq.{output_timestamp_str}.tif")
        highres_template_path = os.path.join(templatePath, highres_template)
        prerequisites = []
        if not highres_maskgrid:
            prerequisites.append("mask grid path not set")
        elif not os.path.exists(highres_maskgrid):
            prerequisites.append(f"mask grid missing ({highres_maskgrid})")
        if not highres_gauge_list:
            prerequisites.append("gauge list path not set")
        elif not os.path.exists(highres_gauge_list):
            prerequisites.append(f"gauge list missing ({highres_gauge_list})")
        if not os.path.exists(highres_template_path):
            prerequisites.append(f"high-res template missing ({highres_template_path})")

        if prerequisites:
            print("High-res EF5 rerun skipped due to configuration issues:")
            for issue in prerequisites:
                print(f"    - {issue}")
        else:
            selection = None
            try:
                selection = prepare_highres_control(
                    maxunitq_path=maxunitq_path,
                    mask_grid_path=highres_maskgrid,
                    gauge_list_path=highres_gauge_list,
                    template_path=highres_template_path,
                    threshold=highres_threshold,
                    gauge_name_prefix=f"{subdomain}_{highres_resolution_tag}",
                )
            except Exception as exc:
                print(f"High-res preprocessing failed: {exc}")

            selected_count = selection.count if selection else 0
            if selection and selected_count >= max(1, highres_min_gauges):
                newline(1)
                print(f"***_________Preparing the high-resolution EF5 run ({selected_count} gauges)_________***")
                _sync_highres_states(statesPath, statesHighResPath, highres_state_models)
                hr_real_start, hr_control_file = prepare_ef5(
                    precipEF5Folder,
                    precipFolder,
                    statesHighResPath,
                    highres_state_models,
                    systemStartTime,
                    failTime,
                    currentTime,
                    systemName,
                    SEND_ALERTS,
                    alert_recipients,
                    smtp_config,
                    highres_tmpOutput,
                    highres_dataPath,
                    subdomain,
                    systemModel,
                    templatePath,
                    highres_template,
                    systemStartLRTime,
                    systemWarmEndTime,
                    systemStateEndTime,
                    systemEndTime,
                    LR_TimeStep,
                    LR_run,
                )
                print(f"    Running high-res simulation with {highres_resolution_tag} grids")
                run_ef5_simulation(
                    ef5Path,
                    highres_tmpOutput,
                    hr_control_file,
                    output_timestamp_str,
                    resolution_tag=highres_resolution_tag,
                )
                newline(1)
                print("******** High-resolution EF5 Outputs are ready!!! ********")
            else:
                print(
                    f"High-res EF5 rerun skipped (selected {selected_count} gauge(s), "
                    f"needs at least {highres_min_gauges})."
                )
             
"""
Run the main() function when invoked as a script
"""
if __name__ == "__main__":
    main(sys.argv)