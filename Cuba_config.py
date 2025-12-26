domain = "Cuba"
subdomain = "Regional"
model_resolution = "1km" #change
systemModel = "crest"
systemTimestep = 60 #in minutes

# Coordinates used for generating Nowcast / QPF files.
# For ML-based nowcasting, these coordinates should cover a region of size 518 x 360 pixels.
xmin = -101.0
xmax = -49.2
ymin = 3.5
ymax = 39.5
nowcast_model_name = "convlstm" 
systemName = systemModel.upper() + " " + domain.upper() + " " + subdomain.upper()
ef5Path = "/home/naman/EF5/EF5LatestRelease/EF5/bin/ef5"
statesPath = "states/"
statesHighResPath = "statesHighRes/"
precipFolder = "precip/"
precipEF5Folder = "precipEF5/"
modelStates = ["crest_SM", "kwr_IR", "kwr_pCQ", "kwr_pOQ"]
highres_state_models = ["crest_SM", "kwr_IR"]
templatePath = "templates/"
templates = "ef5_control_template.txt"
dataPath = "outputs/"
qpf_store_path = 'qpf_store/'
tmpOutput = dataPath + "tmp_output_" + systemModel + "/"

# High-resolution EF5 rerun settings
run_highres = True
highres_threshold = 1.0  # maxunitq threshold to trigger 25m rerun
highres_template = "ef5_control_highRes_templeate.txt"
highres_maskgrid = "basic/maskgrid.tif"
highres_gauge_list = "templates/25mGaugeList.txt"
highres_resolution_tag = "25m"
highres_min_gauges = 1
highres_dataPath = "outputs_25m/"
highres_tmpOutput = highres_dataPath + "tmp_output_" + systemModel + "_25m/"

# Data Assimilation (DA) configuration
run_withDA = True
DA_climatology_path = "DA_Climatology/"
DA_manual_path = "DA_Manual/"
DA_consolidated_path = "DA_Consolidated/"
DA_list_path = "templates/DA_list.txt"

#Alerts configuration
SEND_ALERTS = False
smtp_server = "smtp.gmail.com"
smtp_port = 587
account_address = "model_alerts@gmail.com"
account_password = "supersecurepassword9000"
alert_sender = "Real Time Model Alert" # can also be the same as account_address
alert_recipients = ["fixer1@company.com", "fixer2@company.com", "panic@company.com",...]
copyToWeb = False

#Simulation times 
"""
If Hindcast and LR_mode is True, user MUST define StartLRtime, EndLRTime, LR_timestep,GFS_archive_path
If running in operational mode (Hindcast False) and LR_mode = True, user only have to define LR_timestep, GFS_archive_path
"""
HindCastMode = False
HindCastDate = "2023-06-09 00:00" #"%Y-%m-%d %H:%M" UTC

run_LR = True
StartLRtime = "2023-06-09 00:00" #"%Y-%m-%d %H:%M" UTC. Date of first QPF file
EndLRTime = "2023-06-10 00:00" #"%Y-%m-%d %H:%M" UTC. Date of last QPF file
LR_timestep = "60u"
# Path to archived GFS GeoTIFFs used by LR/QPF. Must point to the in-repo folder
# that actually contains the files (see precip/GFS/GFSData/ in this repo).
# Use a path relative to the orchestrator working directory (repo root) or set
# an absolute path if you run from elsewhere.
QPF_archive_path = "precip/GFS"

# Email associated to GPM account
email_gpm = 'vrobledodelgado@uiowa.edu'
server = 'https://jsimpsonhttps.pps.eosdis.nasa.gov/imerg/gis/early/'