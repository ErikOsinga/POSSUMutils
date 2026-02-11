# download + ingest tiles to prepare for a 3D pipeline run
# or in the future, a 1D pipeline run with full-depth tiles

echo "Preparing test pipeline run"
# set which port to communicate results to 
echo "Setting PREFECT URL"
# automatically export all variables for Prefect
set -a
source /arc/projects/CIRADA/polarimetry/software/POSSUMutils/automation/config.env
set +a

# Run possum_run_remote to download and ingest tiles
cd /arc/projects/CIRADA/polarimetry/software/POSSUMutils
python -m possum_pipeline_control.3d_pipeline_download_ingest