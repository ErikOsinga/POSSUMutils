
# download + ingest tiles to prepare for a 3D pipeline run
# or in the future, a 1D pipeline run with full-depth tiles

echo "Preparing test pipeline run"
# set which port to communicate results to 
echo "Setting PREFECT URL"
export PREFECT_API_URL=$1
export PREFECT_API_AUTH_STRING=$2

# Run possum_run_remote to download and ingest tiles
cd /arc/projects/CIRADA/polarimetry/software/POSSUMutils
python -m possum_pipeline_control.3d_pipeline_download_ingest