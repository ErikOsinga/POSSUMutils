
# download + ingest tiles to prepare for a 3D pipeline run
# or in the future, a 1D pipeline run with full-depth tiles

echo "Preparing test pipeline run"
p1user=$1

echo "Opening SSH tunnel to prefect server host (p1) as $p1user"
# open connection
ssh -fNT -L 4200:localhost:4200 $p1user@206.12.93.32
# set PREFECT URL
set -a
source automation/config.env
set +a

# Run possum_run_remote to download and ingest tiles
cd /arc/projects/CIRADA/polarimetry/software/POSSUMutils
python -m possum_pipeline_control.3d_pipeline_download_ingest
