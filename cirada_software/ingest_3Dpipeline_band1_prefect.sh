echo "Preparing pipeline ingest for tile number $1 band $2"
echo "Setting PREFECT URL"
export PREFECT_API_URL=$3
export PREFECT_API_AUTH_STRING=$4
#RMtools not used but cant hurt to add it to path
echo "TEMPORARILY: adding RMtools[dev] to pythonpath until new release (>v1.4.6)"
export PYTHONPATH="/arc/projects/CIRADA/polarimetry/software/RMtoolsdev/:$PYTHONPATH"

# move to POSSUMutils base directory
cd /arc/projects/CIRADA/polarimetry/software/POSSUMutils/
# arguments: tile_number, band ("943MHz" or "1367MHz")
python -m possum_pipeline_control.ingest3Dpipeline $1 $2