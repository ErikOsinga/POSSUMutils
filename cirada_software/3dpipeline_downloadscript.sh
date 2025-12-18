
echo " this script is deprecated, see 3d_pipeline_tile_download_ingest.sh instead" 


# echo "Starting download script"

# echo "Opening SSH tunnel to prefect server host (p1) as user $1"
# # open connection
# ssh -fNT -L 4200:localhost:4200 $1@206.12.93.32
# # set which port to communicate results to 
# export PREFECT_API_URL="http://localhost:4200/api"

# #RMtools not used but cant hurt to add it to path
# echo "TEMPORARILY: adding RMtools[dev] to pythonpath until new release (>v1.4.6)"
# export PYTHONPATH="/arc/projects/CIRADA/polarimetry/software/RMtoolsdev/:$PYTHONPATH"

# python /arc/projects/CIRADA/polarimetry/software/POSSUMutils/cirada_software/3dpipeline_downloadscript.py