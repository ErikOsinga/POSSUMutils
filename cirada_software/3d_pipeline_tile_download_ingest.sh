
# download + ingest tiles to prepare for a 3D pipeline run

cd /arc/projects/CIRADA/polarimetry/ASKAP/Tiles/downloads

file=./config.yml
if [ -e "$file" ]; then
    echo "Running possum_run_remote"

    possum_run_remote

else 
    echo "possum_run_remote config.yml file does not exist!"
fi

echo "possum_run_remote finished"

