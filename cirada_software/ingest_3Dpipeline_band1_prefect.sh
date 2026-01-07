echo "Preparing pipeline ingest for tile number $1 band $2"

p1user=$3

echo "Opening SSH tunnel to prefect server host (p1) as user $p1user"
# open connection
ssh -fNT -L 4200:localhost:4200 $p1user@206.12.93.32
# set PREFECT URL
set -a
source automation/config.env
set +a

#RMtools not used but cant hurt to add it to path
echo "TEMPORARILY: adding RMtools[dev] to pythonpath until new release (>v1.4.6)"
export PYTHONPATH="/arc/projects/CIRADA/polarimetry/software/RMtoolsdev/:$PYTHONPATH"

# move to POSSUMutils base directory
cd /arc/projects/CIRADA/polarimetry/software/POSSUMutils/
# arguments: tile_number, band ("943MHz" or "1367MHz")
python -m possum_pipeline_control.ingest3Dpipeline $1 $2