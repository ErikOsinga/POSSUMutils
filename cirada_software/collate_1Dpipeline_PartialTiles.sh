# parse the argument basedir
echo "Running collate job from directory $1"
# e.g. /arc/projects/CIRADA/polarimetry/pipeline_runs/partial_tiles/943MHz/
workdir=$1/combined/

echo "Setting PREFECT URL"
export PREFECT_API_URL=$2
export PREFECT_API_AUTH_STRING=$3

# for band 1, directory is also "943MHz"
psrecord "python /arc/projects/CIRADA/polarimetry/software/POSSUM_Polarimetry_Pipeline/pipeline/pipeline_prefect.py $workdir/config_collate_band1.ini collate" --include-children --log $workdir/psrecord.txt --plot $workdir/psrecord.png --interval 1

# move all logfiles to the logdir to keep it clean
mv $workdir/*log $workdir/logs/