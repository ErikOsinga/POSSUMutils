# parse the argument basedir
echo "Running collate job from directory $1"
workdir=$1/combined/

# e.g. /arc/projects/CIRADA/polarimetry/pipeline_runs/partial_tiles/943MHz/

# automatically export all variables for Prefect
set -a
source /arc/projects/CIRADA/polarimetry/software/POSSUMutils/automation/config.env
set +a

# for band 1, directory is also "943MHz"
psrecord "python /arc/projects/CIRADA/polarimetry/software/POSSUM_Polarimetry_Pipeline/pipeline/pipeline_prefect.py $workdir/config_collate_band1.ini collate" --include-children --log $workdir/psrecord.txt --plot $workdir/psrecord.png --interval 1

# move all logfiles to the logdir to keep it clean
mv $workdir/*log $workdir/logs/