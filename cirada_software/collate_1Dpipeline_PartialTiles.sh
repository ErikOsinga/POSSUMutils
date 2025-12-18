# parse the argument basedir
echo "Running collate job from directory $1 as p1 user $2"
workdir=$1/combined/
p1user=$2
# e.g. /arc/projects/CIRADA/polarimetry/pipeline_runs/partial_tiles/943MHz/

echo "Opening SSH tunnel to prefect server host (p1) as user $p1user"
# open connection
ssh -fNT -L 4200:localhost:4200 $p1user@206.12.93.32
# set which port to communicate results to 
export PREFECT_API_URL="http://localhost:4200/api"


# for band 1, directory is also "943MHz"
psrecord "python /arc/projects/CIRADA/polarimetry/software/POSSUM_Polarimetry_Pipeline/pipeline/pipeline_prefect.py $workdir/config_collate_band1.ini collate" --include-children --log $workdir/psrecord.txt --plot $workdir/psrecord.png --interval 1

# move all logfiles to the logdir to keep it clean
mv $workdir/*log $workdir/logs/