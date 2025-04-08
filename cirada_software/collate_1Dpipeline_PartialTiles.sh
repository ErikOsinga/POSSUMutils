# parse the argument basedir
echo "Running collate job from directory $1"
workdir=$1

echo "Opening SSH tunnel to prefect server host (p1)"
# open connection
ssh -fNT -L 4200:localhost:4200 erik@206.12.93.32
# set which port to communicate results to 
export PREFECT_API_URL="http://localhost:4200/api"


# for band 1, directory is also "943MHz"
psrecord "python /arc/projects/CIRADA/polarimetry/software/POSSUM_Polarimetry_Pipeline/pipeline/pipeline_prefect.py config_collate_band1.ini" --include-children --log $workdir/psrecord.txt --plot $workdir/psrecord.png --interval 1