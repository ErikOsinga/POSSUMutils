# parse the argument basedir
echo "Running collate job from directory $1"
workdir=$1

# for band 1, directory is also "943MHz"
psrecord "python /arc/projects/CIRADA/polarimetry/software/POSSUM_Polarimetry_Pipeline/pipeline/pipeline_prefect.py config_collate_band1.ini" --include-children --log $workdir/psrecord.txt --plot $workdir/psrecord.png --interval 1