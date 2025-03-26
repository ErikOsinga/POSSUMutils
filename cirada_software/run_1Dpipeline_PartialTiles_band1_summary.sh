echo "Preparing summary pipeline run name $1 field_ID $2 SB $3"


workdir=/arc/projects/CIRADA/polarimetry/pipeline_runs/partial_tiles/943MHz/$2/$3/
echo "In directory $workdir"

## FOR BAND 1: Create config file and working directory
## config file name e.g. config_943MHz_1412-28_50413_summary.ini
## made in $workdir
echo "Creating config file"
python /arc/projects/CIRADA/polarimetry/software/POSSUMutils/cirada_software/create_config_partialtiles_summary.py /arc/projects/CIRADA/polarimetry/software/POSSUM_Polarimetry_Pipeline/pipeline/canfar_config_templates/config_template_PartialTiles_1d_band1_summaryplot.ini config_943MHz_$2_$3_summary.ini $workdir $2 $3
# arguments: template file, output_filename, workdir, fieldstr, SB_number

echo "Opening SSH tunnel to prefect server host (p1)"
# open connection
ssh -fNT -L 4200:localhost:4200 erik@206.12.93.32
# set which port to communicate results to 
export PREFECT_API_URL="http://localhost:4200/api"

echo "adding RMtools[dev] to pythonpath to work with dev branch of RMtools"
export PYTHONPATH="/arc/projects/CIRADA/polarimetry/software/RMtoolsdev/:$PYTHONPATH"

echo "Starting pipeline run $1 field_ID $2 SBID $3 for summary plot"
psrecord "python /arc/projects/CIRADA/polarimetry/software/POSSUM_Polarimetry_Pipeline/pipeline/pipeline_prefect.py $workdir/config_943MHz_$2_$3_summary.ini" --include-children --log $workdir/psrecord_$2_$3_summary.txt --plot $workdir/psrecord_$2_$3_summary.png --interval 1

echo "Logging pipeline status"
# field, sbid, band
# also uses Prefect
python /arc/projects/CIRADA/polarimetry/software/POSSUMutils/cirada_software/log_processing_status_1D_PartialTiles_summary.py $2 $3 943MHz