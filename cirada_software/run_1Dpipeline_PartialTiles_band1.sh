# parse the arguments of up to 4 tiles into a list with possibly empty strings
tilelist="['$4','$5','$6','$7']"
echo $tilelist
echo "Preparing pipeline run name $1 field_ID $2 SB $3 tiles $tilelist"

# automatically export all variables for Prefect
set -a
source /arc/projects/CIRADA/polarimetry/software/POSSUMutils/automation/config.env
set +a

#### parse that list into the directory we want to create. e.g. ['8843','8971','',''], becomes 8843+8971

# Step 1: Remove square brackets and single quotes
tilelist_clean=$(echo "$tilelist" | tr -d "[]'")
# Step 2: Split the string into an array using comma as the delimiter
IFS=',' read -ra tile_array <<< "$tilelist_clean"
# Step 3: Collect non-empty elements
tile_numbers=()
for element in "${tile_array[@]}"; do
    if [ -n "$element" ]; then
        tile_numbers+=("$element")
    fi
done
# Step 4: Join the elements with '+'
tilelist_dir=$(IFS='+'; echo "${tile_numbers[*]}")
# Output the result, e.g. 8843+8971 # also known as TILESTR
echo "Found tiles $tilelist_dir"
workdir=/arc/projects/CIRADA/polarimetry/pipeline_runs/partial_tiles/943MHz/$2/$3/$tilelist_dir/
echo "Will create directory $workdir"

## FOR BAND 1: Create config file and working directory
## config file name e.g. config_943MHz_1412-28_50413_8843+8971.ini where <> is empty 
## made in $workdir
echo "Creating config file"
python /arc/projects/CIRADA/polarimetry/software/POSSUMutils/cirada_software/create_config_partialtiles.py /arc/projects/CIRADA/polarimetry/software/POSSUM_Polarimetry_Pipeline/pipeline/canfar_config_templates/config_template_PartialTiles_1d_band1.ini config_943MHz_$2_$3_$tilelist_dir.ini $workdir $2 $3 "$tilelist_dir"
# arguments: template file, output_filename, workdir, fieldstr, SB_number, tilelist_dir

echo "adding RMtools[dev] to pythonpath to work with dev branch of RMtools"
export PYTHONPATH="/arc/projects/CIRADA/polarimetry/software/RMtoolsdev/:$PYTHONPATH"

echo "Starting pipeline run $1 field_ID $2 SBID $3 tiles $tilelist_dir"
psrecord "python /arc/projects/CIRADA/polarimetry/software/POSSUM_Polarimetry_Pipeline/pipeline/pipeline_prefect.py $workdir/config_943MHz_$2_$3_$tilelist_dir.ini" --include-children --log $workdir/psrecord_$2_$3_$tilelist_dir.txt --plot $workdir/psrecord_$2_$3_$tilelist_dir.png --interval 1


echo "Logging pipeline status"
# go to POSSUMutils to run as module
cd /arc/projects/CIRADA/polarimetry/software/POSSUMutils
# treat cirada_software as a module to make sure imports work
# field, sbid, tilenumbers ("8764+8953"), band
python -m cirada_software.log_processing_status_1D_PartialTiles $2 $3 $tilelist 943MHz --database_config_path /arc/projects/CIRADA/polarimetry/software/POSSUMutils/automation/config.env