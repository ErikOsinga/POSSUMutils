# parse the arguments of up to 4 tiles into a list with possibly empty strings
tilelist= "['$4','$5','$6','$7']"
echo $tilelist
echo "Preparing pipeline run name $1 field_ID $2 SB $3 tiles $tilelist"


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
# Output the result, e.g. 8843+8971
echo "Found tiles $tilelist_dir"
workdir=/arc/projects/CIRADA/polarimetry/pipeline_runs/partial_tiles/943MHz/$2/$3/$tilelist_dir/
echo "Will create directory $workdir$"

### TODO: write clean script in case of repeat run. Add to create_config.py?
echo "REMOVING POSSIBLE XML FILE FROM PREVIOUS RUN"
rm /arc/projects/CIRADA/polarimetry/ASKAP/PartialTiles/sourcelists/selavy-image.i.EMU_$2.SB$4.cont.taylor.0.restored.conv.components.15sig.$3.tile$3.xml

## FOR BAND 1: Create config file and working directory
## config file name e.g. config_943MHz_1412-28_50413_8843_8971_<>_<>.ini where <> is empty 
## made in $workdir
echo "Creating config file"
python /arc/projects/CIRADA/polarimetry/software/POSSUMutils/create_config_partialtiles.py /arc/projects/CIRADA/polarimetry/ASKAP/PartialTiles/config_templates/config_PartialTiles_1d_centers_band1.ini config_943MHz_$2_$3_$tilelist_dir.ini $workdir $2 $3 $tilelist
# arguments: template file, output_filename, workdir, fieldstr, SB_number, tile_numbers

echo "Opening SSH tunnel to prefect server host (p1)"
# open connection
ssh -fNT -L 4200:localhost:4200 erik@206.12.93.32
# set which port to communicate results to 
export PREFECT_API_URL="http://localhost:4200/api"

echo "adding RMtools[dev] to pythonpath to work with dev branch of RMtools"
export PYTHONPATH="/arc/projects/CIRADA/polarimetry/software/RMtoolsdev/:$PYTHONPATH"

echo "Starting pipeline run $1 field_ID $2 tile number $3"
psrecord "python /arc/projects/CIRADA/polarimetry/software/POSSUM_Polarimetry_Pipeline/pipeline/pipeline_prefect.py $workdir/config_$2_$3_$tilelist_dir.ini" --include-children --log $workdir/psrecord_$2_$3_$tilelist_dir.txt --plot $workdir/psrecord_$2_$3_$tilelist_dir.png --interval 1

echo "Logging pipeline status"
echo "TODO"
# python /arc/projects/CIRADA/polarimetry/software/log_processing_status.py $2 943MHz
# TODO: for the Partial Tile pipeline, check the following status as well:
# If all edges and centers have been done for a certain field
# then create summary plot and delete the partial tiles from CANFAR (otherwise too much storage)
#
