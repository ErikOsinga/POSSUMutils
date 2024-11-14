echo "Preparing pipeline run name $1 field_ID $2 tile number $3 SB$4"

### TODO: write clean script in case of repeat run
echo "REMOVING POSSIBLE XML FILE FROM PREVIOUS RUN"
rm /arc/projects/CIRADA/polarimetry/ASKAP/PartialTiles/sourcelists/selavy-image.i.EMU_$2.SB$4.cont.taylor.0.restored.conv.components.15sig.$3.tile$3.xml

## FOR BAND 1
echo "Creating working directory"
echo "/arc/projects/CIRADA/polarimetry/pipeline_runs/partial_tiles/943MHz/$2/$3"
mkdir -p "/arc/projects/CIRADA/polarimetry/pipeline_runs/partial_tiles/943MHz/$2/$3"

echo "Creating config file"
echo "Band 1, tile centers"
#cp /arc/projects/CIRADA/polarimetry/ASKAP/PartialTiles/tests/config_test_partial_1d_band1_core_BKP.ini /arc/projects/CIRADA/polarimetry/pipeline_runs/partial_tiles/$2/$3/config_$2_$3.ini

python /arc/projects/CIRADA/polarimetry/ASKAP/PartialTiles/config_templates/create_config.py /arc/projects/CIRADA/polarimetry/ASKAP/PartialTiles/config_templates/config_PartialTiles_1d_centers_band1.ini config_$2_$3.ini /arc/projects/CIRADA/polarimetry/pipeline_runs/partial_tiles/943MHz/$2/$3 $2 $3 $4
# arguments: template file, output_filename, workdir, fieldstr, tile_number, SB_number


echo "Opening SSH tunnel to prefect server host (p1)"
# open connection
ssh -fNT -L 4200:localhost:4200 erik@206.12.93.32
# set which port to communicate results to 
export PREFECT_API_URL="http://localhost:4200/api"

echo "adding RMtools[dev] to pythonpath to work with dev branch of RMtools"
export PYTHONPATH="/arc/projects/CIRADA/polarimetry/software/RMtoolsdev/:$PYTHONPATH"

echo "Starting pipeline run $1 field_ID $2 tile number $3"
### TODO update config_filename with band1/band2?
psrecord "python /arc/projects/CIRADA/polarimetry/software/POSSUM_Polarimetry_Pipeline/pipeline/pipeline_prefect.py /arc/projects/CIRADA/polarimetry/pipeline_runs/partial_tiles/943MHz/$2/$3/config_$2_$3.ini" --include-children --log /arc/projects/CIRADA/polarimetry/pipeline_runs/partial_tiles/943MHz/$2/$3/psrecord_$2_$3.txt --plot /arc/projects/CIRADA/polarimetry/pipeline_runs/partial_tiles/943MHz/$2/$3/psrecord_tile$2_$3.png --interval 1

echo "Logging pipeline status"
echo "TODO"
# python /arc/projects/CIRADA/polarimetry/software/log_processing_status.py $2 943MHz
# TODO: for the Partial Tile pipeline, check the following status as well:
# If all edges and centers have been done for a certain field
# then delete the partial tiles from CANFAR (otherwise too much storage)
#
