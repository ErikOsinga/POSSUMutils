echo "Preparing pipeline run name $1 tile number $2"
echo "Creating config file"
# arguments: template file, output_filename, run_name, tile_number, data_dir
python /arc/projects/CIRADA/polarimetry/ASKAP/Pipeline_logs/config_templates/create_config.py \
    /arc/projects/CIRADA/polarimetry/software/POSSUM_Polarimetry_Pipeline/pipeline/canfar_config_templates/config_template_3d_band1.ini \
    config_$2.ini \
    $1 $2 \
    /arc/projects/CIRADA/polarimetry/ASKAP/Tiles/943MHz/$2
# band 1

echo "TEMPORARILY: adding RMtools[dev] to pythonpath until new release (>v1.4.6)"
export PYTHONPATH="/arc/projects/CIRADA/polarimetry/software/RMtoolsdev/:$PYTHONPATH"

echo "Starting pipeline run $1 tile number $2"
### TODO update config_filename with band1/band2?
psrecord "python /arc/projects/CIRADA/polarimetry/software/POSSUM_Polarimetry_Pipeline/pipeline/pipeline_prefect.py /arc/projects/CIRADA/polarimetry/pipeline_runs/943MHz/$1/config_$2.ini 3d" --include-children --log /arc/projects/CIRADA/polarimetry/pipeline_runs/943MHz/$1/psrecord_$2.txt --plot /arc/projects/CIRADA/polarimetry/pipeline_runs/943MHz/$1/psrecord_tile$2.png --interval 1

echo "Logging pipeline status"
cd /arc/projects/CIRADA/polarimetry/software/POSSUMutils/
# treat as a module to make sure imports work
# arguments: tile_number, band ("943MHz" or "1367MHz")
python -m cirada_software.log_processing_status_3Dpipeline $2 943MHz