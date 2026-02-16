

echo "Preparing test pipeline run"
testdir=/arc/projects/CIRADA/polarimetry/pipeline_runs/943MHz/TEST_11224
echo "Will be run in $testdir"

p1user=$1

# echo "Opening SSH tunnel to prefect server host (p1) as $p1user"
# # open connection
# ssh -fNT -L 4200:localhost:4200 $p1user@206.12.93.32
# # set which port to communicate results to 
# export PREFECT_API_URL="http://localhost:4200/api"

# automatically export all variables for Prefect
set -a
source /arc/projects/CIRADA/polarimetry/software/POSSUMutils/automation/config.env
set +a

echo "Adding RMtools[dev] to pythonpath"
export PYTHONPATH="/arc/projects/CIRADA/polarimetry/software/RMtoolsdev/:$PYTHONPATH"

pipeline=/arc/projects/CIRADA/polarimetry/software/POSSUM_Polarimetry_Pipeline/pipeline/pipeline_prefect.py
# Start actual pipeline with psrecord
psrecord "python $pipeline $testdir/config_11224_zoom_test_all_modules.ini test3d" --include-children --log $testdir/psrecord_test_$p1user.txt --plot $testdir/psrecord_test_$p1user.png --interval 1

# Check database access, will report to dashboard as flow "test_db_access"
cd /arc/projects/CIRADA/polarimetry/software/POSSUMutils
python -m possum_pipeline_control.test_database_access --run_as_flow
