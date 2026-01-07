
echo "====== Creating symlinks"


# Move to the POSSUMutils directory
cd /arc/projects/CIRADA/polarimetry/software/POSSUMutils

p1user=$1
echo "Opening SSH tunnel to prefect server host (p1) as $p1user"
# open connection
ssh -fNT -L 4200:localhost:4200 $p1user@206.12.93.32
# set PREFECT URL
set -a
source automation/config.env
set +a

# Create symbolic links from the timeblocked directories in the tiledir
python -m cirada_software.create_symlinks --tiledir /arc/projects/CIRADA/polarimetry/ASKAP/Tiles


echo "====== Fixing duplicate downloads"

# Fix duplicate downloads, downloads are found in the downloaddir, symbolic link logs in the logdir
python -m cirada_software.fix_duplicate_downloads \
  --downloaddir /arc/projects/CIRADA/polarimetry/ASKAP/Tiles/downloads \
  --logdir /arc/projects/CIRADA/polarimetry/ASKAP/Tiles/symlink_logs/

echo "====== Creating symlinks again."
# Create symbolic links again now that duplicates are removed
python -m cirada_software.create_symlinks --tiledir /arc/projects/CIRADA/polarimetry/ASKAP/Tiles


echo "====== Finished."