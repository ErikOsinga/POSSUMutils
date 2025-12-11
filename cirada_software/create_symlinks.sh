


# Move to the POSSUMutils directory
cd /arc/projects/CIRADA/polarimetry/software/POSSUMutils

# Create symbolic links from the timeblocked directories in the tiledir
python -m cirada_software.create_symlinks --tiledir /arc/projects/CIRADA/polarimetry/ASKAP/Tiles

# Fix duplicate downloads, downloads are found in the downloaddir, symbolic link logs in the logdir
python -m cirada_software.fix_duplicate_downloads \
  --downloaddir /arc/projects/CIRADA/polarimetry/ASKAP/Tiles/downloads \
  --logdir /arc/projects/CIRADA/polarimetry/ASKAP/Tiles/symlink_logs/



