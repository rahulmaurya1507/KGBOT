#!/bin/bash

# Source directory containing your folders (replace with your actual source directory)
src_dir="../ot-data"

# Destination directory where you want to copy the files (replace with your actual destination directory)
dest_dir="data/ot_files"

# Create the destination directory if it doesn't exist
mkdir -p "$dest_dir"

# Loop through the directories in the source directory
for subdir in "$src_dir"/association*/; do  # Only process directories that start with "association"
    # Extract the directory name (remove the trailing slash)
    subdir_name=$(basename "$subdir")

    # Create the corresponding subdirectory in the destination directory
    mkdir -p "$dest_dir/$subdir_name"

    # Copy the first two files from the current directory to the new subdirectory
    files=("$subdir"/*)
    cp "${files[@]:0:2}" "$dest_dir/$subdir_name/"
done

echo "Copied 2 files from each 'association*' subdirectory to $dest_dir"
