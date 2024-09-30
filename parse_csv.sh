#!/bin/bash

# Directory containing CSV files
DIR="build2neo"

# Iterate over each CSV file in the directory
for FILE in "$DIR"/*.csv; do
    # Remove single and double quotes and save to a temporary file
    TEMP_FILE="${FILE}.tmp"
    sed "s/'//g; s/\"//g" "$FILE" > "$TEMP_FILE"

    # Move the temporary file to overwrite the original file
    mv "$TEMP_FILE" "$FILE"
done
