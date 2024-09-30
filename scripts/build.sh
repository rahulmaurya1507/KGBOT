#!/bin/bash -c
cd /usr/app/
cp -r /src/* .
cp config/biocypher_docker_config.yaml config/biocypher_config.yaml
pip install -r requirements.txt

# python3 scripts/target_disease_script.py
python3 target_disease_script.py
chmod -R 777 biocypher-log
chmod -R 777 data/build2neo/


# Directory containing CSV files
DIR="data/build2neo"

# Iterate over each CSV file in the directory
for FILE in "$DIR"/*.csv; do
    # Remove single and double quotes and save to a temporary file
    TEMP_FILE="${FILE}.tmp"
    sed "s/'//g; s/\"//g" "$FILE" > "$TEMP_FILE"

    # Move the temporary file to overwrite the original file
    mv "$TEMP_FILE" "$FILE"
done
