#!/bin/bash

# Directory containing CSV files
DIR="build2neo"

# Iterate over each CSV file in the directory
for FILE in "$DIR"/*.csv; do
    # Remove single and double quotes and save to a new file
    sed "s/'//g; s/\"//g" "$FILE" > "biocypher_neo4j_volume/${FILE%.csv}.csv"
done
