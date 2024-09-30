#!/bin/bash -eu

if [  == 'yes' ]; then
  bash import.sh
  bash create_table.sh &
fi
