#!/bin/bash

echo "Extract id, concat-adress ongoing..."
python ./1_extract_min_info.py
echo "Split batch 100 lines ongoing..."
./2_csv_split_tiny_batch.sh
echo "geocode ongoing..."
python 3_geocode.py 15
echo "reagregation ongoing..."
python ./4_reagregation.py
