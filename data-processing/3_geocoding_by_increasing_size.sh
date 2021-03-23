#! /bin/bash
echo "Geocoding by increasing size of departments"
#cd /srv/sirene/geocodage-sirene/
# extrait la liste des anciens codes INSEE et nouveau correspondant
csvgrep -c POLE -r '^.+$' -t France2018.tsv -e iso8859-1 | csvcut -c 6,7,11,14 | sed 's/,//' > histo_depcom.csv
time wc -l dpe_data/by_dep/dep_*.csv | sort -n -r | grep dep | sed 's/^.*_\(.*\).csv/\1/' | \
  parallel -j 36 -t python3 geocode.py dpe_data/by_dep/dep_{}.csv dpe_data/by_dep/geo_dep_{}.csv.gz dpe_data/cache_geo/cache_addok_{}.csv.db \> dpe_data/by_dep/geo_dep_{}.log
echo "Geocode OK!"
