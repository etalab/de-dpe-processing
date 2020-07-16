mkdir ../data
# fichiers bruts en .sql
mkdir ../data/0-sql
# tables converties en csv
mkdir ../data/1-csv 
# split csv par département
mkdir ../data/2-dpt 
# aggrégation des csvs pour avoir un csv par département
mkdir ../data-3-dpt-agg
# données intéressantes uniquement
mkdir ../data/4-dpt-agg-ano

mkdir ../data/5-geocodage
# données minimales pour le géocodage
mkdir ../data/5-geocodage/1-mini
# splits des données par batch de 100 lignes pour faciliter le géocodage
mkdir ../data/5-geocodage/2-mini-split
# données géocodés (toujours par batch de 100 lignes)
mkdir ../data/5-geocodage/3-mini-split-geocoded
# données aggrégées par département (id + adresse)
mkdir ../data/5-geocodage/4-mini-geocoded
# données géocodées avec toutes les colonnes
mkdir ../data/5-geocodage/5-geocoded


# KPIs
mkdir ../data/6-kpis

mkdir ../logs
mkdir ../logs/geocodage