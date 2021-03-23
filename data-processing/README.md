# Scripts 

Ces scripts permettent de géocoder les données DPE. A terme, ils doivent aussi permettre l'ajout d'informations issues des tables des valeurs et de références

## Pré-requis
* Avoir un dossier `dpe_data` à la racine de ce dossier contenant :
	* Le fichier de la base DPE tertiaire : `td001_dpe_tertiaire.csv`
	* Un sous-dossier `by_dep` contenant les fichiers de la base DPE logements par département, dans des fichiers CSV nommés `dep_XX.csv`
* Pour chaque csv, avoir créé sa base de donnée équivalente .db en SQLite, dans un sous-dossier `cache_geo`, et sous un nom respectant le format : `dpe_data/cache_geo/cache_addok_XX.csv.db`, en remplaçant XX par le numéro de département
* Les CSV d'entrée et de sortie ont tous le caractère `|` comme séparateur (paramètre modifiable dans le fichier `geocode.py`)

## Utilisation

Pour géocoder le fichier DPE tertiaire, se placer à la racine de ce dossier et lancer :

```
python3 geocode.py dpe_data/td001_dpe_tertiaire.csv dpe_data/geo_td001_dpe_tertiaire.csv.gz dpe_data/cache_geo/cache_addok_tertiaire.csv.db
```

Pour géocoder les fichiers DPE logement (par ordre croissant de taille de département), se placer à la racine de ce dossier et lancer le script 3 :

```
./3_geocoding_by_increasing_size.sh
```

## TODO
* Ajouter une étape d'export direct des fichiers logement par département à partir de la base SQL (pour le moment un split de la base complète est fait en amont hors de ces scripts)
* Insérer une étape de jointure avec les tables de valeurs (TV) et de références (TR) pour insérer des informations supplémentaires