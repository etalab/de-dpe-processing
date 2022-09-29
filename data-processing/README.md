# Scripts 

Ces scripts permettent de géocoder les données DPE. A terme, ils doivent aussi permettre l'ajout d'informations issues des tables des valeurs et de références

## Pré-requis

* Avoir les dumps DPE Tertiaire et DPE Logement de l'ADEME au format SQL. Disponible ici : https://files.data.gouv.fr/ademe/
* Docker installé
* sqlite3 installé

## Etape 1 : Génération des fichiers CSV

* 1. Création d'un container docker mysql

```
docker run --name my-container-mysql -e MYSQL_ROOT_PASSWORD=my-secret-pw -d mysql:5.5
```

* 2. Copie des dumps dans le container

```
docker cp *.sql my-container-mysql:/
```

* 3. Connection au container en bash

```
docker exec -it my-container-mysql /bin/bash
```


* 4. Elargissement des droits d'accès pour éviter les erreurs avec la base de données 
```
chmod -777 /var/lib
```

* 5. Connection à la base mysql

```
mysql -u root -p
# Entrer votre mot de passe (étape 1)
```

* 6. Création des bases mysql

```
CREATE DATABASE dpe_tertiaire;
CREATE DATABASE dpe_logement;
```

* 7. Import des dumps dans la base mysql

```
mysql -u root -p dpe_tertiaire < dpe_tertiaire_202103.sql
mysql -u root -p dpe_logement < dpe_logement_202103.sql
```

* 8. Opérations sur les bases

```
mysql -u root -p

ALTER DATABASE dpe_logement CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE dpe_logement.td001_dpe CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

ALTER DATABASE dpe_tertiaire CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE dpe_tertiaire.td001_dpe CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

update dpe_tertiaire.td001_dpe set id = replace(convert(id using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set numero_dpe = replace(convert(numero_dpe using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set usr_diagnostiqueur_id = replace(convert(usr_diagnostiqueur_id using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set usr_logiciel_id = replace(convert(usr_logiciel_id using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set tr001_modele_dpe_id = replace(convert(tr001_modele_dpe_id using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set nom_methode_dpe = replace(convert(nom_methode_dpe using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set version_methode_dpe = replace(convert(version_methode_dpe using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set nom_methode_etude_thermique = replace(convert(nom_methode_etude_thermique using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set version_methode_etude_thermique = replace(convert(version_methode_etude_thermique using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set date_visite_diagnostiqueur = replace(convert(date_visite_diagnostiqueur using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set date_etablissement_dpe = replace(convert(date_etablissement_dpe using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set date_arrete_tarifs_energies = replace(convert(date_arrete_tarifs_energies using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set commentaires_ameliorations_recommandations = replace(convert(commentaires_ameliorations_recommandations using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set explication_personnalisee = replace(convert(explication_personnalisee using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set consommation_energie = replace(convert(consommation_energie using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set classe_consommation_energie = replace(convert(classe_consommation_energie using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set estimation_ges = replace(convert(estimation_ges using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set classe_estimation_ges = replace(convert(classe_estimation_ges using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set tr002_type_batiment_id = replace(convert(tr002_type_batiment_id using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set secteur_activite = replace(convert(secteur_activite using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set tr012_categorie_erp_id = replace(convert(tr012_categorie_erp_id using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set tr013_type_erp_id = replace(convert(tr013_type_erp_id using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set annee_construction = replace(convert(annee_construction using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set surface_habitable = replace(convert(surface_habitable using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set surface_thermique_lot = replace(convert(surface_thermique_lot using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set tv016_departement_id = replace(convert(tv016_departement_id using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set commune = replace(convert(commune using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set arrondissement = replace(convert(arrondissement using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set type_voie = replace(convert(type_voie using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set nom_rue = replace(convert(nom_rue using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set numero_rue = replace(convert(numero_rue using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set batiment = replace(convert(batiment using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set escalier = replace(convert(escalier using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set etage = replace(convert(etage using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set porte = replace(convert(porte using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set code_postal = replace(convert(code_postal using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set code_insee_commune = replace(convert(code_insee_commune using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set code_insee_commune_actualise = replace(convert(code_insee_commune_actualise using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set numero_lot = replace(convert(numero_lot using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set quote_part = replace(convert(quote_part using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set nom_centre_commercial = replace(convert(nom_centre_commercial using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set surface_commerciale_contractuelle = replace(convert(surface_commerciale_contractuelle using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set portee_dpe_batiment = replace(convert(portee_dpe_batiment using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set partie_batiment = replace(convert(partie_batiment using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set shon = replace(convert(shon using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set surface_utile = replace(convert(surface_utile using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set surface_thermique_parties_communes = replace(convert(surface_thermique_parties_communes using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set en_souterrain = replace(convert(en_souterrain using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set en_surface = replace(convert(en_surface using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set nombre_niveaux = replace(convert(nombre_niveaux using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set nombre_circulations_verticales = replace(convert(nombre_circulations_verticales using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set nombre_boutiques = replace(convert(nombre_boutiques using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set presence_verriere = replace(convert(presence_verriere using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set surface_verriere = replace(convert(surface_verriere using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set type_vitrage_verriere = replace(convert(type_vitrage_verriere using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set nombre_entrees_avec_sas = replace(convert(nombre_entrees_avec_sas using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set nombre_entrees_sans_sas = replace(convert(nombre_entrees_sans_sas using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set surface_baies_orientees_nord = replace(convert(surface_baies_orientees_nord using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set surface_baies_orientees_est_ouest = replace(convert(surface_baies_orientees_est_ouest using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set surface_baies_orientees_sud = replace(convert(surface_baies_orientees_sud using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set surface_planchers_hauts_deperditifs = replace(convert(surface_planchers_hauts_deperditifs using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set surface_planchers_bas_deperditifs = replace(convert(surface_planchers_bas_deperditifs using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set surface_parois_verticales_opaques_deperditives = replace(convert(surface_parois_verticales_opaques_deperditives using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set etat_avancement = replace(convert(etat_avancement using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set organisme_certificateur = replace(convert(organisme_certificateur using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set adresse_organisme_certificateur = replace(convert(adresse_organisme_certificateur using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set dpe_vierge = replace(convert(dpe_vierge using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set est_efface = replace(convert(est_efface using utf8) ,'\r','');
update dpe_tertiaire.td001_dpe set date_reception_dpe = replace(convert(date_reception_dpe using utf8) ,'\r','');


update dpe_logement.td001_dpe set id = replace(convert(id using utf8) ,'\r','');
update dpe_logement.td001_dpe set numero_dpe = replace(convert(numero_dpe using utf8) ,'\r','');
update dpe_logement.td001_dpe set usr_diagnostiqueur_id = replace(convert(usr_diagnostiqueur_id using utf8) ,'\r','');
update dpe_logement.td001_dpe set usr_logiciel_id = replace(convert(usr_logiciel_id using utf8) ,'\r','');
update dpe_logement.td001_dpe set tr001_modele_dpe_id = replace(convert(tr001_modele_dpe_id using utf8) ,'\r','');
update dpe_logement.td001_dpe set nom_methode_dpe = replace(convert(nom_methode_dpe using utf8) ,'\r','');
update dpe_logement.td001_dpe set version_methode_dpe = replace(convert(version_methode_dpe using utf8) ,'\r','');
update dpe_logement.td001_dpe set nom_methode_etude_thermique = replace(convert(nom_methode_etude_thermique using utf8) ,'\r','');
update dpe_logement.td001_dpe set version_methode_etude_thermique = replace(convert(version_methode_etude_thermique using utf8) ,'\r','');
update dpe_logement.td001_dpe set date_visite_diagnostiqueur = replace(convert(date_visite_diagnostiqueur using utf8) ,'\r','');
update dpe_logement.td001_dpe set date_etablissement_dpe = replace(convert(date_etablissement_dpe using utf8) ,'\r','');
update dpe_logement.td001_dpe set date_arrete_tarifs_energies = replace(convert(date_arrete_tarifs_energies using utf8) ,'\r','');
update dpe_logement.td001_dpe set commentaires_ameliorations_recommandations = replace(convert(commentaires_ameliorations_recommandations using utf8) ,'\r','');
update dpe_logement.td001_dpe set explication_personnalisee = replace(convert(explication_personnalisee using utf8) ,'\r','');
update dpe_logement.td001_dpe set consommation_energie = replace(convert(consommation_energie using utf8) ,'\r','');
update dpe_logement.td001_dpe set classe_consommation_energie = replace(convert(classe_consommation_energie using utf8) ,'\r','');
update dpe_logement.td001_dpe set estimation_ges = replace(convert(estimation_ges using utf8) ,'\r','');
update dpe_logement.td001_dpe set classe_estimation_ges = replace(convert(classe_estimation_ges using utf8) ,'\r','');
update dpe_logement.td001_dpe set tr002_type_batiment_id = replace(convert(tr002_type_batiment_id using utf8) ,'\r','');
update dpe_logement.td001_dpe set secteur_activite = replace(convert(secteur_activite using utf8) ,'\r','');
update dpe_logement.td001_dpe set tr012_categorie_erp_id = replace(convert(tr012_categorie_erp_id using utf8) ,'\r','');
update dpe_logement.td001_dpe set tr013_type_erp_id = replace(convert(tr013_type_erp_id using utf8) ,'\r','');
update dpe_logement.td001_dpe set annee_construction = replace(convert(annee_construction using utf8) ,'\r','');
update dpe_logement.td001_dpe set surface_habitable = replace(convert(surface_habitable using utf8) ,'\r','');
update dpe_logement.td001_dpe set surface_thermique_lot = replace(convert(surface_thermique_lot using utf8) ,'\r','');
update dpe_logement.td001_dpe set tv016_departement_id = replace(convert(tv016_departement_id using utf8) ,'\r','');
update dpe_logement.td001_dpe set commune = replace(convert(commune using utf8) ,'\r','');
update dpe_logement.td001_dpe set arrondissement = replace(convert(arrondissement using utf8) ,'\r','');
update dpe_logement.td001_dpe set type_voie = replace(convert(type_voie using utf8) ,'\r','');
update dpe_logement.td001_dpe set nom_rue = replace(convert(nom_rue using utf8) ,'\r','');
update dpe_logement.td001_dpe set numero_rue = replace(convert(numero_rue using utf8) ,'\r','');
update dpe_logement.td001_dpe set batiment = replace(convert(batiment using utf8) ,'\r','');
update dpe_logement.td001_dpe set escalier = replace(convert(escalier using utf8) ,'\r','');
update dpe_logement.td001_dpe set etage = replace(convert(etage using utf8) ,'\r','');
update dpe_logement.td001_dpe set porte = replace(convert(porte using utf8) ,'\r','');
update dpe_logement.td001_dpe set code_postal = replace(convert(code_postal using utf8) ,'\r','');
update dpe_logement.td001_dpe set code_insee_commune = replace(convert(code_insee_commune using utf8) ,'\r','');
update dpe_logement.td001_dpe set code_insee_commune_actualise = replace(convert(code_insee_commune_actualise using utf8) ,'\r','');
update dpe_logement.td001_dpe set numero_lot = replace(convert(numero_lot using utf8) ,'\r','');
update dpe_logement.td001_dpe set quote_part = replace(convert(quote_part using utf8) ,'\r','');
update dpe_logement.td001_dpe set nom_centre_commercial = replace(convert(nom_centre_commercial using utf8) ,'\r','');
update dpe_logement.td001_dpe set surface_commerciale_contractuelle = replace(convert(surface_commerciale_contractuelle using utf8) ,'\r','');
update dpe_logement.td001_dpe set portee_dpe_batiment = replace(convert(portee_dpe_batiment using utf8) ,'\r','');
update dpe_logement.td001_dpe set partie_batiment = replace(convert(partie_batiment using utf8) ,'\r','');
update dpe_logement.td001_dpe set shon = replace(convert(shon using utf8) ,'\r','');
update dpe_logement.td001_dpe set surface_utile = replace(convert(surface_utile using utf8) ,'\r','');
update dpe_logement.td001_dpe set surface_thermique_parties_communes = replace(convert(surface_thermique_parties_communes using utf8) ,'\r','');
update dpe_logement.td001_dpe set en_souterrain = replace(convert(en_souterrain using utf8) ,'\r','');
update dpe_logement.td001_dpe set en_surface = replace(convert(en_surface using utf8) ,'\r','');
update dpe_logement.td001_dpe set nombre_niveaux = replace(convert(nombre_niveaux using utf8) ,'\r','');
update dpe_logement.td001_dpe set nombre_circulations_verticales = replace(convert(nombre_circulations_verticales using utf8) ,'\r','');
update dpe_logement.td001_dpe set nombre_boutiques = replace(convert(nombre_boutiques using utf8) ,'\r','');
update dpe_logement.td001_dpe set presence_verriere = replace(convert(presence_verriere using utf8) ,'\r','');
update dpe_logement.td001_dpe set surface_verriere = replace(convert(surface_verriere using utf8) ,'\r','');
update dpe_logement.td001_dpe set type_vitrage_verriere = replace(convert(type_vitrage_verriere using utf8) ,'\r','');
update dpe_logement.td001_dpe set nombre_entrees_avec_sas = replace(convert(nombre_entrees_avec_sas using utf8) ,'\r','');
update dpe_logement.td001_dpe set nombre_entrees_sans_sas = replace(convert(nombre_entrees_sans_sas using utf8) ,'\r','');
update dpe_logement.td001_dpe set surface_baies_orientees_nord = replace(convert(surface_baies_orientees_nord using utf8) ,'\r','');
update dpe_logement.td001_dpe set surface_baies_orientees_est_ouest = replace(convert(surface_baies_orientees_est_ouest using utf8) ,'\r','');
update dpe_logement.td001_dpe set surface_baies_orientees_sud = replace(convert(surface_baies_orientees_sud using utf8) ,'\r','');
update dpe_logement.td001_dpe set surface_planchers_hauts_deperditifs = replace(convert(surface_planchers_hauts_deperditifs using utf8) ,'\r','');
update dpe_logement.td001_dpe set surface_planchers_bas_deperditifs = replace(convert(surface_planchers_bas_deperditifs using utf8) ,'\r','');
update dpe_logement.td001_dpe set surface_parois_verticales_opaques_deperditives = replace(convert(surface_parois_verticales_opaques_deperditives using utf8) ,'\r','');
update dpe_logement.td001_dpe set etat_avancement = replace(convert(etat_avancement using utf8) ,'\r','');
update dpe_logement.td001_dpe set organisme_certificateur = replace(convert(organisme_certificateur using utf8) ,'\r','');
update dpe_logement.td001_dpe set adresse_organisme_certificateur = replace(convert(adresse_organisme_certificateur using utf8) ,'\r','');
update dpe_logement.td001_dpe set dpe_vierge = replace(convert(dpe_vierge using utf8) ,'\r','');
update dpe_logement.td001_dpe set est_efface = replace(convert(est_efface using utf8) ,'\r','');
update dpe_logement.td001_dpe set date_reception_dpe = replace(convert(date_reception_dpe using utf8) ,'\r','');
```

* 9. Export csv

```
SELECT 
'id',
'numero_dpe',
'usr_diagnostiqueur_id',
'usr_logiciel_id',
'tr001_modele_dpe_id',
'nom_methode_dpe',
'version_methode_dpe',
'nom_methode_etude_thermique',
'version_methode_etude_thermique',
'date_visite_diagnostiqueur',
'date_etablissement_dpe',
'date_arrete_tarifs_energies',
'commentaires_ameliorations_recommandations',
'explication_personnalisee',
'consommation_energie',
'classe_consommation_energie',
'estimation_ges',
'classe_estimation_ges',
'tr002_type_batiment_id',
'secteur_activite',
'tr012_categorie_erp_id',
'tr013_type_erp_id',
'annee_construction',
'surface_habitable',
'surface_thermique_lot',
'tv016_departement_id',
'commune',
'arrondissement',
'type_voie',
'nom_rue',
'numero_rue',
'batiment',
'escalier',
'etage',
'porte',
'code_postal',
'code_insee_commune',
'code_insee_commune_actualise',
'numero_lot',
'quote_part',
'nom_centre_commercial',
'surface_commerciale_contractuelle',
'portee_dpe_batiment',
'partie_batiment',
'shon',
'surface_utile',
'surface_thermique_parties_communes',
'en_souterrain',
'en_surface',
'nombre_niveaux',
'nombre_circulations_verticales',
'nombre_boutiques',
'presence_verriere',
'surface_verriere',
'type_vitrage_verriere',
'nombre_entrees_avec_sas',
'nombre_entrees_sans_sas',
'surface_baies_orientees_nord',
'surface_baies_orientees_est_ouest',
'surface_baies_orientees_sud',
'surface_planchers_hauts_deperditifs',
'surface_planchers_bas_deperditifs',
'surface_parois_verticales_opaques_deperditives',
'etat_avancement',
'organisme_certificateur',
'adresse_organisme_certificateur',
'dpe_vierge',
'est_efface',
'date_reception_dpe'
UNION ALL
SELECT
REPLACE(REPLACE(REPLACE(REPLACE(id,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(numero_dpe,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(usr_diagnostiqueur_id,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(usr_logiciel_id,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(tr001_modele_dpe_id,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(nom_methode_dpe,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(version_methode_dpe,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(nom_methode_etude_thermique,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(version_methode_etude_thermique,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(date_visite_diagnostiqueur,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(date_etablissement_dpe,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(date_arrete_tarifs_energies,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(commentaires_ameliorations_recommandations,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(explication_personnalisee,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(consommation_energie,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(classe_consommation_energie,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(estimation_ges,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(classe_estimation_ges,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(tr002_type_batiment_id,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(secteur_activite,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(tr012_categorie_erp_id,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(tr013_type_erp_id,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(annee_construction,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_habitable,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_thermique_lot,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(tv016_departement_id,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(commune,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(arrondissement,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(type_voie,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(nom_rue,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(numero_rue,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(batiment,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(escalier,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(etage,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(porte,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(code_postal,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(code_insee_commune,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(code_insee_commune_actualise,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(numero_lot,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(quote_part,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(nom_centre_commercial,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_commerciale_contractuelle,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(portee_dpe_batiment,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(partie_batiment,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(shon,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_utile,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_thermique_parties_communes,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(en_souterrain,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(en_surface,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(nombre_niveaux,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(nombre_circulations_verticales,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(nombre_boutiques,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(presence_verriere,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_verriere,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(type_vitrage_verriere,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(nombre_entrees_avec_sas,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(nombre_entrees_sans_sas,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_baies_orientees_nord,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_baies_orientees_est_ouest,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_baies_orientees_sud,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_planchers_hauts_deperditifs,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_planchers_bas_deperditifs,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_parois_verticales_opaques_deperditives,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(etat_avancement,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(organisme_certificateur,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(adresse_organisme_certificateur,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(dpe_vierge,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(est_efface,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(date_reception_dpe,'\n',' '), '\\N', ' '),'|',' '), '"',' ')
FROM dpe_logement.td001_dpe
INTO OUTFILE '/var/lib/mysql-files/td001_dpe_logement.csv'
FIELDS ESCAPED BY '\\' TERMINATED BY '|' ENCLOSED BY '"' LINES TERMINATED BY '\n';


SELECT 
'id',
'numero_dpe',
'usr_diagnostiqueur_id',
'usr_logiciel_id',
'tr001_modele_dpe_id',
'nom_methode_dpe',
'version_methode_dpe',
'nom_methode_etude_thermique',
'version_methode_etude_thermique',
'date_visite_diagnostiqueur',
'date_etablissement_dpe',
'date_arrete_tarifs_energies',
'commentaires_ameliorations_recommandations',
'explication_personnalisee',
'consommation_energie',
'classe_consommation_energie',
'estimation_ges',
'classe_estimation_ges',
'tr002_type_batiment_id',
'secteur_activite',
'tr012_categorie_erp_id',
'tr013_type_erp_id',
'annee_construction',
'surface_habitable',
'surface_thermique_lot',
'tv016_departement_id',
'commune',
'arrondissement',
'type_voie',
'nom_rue',
'numero_rue',
'batiment',
'escalier',
'etage',
'porte',
'code_postal',
'code_insee_commune',
'code_insee_commune_actualise',
'numero_lot',
'quote_part',
'nom_centre_commercial',
'surface_commerciale_contractuelle',
'portee_dpe_batiment',
'partie_batiment',
'shon',
'surface_utile',
'surface_thermique_parties_communes',
'en_souterrain',
'en_surface',
'nombre_niveaux',
'nombre_circulations_verticales',
'nombre_boutiques',
'presence_verriere',
'surface_verriere',
'type_vitrage_verriere',
'nombre_entrees_avec_sas',
'nombre_entrees_sans_sas',
'surface_baies_orientees_nord',
'surface_baies_orientees_est_ouest',
'surface_baies_orientees_sud',
'surface_planchers_hauts_deperditifs',
'surface_planchers_bas_deperditifs',
'surface_parois_verticales_opaques_deperditives',
'etat_avancement',
'organisme_certificateur',
'adresse_organisme_certificateur',
'dpe_vierge',
'est_efface',
'date_reception_dpe'
UNION ALL
SELECT
REPLACE(REPLACE(REPLACE(REPLACE(id,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(numero_dpe,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(usr_diagnostiqueur_id,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(usr_logiciel_id,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(tr001_modele_dpe_id,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(nom_methode_dpe,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(version_methode_dpe,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(nom_methode_etude_thermique,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(version_methode_etude_thermique,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(date_visite_diagnostiqueur,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(date_etablissement_dpe,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(date_arrete_tarifs_energies,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(commentaires_ameliorations_recommandations,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(explication_personnalisee,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(consommation_energie,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(classe_consommation_energie,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(estimation_ges,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(classe_estimation_ges,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(tr002_type_batiment_id,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(secteur_activite,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(tr012_categorie_erp_id,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(tr013_type_erp_id,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(annee_construction,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_habitable,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_thermique_lot,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(tv016_departement_id,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(commune,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(arrondissement,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(type_voie,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(nom_rue,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(numero_rue,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(batiment,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(escalier,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(etage,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(porte,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(code_postal,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(code_insee_commune,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(code_insee_commune_actualise,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(numero_lot,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(quote_part,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(nom_centre_commercial,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_commerciale_contractuelle,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(portee_dpe_batiment,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(partie_batiment,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(shon,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_utile,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_thermique_parties_communes,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(en_souterrain,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(en_surface,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(nombre_niveaux,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(nombre_circulations_verticales,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(nombre_boutiques,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(presence_verriere,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_verriere,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(type_vitrage_verriere,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(nombre_entrees_avec_sas,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(nombre_entrees_sans_sas,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_baies_orientees_nord,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_baies_orientees_est_ouest,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_baies_orientees_sud,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_planchers_hauts_deperditifs,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_planchers_bas_deperditifs,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(surface_parois_verticales_opaques_deperditives,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(etat_avancement,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(organisme_certificateur,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(adresse_organisme_certificateur,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(dpe_vierge,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(est_efface,'\n',' '), '\\N', ' '),'|',' '), '"',' '),
REPLACE(REPLACE(REPLACE(REPLACE(date_reception_dpe,'\n',' '), '\\N', ' '),'|',' '), '"',' ')
FROM dpe_tertiaire.td001_dpe
INTO OUTFILE '/var/lib/mysql-files/td001_dpe_tertiaire.csv'
FIELDS ESCAPED BY '\\' TERMINATED BY '|' ENCLOSED BY '"' LINES TERMINATED BY '\n';
```

* 10. Récupération des dumps csv

```
exit
docker cp my-container-mysql:/var/lib/mysql-files/td001_dpe_tertiaire.csv .
docker cp my-container-mysql:/var/lib/mysql-files/td001_dpe_logement.csv .
```

* Les CSV d'entrée et de sortie ont tous le caractère `|` comme séparateur (paramètre modifiable dans le fichier `geocode.py`)

## Etape 2 : Prétraitements sur les données

Prérequis :
* Avoir un dossier `dpe_data` à la racine de ce dossier contenant :
	* Le fichier de la base DPE tertiaire : `td001_dpe_tertiaire.csv` et de la base DPE logement `td001_dpe_logement.csv`
	* Un sous-dossier `by_dep`

Appliquer les opérations python suivantes sur les fichiers csv 

```
import csv
import os
import pandas as pd

df = pd.read_csv('td001_dpe_logement.csv', lineterminator='\n', dtype=str,sep="|", encoding="iso8859-1")
# Remove some \N in data
for d in df.columns:
    df[d] = df[d].apply(lambda x: str(x).replace("\\N","").replace("\\R","").replace("\\n","").replace("\\r","") if x == x else x)

# Split one file per department
deps = df.tv016_departement_id.unique()
for d in deps: 
    df[df['tv016_departement_id'] == d].to_csv('by_dep/dep_'+d+'.csv',sep="|",index=False, encoding='iso8859-1', quoting=csv.QUOTE_NONNUMERIC)

for dep_filename in os.listdir('by_dep'):
    os.rename(f'by_dep/{dep_filename}', f'by_dep/{dep_filename.replace("_0", "_")}')
os.rename('by_dep/dep_96.csv', 'by_dep/dep_2A.csv')
os.rename('by_dep/dep_97.csv', 'by_dep/dep_2B.csv')
```

Le sous-dossier by_dep contient désormais les fichiers de la base DPE logements par département, dans des fichiers CSV nommés `dep_XX.csv`


## Etape 3 : Préparation au géocodage


* Pour chaque csv, avoir créé sa base de donnée équivalente .db en SQLite, dans un sous-dossier `cache_geo`, et sous un nom respectant le format : `dpe_data/cache_geo/cache_addok_XX.csv.db`, en remplaçant XX par le numéro de département

```
# shell
sqlite3
# sqlite3
.open test.csv.db
CREATE TABLE cache_addok (adr text, geo text,
                     score numeric);
CREATE INDEX cache_addok_adr ON cache_addok (adr);
.exit
# shell
for i in {1..95}; do cp test.csv.db cache_addok_$i.csv.cb; done
mv test.csv.db cache_addok_2A.csv.db
mv cache_addok_20.csv.db cache_addok2B.csv.db
```

## Etape 4 : Géocodage

Pour géocoder le fichier DPE tertiaire, se placer à la racine de ce dossier et lancer :

```
python3 geocode.py dpe_data/td001_dpe_tertiaire.csv dpe_data/geo_td001_dpe_tertiaire.csv.gz dpe_data/cache_geo/cache_addok_tertiaire.csv.db
```

Pour géocoder les fichiers DPE logement (par ordre croissant de taille de département), se placer à la racine de ce dossier et lancer le script 3 :

```
./3_geocoding_by_increasing_size.sh
```

## Etape 5 : Posttraitements

* Jointure des fichiers de sortie avec les tables tv/tr

Exécuter le script python : 

```
import pandas as pd
import glob
files = glob.glob("de-geocodage-dpe/results/*.csv.gz")

tr001 = pd.read_csv("tables-references/tr001_modele_dpe.csv",dtype=str)
tr001t = pd.read_csv("tables-references/tr001_modele_dpe_type.csv",dtype=str)
tr002 = pd.read_csv("tables-references/tr002_type_batiment.csv",dtype=str)
tr012 = pd.read_csv("tables-references/tr012_categorie_erp.csv",dtype=str)
tr013 = pd.read_csv("tables-references/tr013_type_erp.csv",dtype=str)
tr013t = pd.read_csv("tables-references/tr013_type_erp_categorie.csv",dtype=str)
tr014 = pd.read_csv("tables-references/tr014_type_parois_opaque.csv",dtype=str)

tv016 = pd.read_csv("tables-valeurs/tv016_departement.csv",dtype=str)
tv017 = pd.read_csv("tables-valeurs/tv017_zone_hiver.csv",dtype=str)
tv018 = pd.read_csv("tables-valeurs/tv018_zone_ete.csv",dtype=str)


tr001t = tr001t.rename(columns={
    'id':'tr001_modele_dpe_type_id',
    'type':'tr001_modele_dpe_type',
    'libelle':'tr001_modele_dpe_type_libelle',
    'ordre':'tr001_modele_dpe_type_ordre'
})
tr001 = pd.merge(tr001,tr001t,on='tr001_modele_dpe_type_id',how='left')
tr001 = tr001.rename(columns={
    'id':'tr001_modele_dpe_id',
    'code':'tr001_modele_dpe_code',
    'modele':'tr001_modele_dpe_modele',
    'description':'tr001_modele_dpe_description',
    'fichier_vierge':'tr001_modele_dpe_fichier_vierge',
    'est_efface':'tr001_modele_dpe_est_efface'
})
tr002 = tr002.rename(columns={
    'id':'tr002_type_batiment_id',
    'code':'tr002_type_batiment_code',
    'description':'tr002_type_batiment_description',
    'libelle':'tr002_type_batiment_libelle',
    'est_efface':'tr002_type_batiment_est_efface',
    'ordre':'tr002_type_batiment_ordre',
    'simulateur':'tr002_type_batiment_simulateur'
})
tr012 = tr012.rename(columns={
    'id':'tr012_categorie_erp_id',
    'code':'tr012_categorie_erp_code',
    'categorie':'tr012_categorie_erp_categorie',
    'groupe':'tr012_categorie_erp_groupe',
    'est_efface':'tr012_categorie_erp_est_efface'
})

tr013t = tr013t.rename(columns={
    'id':'tr013_type_erp_categorie_id',
    'categorie':'tr013_type_erp_categorie'
})
tr013 = pd.merge(tr013,tr013t,on='tr013_type_erp_categorie_id',how='left')
tr013 = tr013.rename(columns={
    'id':'tr013_type_erp_id',
    'code':'tr013_type_erp_code',
    'type':'tr013_type_erp_type',
    'est_efface':'tr013_type_erp_est_efface',
})

tv017 = tv017.rename(columns={
    'id':'tv017_zone_hiver_id',
    'code':'tv017_zone_hiver_code',
    't_ext_moyen':'tv017_zone_hiver_t_ext_moyen',
    'peta_cw':'tv017_zone_hiver_peta_cw',
    'dh14':'tv017_zone_hiver_dh14',
    'prs1':'tv017_zone_hiver_prs1'
})
tv016 = pd.merge(tv016,tv017,on='tv017_zone_hiver_id',how='left')
tv018 = tv018.rename(columns={
    'id':'tv018_zone_ete_id',
    'code':'tv018_zone_ete_code',
    'sclim_inf_150':'tv018_zone_ete_sclim_inf_150',
    'sclim_sup_150':'tv018_zone_ete_sclim_sup_150',
    'rclim_autres_etages':'tv018_zone_ete_rclim_autres_etages',
    'rclim_dernier_etage':'tv018_zone_ete_rclim_dernier_etage'
})
tv016 = pd.merge(tv016,tv018,on='tv018_zone_ete_id',how='left')
tv016 = tv016.rename(columns={
    'id':'tv016_departement_id',
    'code':'tv016_departement_code', 
    'departement':'tv016_departement_departement', 
    'altmin':'tv016_departement_altmin', 
    'altmax':'tv016_departement_altmax', 
    'nref':'tv016_departement_nref', 
    'dhref':'tv016_departement_dhref', 
    'pref':'tv016_departement_pref', 
    'c2':'tv016_departement_c2', 
    'c3':'tv016_departement_c3', 
    'c4':'tv016_departement_c4',
    't_ext_basse':'tv016_departement_t_ext_basse', 
    'e':'tv016_departement_e', 
    'fch':'tv016_departement_fch', 
    'fecs_ancienne_m_i':'tv016_departement_fecs_ancienne_m_i', 
    'fecs_recente_m_i':'tv016_departement_fecs_recente_m_i',
    'fecs_solaire_m_i':'tv016_departement_fecs_solaire_m_i', 
    'fecs_ancienne_i_c':'tv016_departement_fecs_ancienne_i_c', 
    'fecs_recente_i_c':'tv016_departement_fecs_recente_i_c'
})

for f in files:
    print(f)
    df = pd.read_csv(f,dtype=str,sep='|',compression='gzip')
    df = pd.merge(df,tr001,on='tr001_modele_dpe_id',how='left')
    df = pd.merge(df,tr002,on='tr002_type_batiment_id',how='left')
    df = pd.merge(df,tr012,on='tr012_categorie_erp_id',how='left')
    df = pd.merge(df,tr013,on='tr013_type_erp_id',how='left')
    df = pd.merge(df,tv016,on='tv016_departement_id',how='left')
    df = df[['id',
     'numero_dpe',
     'usr_diagnostiqueur_id',
     'usr_logiciel_id',
     'tr001_modele_dpe_id',
     'nom_methode_dpe',
     'version_methode_dpe',
     'nom_methode_etude_thermique',
     'version_methode_etude_thermique',
     'date_visite_diagnostiqueur',
     'date_etablissement_dpe',
     'date_arrete_tarifs_energies',
     'commentaires_ameliorations_recommandations',
     'explication_personnalisee',
     'consommation_energie',
     'classe_consommation_energie',
     'estimation_ges',
     'classe_estimation_ges',
     'tr002_type_batiment_id',
     'secteur_activite',
     'tr012_categorie_erp_id',
     'tr013_type_erp_id',
     'annee_construction',
     'surface_habitable',
     'surface_thermique_lot',
     'tv016_departement_id',
     'commune',
     'arrondissement',
     'type_voie',
     'nom_rue',
     'numero_rue',
     'batiment',
     'escalier',
     'etage',
     'porte',
     'code_postal',
     'code_insee_commune',
     'code_insee_commune_actualise',
     'numero_lot',
     'quote_part',
     'nom_centre_commercial',
     'surface_commerciale_contractuelle',
     'portee_dpe_batiment',
     'partie_batiment',
     'shon',
     'surface_utile',
     'surface_thermique_parties_communes',
     'en_souterrain',
     'en_surface',
     'nombre_niveaux',
     'nombre_circulations_verticales',
     'nombre_boutiques',
     'presence_verriere',
     'surface_verriere',
     'type_vitrage_verriere',
     'nombre_entrees_avec_sas',
     'nombre_entrees_sans_sas',
     'surface_baies_orientees_nord',
     'surface_baies_orientees_est_ouest',
     'surface_baies_orientees_sud',
     'surface_planchers_hauts_deperditifs',
     'surface_planchers_bas_deperditifs',
     'surface_parois_verticales_opaques_deperditives',
     'etat_avancement',
     'organisme_certificateur',
     'adresse_organisme_certificateur',
     'dpe_vierge',
     'est_efface',
     'date_reception_dpe',
     'longitude',
     'latitude',
     'geo_score',
     'geo_type',
     'geo_adresse',
     'geo_id',
     'geo_l4',
     'geo_l5',
     'tr001_modele_dpe_code',
     'tr001_modele_dpe_type_id',
     'tr001_modele_dpe_modele',
     'tr001_modele_dpe_description',
     'tr001_modele_dpe_fichier_vierge',
     'tr001_modele_dpe_est_efface',
     'tr001_modele_dpe_type',
     'tr001_modele_dpe_type_libelle',
     'tr001_modele_dpe_type_ordre',
     'tr002_type_batiment_code',
     'tr002_type_batiment_description',
     'tr002_type_batiment_libelle',
     'tr002_type_batiment_est_efface',
     'tr002_type_batiment_ordre',
     'tr002_type_batiment_simulateur',
     'tr012_categorie_erp_code',
     'tr012_categorie_erp_categorie',
     'tr012_categorie_erp_groupe',
     'tr012_categorie_erp_est_efface',
     'tr013_type_erp_code',
     'tr013_type_erp_type',
     'tr013_type_erp_categorie_id',
     'tr013_type_erp_est_efface',
     'tr013_type_erp_categorie',
     'tv016_departement_code',
     'tv016_departement_departement',
     'tv017_zone_hiver_id',
     'tv018_zone_ete_id',
     'tv016_departement_altmin',
     'tv016_departement_altmax',
     'tv016_departement_nref',
     'tv016_departement_dhref',
     'tv016_departement_pref',
     'tv016_departement_c2',
     'tv016_departement_c3',
     'tv016_departement_c4',
     'tv016_departement_t_ext_basse',
     'tv016_departement_e',
     'tv016_departement_fch',
     'tv016_departement_fecs_ancienne_m_i',
     'tv016_departement_fecs_recente_m_i',
     'tv016_departement_fecs_solaire_m_i',
     'tv016_departement_fecs_ancienne_i_c',
     'tv016_departement_fecs_recente_i_c',
     'tv017_zone_hiver_code',
     'tv017_zone_hiver_t_ext_moyen',
     'tv017_zone_hiver_peta_cw',
     'tv017_zone_hiver_dh14',
     'tv017_zone_hiver_prs1',
     'tv018_zone_ete_code',
     'tv018_zone_ete_sclim_inf_150',
     'tv018_zone_ete_sclim_sup_150',
     'tv018_zone_ete_rclim_autres_etages',
     'tv018_zone_ete_rclim_dernier_etage']]
    df.to_csv(f,compression='gzip',index=False,sep=",")

```
