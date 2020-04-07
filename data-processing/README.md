# Scripts

Ces scripts permettent de prétraiter les données dpe, notamment via du géocodage.

Prérequis : 
- Avoir les données .sql
- Installer en amont des instances permettant de faire du géocodage. Vous pouvez monter vos instances via le repo suivant : 
	- https://github.com/geoffreyaldebert/geocodage_ban
	- A titre de comparaison, le géocodage de la table td001 (~9 millions de lignes) a pris 5h30 sur une machine avec 8coeurs /16Go de RAM et 4 instances addok + 4 instances addok-redis

Description des différentes étapes déroulées par les scripts :

| Etape | Explication |
|---|---|
| 0 | Préparation des dossiers |
| 1 | Récupération des .sql (ne fonctionne pas) |
| 2 | Extraire les dumps sql en fichiers csv |
| 3 | Split des fichiers csv en fichiers par département |
| 4 | Agrégation des différents fichiers à la maille département (un unique fichier par département) | 
| 5 | Suppression des champs non pertinents |
| 6 | Géocodage en plusieurs étapes |
| 7 | Calculs de KPIs à la maille départementale |
