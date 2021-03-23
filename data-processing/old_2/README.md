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
| 0 | Script permettant d'exécuter tout le process |
| 1 | Création de fichier avec le minimum d'information |
| 2 | Split des fichiers en fichiers de 100 lignes pour faciliter le géocodage |
| 3 | Script de géocodage (powered by addok) |
| 4 | Agrégation des différents fichiers à la maille département (un unique fichier par département) | 
| 5 | Aggrégation en un fichier unique |
