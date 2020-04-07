# Scripts

Ces scripts permettent de prétraiter les données dpe, notamment via du géocodage.

Prérequis : 
- Avoir les données .sql

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
