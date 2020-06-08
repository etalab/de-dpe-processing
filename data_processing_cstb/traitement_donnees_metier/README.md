

# Description des tables annexes

## préambule

Les tables annexes générées ici par le CSTB sont à disposition pour aider à analyser la base DPE sans se plonger dans le détail dans chacune des sous tables métier. Ces tables fournissent des indicateurs simples à interpréter sur les
systèmes énergétiques et les caractéristiques de l'enveloppe pour la base DPE.  
Ces tables sont produites à partir des sous tables td007 à td014 qui sont des tables fournies pour tous les DPE utilisant une méthode de calcul 3CL.  
Les informations des tables annexes seront donc systèmatiquement vides pour tout DPE facture (bâtiment avec 1948).

## td001_annexe_generale

contient une seule colonne pour le moment qui est nom_methode_dpe_norm : (nom de méthdoe dpe normalisée)

## td001_annexe_enveloppe_agg

table qui contient les éléments principaux concernant l'enveloppe du bâtiment (murs/baies/plafonds/planchers) issus d'extractions des tables td007_paroi_opaque/td008_baie.
Cette table contient nottament : les matériaux principaux de structure des murs/planchers/plafonds, les coefficient de deperdition moyens, les surfaces de baies par orientation,
les surfaces deperditives des murs/planchers/plafonds, des indicateurs d'isolation des parois, des indicae

## td001_annexe_sys_ch_agg

table qui contient les éléments principaux qui ont pus être extraits de la partie système de chauffage de la base DPE (tables td011_installation_chauffage + td012_generateur_chauffage)
cette table contient le(s) libéllé(s) de(s) générateur(s) , le type d'energie des systèmes , le nomb cette table contient le(s) libéllé(s) de(s) générateur(s) , le type d'energie des systèmes , le nombre de générateur , le classement des générateurs en principal,secondaire,tertiaire en fonction du niveau de besoin associé
re de générateur , le classement des générateurs en principal,secondaire,tertiaire en fonction du niveau de besoin associé.
* NB1 : Si deux systèmes identiques sont déclarés (ex : 2 chaudières gaz standard elles sont regroupées sous un même système)
* NB2 : le système de chauffage "tertiaire" contient tous les systèmes au dela des deux premiers (cas très rare de dépasser 3 types de systèmes différents).

## td001_annexe_sys_ecs_agg

table qui contient les éléments principaux qui ont pus être extraits de la partie système de ecs de la base DPE (tables td013_installation_ecs + td014_generateur_ecs)
cette table contient le(s) libéllé(s) de(s) générateur(s) , le type d'energie des systèmes , le nomb cette table contient le(s) libéllé(s) de(s) générateur(s) , le type d'energie des systèmes , le nombre de générateur , le classement des générateurs en principal,secondaire,tertiaire en fonction du type de systeme
re de générateur , le classement des générateurs en principal,secondaire,tertiaire en fonction du niveau de besoin associé.
* NB1 : Si deux systèmes identiques sont déclarés (ex : 2 chaudières gaz standard elles sont regroupées sous un même système)
* NB2 : le système de ecs "tertiaire" contient tous les systèmes au dela des deux premiers (cas très rare de dépasser 3 types de systèmes différents).

## td007_annexe_paroi_opaque

table qui contient toutes les valeurs issues des tables tv/tr associées à la table td011 + des indicateurs calculés par le cstb dont le matériau de structure et le caractère isolé ou non de chaque paroi(voir doc pour les variables calculées).

## td008_annexe_baie

table qui contient toutes les valeurs issues des tables tv/tr associées à la table td011 + des indicateurs calculés par le cstb dont le type de vitrage, le type de baie et l'orientation de chaque baie (voir doc pour les variables calculées).

## td011_annexe_installation_chauffage

table qui contient toutes les valeurs issues des tables tv/tr associées à la table td011

## td012_annexe_generateur_chauffage

table qui contient toutes les valeurs issues des tables tv/tr associées à la table td012 + des indicateurs table qui contient toutes les valeurs issues des tables tv/tr associées à la table td012 + des indicateurs calculés par le cstb dont principalement un libélé standardisé du type de générateur (voir doc pour les variables calculées).
calculés par le cstb dont principalement un libélé standardisé du type de générateur (voir doc pour les variables calculées).

## td013_annexe_installation_chauffage

table qui contient toutes les valeurs issues des tables tv/tr associées à la table td013

## td014_annexe_generateur_ecs

table qui contient toutes les valeurs des tables tv/tr associées à la table td014 + des indicateurs calculés par le cstb dont principalement un libélé standardisé du type de générateur (voir doc pour les variables calculées).
calculés par le cstb dont principalement un libélé standardisé du type de générateur (voir doc pour les variables calculées).

# conventions traitement CSTB des données de la base DPE

## conventions de nommages :

convention de nommage des variables : snake case

## abreviations :

* _ch : chauffage
* _ecs : eau chaude sanitaire
* _infer : donnnée inférée/déduite de multiples autres informations dans la base de données. ceci n'est pas directement une donnée brute de la base de donnée
* _simp : le champs a été simplifié (reduction du nombre d'énumérés possibles en faisant des regroupements.
* gen_ch : generateurs de chauffage
* gen_ecs : generateurs d'ECS
* sys_ch : concerne le système de chauffage
* sys_ecs : concerne le système d'ECS
* mat_ : matériaux de l'élément considéré
* _calc : recalculé a partir d'autres données numériques présentes dans la base
* _sum : donnée numérique sommée
* _avg : donnée numérique moyennée (souvent pondéré à la surface)
* is_ : prefix d'une colonne qui répond à une question de type booléen (oui/non)
* surf_ : surface
* _top : libéllé d'un énumérateur le plus fréquent pour l'object considéré (souvent pondéré à la surface associé)

## convention de concatenation:

toutes les concaténations de texte sont effectué avec un séparateur + et de deux espaces :  " + "


# champs spéciaux :

* NULL/None etc.  : information non présente
* NONDEF : libéllé spécial pour les données inférées/déduites non défini pour cette ligne (en général abscence de declaration des valeurs tv qui permettent de trouver l'information
* INCOHERENT : libéllé spécial pour les données inférées/déduites quand deux informations contradictoires ne permettent pas de determiner la nature de l'objet.
* non affecte : libéllé spécial pour les données inférées/déduites qui correspond à un echec de l'algorithme qui affecte le libéllé.

