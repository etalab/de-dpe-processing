# -*- coding: utf-8 -*-
from .td011_td012_processing import gen_ch_normalized_lib_matching_dict, gen_ch_lib_simp_dict
from .td013_td014_processing import gen_ecs_normalized_lib_matching_dict, gen_ecs_lib_simp_dict
from .utils import unique_ordered

td001_annexe_generale_desc = {
    'nom_methode_dpe_norm': 'nom méthode dpe normalisé et simplifié (voir enum : nom_methode_dpe_norm)'}

td001_annexe_enveloppe_agg_desc = {
    "td001_dpe_id": "identifiant de la table td001_dpe_id",
    'Umurs_ext_avg': "coefficient de déperdition des murs donnant sur l'extérieur moyen (pondéré à la surface)",
    'Umurs_deper_avg': "coefficient de déperdition des murs donnant sur l'extérieur ou un local non chauffé moyen (pondéré à la surface)",
    'Uplancher_bas_deper_avg': "coefficient de déperdition des planchers bas ne donnant pas sur un logement mitoyen moyen (pondéré à la surface)",
    'Uplancher_haut_deper_avg': "coefficient de déperdition des planchers hauts ne donnant pas sur un logement mitoyen moyen (pondéré à la surface)",
    # 'is_plancher_bas_deper_isole': "texte qui renseigne sur le status isolé ou non du plancher bas (['Terre Plein', 'Oui', 'Non']).",
    # 'is_plancher_haut_deper_isole': "booleen qui renseigne sur le status isolé ou non du plancher haut (['Oui', 'Non']).",
    # 'is_murs_ext_isole': "booleen qui renseigne sur le status isolé ou non des murs donnant sur l'extérieur (['Oui', 'Non']).",
    # 'is_murs_deper_isole': "booleen qui renseigne sur le status isolé ou non des murs donnant sur l'extérieur ou un local non chauffé (['Oui', 'Non']).",
    'mat_murs_deper_top': "matériau principal des murs donnant sur l'extérieur ou un local non chauffé (avec la plus grande surface ) (matériau au sens de la TV004).",
    'mat_murs_ext_top': "matériau principal des murs donnants sur l'extérieur (avec la plus grande surface ) (matériau au sens de la TV004).",
    'mat_plancher_bas_deper_top': "matériau principal des planchers basne donnant pas sur un logement mitoyen(avec la plus grande surface associée ) (matériau au sens de la TV006).",
    'mat_plancher_haut_deper_top': "matériau principal des murs donnant sur l'extérieur ou un local non chauffé (avec la plus grande surface ) (matériau au sens de la TV008).",
    'Ubaie_avg': "coefficient de déperdition des baies donnant sur l'extérieur moyen (pondéré à la surface)",
    'type_vitrage_simple_top': 'type de vitrage principal (avec la plus grande surface associée ) (simple,double ou triple vitrage)',
    'orientation_concat': "concaténation des orientations des vitrages déclarés ex. Nord,Est,Ouest : vitrages sur les surfaces nord est et ouest.",
    'surf_murs_totale': 'surface de murs totale',
    'surf_murs_deper': 'surface de murs déperditive(extérieur + locaux non chauffés)',
    'surf_murs_ext': 'surface de murs extérieurs',
    'surf_pb_totale': 'surface de planchers bas totale',
    'surf_pb_deper': 'surface de planchers bas ne donnant pas sur un logement adjacent',
    'surf_ph_totale': 'surface de planchers hauts totale',
    'surf_ph_deper': 'surface de planchers hauts ne donnant pas sur un logement adjacent',
    'surface_vitree_totale': 'surface de paroi vitrée (fenêtre/porte fenêtre et parois vitrées)',
    'surface_porte_totale': 'surface de portes',
    'surfaces_vitree_orientee_est': 'surfaces vitrées orientées est',
    'surfaces_vitree_orientee_est_ou_ouest': 'surfaces vitrées orientées est ou ouest',
    'surfaces_vitree_orientee_horizontale': 'surfaces vitrées horizontales',
    'surfaces_vitree_orientee_nondef': 'surfaces vitrées avec orientation non définies',
    'surfaces_vitree_orientee_nord': 'surfaces vitrées orientées nord',
    'surfaces_vitree_orientee_ouest': 'surfaces vitrées orientées ouest',
    'surfaces_vitree_orientee_sud': 'surfaces vitrées orientées sud',
    'ratio_surface_vitree_exterieur': 'ratio entre la surface vitrée totale et la surface de murs extérieurs.',
    'ratio_surface_vitree_deperditif': 'ratio entre la surface vitrée totale et la surface de murs déperditifs.',
    'ratio_surface_vitree_total': 'ratio entre la surface vitrée totale et la surface de murs totaux.',
}

td001_sys_ch_agg_desc = {
    'td001_dpe_id': 'identifiant de la table td001_dpe_id',
    'nombre_installations_ch_total': "nombre d'installations totale de chauffage pour le dpe.",
    'nombre_generateurs_ch_total': 'nombre de générateurs de chauffage totaux pour le dpe ',
    'mix_energetique_chauffage': "type d'energie de chauffage concaténé  (voir enumérateur type_energie_chauffage)",
    'type_installation_chauffage_concat': "type d'installation de chauffage concaténé (collectif ou individuel)",
    'gen_ch_lib_infer_concat': 'concaténation du libéllé générateur chauffage (voir enumerateur : gen_ch_lib_infer)',
    'gen_ch_lib_infer_simp_concat': "libéllé générateur chauffage simplifié concaténé (voir enumerateur : gen_ch_lib_infer_simp)",
    'configuration_sys_chauffage': 'configuration des systèmes de chauffage pour le DPE (voir enumerateur : configuration_sys_chauffage)',
    'sys_ch_principal_consommation_chauffage': 'consommation de chauffage du système de chauffage principal',
    'sys_ch_principal_gen_ch_lib_infer': 'libéllé générateur chauffage pour le système de chauffage principal (voir enumerateur : gen_ch_lib_infer)',
    'sys_ch_principal_gen_ch_lib_infer_simp': 'libéllé générateur chauffage simplifié pour le système de chauffage principal (voir enumerateur : gen_ch_lib_infer_simp)',
    'sys_ch_principal_nb_generateur': 'nombre de générateurs correspondant au système de chauffage principal (si plusieurs chaudières de même type ou plusieurs emetteurs effet joule ils sont regroupés)',
    'sys_ch_principal_type_energie_chauffage': "type d'energie de chauffage du système de chauffage principal  (voir enumérateur type_energie_chauffage)",
    'sys_ch_principal_type_installation_chauffage': "type d'installation de chauffage pour le système de chauffage principal (collectif ou individuel)",
    'sys_ch_secondaire_consommation_chauffage': 'consommation de chauffage du système de chauffage secondaire',
    'sys_ch_secondaire_gen_ch_lib_infer': 'libéllé générateur chauffage pour le système de chauffage secondaire (voir enumerateur : gen_ch_lib_infer)',
    'sys_ch_secondaire_gen_ch_lib_infer_simp': 'libéllé générateur chauffage simplifié pour le système de chauffage secondaire (voir enumerateur : gen_ch_lib_infer_simp)',
    'sys_ch_secondaire_nb_generateur': 'nombre de générateurs correspondant au système de chauffage secondaire (si plusieurs chaudières de même type ou plusieurs emetteurs effet joule ils sont regroupés)',
    'sys_ch_secondaire_type_energie_chauffage': "type d'energie de chauffage du système de chauffage secondaire  (voir enumérateur type_energie_chauffage)",
    'sys_ch_secondaire_type_installation_chauffage': "type d'installation de chauffage pour le système de chauffage secondaire (collectif ou individuel)",
    'sys_ch_tertiaire_consommation_chauffage_concat': 'consommation de chauffage du système de chauffage tertiaire concaténé',
    'sys_ch_tertiaire_gen_ch_lib_infer_concat': 'libéllé générateur chauffage pour le système de chauffage tertiaire concaténé (voir enumerateur : gen_ch_lib_infer)',
    'sys_ch_tertiaire_gen_ch_lib_infer_simp_concat': 'libéllé générateur chauffage simplifié pour le système de chauffage tertiaire concaténé (voir enumerateur : gen_ch_lib_infer_simp)',
    'sys_ch_tertiaire_nb_generateur': 'nombre de générateurs correspondant au système de chauffage tertiaire (tous types de générateurs confondus)',
    'sys_ch_tertiaire_type_energie_chauffage_concat': "type d'energie de chauffage du système de chauffage tertiaire concaténé  (voir enumérateur type_energie_chauffage)",
    'sys_ch_tertiaire_type_installation_chauffage_concat': "type d'installation de chauffage pour le système de chauffage tertiaire concaténé (collectif ou individuel)",
}
td001_sys_ecs_agg_desc = {
    "td001_dpe_id": "identifiant de la table td001_dpe_id",
    "nombre_installations_ecs_total": "nombre d'installations totale d'ecs pour le dpe.",
    "nombre_generateurs_ecs_total": "nombre de générateurs d'ecs totaux pour le dpe ",
    "mix_energetique_ecs": "type d'energie d'ecs concaténé  (voir enumérateur type_energie_ecs)",
    "type_installation_ecs_concat": "type d'installation d'ecs concaténé (collectif ou individuel)",
    "gen_ecs_lib_infer_concat": "concaténation du libéllé générateur ecs (voir enumerateur : gen_ecs_lib_infer)",
    "gen_ecs_lib_infer_simp_concat": "libéllé générateur ecs simplifié concaténé (voir enumerateur : gen_ecs_lib_infer_simp)",
    "configuration_sys_ecs": "configuration des systèmes d'ecs pour le DPE (voir enumerateur : configuration_sys_ecs)",
    "sys_ecs_principal_consommation_ecs": "consommation d'ecs du système d'ecs principal",
    "sys_ecs_principal_gen_ecs_lib_infer": "libéllé générateur ecs pour le système d'ecs principal (voir enumerateur : gen_ecs_lib_infer)",
    "sys_ecs_principal_gen_ecs_lib_infer_simp": "libéllé générateur ecs simplifié pour le système d'ecs principal (voir enumerateur : gen_ecs_lib_infer_simp)",
    "sys_ecs_principal_nb_generateur": "nombre de générateurs correspondant au système d'ecs principal (si plusieurs chaudières de même type ou plusieurs emetteurs effet joule ils sont regroupés)",
    "sys_ecs_principal_type_energie_ecs": "type d'energie d'ecs du système d'ecs principal  (voir enumérateur type_energie_ecs)",
    "sys_ecs_principal_type_installation_ecs": "type d'installation d'ecs pour le système d'ecs principal (collectif ou individuel)",
    "sys_ecs_secondaire_consommation_ecs": "consommation d'ecs du système d'ecs secondaire",
    "sys_ecs_secondaire_gen_ecs_lib_infer": "libéllé générateur ecs pour le système d'ecs secondaire (voir enumerateur : gen_ecs_lib_infer)",
    "sys_ecs_secondaire_gen_ecs_lib_infer_simp": "libéllé générateur ecs simplifié pour le système d'ecs secondaire (voir enumerateur : gen_ecs_lib_infer_simp)",
    "sys_ecs_secondaire_nb_generateur": "nombre de générateurs correspondant au système d'ecs secondaire (si plusieurs chaudières de même type ou plusieurs emetteurs effet joule ils sont regroupés)",
    "sys_ecs_secondaire_type_energie_ecs": "type d'energie d'ecs du système d'ecs secondaire  (voir enumérateur type_energie_ecs)",
    "sys_ecs_secondaire_type_installation_ecs": "type d'installation d'ecs pour le système d'ecs secondaire (collectif ou individuel)",
    "sys_ecs_tertiaire_consommation_ecs_concat": "consommation d'ecs du système d'ecs tertiaire concaténé",
    "sys_ecs_tertiaire_gen_ecs_lib_infer_concat": "libéllé générateur ecs pour le système d'ecs tertiaire concaténé (voir enumerateur : gen_ecs_lib_infer)",
    "sys_ecs_tertiaire_gen_ecs_lib_infer_simp_concat": "libéllé générateur ecs simplifié pour le système d'ecs tertiaire concaténé (voir enumerateur : gen_ecs_lib_infer_simp)",
    "sys_ecs_tertiaire_nb_generateur": "nombre de générateurs correspondant au système d'ecs tertiaire (tous types de générateurs confondus)",
    "sys_ecs_tertiaire_type_energie_ecs_concat": "type d'energie d'ecs du système d'ecs tertiaire concaténé  (voir enumérateur type_energie_ecs)",
    "sys_ecs_tertiaire_type_installation_ecs_concat": "type d'installation d'ecs pour le système d'ecs tertiaire concaténé (collectif ou individuel)",
}

td001_td007_murs_agg_annexe_desc = {
    'type_adjacence_top': "type d'adajcence la plus fréquente des murs.",
    'type_adjacence_array': "liste des types d'adjacences des murs",
    'type_LNC_murs_array': "liste des type de locaux non chauffés en contact avec les murs.",
    'type_LNC_murs_top': "type  de locaux non chauffés en contact avec les murs le plus fréquent.",
    "surface_murs_{type_adjacence}": "surface totale de murs correspondant au type d'adjacence.",
    "meth_calc_U_murs_top": "methode de calcul du coefficient de déperdition du mur (U) principale.",
    "U_murs_top": "coefficient de deperdition des murs le plus fréquent.",
    'epaisseur_isolation_murs_top': "epaisseur d'isolation la plus fréquente (quand elle est déclarée)",
    'resistance_thermique_isolation_murs_top': "resistance d'isolation la plus fréquente (quand elle est déclarée)",
    'isolation_murs_top': "méthode de determination de l'isolation la plus fréquente (isolation par défaut,isolation saisie ,non isolé etc...",
    'annee_isole_uniforme_min_murs_top': "annee d'isolation minimum la plus fréquente (quand l'isolation est determinée par défaut par rapport à l'année de construction",
    'annee_isole_uniforme_max_murs_top': "annee d'isolation maximum la plus fréquente (quand l'isolation est determinée par défaut par rapport à l'année de construction",
    'materiaux_structure_murs_top': "materiaux de structure des murs le plus fréquent",
    'epaisseur_structure_murs_top': "epaisseur du matériau de structure le plus fréquent.",
}

td001_td007_murs_agg_annexe_desc = {
    'type_adjacence_top': "type d'adajcence la plus fréquente des murs.",
    'type_adjacence_array': "liste des types d'adjacences des murs",
    'type_LNC_murs_array': "liste des type de locaux non chauffés en contact avec les murs.",
    'type_LNC_murs_top': "type  de locaux non chauffés en contact avec les murs le plus fréquent.",
    "surface_murs_{type_adjacence}": "surface totale de murs correspondant au type d'adjacence.",
    "meth_calc_U_murs_top": "methode de calcul du coefficient de déperdition du mur (U) principale.",
    "U_murs_top": "coefficient de deperdition des murs le plus fréquent.",
    'epaisseur_isolation_murs_top': "epaisseur d'isolation la plus fréquente (quand elle est déclarée)",
    'resistance_thermique_isolation_murs_top': "resistance d'isolation la plus fréquente (quand elle est déclarée)",
    'isolation_murs_top': "méthode de determination de l'isolation la plus fréquente (isolation par défaut,isolation saisie ,non isolé etc...",
    'annee_isole_uniforme_min_murs_top': "annee d'isolation minimum la plus fréquente (quand l'isolation est determinée par défaut par rapport à l'année de construction",
    'annee_isole_uniforme_max_murs_top': "annee d'isolation maximum la plus fréquente (quand l'isolation est determinée par défaut par rapport à l'année de construction",
    'materiaux_structure_murs_top': "materiaux de structure des murs le plus fréquent",
    'epaisseur_structure_murs_top': "epaisseur du matériau de structure le plus fréquent.",
}

td001_td007_plafonds_agg_annexe_desc = {
    'type_adjacence_top': "type d'adajcence la plus fréquente des plafonds.",
    'type_adjacence_array': "liste des types d'adjacences des plafonds",
    'type_LNC_plafonds_array': "liste des type de locaux non chauffés en contact avec les plafonds.",
    'type_LNC_plafonds_top': "type  de locaux non chauffés en contact avec les plafonds le plus fréquent.",
    "surface_plafonds_{type_adjacence}": "surface totale de plafonds correspondant au type d'adjacence.",
    "meth_calc_U_plafonds_top": "methode de calcul du coefficient de déperdition du mur (U) principale.",
    "U_plafonds_top": "coefficient de deperdition des plafonds le plus fréquent.",
    'epaisseur_isolation_plafonds_top': "epaisseur d'isolation la plus fréquente (quand elle est déclarée)",
    'resistance_thermique_isolation_plafonds_top': "resistance d'isolation la plus fréquente (quand elle est déclarée)",
    'isolation_plafonds_top': "méthode de determination de l'isolation la plus fréquente (isolation par défaut,isolation saisie ,non isolé etc...",
    'annee_isole_uniforme_min_plafonds_top': "annee d'isolation minimum la plus fréquente (quand l'isolation est determinée par défaut par rapport à l'année de construction",
    'annee_isole_uniforme_max_plafonds_top': "annee d'isolation maximum la plus fréquente (quand l'isolation est determinée par défaut par rapport à l'année de construction",
    'materiaux_structure_plafonds_top': "materiaux de structure des plafonds le plus fréquent",
    'epaisseur_structure_plafonds_top': "epaisseur du matériau de structure le plus fréquent.",
}


td007_annexe_desc = {
    "td001_dpe_id": "identifiant de la table td001_dpe_id",
    'materiaux_structure': "matériau de structure du composant d'enveloppe considéré au sens des tables valeurs tv004,tv006,tv008",
    'is_custom_resistance_thermique_isolation': "booleen si vrai : alors le diagnostiqueur a rentré une resistance thermique d'isolation et n'est pas passé par la table tv003/tv005/tv007",
    'is_custom_epaisseur_isolation': "booleen si vrai : alors le diagnostiqueur a rentré une epaisseur d'isolant thermique et n'est pas passé par la table tv003/tv005/tv007",
    'resistance_thermique_isolation_calc': "resistance thermique d'isolation recalculée a partir du coefficient de deperditions final et non isolé",
    'epaisseur_isolation_calc': "epaisseur d'isolation thermique recalculée a partir du coefficient de deperditions final et non isolé (en prenant hypothèse lambda =0.04)",
    'epaisseur_isolation_glob': "variable synthèse de epaisseur_isolation et epaisseur_isolation_calc",
    'resistance_thermique_isolation_glob': "variable synthèse de resistance_thermique_isolation et resistance_thermique_isolation_calc",
    'is_paroi_isole': 'booleen qui dit si la paroi est isolée ou non (epaisseur isolation >=5cm)',
    'surface_baie_sum': 'somme de la surface des baie',
    'surfacexnb_baie_calc_sum': 'donnée redressée de la surface de baie (surface x nombre_baie)',
    'nb_baie_calc': 'nombre de baies recalculée',
    'surface_paroi_opaque_infer': 'surface de paroi opaque calculé à partir de surface_paroi et des surfaces de baies',
    'surface_paroi_opaque_deperditive_infer': 'surface de paroi opaque deperditive calculé à partir de surface_paroi et des surfaces de baies et du coefficient de reduction des deperditions.(b>0)',
    'b_infer': 'coefficient b de réduction des deperditions déduit des tables tv.',
    'surface_paroi_opaque_exterieur_infer': 'surface de paroi opaque donnant sur exterieur calculé à partir de surface_paroi et des surfaces de baies et du coefficient de reduction des deperditions.(b>0.95)', }

td008_annexe_desc = {
    "td001_dpe_id": "identifiant de la table td001_dpe_id",
    'orientation_infer': 'orientation de la baie déduit des tables tv et de la description texte de la fenetre',
    'type_vitrage_simple_infer': 'catégorie de vitrage déduit des informations des tv (voir enum :cat_baie_simple_infer)',
    'nb_baie_calc': 'nombre de baies calculées à partir de la surface déclarée du U de la baie et de la deperdition',
    'surfacexnb_baie_calc': 'surface multipliée par le nombre de baies',
    'cat_baie_simple_infer': 'catégorie de baie déduit des informations des tv.(voir enum :cat_baie_simple_infer)', }

td012_annexe_desc = {
    "td001_dpe_id": "identifiant de la table td001_dpe_id",
    'gen_ch_lib_infer': 'libéllé du générateur de chauffage déduit en utilisant les informations depuis les tv(tables de valeurs) (voir enum gen_ch_lib_infer)',
    'gen_ch_lib_infer_simp': 'libéllé simplifié du générateur de chauffage (voir enum gen_ecs_lib_infer)',
    'type_energie_chauffage': "type d'energie de chauffage (voir enum type_energie_chauffage) issu de tv045_energie principalement"}

td014_annexe_desc = {
    "td001_dpe_id": "identifiant de la table td001_dpe_id",
    "gen_ecs_lib_infer": "libéllé du générateur d'ecs déduit en utilisant les informations depuis les tv(tables de valeurs) (voir enum gen_ecs_lib_infer)",
    "gen_ecs_lib_infer_simp": "libéllé simplifié du générateur d'ecs (voir enum gen_ecs_lib_infer)",
    "type_energie_ecs": "type d'energie d'ecs (voir enum type_energie_ecs)"}

enums_cstb = {
    'gen_ch_lib_infer': list(gen_ch_normalized_lib_matching_dict.keys()) + ['non affecte'],
    'gen_ch_lib_infer_simp': unique_ordered(list(gen_ch_lib_simp_dict.values())) + ['non affecte'],
    'type_energie_chauffage': ['Electricité non renouvelable', 'Gaz naturel', 'Bois, biomasse',
                               'Fioul domestique', 'Gaz propane ou butane', 'Réseau de chaleurs',
                               "Electricité d'origine renouvelable", 'Charbon'],
    'type_installation_chauffage': ['Chauffage Individuel', 'Chauffage Collectif'],
    'configuration_sys_chauffage': ['type de générateur unique/installation unique',
                                    'types de générateur multiples/installations multiples',
                                    'types de générateur multiples/installation unique'],
    'gen_ecs_lib_infer': list(gen_ecs_normalized_lib_matching_dict.keys()) + ['non affecte'],
    'gen_ecs_lib_infer_simp': unique_ordered(list(gen_ecs_lib_simp_dict.values())) + ['non affecte'],
    'type_energie_ecs': ['Electricité non renouvelable', 'Gaz naturel', 'Bois, biomasse',
                         'Fioul domestique', 'Gaz propane ou butane', 'Réseau de chaleurs',
                         "Electricité d'origine renouvelable", 'Charbon'],

    'type_vitrage_simple_infer': ['INCOHERENT', 'NONDEF', 'brique de verre ou polycarbonate',
                                  'double vitrage', 'porte', 'simple vitrage', 'triple vitrage'],
    'orientation_infer': ['Est', 'Est ou Ouest', 'Horizontale', 'NONDEF', 'Nord', 'Ouest', 'Sud'],
    'cat_baie_simple_infer': ['baie vitrée', 'paroi en brique de verre ou polycarbonate', 'porte'],
    'nom_methode_dpe_norm': ['FACTURE', '3CL 2012', 'THBCE(RT2012)/THC(RT2005)', 'DPE vierge',
                             '3CL 2005', 'NON DEFINI'],
    "type_LNC_murs": ['Garage', 'Sous-sols', 'Véranda',
                      "Circulation sans ouverture directe sur l'extérieur", 'Cellier',
                      "Circulation avec ouverture directe sur l'extérieur",
                      'Comble fortement ventilé',
                      "Hall d'entrée avec dispositif de fermeture automatique",
                      'Comble faiblement ventilé', 'Autres dépendances',
                      'Garage privé collectif',
                      "Hall d'entrée sans dispositif de fermeture automatique",
                      'Circulation avec bouche ou gaine de désenfumage ouverte en permanence'],
    'type_adjacence': ['EXTERIEUR', 'LNC', 'BAT_ADJ', 'NONDEF', 'PAROI_ENTERREE'],
    "meth_calc_U_murs": ['EPAISSEUR ISOLATION SAISIE',
                         'PAR DEFAUT PERIODE : ISOLATION INCONNUE', 'MUR NON ISOLE U=2',
                         'PAR DEFAUT PERIODE : ISOLE', 'RESISTANCE ISOLATION SAISIE',
                         'STRUCTURE ISOLANTE (ITR) U<1', 'U SAISI DIRECTEMENT : ISOLE',
                         'INCONNUE'],

    "isolation_murs": ['ISOLE SAISI', 'ISOLATION INCONNUE (DEFAUT)', 'NON ISOLE',
                       'ISOLE DEFAUT POST 1988', 'ISOLE DEFAUT PRE 1988',
                       'STRUCTURE ISOLANTE (ITR)']

}
