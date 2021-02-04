# -*- coding: utf-8 -*-
from .td011_td012_processing import td012_gen_ch_search_dict, gen_ch_lib_simp_dict
from .td013_td014_processing import td014_gen_ecs_search_dict, gen_ecs_lib_simp_dict
from .utils import unique_ordered

td001_annexe_generale_desc = {
    'nom_methode_dpe_norm': 'nom méthode dpe normalisé et simplifié (voir enum : nom_methode_dpe_norm)'}

td001_annexe_enveloppe_agg_desc = {
    "td001_dpe_id": "identifiant de la table td001_dpe_id",
    'u_mur_ext_avg': "coefficient de déperdition des murs donnant sur l'extérieur moyen (pondéré à la surface)",
    'u_mur_deper_avg': "coefficient de déperdition des murs donnant sur l'extérieur ou un local non chauffé moyen (pondéré à la surface)",
    'u_pb_deper_avg': "coefficient de déperdition des planchers bas ne donnant pas sur un logement mitoyen moyen (pondéré à la surface)",
    'u_ph_deper_avg': "coefficient de déperdition des planchers hauts ne donnant pas sur un logement mitoyen moyen (pondéré à la surface)",
    # 'is_pb_deper_isole': "texte qui renseigne sur le status isolé ou non du plancher bas (['Terre Plein', 'Oui', 'Non']).",
    # 'is_ph_deper_isole': "booleen qui renseigne sur le status isolé ou non du plancher haut (['Oui', 'Non']).",
    # 'is_mur_ext_isole': "booleen qui renseigne sur le status isolé ou non des murs donnant sur l'extérieur (['Oui', 'Non']).",
    # 'is_mur_deper_isole': "booleen qui renseigne sur le status isolé ou non des murs donnant sur l'extérieur ou un local non chauffé (['Oui', 'Non']).",
    'mat_mur_deper_top': "matériau le plus fréquent des murs donnant sur l'extérieur ou un local non chauffé (avec la plus grande surface ) (matériau au sens de la TV004).",
    'mat_mur_ext_top': "matériau le plus fréquent des murs donnants sur l'extérieur (avec la plus grande surface ) (matériau au sens de la TV004).",
    'mat_pb_deper_top': "matériau le plus fréquent des planchers basne donnant pas sur un logement mitoyen(avec la plus grande surface associée ) (matériau au sens de la TV006).",
    'mat_ph_deper_top': "matériau le plus fréquent des murs donnant sur l'extérieur ou un local non chauffé (avec la plus grande surface ) (matériau au sens de la TV008).",
    'u_baie_avg': "coefficient de déperdition des baies donnant sur l'extérieur moyen (pondéré à la surface)",
    'type_vitrage_simple_top': 'type de vitrage le plus fréquent (avec la plus grande surface associée ) (simple,double ou triple vitrage)',
    'orientation_concat': "concaténation des orientations des vitrages déclarés ex. Nord,Est,Ouest : vitrages sur les surface nord est et ouest.",
    'surf_mur_totale': 'surface de murs totale',
    'surf_mur_deper': 'surface de murs déperditive(extérieur + locaux non chauffés)',
    'surf_mur_ext': 'surface de murs extérieurs',
    'surf_pb_totale': 'surface de planchers bas totale',
    'surf_pb_deper': 'surface de planchers bas ne donnant pas sur un logement adjacent',
    'surf_ph_totale': 'surface de planchers hauts totale',
    'surf_ph_deper': 'surface de planchers hauts ne donnant pas sur un logement adjacent',
    'surf_vitree_totale': 'surface de paroi vitrée (fenêtre/porte fenêtre et parois vitrées)',
    'surf_porte_totale': 'surface de portes',
    'surf_vitree_est': 'surface vitrées orientées est',
    'surf_vitree_est_ou_ouest': 'surface vitrées orientées est ou ouest',
    'surf_vitree_horizontale': 'surface vitrées horizontales',
    'surf_vitree_nondef': 'surface vitrées avec orientation non définies',
    'surf_vitree_nord': 'surface vitrées orientées nord',
    'surf_vitree_ouest': 'surface vitrées orientées ouest',
    'surf_vitree_sud': 'surface vitrées orientées sud',
    'perc_surf_vitree_ext': 'pourcentage de surface vitrée totale par rapport à la surface totale des murs extérieurs.',
    'perc_surf_vitree_deperditif': 'pourcentage de surface vitrée totale par rapport à la surface totale des murs déperditifs.',
    'perc_surf_vitree_total': 'pourcentage de surface vitrée totale par rapport à la surface totale des murs totaux.',
}

td001_sys_ch_agg_desc = {
    'td001_dpe_id': 'identifiant de la table td001_dpe_id',
    'nb_installations_ch_total': "nombre d'installations totale de chauffage pour le dpe.",
    'nb_gen_ch_total': 'nombre de générateurs de chauffage totaux pour le dpe ',
    'mix_energetique_ch': "type d'energie de chauffage concaténé  (voir enumérateur type_energie_ch)",
    'type_installation_ch_concat': "type d'installation de chauffage concaténé (collectif ou individuel)",
    'gen_ch_lib_infer_concat': 'concaténation du libéllé générateur chauffage (voir enumerateur : gen_ch_lib_infer)',
    'gen_ch_lib_infer_simp_concat': "libéllé générateur chauffage simplifié concaténé (voir enumerateur : gen_ch_lib_infer_simp)",
    'cfg_sys_ch': 'configuration des systèmes de chauffage pour le DPE (voir enumerateur : cfg_sys_ch)',
    'sys_ch_princ_conso_ch': 'consommation de chauffage du système de chauffage le plus fréquent',
    'sys_ch_princ_gen_ch_lib_infer': 'libéllé générateur chauffage pour le système de chauffage le plus fréquent (voir enumerateur : gen_ch_lib_infer)',
    'sys_ch_princ_gen_ch_lib_infer_simp': 'libéllé générateur chauffage simplifié pour le système de chauffage le plus fréquent (voir enumerateur : gen_ch_lib_infer_simp)',
    'sys_ch_princ_nb_generateur': 'nombre de générateurs correspondant au système de chauffage le plus fréquent (si plusieurs chaudières de même type ou plusieurs emetteurs effet joule ils sont regroupés)',
    'sys_ch_princ_type_energie_ch': "type d'energie de chauffage du système de chauffage le plus fréquent  (voir enumérateur type_energie_ch)",
    'sys_ch_princ_type_installation_ch': "type d'installation de chauffage pour le système de chauffage le plus fréquent (collectif ou individuel)",
    'sys_ch_sec_conso_ch': 'consommation de chauffage du système de chauffage secondaire',
    'sys_ch_sec_gen_ch_lib_infer': 'libéllé générateur chauffage pour le système de chauffage secondaire (voir enumerateur : gen_ch_lib_infer)',
    'sys_ch_sec_gen_ch_lib_infer_simp': 'libéllé générateur chauffage simplifié pour le système de chauffage secondaire (voir enumerateur : gen_ch_lib_infer_simp)',
    'sys_ch_sec_nb_generateur': 'nombre de générateurs correspondant au système de chauffage secondaire (si plusieurs chaudières de même type ou plusieurs emetteurs effet joule ils sont regroupés)',
    'sys_ch_sec_type_energie_ch': "type d'energie de chauffage du système de chauffage secondaire  (voir enumérateur type_energie_ch)",
    'sys_ch_sec_type_installation_ch': "type d'installation de chauffage pour le système de chauffage secondaire (collectif ou individuel)",
    'sys_ch_tert_conso_ch_concat': 'consommation de chauffage du système de chauffage tertiaire concaténé',
    'sys_ch_tert_gen_ch_lib_infer_concat': 'libéllé générateur chauffage pour le système de chauffage tertiaire concaténé (voir enumerateur : gen_ch_lib_infer)',
    'sys_ch_tert_gen_ch_lib_infer_simp_concat': 'libéllé générateur chauffage simplifié pour le système de chauffage tertiaire concaténé (voir enumerateur : gen_ch_lib_infer_simp)',
    'sys_ch_tert_nb_generateur': 'nombre de générateurs correspondant au système de chauffage tertiaire (tous types de générateurs confondus)',
    'sys_ch_tert_type_energie_ch_concat': "type d'energie de chauffage du système de chauffage tertiaire concaténé  (voir enumérateur type_energie_ch)",
    'sys_ch_tert_type_installation_ch_concat': "type d'installation de chauffage pour le système de chauffage tertiaire concaténé (collectif ou individuel)",
}
td001_sys_ecs_agg_desc = {
    "td001_dpe_id": "identifiant de la table td001_dpe_id",
    "nb_installations_ecs_total": "nombre d'installations totale d'ecs pour le dpe.",
    "nb_gen_ecs_total": "nombre de générateurs d'ecs totaux pour le dpe ",
    "mix_energetique_ecs": "type d'energie d'ecs concaténé  (voir enumérateur type_energie_ecs)",
    "type_installation_ecs_concat": "type d'installation d'ecs concaténé (collectif ou individuel)",
    "gen_ecs_lib_infer_concat": "concaténation du libéllé générateur ecs (voir enumerateur : gen_ecs_lib_infer)",
    "gen_ecs_lib_infer_simp_concat": "libéllé générateur ecs simplifié concaténé (voir enumerateur : gen_ecs_lib_infer_simp)",
    "cfg_sys_ecs": "configuration des systèmes d'ecs pour le DPE (voir enumerateur : cfg_sys_ecs)",
    "sys_ecs_princ_conso_ecs": "consommation d'ecs du système d'ecs le plus fréquent",
    "sys_ecs_princ_gen_ecs_lib_infer": "libéllé générateur ecs pour le système d'ecs le plus fréquent (voir enumerateur : gen_ecs_lib_infer)",
    "sys_ecs_princ_gen_ecs_lib_infer_simp": "libéllé générateur ecs simplifié pour le système d'ecs le plus fréquent (voir enumerateur : gen_ecs_lib_infer_simp)",
    "sys_ecs_princ_nb_generateur": "nombre de générateurs correspondant au système d'ecs le plus fréquent (si plusieurs chaudières de même type ou plusieurs emetteurs effet joule ils sont regroupés)",
    "sys_ecs_princ_type_energie_ecs": "type d'energie d'ecs du système d'ecs le plus fréquent  (voir enumérateur type_energie_ecs)",
    "sys_ecs_princ_type_installation_ecs": "type d'installation d'ecs pour le système d'ecs le plus fréquent (collectif ou individuel)",
    "sys_ecs_sec_conso_ecs": "consommation d'ecs du système d'ecs secondaire",
    "sys_ecs_sec_gen_ecs_lib_infer": "libéllé générateur ecs pour le système d'ecs secondaire (voir enumerateur : gen_ecs_lib_infer)",
    "sys_ecs_sec_gen_ecs_lib_infer_simp": "libéllé générateur ecs simplifié pour le système d'ecs secondaire (voir enumerateur : gen_ecs_lib_infer_simp)",
    "sys_ecs_sec_nb_generateur": "nombre de générateurs correspondant au système d'ecs secondaire (si plusieurs chaudières de même type ou plusieurs emetteurs effet joule ils sont regroupés)",
    "sys_ecs_sec_type_energie_ecs": "type d'energie d'ecs du système d'ecs secondaire  (voir enumérateur type_energie_ecs)",
    "sys_ecs_sec_type_installation_ecs": "type d'installation d'ecs pour le système d'ecs secondaire (collectif ou individuel)",
    "sys_ecs_tert_conso_ecs_concat": "consommation d'ecs du système d'ecs tertiaire concaténé",
    "sys_ecs_tert_gen_ecs_lib_infer_concat": "libéllé générateur ecs pour le système d'ecs tertiaire concaténé (voir enumerateur : gen_ecs_lib_infer)",
    "sys_ecs_tert_gen_ecs_lib_infer_simp_concat": "libéllé générateur ecs simplifié pour le système d'ecs tertiaire concaténé (voir enumerateur : gen_ecs_lib_infer_simp)",
    "sys_ecs_tert_nb_generateur": "nombre de générateurs correspondant au système d'ecs tertiaire (tous types de générateurs confondus)",
    "sys_ecs_tert_type_energie_ecs_concat": "type d'energie d'ecs du système d'ecs tertiaire concaténé  (voir enumérateur type_energie_ecs)",
    "sys_ecs_tert_type_installation_ecs_concat": "type d'installation d'ecs pour le système d'ecs tertiaire concaténé (collectif ou individuel)",
}

td001_td007_mur_agg_annexe_desc = {
    'type_adjacence_top': "type d'adajcence la plus fréquente des murs.",
    'type_adjacence_array': "liste des types d'adjacences des murs",
    'type_lnc_mur_array': "liste des type de locaux non chauffés en contact avec les murs.",
    'type_lnc_mur_top': "type  de locaux non chauffés en contact avec les murs le plus fréquent.",
    "surf_mur_{type_adjacence}": "surface totale de murs correspondant au type d'adjacence.",
    "meth_calc_u_mur_top": "methode de calcul du coefficient de déperdition du mur (U) la plus fréquente.",
    "U_mur_top": "coefficient de deperdition des murs le plus fréquent.",
    'epaisseur_isolation_mur_top': "epaisseur d'isolation la plus fréquente (quand elle est déclarée)",
    'resistance_thermique_isolation_mur_top': "resistance d'isolation la plus fréquente (quand elle est déclarée)",
    'isolation_mur_top': "méthode de determination de l'isolation la plus fréquente (isolation par défaut,isolation saisie ,non isolé etc...",
    'annee_isole_uniforme_min_mur_top': "annee d'isolation minimum la plus fréquente (quand l'isolation est determinée par défaut par rapport à l'année de construction",
    'annee_isole_uniforme_max_mur_top': "annee d'isolation maximum la plus fréquente (quand l'isolation est determinée par défaut par rapport à l'année de construction",
    'mat_struct_mur_top': "materiaux de structure des murs le plus fréquent",
    'ep_mat_struct_mur_top': "epaisseur du matériau de structure le plus fréquent.",
}

td001_td007_ph_agg_annexe_desc = {
    'type_adjacence_top': "type d'adajcence la plus fréquente des plafonds.",
    'type_adjacence_array': "liste des types d'adjacences des plafonds",
    'type_lnc_ph_array': "liste des type de locaux non chauffés en contact avec les plafonds.",
    'type_lnc_ph_top': "type  de locaux non chauffés en contact avec les plafonds le plus fréquent.",
    "surf_ph_{type_adjacence}": "surface totale de plafonds correspondant au type d'adjacence.",
    "meth_calc_u_ph_top": "methode de calcul du coefficient de déperdition du mur (U) la plus fréquente.",
    "U_ph_top": "coefficient de deperdition des plafonds le plus fréquent.",
    'epaisseur_isolation_ph_top': "epaisseur d'isolation la plus fréquente (quand elle est déclarée)",
    'resistance_thermique_isolation_ph_top': "resistance d'isolation la plus fréquente (quand elle est déclarée)",
    'isolation_ph_top': "méthode de determination de l'isolation la plus fréquente (isolation par défaut,isolation saisie ,non isolé etc...",
    'annee_isole_uniforme_min_ph_top': "annee d'isolation minimum la plus fréquente (quand l'isolation est determinée par défaut par rapport à l'année de construction",
    'annee_isole_uniforme_max_ph_top': "annee d'isolation maximum la plus fréquente (quand l'isolation est determinée par défaut par rapport à l'année de construction",
    'mat_struct_ph_top': "materiaux de structure des plafonds le plus fréquent",
}


td001_td007_pb_agg_annexe_desc = {
    'type_adjacence_top': "type d'adajcence la plus fréquente des planchers.",
    'type_adjacence_array': "liste des types d'adjacences des planchers",
    'type_lnc_pb_array': "liste des type de locaux non chauffés en contact avec les planchers.",
    'type_lnc_pb_top': "type  de locaux non chauffés en contact avec les planchers le plus fréquent.",
    "surf_pb_{type_adjacence}": "surface totale de planchers correspondant au type d'adjacence.",
    "meth_calc_u_pb_top": "methode de calcul du coefficient de déperdition du mur (U) la plus fréquente.",
    "U_pb_top": "coefficient de deperdition des planchers le plus fréquent.",
    'epaisseur_isolation_pb_top': "epaisseur d'isolation la plus fréquente (quand elle est déclarée)",
    'resistance_thermique_isolation_pb_top': "resistance d'isolation la plus fréquente (quand elle est déclarée)",
    'isolation_pb_top': "méthode de determination de l'isolation la plus fréquente (isolation par défaut,isolation saisie ,non isolé etc...",
    'annee_isole_uniforme_min_pb_top': "annee d'isolation minimum la plus fréquente (quand l'isolation est determinée par défaut par rapport à l'année de construction",
    'annee_isole_uniforme_max_pb_top': "annee d'isolation maximum la plus fréquente (quand l'isolation est determinée par défaut par rapport à l'année de construction",
    'mat_struct_pb_top': "materiaux de structure des planchers le plus fréquent",
    'ep_mat_struct_pb_top': "epaisseur du matériau de structure le plus fréquent.",
}
# TODO : doc agg
td001_td008_agg_annexe_desc = {

}

td001_td010_agg_annexe_desc = {
    'pos_isol_mur': "position de l'isolation dans le mur (ITE/ITI/ITR/Non isolé)",
    'pos_isol_pb': "position de l'isolation dans le plancher (ITE/ITI/ITR/Non isolé)",
    'pos_isol_ph': "position de l'isolation dans le plafond (ITE/ITI/ITR/Non isolé)",
}

td007_annexe_desc = {
    "td001_dpe_id": "identifiant de la table td001_dpe_id",
    'mat_struct': "matériau de structure du composant d'enveloppe considéré au sens des tables valeurs tv004,tv006,tv008",
    'is_custom_resistance_thermique_isolation': "booleen si vrai : alors le diagnostiqueur a rentré une resistance thermique d'isolation et n'est pas passé par la table tv003/tv005/tv007",
    'is_custom_epaisseur_isolation': "booleen si vrai : alors le diagnostiqueur a rentré une epaisseur d'isolant thermique et n'est pas passé par la table tv003/tv005/tv007",
    'resistance_thermique_isolation_calc': "resistance thermique d'isolation recalculée a partir du coefficient de deperditions final et non isolé",
    'epaisseur_isolation_calc': "epaisseur d'isolation thermique recalculée a partir du coefficient de deperditions final et non isolé (en prenant hypothèse lambda =0.04)",
    'epaisseur_isolation_glob': "variable synthèse de epaisseur_isolation et epaisseur_isolation_calc",
    'resistance_thermique_isolation_glob': "variable synthèse de resistance_thermique_isolation et resistance_thermique_isolation_calc",
    'is_paroi_isole': 'booleen qui dit si la paroi est isolée ou non (epaisseur isolation >=5cm)',
    'surf_baie_sum': 'somme de la surface des baie',
    'surfacexnb_baie_calc_sum': 'donnée redressée de la surface de baie (surface x nb_baie)',
    'nb_baie_calc': 'nombre de baies recalculée',
    'surf_paroi_opaque_infer': 'surface de paroi opaque calculé à partir de surf_paroi et des surface de baies',
    'surf_paroi_opaque_deperditive_infer': 'surface de paroi opaque deperditive calculé à partir de surf_paroi et des surface de baies et du coefficient de reduction des deperditions.(b>0)',
    'b_infer': 'coefficient b de réduction des deperditions déduit des tables tv.',
    'surf_paroi_opaque_ext_infer': 'surface de paroi opaque donnant sur exterieur calculé à partir de surf_paroi et des surface de baies et du coefficient de reduction des deperditions.(b>0.95)', }

td008_annexe_desc = {
    "td001_dpe_id": "identifiant de la table td001_dpe_id",
    'orientation_infer': 'orientation de la baie déduit des tables tv et de la description texte de la fenetre',
    'type_vitrage_simple_infer': 'catégorie de vitrage déduit des informations des tv (voir enum :cat_baie_simple_infer)',
    'nb_baie_calc': 'nombre de baies calculées à partir de la surface déclarée du U de la baie et de la deperdition',
    'surfacexnb_baie_calc': 'surface multipliée par le nombre de baies',
    'cat_baie_simple_infer': 'catégorie de baie déduit des informations des tv.(voir enum :cat_baie_simple_infer)', }

td010_annexe_desc= {
    'type_liaison' : 'type de liaison de pont thermique'

}

td012_annexe_desc = {
    "td001_dpe_id": "identifiant de la table td001_dpe_id",
    'gen_ch_lib_infer': 'libéllé du générateur de chauffage déduit en utilisant les informations depuis les tv(tables de valeurs) (voir enum gen_ch_lib_infer)',
    'gen_ch_lib_infer_simp': 'libéllé simplifié du générateur de chauffage (voir enum gen_ecs_lib_infer)',
    'type_energie_ch': "type d'energie de chauffage (voir enum type_energie_ch) issu de tv045_energie la plus fréquentement"}

td014_annexe_desc = {
    "td001_dpe_id": "identifiant de la table td001_dpe_id",
    "gen_ecs_lib_infer": "libéllé du générateur d'ecs déduit en utilisant les informations depuis les tv(tables de valeurs) (voir enum gen_ecs_lib_infer)",
    "gen_ecs_lib_infer_simp": "libéllé simplifié du générateur d'ecs (voir enum gen_ecs_lib_infer)",
    "type_energie_ecs": "type d'energie d'ecs (voir enum type_energie_ecs)"}

enums_cstb = {
    'gen_ch_lib_infer': list(td012_gen_ch_search_dict.keys()) + ['indetermine'],
    'gen_ch_lib_infer_simp': unique_ordered(list(gen_ch_lib_simp_dict.values())) + ['indetermine'],
    'type_energie_ch': ['Electricité non renouvelable', 'Gaz naturel', 'Bois, biomasse',
                               'Fioul domestique', 'Gaz propane ou butane', 'Réseau de chaleurs',
                               "Electricité d'origine renouvelable", 'Charbon'],
    'type_installation_ch': ['Chauffage Individuel', 'Chauffage Collectif'],
    'cfg_sys_ch': ['type de générateur unique/installation unique',
                                    'types de générateur multiples/installations multiples',
                                    'types de générateur multiples/installation unique'],
    'gen_ecs_lib_infer': list(td014_gen_ecs_search_dict.keys()) + ['indetermine'],
    'gen_ecs_lib_infer_simp': unique_ordered(list(gen_ecs_lib_simp_dict.values())) + ['indetermine'],
    'type_energie_ecs': ['Electricité non renouvelable', 'Gaz naturel', 'Bois, biomasse',
                         'Fioul domestique', 'Gaz propane ou butane', 'Réseau de chaleurs',
                         "Electricité d'origine renouvelable", 'Charbon'],

    'type_vitrage_simple_infer': ['INCOHERENT', 'NONDEF', 'brique de verre ou polycarbonate',
                                  'double vitrage', 'porte', 'simple vitrage', 'triple vitrage'],
    'orientation_infer': ['Est', 'Est ou Ouest', 'Horizontale', 'NONDEF', 'Nord', 'Ouest', 'Sud'],
    'cat_baie_simple_infer': ['baie vitrée', 'paroi en brique de verre ou polycarbonate', 'porte'],
    'nom_methode_dpe_norm': ['FACTURE', '3CL 2012', 'THBCE(RT2012)/THC(RT2005)', 'DPE vierge',
                             '3CL 2005', 'NON DEFINI'],
    "type_lnc_mur": ['Garage', 'Sous-sols', 'Véranda',
                      "Circulation sans ouverture directe sur l'extérieur", 'Cellier',
                      "Circulation avec ouverture directe sur l'extérieur",
                      'Comble fortement ventilé',
                      "Hall d'entrée avec dispositif de fermeture automatique",
                      'Comble faiblement ventilé', 'Autres dépendances',
                      'Garage privé collectif',
                      "Hall d'entrée sans dispositif de fermeture automatique",
                      'Circulation avec bouche ou gaine de désenfumage ouverte en permanence'],
    'type_adjacence': ['EXTERIEUR', 'LNC', 'BAT_ADJ', 'NONDEF', 'PAROI_ENTERREE'],
    "meth_calc_u_mur": ['EPAISSEUR ISOLATION SAISIE',
                         'PAR DEFAUT PERIODE : ISOLATION INCONNUE', 'MUR NON ISOLE U=2',
                         'PAR DEFAUT PERIODE : ISOLE', 'RESISTANCE ISOLATION SAISIE',
                         'STRUCTURE ISOLANTE (ITR) U<1', 'U SAISI DIRECTEMENT : ISOLE',
                         'INCONNUE'],

    "isolation_mur": ['ISOLE SAISI', 'ISOLATION INCONNUE (DEFAUT)', 'NON ISOLE',
                       'ISOLE DEFAUT POST 1988', 'ISOLE DEFAUT PRE 1988',
                       'STRUCTURE ISOLANTE (ITR)']

}



gorenove_vars = {
    # TD001
    "numero_dpe": "numéro de dpe",
    "tr002_type_batiment_id": "type de logement : 1 MI , 2 : APPT , 3 : IMMEUBLE, ",
    "conso_ener": "consommation d'energie réglementaire DPE 2006 (kWhep/m²/an)",
    "estim_ges":  "estimation GES réglementaire DPE 2006 (KgCO2/m²/an)",
    "classe_conso_ener": "classe de consommation energie DPE 2006",
    "classe_estim_ges": "classe de consommation GES DPE 2006",
    # ENV
    "mur_u_ext": "U du mur le plus fréquent en surface donnant sur l'extérieur en W/m²/K",
    "pb_u": "U du plancher bas le plus fréquent en surface en W/m²/k",
    "ph_u": "U du plancher haut le plus fréquent en surface en W/m²/K",
    "baie_u": "U de la baie (Ujn ou Uw) le plus fréquent en surface en W/m²/K",
    "baie_fs": "Facteur solaire de la baie le plus fréquent en surface",
    "mur_mat_ext": "matériaux de la structure des murs extérieurs le plus fréquent en surface (sans l'isolation)",
    "mur_ep_mat_ext": "epaisseur de la structure des murs le plus fréquent en surface",
    "pb_mat": "matériaux ou principe constructif du plancher bas le plus fréquent en surface",
    "ph_mat": "matériaux ou principe constructif du plancher haut le plus fréquent en surface",
    "baie_mat": "matériaux de l'encadrement de la baie le plus fréquent en surface",
    "baie_type_vitrage": "type de vitrage le plus fréquent en surface des baies",
    "baie_remplissage": "remplissage argon ou air le plus fréquent en surface trouvé pour les vitrages",
    #"baie_type_occultation": "type d'occulation le plus fréquent des vitrages.",
    "baie_orientation": "concaténation des orientations des vitrages du logement (concaténé + )",
    "mur_pos_isol_ext": "position de l'isolation des murs extérieurs le plus fréquent en surface (ITI/ITE/ITR/non isole/isole)",
    "pb_pos_isol": "position de l'isolation des plancher bas le plus fréquent en surface (ITI/ITE/ITR/non isole/isole)",
    "ph_pos_isol": "position de l'isolation des plancher haut le plus fréquent en surface (ITI/ITE/ITR/non isole/isole)",
    "pb_type_adjacence": "type d'adjacence la plus fréquente en surface des planchers bas",
    "ph_type_adjacence": "type d'adjacence la plus fréquente en surface des planchers haut",
    "presence_balcon": "booléen présence de balcon sur les logements du bâtiment",
    "avancee_masque_max": "avancée des masques maximum sur les logements du bâtiment",
    # ENV SURF
    "perc_surf_vitree_ext": "pourcentage de surface vitrée d'un logement du bâtiment",
    # GEN
    "nom_methode_dpe": "nom de méthode DPE utilisée pour le bâtiment",
    "periode_construction": "période de construction renseignée dans le DPE",
    "inertie":  "inertie du logement",
    "type_ventilation": "type de ventilation référencé par les DPE",
    "presence_climatisation": "booléen présence de climatisation",
    'enr':"enr présentes dans le bâtiment.",
    'enr':"enr présentes dans le bâtiment.",
    # SYS
    "ch_type_inst": "type d'installation de chauffage '",
    "ch_type_ener": "type d'energie de chauffage (concaténé + )",
    "ch_gen_lib": "type de générateur de chauffage  (concaténé + )",
    "ch_gen_lib_princ": "type de générateur principal ",
    'ch_gen_lib_appoint': "type de générateur appoint ",
    'ch_is_solaire':  "présence d'énergie solaire pour le chauffage",
    "ecs_type_inst":"type d'installation de ecs '",
    "ecs_type_ener": "type d'energie de ecs (concaténé + )",
    "ecs_gen_lib": "type de générateur de ecs  (concaténé + )",
    "ecs_gen_lib_princ": "type de générateur principal ",
    'ecs_gen_lib_appoint':  "type de générateur appoint ",
    'ecs_is_solaire':"présence d'énergie solaire pour le ecs",

}

