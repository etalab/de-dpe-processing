# -*- coding: utf-8 -*-
from .td011_td012_processing import gen_ch_normalized_lib_matching_dict, gen_ch_lib_simp_dict
from .td013_td014_processing import gen_ecs_normalized_lib_matching_dict, gen_ecs_lib_simp_dict
from .utils import unique_ordered

td001_annexe_generale_desc = {
    'nom_methode_dpe_norm': 'nom méthode dpe normalisé et simplifié (voir enum : nom_methode_dpe_norm)'}

td001_annexe_enveloppe_agg_desc = {
    "td001_dpe_id": "identifiant de la table td001_dpe_id",
    'u_mur_ext_avg': "coefficient de déperdition des murs donnant sur l'extérieur moyen (pondéré à la surface)",
    'u_mur_deper_avg': "coefficient de déperdition des murs donnant sur l'extérieur ou un local non chauffé moyen (pondéré à la surface)",
    'u_plancher_bas_deper_avg': "coefficient de déperdition des planchers bas ne donnant pas sur un logement mitoyen moyen (pondéré à la surface)",
    'u_plancher_haut_deper_avg': "coefficient de déperdition des planchers hauts ne donnant pas sur un logement mitoyen moyen (pondéré à la surface)",
    # 'is_plancher_bas_deper_isole': "texte qui renseigne sur le status isolé ou non du plancher bas (['Terre Plein', 'Oui', 'Non']).",
    # 'is_plancher_haut_deper_isole': "booleen qui renseigne sur le status isolé ou non du plancher haut (['Oui', 'Non']).",
    # 'is_mur_ext_isole': "booleen qui renseigne sur le status isolé ou non des murs donnant sur l'extérieur (['Oui', 'Non']).",
    # 'is_mur_deper_isole': "booleen qui renseigne sur le status isolé ou non des murs donnant sur l'extérieur ou un local non chauffé (['Oui', 'Non']).",
    'mat_mur_deper_top': "matériau principal des murs donnant sur l'extérieur ou un local non chauffé (avec la plus grande surface ) (matériau au sens de la TV004).",
    'mat_mur_ext_top': "matériau principal des murs donnants sur l'extérieur (avec la plus grande surface ) (matériau au sens de la TV004).",
    'mat_plancher_bas_deper_top': "matériau principal des planchers basne donnant pas sur un logement mitoyen(avec la plus grande surface associée ) (matériau au sens de la TV006).",
    'mat_plancher_haut_deper_top': "matériau principal des murs donnant sur l'extérieur ou un local non chauffé (avec la plus grande surface ) (matériau au sens de la TV008).",
    'u_baie_avg': "coefficient de déperdition des baies donnant sur l'extérieur moyen (pondéré à la surface)",
    'type_vitrage_simple_top': 'type de vitrage principal (avec la plus grande surface associée ) (simple,double ou triple vitrage)',
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
    'surf_vitree_orientee_est': 'surface vitrées orientées est',
    'surf_vitree_orientee_est_ou_ouest': 'surface vitrées orientées est ou ouest',
    'surf_vitree_orientee_horizontale': 'surface vitrées horizontales',
    'surf_vitree_orientee_nondef': 'surface vitrées avec orientation non définies',
    'surf_vitree_orientee_nord': 'surface vitrées orientées nord',
    'surf_vitree_orientee_ouest': 'surface vitrées orientées ouest',
    'surf_vitree_orientee_sud': 'surface vitrées orientées sud',
    'perc_surf_vitree_exterieur': 'pourcentage de surface vitrée totale par rapport à la surface totale des murs extérieurs.',
    'perc_surf_vitree_deperditif': 'pourcentage de surface vitrée totale par rapport à la surface totale des murs déperditifs.',
    'perc_surf_vitree_total': 'pourcentage de surface vitrée totale par rapport à la surface totale des murs totaux.',
}

td001_sys_ch_agg_desc = {
    'td001_dpe_id': 'identifiant de la table td001_dpe_id',
    'nb_installations_ch_total': "nombre d'installations totale de chauffage pour le dpe.",
    'nb_gen_ch_total': 'nombre de générateurs de chauffage totaux pour le dpe ',
    'mix_energetique_chauffage': "type d'energie de chauffage concaténé  (voir enumérateur type_energie_chauffage)",
    'type_installation_chauffage_concat': "type d'installation de chauffage concaténé (collectif ou individuel)",
    'gen_ch_lib_infer_concat': 'concaténation du libéllé générateur chauffage (voir enumerateur : gen_ch_lib_infer)',
    'gen_ch_lib_infer_simp_concat': "libéllé générateur chauffage simplifié concaténé (voir enumerateur : gen_ch_lib_infer_simp)",
    'cfg_sys_chauffage': 'configuration des systèmes de chauffage pour le DPE (voir enumerateur : cfg_sys_chauffage)',
    'sys_ch_princ_conso_chauffage': 'consommation de chauffage du système de chauffage principal',
    'sys_ch_princ_gen_ch_lib_infer': 'libéllé générateur chauffage pour le système de chauffage principal (voir enumerateur : gen_ch_lib_infer)',
    'sys_ch_princ_gen_ch_lib_infer_simp': 'libéllé générateur chauffage simplifié pour le système de chauffage principal (voir enumerateur : gen_ch_lib_infer_simp)',
    'sys_ch_princ_nb_generateur': 'nombre de générateurs correspondant au système de chauffage principal (si plusieurs chaudières de même type ou plusieurs emetteurs effet joule ils sont regroupés)',
    'sys_ch_princ_type_energie_chauffage': "type d'energie de chauffage du système de chauffage principal  (voir enumérateur type_energie_chauffage)",
    'sys_ch_princ_type_installation_chauffage': "type d'installation de chauffage pour le système de chauffage principal (collectif ou individuel)",
    'sys_ch_sec_conso_chauffage': 'consommation de chauffage du système de chauffage secondaire',
    'sys_ch_sec_gen_ch_lib_infer': 'libéllé générateur chauffage pour le système de chauffage secondaire (voir enumerateur : gen_ch_lib_infer)',
    'sys_ch_sec_gen_ch_lib_infer_simp': 'libéllé générateur chauffage simplifié pour le système de chauffage secondaire (voir enumerateur : gen_ch_lib_infer_simp)',
    'sys_ch_sec_nb_generateur': 'nombre de générateurs correspondant au système de chauffage secondaire (si plusieurs chaudières de même type ou plusieurs emetteurs effet joule ils sont regroupés)',
    'sys_ch_sec_type_energie_chauffage': "type d'energie de chauffage du système de chauffage secondaire  (voir enumérateur type_energie_chauffage)",
    'sys_ch_sec_type_installation_chauffage': "type d'installation de chauffage pour le système de chauffage secondaire (collectif ou individuel)",
    'sys_ch_tert_conso_chauffage_concat': 'consommation de chauffage du système de chauffage tertiaire concaténé',
    'sys_ch_tert_gen_ch_lib_infer_concat': 'libéllé générateur chauffage pour le système de chauffage tertiaire concaténé (voir enumerateur : gen_ch_lib_infer)',
    'sys_ch_tert_gen_ch_lib_infer_simp_concat': 'libéllé générateur chauffage simplifié pour le système de chauffage tertiaire concaténé (voir enumerateur : gen_ch_lib_infer_simp)',
    'sys_ch_tert_nb_generateur': 'nombre de générateurs correspondant au système de chauffage tertiaire (tous types de générateurs confondus)',
    'sys_ch_tert_type_energie_chauffage_concat': "type d'energie de chauffage du système de chauffage tertiaire concaténé  (voir enumérateur type_energie_chauffage)",
    'sys_ch_tert_type_installation_chauffage_concat': "type d'installation de chauffage pour le système de chauffage tertiaire concaténé (collectif ou individuel)",
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
    "sys_ecs_princ_conso_ecs": "consommation d'ecs du système d'ecs principal",
    "sys_ecs_princ_gen_ecs_lib_infer": "libéllé générateur ecs pour le système d'ecs principal (voir enumerateur : gen_ecs_lib_infer)",
    "sys_ecs_princ_gen_ecs_lib_infer_simp": "libéllé générateur ecs simplifié pour le système d'ecs principal (voir enumerateur : gen_ecs_lib_infer_simp)",
    "sys_ecs_princ_nb_generateur": "nombre de générateurs correspondant au système d'ecs principal (si plusieurs chaudières de même type ou plusieurs emetteurs effet joule ils sont regroupés)",
    "sys_ecs_princ_type_energie_ecs": "type d'energie d'ecs du système d'ecs principal  (voir enumérateur type_energie_ecs)",
    "sys_ecs_princ_type_installation_ecs": "type d'installation d'ecs pour le système d'ecs principal (collectif ou individuel)",
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
    "meth_calc_u_mur_top": "methode de calcul du coefficient de déperdition du mur (U) principale.",
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
    'type_lnc_plafond_array': "liste des type de locaux non chauffés en contact avec les plafonds.",
    'type_lnc_plafond_top': "type  de locaux non chauffés en contact avec les plafonds le plus fréquent.",
    "surf_plafond_{type_adjacence}": "surface totale de plafonds correspondant au type d'adjacence.",
    "meth_calc_u_plafond_top": "methode de calcul du coefficient de déperdition du mur (U) principale.",
    "U_plafond_top": "coefficient de deperdition des plafonds le plus fréquent.",
    'epaisseur_isolation_plafond_top': "epaisseur d'isolation la plus fréquente (quand elle est déclarée)",
    'resistance_thermique_isolation_plafond_top': "resistance d'isolation la plus fréquente (quand elle est déclarée)",
    'isolation_plafond_top': "méthode de determination de l'isolation la plus fréquente (isolation par défaut,isolation saisie ,non isolé etc...",
    'annee_isole_uniforme_min_plafond_top': "annee d'isolation minimum la plus fréquente (quand l'isolation est determinée par défaut par rapport à l'année de construction",
    'annee_isole_uniforme_max_plafond_top': "annee d'isolation maximum la plus fréquente (quand l'isolation est determinée par défaut par rapport à l'année de construction",
    'mat_struct_plafond_top': "materiaux de structure des plafonds le plus fréquent",
}


td001_td007_pb_agg_annexe_desc = {
    'type_adjacence_top': "type d'adajcence la plus fréquente des planchers.",
    'type_adjacence_array': "liste des types d'adjacences des planchers",
    'type_lnc_plancher_array': "liste des type de locaux non chauffés en contact avec les planchers.",
    'type_lnc_plancher_top': "type  de locaux non chauffés en contact avec les planchers le plus fréquent.",
    "surf_plancher_{type_adjacence}": "surface totale de planchers correspondant au type d'adjacence.",
    "meth_calc_u_plancher_top": "methode de calcul du coefficient de déperdition du mur (U) principale.",
    "U_plancher_top": "coefficient de deperdition des planchers le plus fréquent.",
    'epaisseur_isolation_plancher_top': "epaisseur d'isolation la plus fréquente (quand elle est déclarée)",
    'resistance_thermique_isolation_plancher_top': "resistance d'isolation la plus fréquente (quand elle est déclarée)",
    'isolation_plancher_top': "méthode de determination de l'isolation la plus fréquente (isolation par défaut,isolation saisie ,non isolé etc...",
    'annee_isole_uniforme_min_plancher_top': "annee d'isolation minimum la plus fréquente (quand l'isolation est determinée par défaut par rapport à l'année de construction",
    'annee_isole_uniforme_max_plancher_top': "annee d'isolation maximum la plus fréquente (quand l'isolation est determinée par défaut par rapport à l'année de construction",
    'mat_struct_plancher_top': "materiaux de structure des planchers le plus fréquent",
    'ep_mat_struct_plancher_top': "epaisseur du matériau de structure le plus fréquent.",
}
# TODO : doc agg
td001_td008_agg_annexe_desc = {

}

td001_td010_agg_annexe_desc = {
    'pos_isol_mur': "position de l'isolation dans le mur (ITE/ITI/ITR/Non isolé)",
    'pos_isol_plancher': "position de l'isolation dans le plancher (ITE/ITI/ITR/Non isolé)",
    'pos_isol_plafond': "position de l'isolation dans le plafond (ITE/ITI/ITR/Non isolé)",
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
    'surf_paroi_opaque_exterieur_infer': 'surface de paroi opaque donnant sur exterieur calculé à partir de surf_paroi et des surface de baies et du coefficient de reduction des deperditions.(b>0.95)', }

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
    'cfg_sys_chauffage': ['type de générateur unique/installation unique',
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
