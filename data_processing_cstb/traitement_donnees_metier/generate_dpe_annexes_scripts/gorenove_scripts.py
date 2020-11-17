gorenove_variables = {
"u_mur_ext":"float",
"u_pb":"float",
"u_ph":"float",
"u_baie_vitree":"float",
"u_porte":"float",
"fs_baie_vitree":"float",
"mat_struct_mur_ext":"category",
"ep_mat_struct_mur_ext":"float",
"mat_struct_pb_ext":"category",
"mat_struct_ph_ext":"category",
"mat_baie_vitree":"category",
"type_vitrage_baie_vitree":"category",
"type_occultation_baie_vitree":"category",
"pos_isol_mur":"category",
"pos_isol_pb":"category",
"pos_isol_ph":"category",
"presence_balcon":"category",
"avancee_masque_max":"category",
"perc_surf_vitree_ext":"category",
"inertie":"category",
"type_ventilation":"category",
"gen_ch_lib_simp":"category",
"gen_ecs_lib_simp":"category",
"mix_energetique_chauffage":"category",
"mix_energetique_ecs":"category",
"type_installation_ch":"category",
"type_installation_ecs":"category",
"tr002_type_batiment_id":"category",
"consommation_energie":"float",
"estimation_ges":"float",
"classe_consommation_energie":"category",
"classe_estimation_ges":"category",
"numero_dpe":"str"
}




def rename_dpe_table_light(table):
    rep = {'epaisseur': 'ep',
           'isolation': 'isol',
           'resistance_thermique': 'res_therm',
           'uniforme_': '',
           'facteur_solaire_corr': 'fs',
           'exterieur': 'ext',

           'chauffage': 'ch',
           'u_baie': 'u',
           'materiaux': 'mat'}

    remove = ['_infer', '_top', '_avg', '_orientee', '_concat']

    cols = list()
    for col in table:
        for el in rep.items():
            col = col.replace(*el)
        for el in remove:
            col = col.replace(el, '')
        cols.append(col)
    table.columns = cols

def concat_td001_gorenove(td001_list):
    """
    concatenation des td001_annexes en une seule table avec les variables gorenove.

    Parameters
    ----------
    td001_list

    Returns
    -------

    """

