
gorenove_types = {
    "numero_dpe": "str",
    "annee_construction": 'str',
    "surface_habitable":'float',
    "u_mur_ext": "float",
    "u_pb": "float",
    "u_ph": "float",
    "u_baie_vitree": "float",
    "u_porte": "float",
    "fs_baie_vitree": "float",
    "mat_struct_mur_ext": "category",
    "ep_mat_struct_mur_ext": "category",
    "mat_struct_pb_ext": "category",
    "mat_struct_ph_ext": "category",
    "mat_baie_vitree": "category",
    "type_vitrage_baie_vitree": "category",
    "type_occultation_baie_vitree": "category",
    "pos_isol_mur": "category",
    "pos_isol_pb": "category",
    "pos_isol_ph": "category",
    "presence_balcon": "category",
    "avancee_masque_max": "category",
    "perc_surf_vitree_ext": "float",
    "inertie": "category",
    "type_ventilation": "category",
    "gen_ch_lib_simp": "category",
    "gen_ecs_lib_simp": "category",
    "mix_energetique_ch": "category",
    "mix_energetique_ecs": "category",
    "type_installation_ch": "category",
    "type_installation_ecs": "category",
    "tr002_type_batiment_id": "category",
    "consommation_energie": "float",
    "estimation_ges": "float",
    "classe_consommation_energie": "category",
    "classe_estimation_ges": "category",

}


def rename_dpe_table_light(table, reformat_gorenove=False):
    rep = {'epaisseur': 'ep',
           'isolation': 'isol',
           'resistance_thermique': 'res_therm',
           'uniforme_': '',
           'facteur_solaire_corr': 'fs',
           'exterieur': 'ext',

           'chauffage': 'ch',
           'baie_baie_vitree': 'baie',
           'baie_vitree': 'baie',
           'materiaux': 'mat'}

    remove = ['_infer', '_top', '_avg', '_orientee', '_concat']

    compo = ['mur', 'pb', 'ph', 'baie', 'ch', 'ecs']

    cols = list()
    for col in table:
        for el in rep.items():
            col = col.replace(*el)
        for el in remove:
            col = col.replace(el, '')
        if reformat_gorenove:
            for el in compo:
                if el in col:
                    col = col.replace(el + '_', '')
                    col = col.replace('_' + el, '')
                    col = el + '_' + col

        cols.append(col)
    table.columns = cols
    return table

def concat_td001_gorenove(td001, td001_list):
    """
    concatenation des td001_annexes en une seule table avec les variables gorenove.

    Parameters
    ----------
    td001_list

    Returns
    -------

    """
    td001 = td001.copy()

    td001 = rename_dpe_table_light(td001)

    td001 = td001.rename(columns={'id': 'td001_dpe_id'})

    td001 = td001.drop_duplicates('numero_dpe', keep='last')
    sel = {k: v for k, v in gorenove_types.items() if k in td001}
    td001 = td001[list(sel.keys()) + ['td001_dpe_id']].astype(sel)

    for table in td001_list:
        table = rename_dpe_table_light(table).reset_index()
        sel = {k: v for k, v in gorenove_types.items() if k in table}
        table = table[list(sel.keys()) + ['td001_dpe_id']].astype(sel)
        not_common_cols = ['td001_dpe_id']+[el for el in td001 if el not in table]
        td001 = td001[not_common_cols].merge(table, on='td001_dpe_id', how='left')
    return td001



