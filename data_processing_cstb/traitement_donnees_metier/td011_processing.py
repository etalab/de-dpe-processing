import pandas as pd
import numpy as np
from utils import concat_string_cols
from assets_orm import DPEMetaData

td011_types={'td011_installation_chauffage_id': 'str',
 'td006_batiment_id': 'str',
 'tr003_type_installation_chauffage_id': 'category',
 'surface_chauffee': 'float',
 'nombre_appartements_echantillon': 'float',
 'surface_habitable_echantillon': 'float',
 'tv025_intermittence_id': 'category',
 'td001_dpe_id': 'str'}

def merge_td011_tr_tv(td011):
    meta = DPEMetaData()
    table = td011.copy()
    table = meta.merge_all_tr_table(table)
    table = meta.merge_all_tv_table(table)
    table = table.astype(td011_types)
    table = table.rename(columns={'id': 'td007_paroi_opaque_id'})

    return table


def agg_systeme_chauffage_essential(td001,td011,td012):

    sys_ch_principal_rename = {
        'td001_dpe_id': 'td001_dpe_id',
        'gen_ch_lib_infer': 'sys_ch_principal_gen_ch_lib_infer',
        'gen_ch_lib_infer_simp': 'sys_ch_principal_gen_ch_lib_infer_simp',
        'type_energie_chauffage': 'sys_ch_principal_type_energie_chauffage',
        'consommation_chauffage': 'sys_ch_principal_consommation_chauffage',
        "tv025_Type d'installation": 'sys_ch_principal_type_installation_chauffage',
        'nombre_generateurs': 'sys_ch_principal_nb_generateur'
    }

    sys_ch_secondaire_rename = {
        'td001_dpe_id': 'td001_dpe_id',
        'gen_ch_lib_infer': 'sys_ch_secondaire_gen_ch_lib_infer',
        'gen_ch_lib_infer_simp': 'sys_ch_secondaire_gen_ch_lib_infer_simp',
        'type_energie_chauffage': 'sys_ch_secondaire_type_energie_chauffage',
        'consommation_chauffage': 'sys_ch_secondaire_consommation_chauffage',
        "tv025_Type d'installation": 'sys_ch_secondaire_type_installation_chauffage',
        'nombre_generateurs': 'sys_ch_secondaire_nb_generateur'
    }

    sys_ch_tertiaire_concat_rename = {
        'td001_dpe_id': 'td001_dpe_id',
        'gen_ch_lib_infer': 'sys_ch_tertiaire_gen_ch_lib_infer_concat',
        'gen_ch_lib_infer_simp': 'sys_ch_tertiaire_gen_ch_lib_infer_simp_concat',
        'type_energie_chauffage': 'sys_ch_tertiaire_type_energie_chauffage_concat',
        'consommation_chauffage': 'sys_ch_tertiaire_consommation_chauffage',
        "tv025_Type d'installation": 'sys_ch_tertiaire_type_installation_chauffage_concat',
        'nombre_generateurs': 'sys_ch_tertiaire_nb_generateurs'
    }

    td012 = td012.merge(
        td011[['td011_installation_chauffage_id', 'tv025_intermittence_id', "tv025_Type d'installation"]],
        on='td011_installation_chauffage_id', how='left')
    table = td012
    rendement_gen_u = table[['rendement_generation', 'coefficient_performance']].max(axis=1)

    s_rendement = pd.Series(index=table.index)
    s_rendement[:] = 1
    for rendement in ['rendement_distribution_systeme_chauffage',
                      'rendement_emission_systeme_chauffage']:
        r = table[rendement].astype(float)
        r[r == 0] = 1
        r[r.isnull()] = 1
        s_rendement = s_rendement * r

    rendement_gen_u[rendement_gen_u == 0] = 1
    rendement_gen_u[rendement_gen_u.isnull()] = 1
    s_rendement = s_rendement * rendement_gen_u
    table['besoin_chauffage_infer'] = table['consommation_chauffage'] * s_rendement

    table['nombre_generateurs'] = 1

    cols = ['td001_dpe_id', 'gen_ch_lib_infer_simp', 'gen_ch_lib_infer', 'type_energie_chauffage',
            'consommation_chauffage', 'besoin_chauffage_infer']
    cols += ['tv025_intermittence_id', "tv025_Type d'installation", 'nombre_generateurs', 'id_unique']

    agg_cols = ['td001_dpe_id', 'gen_ch_lib_infer', 'tv025_intermittence_id']

    table['id_unique'] = table.td001_dpe_id + table.gen_ch_lib_infer + table.tv025_intermittence_id.astype(str)

    is_unique = table.groupby('td001_dpe_id').td001_dpe_id.count() == 1
    ist_unique = is_unique[is_unique].index

    table_gen_unique = table.loc[table.td001_dpe_id.isin(ist_unique)][cols]

    table_gen_multiple = table.loc[~table.td001_dpe_id.isin(ist_unique)][cols]

    table_gen_multiple[agg_cols] = table_gen_multiple[agg_cols].astype(str)

    agg = table_gen_multiple.groupby(agg_cols).agg({
        'gen_ch_lib_infer_simp': 'first',
        'type_energie_chauffage': 'first',
        'consommation_chauffage': 'sum',
        'besoin_chauffage_infer': 'sum',
        "tv025_Type d'installation": 'first',
        "nombre_generateurs": 'sum'

    }).reset_index()

    agg['id_unique'] = agg.td001_dpe_id + agg.gen_ch_lib_infer + agg.tv025_intermittence_id

    sys_principal = agg.sort_values('consommation_chauffage', ascending=False).drop_duplicates(subset='td001_dpe_id')

    id_sys_principal = sys_principal.id_unique.unique().tolist()

    sys_secondaires = agg.loc[~agg.id_unique.isin(id_sys_principal)]

    sys_secondaire = sys_secondaires.sort_values('consommation_chauffage', ascending=False).drop_duplicates(
        'td001_dpe_id')

    id_sys_secondaire = sys_secondaire.id_unique.unique().tolist()

    sys_tertiaires = agg.loc[~agg.id_unique.isin(id_sys_principal + id_sys_secondaire)]

    sys_tertiaire_concat = sys_tertiaires.groupby('td001_dpe_id').agg({
        'gen_ch_lib_infer': lambda x: ' + '.join(list(set(x))),

        'gen_ch_lib_infer_simp': lambda x: ' + '.join(list(set(x))),
        'type_energie_chauffage': lambda x: ' + '.join(list(set(x))),
        'consommation_chauffage': 'sum',
        'besoin_chauffage_infer': 'sum',
        "tv025_Type d'installation": lambda x: ' + '.join(list(set(x))),
        'nombre_generateurs': 'sum'
    }).reset_index()

    sys_principal = sys_principal.append(table_gen_unique[sys_principal.columns])

    sys_principal = sys_principal.rename(columns=sys_ch_principal_rename)[sys_ch_principal_rename.values()]

    sys_secondaire = sys_secondaire.rename(columns=sys_ch_secondaire_rename)[sys_ch_secondaire_rename.values()]

    sys_tertiaire_concat = sys_tertiaire_concat.rename(columns=sys_ch_tertiaire_concat_rename)[
        sys_ch_tertiaire_concat_rename.values()]

    td001_sys_ch = td001[['td001_dpe_id']].merge(sys_principal, on='td001_dpe_id', how='left')
    td001_sys_ch = td001_sys_ch.merge(sys_secondaire, on='td001_dpe_id', how='left')
    td001_sys_ch = td001_sys_ch.merge(sys_tertiaire_concat, on='td001_dpe_id', how='left')
    nb_installation = td011.groupby('td001_dpe_id').td011_installation_chauffage_id.count().to_frame(
        'nombre_installations')
    td001_sys_ch = td001_sys_ch.merge(nb_installation, on='td001_dpe_id', how='left')

    cols_end = sys_principal.columns.tolist() + sys_secondaire.columns.tolist() + sys_tertiaire_concat.columns.tolist()
    cols_end = np.unique(cols_end).tolist()

    td001_sys_ch['nombre_generateur_total'] = td001_sys_ch.sys_ch_principal_nb_generateur
    td001_sys_ch['nombre_generateur_total'] += td001_sys_ch.sys_ch_secondaire_nb_generateur.fillna(0)
    td001_sys_ch['nombre_generateur_total'] += td001_sys_ch.sys_ch_tertiaire_nb_generateurs.fillna(0)


    cols = ['sys_ch_principal_type_energie_chauffage',
            'sys_ch_secondaire_type_energie_chauffage',
            'sys_ch_tertiaire_type_energie_chauffage_concat']

    td001_sys_ch['mix_energetique_chauffage'] = concat_string_cols(td001_sys_ch, cols=cols, join_string=' + ',
                                                                   is_unique=True, is_sorted=True)

    cols = ['sys_ch_principal_type_installation_chauffage',
            'sys_ch_secondaire_type_installation_chauffage',
            'sys_ch_tertiaire_type_installation_chauffage_concat']

    td001_sys_ch['type_installation_chauffage_concat'] = concat_string_cols(td001_sys_ch, cols=cols, join_string=' + ',
                                                                            is_unique=True, is_sorted=True)

    cols = ['sys_ch_principal_gen_ch_lib_infer',
            'sys_ch_secondaire_gen_ch_lib_infer',
            'sys_ch_tertiaire_gen_ch_lib_infer_concat']

    td001_sys_ch['gen_ch_lib_infer_concat'] = concat_string_cols(td001_sys_ch, cols=cols, join_string=' + ',
                                                                 is_unique=True, is_sorted=True)

    cols = ['sys_ch_principal_gen_ch_lib_infer_simp',
            'sys_ch_secondaire_gen_ch_lib_infer_simp',
            'sys_ch_tertiaire_gen_ch_lib_infer_simp_concat']

    td001_sys_ch['gen_ch_lib_infer_simp_concat'] = concat_string_cols(td001_sys_ch, cols=cols, join_string=' + ',
                                                                      is_unique=True, is_sorted=True)

    isnull = td001_sys_ch.sys_ch_principal_nb_generateur.isnull()
    is_multiple_install = td001_sys_ch.nombre_installations > 1
    td001_sys_ch.loc[isnull, 'configuration_sys_chauffage'] = pd.NA
    td001_sys_ch.loc[~isnull, 'configuration_sys_chauffage'] = 'générateur unique/installation unique'
    isnull = td001_sys_ch.sys_ch_secondaire_nb_generateur.isnull()
    td001_sys_ch.loc[~isnull, 'configuration_sys_chauffage'] = 'générateurs multiples/installation unique'
    td001_sys_ch.loc[(~isnull) & (
        is_multiple_install), 'configuration_sys_chauffage'] = 'générateurs multiples/installation multiples'

    cols_first = [el for el in td001_sys_ch.columns.tolist() if el not in cols_end]

    cols = cols_first + cols_end

    return td001_sys_ch[cols]