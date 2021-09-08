import pandas as pd
import numpy as np

from .td003_td005_text_extraction import extract_td003_baie_variables, extract_td003_murs_variables, \
    extract_td003_pb_variables, extract_td003_ph_variables, extract_td005_baie_variables, extract_td005_murs_variables, \
    extract_td005_pb_variables, extract_td005_ph_variables
from .text_matching_dict import *
from .gorenove_scripts import rename_dpe_table_light

def main_advanced_enveloppe_processing(td001,td003, td005,env_compo_agg_dict):
    env_dict_cols = {
        'td007_mur_agg_annexe': ['td001_dpe_id','u_mur_exterieur_top', 'mat_mur_exterieur_top', 'ep_mat_mur_exterieur_top'],
        "td007_ph_agg_annexe": ['td001_dpe_id','type_adjacence_ph_top', 'u_ph_top', 'mat_ph_top'],
        "td007_pb_agg_annexe": ['td001_dpe_id','type_adjacence_pb_top', 'u_pb_top', 'mat_pb_top'],
        "td010_pont_thermique_agg_annexe": ['td001_dpe_id','pos_isol_mur_ext', 'pos_isol_pb', 'pos_isol_ph'],
        "td008_baie_agg_annexe": ['td001_dpe_id','u_baie_baie_vitree_top', 'facteur_solaire_corr_baie_vitree_top',
                      'type_vitrage_baie_vitree_top', 'remplissage_baie_vitree_top', 'mat_baie_vitree_top',
                      'orientation_baie_infer', 'avancee_masque_max','type_occultation_baie_vitree_top',"u_baie_porte_top",
                      "presence_balcon"],
    }

    td001_env = td001[['td001_dpe_id']]

    for k, v in env_dict_cols.items():
        td001_env = td001_env.merge(env_compo_agg_dict[k][v], on='td001_dpe_id', how='left')

    td001_env = rename_dpe_table_light(td001_env)
    final_cols = td001_env.columns
    # ELASTIC SEARCH descriptif et fiches techniques
    print('ES : murs')
    materiau_mur_ft, isolation_mur_ft = extract_td005_murs_variables(td005)
    print('ES : murs')

    materiau_mur_desc, isolation_mur_desc = extract_td003_murs_variables(td003)
    print('ES : ph')

    materiau_ph_ft, isolation_ph_ft = extract_td005_ph_variables(td005)
    print('ES : ph')

    materiau_ph_desc, isolation_ph_desc = extract_td003_ph_variables(td003)
    print('ES : pb')

    materiau_pb_ft, isolation_pb_ft = extract_td005_pb_variables(td005)
    print('ES : pb')

    materiau_pb_desc, isolation_pb_desc = extract_td003_pb_variables(td003)
    print('ES : vitrage')

    type_vitrage_ft, type_remplissage_ft, materiau_baie_ft, orientation_baie_ft = extract_td005_baie_variables(td005)
    print('ES : vitrage')

    type_vitrage_desc, type_remplissage_desc, materiau_baie_desc, orientation_baie_desc = extract_td003_baie_variables(
        td003)
    print('fusion')

    td001_env = concat_mur_txt(td001_env, materiau_mur_ft=materiau_mur_ft, materiau_mur_desc=materiau_mur_desc,
                               isolation_mur_ft=isolation_mur_ft, isolation_mur_desc=isolation_mur_desc)
    td001_env = concat_pb_txt(td001_env, materiau_pb_ft=materiau_pb_ft, materiau_pb_desc=materiau_pb_desc,
                              isolation_pb_ft=isolation_pb_ft, isolation_pb_desc=isolation_pb_desc)
    td001_env = concat_ph_txt(td001_env, materiau_ph_ft=materiau_ph_ft, materiau_ph_desc=materiau_ph_desc,
                              isolation_ph_ft=isolation_ph_ft, isolation_ph_desc=isolation_ph_desc)
    td001_env = concat_baie_txt(td001_env, type_vitrage_desc=type_vitrage_desc, type_vitrage_ft=type_vitrage_ft,
                                type_remplissage_desc=type_remplissage_desc, type_remplissage_ft=type_remplissage_ft,
                                materiau_baie_desc=materiau_baie_desc, materiau_baie_ft=materiau_baie_ft,
                                orientation_baie_ft=orientation_baie_ft, orientation_baie_desc=orientation_baie_desc)

    # SUB BY TXT
    vars_to_sub = ['mat_mur_ext', 'pos_isol_mur_ext', 'mat_ph',
                   'pos_isol_ph', 'mat_pb',
                   'pos_isol_pb', 'type_vitrage_baie',
                   'remplissage_baie', 'mat_baie', 'orientation_baie']

    # HYPOTHESE les données structurées sont plus fiables que les données textes sur l'enveloppe
    for var in vars_to_sub:
        is_null = (td001_env[var].isnull()) | (td001_env[var] == 'inconnu') | (td001_env[var] == 'indetermine')| (td001_env[var] == 'INCOHERENT')
        td001_env.loc[is_null, var] = td001_env.loc[is_null, var + '_txt']
        is_null = (td001_env[var].isnull()) | (td001_env[var] == 'inconnu') | (td001_env[var] == 'indetermine')| (td001_env[var] == 'INCOHERENT')
        td001_env.loc[is_null, var] = np.nan

    # corr isolation

    is_non_isole = td001_env['u_mur_ext'] > 1.5

    td001_env.loc[is_non_isole, 'pos_isol_mur'] = 'non isole'

    td001_env.pos_isol_mur = td001_env.pos_isol_mur.replace('Non isolé', 'non isole')

    is_non_isole = td001_env['u_pb'] > 1.5

    td001_env.loc[is_non_isole, 'pos_isol_pb'] = 'non isole'

    td001_env.pos_isol_pb = td001_env.pos_isol_pb.replace('Non isolé', 'non isole')

    is_non_isole = td001_env['u_ph'] > 1.5

    td001_env.loc[is_non_isole, 'pos_isol_ph'] = 'non isole'

    td001_env.pos_isol_ph = td001_env.pos_isol_ph.replace('Non isolé', 'non isole')

    return td001_env[final_cols]

def concat_mur_txt(td001_env, materiau_mur_desc, materiau_mur_ft, isolation_mur_desc, isolation_mur_ft):

    materiau_mur_from_txt = pd.concat([materiau_mur_desc[['label', 'td001_dpe_id']],
                                           materiau_mur_ft[['label', 'td001_dpe_id']]], axis=0)

    materiau_mur_from_txt['label'] = pd.Categorical(materiau_mur_from_txt.label,
                                                    categories=list(murs_materiau_search_dict.keys()) + ['indetermine'],
                                                    ordered=True)

    materiau_mur_from_txt = materiau_mur_from_txt.sort_values(by=['td001_dpe_id', 'label'])

    materiau_mur_from_txt = materiau_mur_from_txt.drop_duplicates(subset=['td001_dpe_id'], keep='first')
    # suppression de la dénomination exact qui ne correpond qu'a une méthode de recherche plus rigide
    materiau_mur_from_txt.label = materiau_mur_from_txt.label.str.replace(' exact', '')

    td001_env = td001_env.merge(materiau_mur_from_txt.rename(columns={"label": 'mat_mur_ext_txt'}),
                                on='td001_dpe_id', how='left')

    isolation_mur_from_txt = pd.concat([isolation_mur_desc[['label', 'td001_dpe_id']],
                                        isolation_mur_ft[['label', 'td001_dpe_id']]], axis=0)

    isolation_mur_from_txt['label'] = pd.Categorical(isolation_mur_from_txt.label,
                                                     categories=list(isolation_search_dict.keys()) + ['indetermine'],
                                                     ordered=True)

    isolation_mur_from_txt = isolation_mur_from_txt.sort_values(by=['td001_dpe_id', 'label'])

    isolation_mur_from_txt = isolation_mur_from_txt.drop_duplicates(subset=['td001_dpe_id'], keep='first')
    # suppression de la dénomination exact qui ne correpond qu'a une méthode de recherche plus rigide
    isolation_mur_from_txt.label = isolation_mur_from_txt.label.str.replace(' exact', '')

    td001_env = td001_env.merge(isolation_mur_from_txt.rename(columns={"label": 'pos_isol_mur_ext_txt'}),
                                on='td001_dpe_id', how='left')

    return td001_env


def concat_ph_txt(td001_env, materiau_ph_desc, materiau_ph_ft, isolation_ph_desc, isolation_ph_ft):
    materiau_ph_from_txt = pd.concat([materiau_ph_desc[['label', 'td001_dpe_id']],
                                      materiau_ph_ft[['label', 'td001_dpe_id']]], axis=0)

    materiau_ph_from_txt['label'] = pd.Categorical(materiau_ph_from_txt.label,
                                                   categories=list(ph_materiau_search_dict.keys()) + ['indetermine'],
                                                   ordered=True)

    materiau_ph_from_txt = materiau_ph_from_txt.sort_values(by=['td001_dpe_id', 'label'])

    materiau_ph_from_txt = materiau_ph_from_txt.drop_duplicates(subset=['td001_dpe_id'], keep='first')
    # suppression de la dénomination exact qui ne correpond qu'a une méthode de recherche plus rigide
    materiau_ph_from_txt.label = materiau_ph_from_txt.label.str.replace(' exact', '')

    td001_env = td001_env.merge(materiau_ph_from_txt.rename(columns={"label": 'mat_ph_txt'}),
                                on='td001_dpe_id', how='left')

    isolation_ph_from_txt = pd.concat([isolation_ph_desc[['label', 'td001_dpe_id']],
                                       isolation_ph_ft[['label', 'td001_dpe_id']]], axis=0)

    isolation_ph_from_txt['label'] = pd.Categorical(isolation_ph_from_txt.label,
                                                    categories=list(isolation_search_dict.keys()) + ['indetermine'],
                                                    ordered=True)

    isolation_ph_from_txt = isolation_ph_from_txt.sort_values(by=['td001_dpe_id', 'label'])

    isolation_ph_from_txt = isolation_ph_from_txt.drop_duplicates(subset=['td001_dpe_id'], keep='first')
    # suppression de la dénomination exact qui ne correpond qu'a une méthode de recherche plus rigide
    isolation_ph_from_txt.label = isolation_ph_from_txt.label.str.replace(' exact', '')

    td001_env = td001_env.merge(isolation_ph_from_txt.rename(columns={"label": 'pos_isol_ph_txt'}),
                                on='td001_dpe_id', how='left')
    return td001_env


def concat_pb_txt(td001_env, materiau_pb_desc, materiau_pb_ft, isolation_pb_desc, isolation_pb_ft):
    materiau_pb_from_txt = pd.concat([materiau_pb_desc[['label', 'td001_dpe_id']],
                                      materiau_pb_ft[['label', 'td001_dpe_id']]], axis=0)

    materiau_pb_from_txt['label'] = pd.Categorical(materiau_pb_from_txt.label,
                                                   categories=list(pb_materiau_search_dict.keys()),
                                                   ordered=True)

    materiau_pb_from_txt = materiau_pb_from_txt.sort_values(by=['td001_dpe_id', 'label'])

    materiau_pb_from_txt = materiau_pb_from_txt.drop_duplicates(subset=['td001_dpe_id'], keep='first')
    # suppression de la dénomination exact qui ne correpond qu'a une méthode de recherche plus rigide
    materiau_pb_from_txt.label = materiau_pb_from_txt.label.str.replace(' exact', '')

    td001_env = td001_env.merge(materiau_pb_from_txt.rename(columns={"label": 'mat_pb_txt'}),
                                on='td001_dpe_id', how='left')

    isolation_pb_from_txt = pd.concat([isolation_pb_desc[['label', 'td001_dpe_id']],
                                       isolation_pb_ft[['label', 'td001_dpe_id']]], axis=0)

    isolation_pb_from_txt['label'] = pd.Categorical(isolation_pb_from_txt.label,
                                                    categories=list(isolation_search_dict.keys()) + ['indetermine'],
                                                    ordered=True)

    isolation_pb_from_txt = isolation_pb_from_txt.sort_values(by=['td001_dpe_id', 'label'])

    isolation_pb_from_txt = isolation_pb_from_txt.drop_duplicates(subset=['td001_dpe_id'], keep='first')
    # suppression de la dénomination exact qui ne correpond qu'a une méthode de recherche plus rigide
    isolation_pb_from_txt.label = isolation_pb_from_txt.label.str.replace(' exact', '')

    td001_env = td001_env.merge(isolation_pb_from_txt.rename(columns={"label": 'pos_isol_pb_txt'}),
                                on='td001_dpe_id', how='left')
    return td001_env


def concat_baie_txt(td001_env, type_vitrage_desc, type_vitrage_ft, type_remplissage_desc, type_remplissage_ft,
                    materiau_baie_desc, materiau_baie_ft, orientation_baie_desc, orientation_baie_ft):
    # libéllés des type vitrage issues des données textes
    type_vitrage_from_txt = pd.concat([type_vitrage_desc[['label', 'td001_dpe_id']],
                                       type_vitrage_ft[['label', 'td001_dpe_id']]], axis=0)

    type_vitrage_from_txt['label'] = pd.Categorical(type_vitrage_from_txt.label,
                                                    categories=list(type_vitrage_search_dict.keys()) + ['indetermine'],
                                                    ordered=True)

    type_vitrage_from_txt = type_vitrage_from_txt.sort_values(by=['td001_dpe_id', 'label'])

    type_vitrage_from_txt = type_vitrage_from_txt.drop_duplicates(subset=['td001_dpe_id'], keep='first')
    # suppression de la dénomination exact qui ne correpond qu'a une méthode de recherche plus rigide
    type_vitrage_from_txt.label = type_vitrage_from_txt.label.str.replace(' exact', '')

    td001_env = td001_env.merge(type_vitrage_from_txt.rename(columns={"label": 'type_vitrage_baie_txt'}),
                                on='td001_dpe_id', how='left')

    # libéllés du remplissage des vitrage issues des données textes
    type_remplissage_from_txt = pd.concat([type_remplissage_desc[['label', 'td001_dpe_id']],
                                           type_remplissage_ft[['label', 'td001_dpe_id']]], axis=0)

    type_remplissage_from_txt['label'] = pd.Categorical(type_remplissage_from_txt.label,
                                                        categories=list(type_remplissage_search_dict.keys()) + [
                                                            'indetermine'],
                                                        ordered=True)

    type_remplissage_from_txt = type_remplissage_from_txt.sort_values(by=['td001_dpe_id', 'label'])

    type_remplissage_from_txt = type_remplissage_from_txt.drop_duplicates(subset=['td001_dpe_id'], keep='first')
    # suppression de la dénomination exact qui ne correpond qu'a une méthode de recherche plus rigide
    type_remplissage_from_txt.label = type_remplissage_from_txt.label.str.replace(' exact', '')

    td001_env = td001_env.merge(type_remplissage_from_txt.rename(columns={"label": 'remplissage_baie_txt'}),
                                on='td001_dpe_id', how='left')

    # libéllés des materiau de baie issues des données textes
    materiau_baie_from_txt = pd.concat([materiau_baie_desc[['label', 'td001_dpe_id']],
                                        materiau_baie_ft[['label', 'td001_dpe_id']]], axis=0)

    materiau_baie_from_txt['label'] = pd.Categorical(materiau_baie_from_txt.label,
                                                     categories=list(materiau_baie_search_dict.keys()) + [
                                                         'indetermine'],
                                                     ordered=True)

    materiau_baie_from_txt = materiau_baie_from_txt.sort_values(by=['td001_dpe_id', 'label'])

    materiau_baie_from_txt = materiau_baie_from_txt.drop_duplicates(subset=['td001_dpe_id'], keep='first')
    # suppression de la dénomination exact qui ne correpond qu'a une méthode de recherche plus rigide
    materiau_baie_from_txt.label = materiau_baie_from_txt.label.str.replace(' exact', '')

    td001_env = td001_env.merge(materiau_baie_from_txt.rename(columns={"label": 'mat_baie_txt'}),
                                on='td001_dpe_id', how='left')

    # libéllés des orientation baie issues des données textes
    orientation_baie_from_txt = pd.concat([orientation_baie_desc[['label', 'td001_dpe_id']],
                                           orientation_baie_ft[['label', 'td001_dpe_id']]], axis=0)

    orientation_baie_from_txt['label'] = pd.Categorical(orientation_baie_from_txt.label,
                                                        categories=list(orientation_baie_search_dict.keys()),
                                                        ordered=True)

    orientation_baie_from_txt = orientation_baie_from_txt.groupby('td001_dpe_id').label.apply(
        lambda x: ' + '.join(sorted(list(set(x))))).to_frame('orientation_baie_txt')

    td001_env = td001_env.merge(orientation_baie_from_txt,
                                on='td001_dpe_id', how='left')
    return td001_env
