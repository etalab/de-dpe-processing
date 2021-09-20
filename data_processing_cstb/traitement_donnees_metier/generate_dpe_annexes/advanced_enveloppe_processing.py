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

    # calcul traversant


    td001_env = calculate_traversant(td001_env,td001,env_compo_agg_dict)

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

    # final columns

    final_cols = td001_env.columns + [col for col in td001_env.columns if '_txt' in col]

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

def calculate_traversant(td001_env,td001,env_compo_agg_dict):
    """
    RT2005
    Un logement est dit traversant si, pour chaque orientation
    (verticale nord, vertical est, verticale sud, verticale ouest, horizontale)
    la surface des baies est inférieure à 75 % de la surface totale des baies.
    https://www.legifrance.gouv.fr/jorf/jo/2006/05/25/0121
    """

    surfaces_agg_essential = env_compo_agg_dict['surfaces_agg_essential_annexe'].copy()

    for surf in ['surf_vitree_est', 'surf_vitree_nord', 'surf_vitree_ouest', 'surf_vitree_sud', 'surf_vitree_est_ou_ouest']:
        surfaces_agg_essential[f'perc_temp_{surf}'] = surfaces_agg_essential[surf] / surfaces_agg_essential['surf_vitree_totale']

    surfaces_agg_essential['perc_temp_surf_vitree_indeterminee'] = (surfaces_agg_essential.surf_vitree_horizontale + surfaces_agg_essential.surf_vitree_indetermine) / surfaces_agg_essential[
        'surf_vitree_totale']
    is_traversant = (surfaces_agg_essential.filter(like='perc_temp').fillna(0) < 0.75).min(axis=1)

    null_traversant = (surfaces_agg_essential.filter(like='perc_temp').isnull().mean(axis=1) == 1) | (surfaces_agg_essential['perc_temp_surf_vitree_indeterminee'] >= 0.75)
    is_traversant = is_traversant & (~null_traversant)
    surfaces_agg_essential['traversant'] = is_traversant.replace({True: 'traversant', False: 'non traversant'})
    surfaces_agg_essential.loc[null_traversant, 'traversant'] = np.nan

    traversant_nord_sud = td001_env.orientation_baie.str.contains('sud') & td001_env.orientation_baie.str.contains('nord')
    traversant_est_ouest = td001_env.orientation_baie.str.contains('ouest') & td001_env.orientation_baie.str.contains('est')
    traversant_2_facades = td001_env.orientation_baie.str.count('\+') >= 1
    traversant_3_facades = td001_env.orientation_baie.str.count('\+') >= 2
    traversant_4_facades = traversant_nord_sud & traversant_est_ouest
    non_traversant = td001_env.orientation_baie.str.count('\+') == 0

    traversant = td001_env[['td001_dpe_id']]
    traversant = traversant.merge(td001[['tr002_type_batiment_id', 'td001_dpe_id']], on='td001_dpe_id', how='left')
    traversant = traversant.merge(surfaces_agg_essential[['traversant', 'td001_dpe_id']], on='td001_dpe_id', how='left')
    null_traversant = (traversant.traversant.isnull())
    is_traversant = (traversant.traversant == "traversant")
    is_traversant_or_null = null_traversant | (traversant.traversant == 'traversant')
    traversant.loc[traversant_nord_sud & is_traversant_or_null, "traversant"] = 'traversant nord sud'
    traversant.loc[traversant_est_ouest & is_traversant_or_null, "traversant"] = 'traversant est ouest'
    traversant.loc[traversant_2_facades & is_traversant_or_null, "traversant"] = 'traversant 90°'
    traversant.loc[traversant_3_facades & is_traversant_or_null, "traversant"] = 'traversant tout venant'
    traversant.loc[traversant_4_facades & is_traversant_or_null, "traversant"] = 'traversant tout venant'
    traversant.loc[traversant_nord_sud & null_traversant, "traversant"] = 'traversant nord sud (faible)'
    traversant.loc[traversant_est_ouest & null_traversant, "traversant"] = 'traversant est ouest (faible)'
    traversant.loc[traversant_2_facades & null_traversant, "traversant"] = 'traversant 90° (faible)'
    traversant.loc[traversant_3_facades & null_traversant, "traversant"] = 'traversant tout venant (faible)'
    traversant.loc[traversant_4_facades & null_traversant, "traversant"] = 'traversant tout venant (faible)'
    traversant.loc[non_traversant, 'traversant'] = "non traversant"

    return td001_env.merge(traversant[['traversant', 'td001_dpe_id']], on='td001_dpe_id', how='left')