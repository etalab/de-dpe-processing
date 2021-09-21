import pandas as pd
import numpy as np
from generate_dpe_annexes.text_matching_dict import enr_search_dict, type_ventilation_search_dict, presence_climatisation_search_dict
from generate_dpe_annexes.td003_td005_text_extraction import extract_td003_td005_ventilation_variables, \
    extract_td003_td005_climatisation_variables, extract_td003_td005_enr_variables

def main_advanced_general_processing(td001, td003, td005, td001_td006):

    td001_gen = td001.merge(td001_td006, on='td001_dpe_id', how='left')

    # ELASTIC SEARCH descriptif et fiches techniques

    type_ventilation_ft, type_ventilation_desc = extract_td003_td005_ventilation_variables(
        td003, td005)

    presence_climatisation_ft, presence_climatisation_desc = extract_td003_td005_climatisation_variables(
        td003, td005)

    enr_ft, enr_desc = extract_td003_td005_enr_variables(
        td003, td005)

    # libéllés des generateurs chauffage issues des données textes
    type_ventilation_from_txt = pd.concat([type_ventilation_desc[['label', 'td001_dpe_id']],
                                           type_ventilation_ft[['label', 'td001_dpe_id']]], axis=0)

    type_ventilation_from_txt['label'] = pd.Categorical(type_ventilation_from_txt.label,
                                                        categories=list(type_ventilation_search_dict.keys()) + [
                                                            'indetermine'],
                                                        ordered=True)

    type_ventilation_from_txt = type_ventilation_from_txt.sort_values(by=['td001_dpe_id', 'label'])

    type_ventilation_from_txt = type_ventilation_from_txt.drop_duplicates(subset=['td001_dpe_id'], keep='first')
    # suppression de la dénomination exact qui ne correpond qu'a une méthode de recherche plus rigide
    type_ventilation_from_txt.label = type_ventilation_from_txt.label.str.replace(' exact', '')

    td001_gen = td001_gen.merge(type_ventilation_from_txt.rename(columns={"label": 'type_ventilation_txt'}),
                                on='td001_dpe_id', how='left')

    ## climatisation

    # libéllés des generateurs chauffage issues des données textes
    presence_climatisation_from_txt = pd.concat([presence_climatisation_desc[['label', 'td001_dpe_id']],
                                                 presence_climatisation_ft[['label', 'td001_dpe_id']]], axis=0)

    presence_climatisation_from_txt['label'] = pd.Categorical(presence_climatisation_from_txt.label,
                                                              categories=list(
                                                                  presence_climatisation_search_dict.keys()) + [
                                                                             'indetermine'],
                                                              ordered=True)

    presence_climatisation_from_txt = presence_climatisation_from_txt.sort_values(by=['td001_dpe_id', 'label'])

    presence_climatisation_from_txt = presence_climatisation_from_txt.drop_duplicates(subset=['td001_dpe_id'],
                                                                                      keep='first')

    td001_gen = td001_gen.merge(presence_climatisation_from_txt.rename(columns={"label": 'presence_climatisation'}),
                                on='td001_dpe_id', how='left')

    ## ENR

    # libéllés des generateurs chauffage issues des données textes
    enr_from_txt = pd.concat([enr_desc[['label', 'td001_dpe_id']],
                              enr_ft[['label', 'td001_dpe_id']]], axis=0)
    enr_from_txt.label = enr_from_txt.label.replace('solaire thermique', 'solaire thermique (ecs)')
    enr_from_txt['label'] = pd.Categorical(enr_from_txt.label,
                                           categories=list(enr_search_dict.keys()),
                                           ordered=True)

    enr_from_txt = enr_from_txt.groupby('td001_dpe_id').label.apply(
        lambda x: ' + '.join(sorted(list(set(x))))).to_frame('enr')

    td001_gen = td001_gen.merge(enr_from_txt,
                                on='td001_dpe_id', how='left')

    is_null = td001_gen['type_ventilation'].isnull()
    td001_gen.loc[is_null, 'type_ventilation'] = td001_gen.loc[is_null, 'type_ventilation_txt']

    td001_gen.presence_climatisation = (~td001_gen.presence_climatisation.isnull()).astype(int)

    useful_cols = ['td001_dpe_id','type_prise_air', 'type_ventilation',
       'inertie','presence_climatisation','enr']
    td001_processing_cols = ['type_batiment','coherence_data_methode_dpe', 'nom_methode_dpe_norm','periode_construction','classe_consommation_energie',
                             'classe_estimation_ges','is_conso_table', 'is_facture_table', 'is_neuf_table', 'is_paroi_opaque_table', 'is_systeme_ch_table','is_3cl','is_data_model_propre']
    useful_cols = td001_processing_cols+useful_cols

    return td001_gen[useful_cols]
