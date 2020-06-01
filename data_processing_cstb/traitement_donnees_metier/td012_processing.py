import numpy as np
import pandas as pd
from utils import agg_pond_avg,agg_pond_top_freq,clean_str,strip_accents,affect_lib_by_matching_score
td012_types = {'id': 'str',
               'systeme_chauffage_cogeneration_id': 'string',
               'td011_installation_chauffage_id': 'str',
               'tr004_type_energie_id': 'category',
               'tv045_conversion_kwh_co2_id': 'category',
               'tv046_evaluation_contenu_co2_reseaux_id': 'category',
               'rendement_emission_systeme_chauffage': 'float',
               'tv028_rendement_emission_systeme_chauffage_id': 'category',
               'rendement_distribution_systeme_chauffage': 'str',
               'tv029_rendement_distribution_systeme_chauffage_id': 'category',
               'tv030_rendement_regulation_systeme_chauffage_id': 'category',
               'rendement_generation': 'float',
               'tv031_rendement_generation_id': 'category',
               'presence_regulation': 'int',
               'coefficient_performance': 'float',
               'tv032_coefficient_performance_id': 'category',
               'tv033_coefficient_correction_regulation_id': 'category',
               'tv034_temperature_fonctionnement_chaudiere_100_id': 'category',
               'tv035_temperature_fonctionnement_chaudiere_30_id': 'category',
               'rpn': 'float',
               'rpint': 'float',
               'qp0': 'float',
               'puissance_veilleuse': 'float',
               'tv036_puissance_veilleuse_id': 'category',
               'puissance_nominale': 'float',
               'tv038_puissance_nominale_id': 'category',
               'consommation_chauffage': 'float'}

# ===================== DICTIONARIES OF NORMALIZATION AND SIMPLIFICATION OF FIELDS ====================================

# dictionaries used to normalized chauffage names with simple labels.
gen_ch_lib_simp_dict = {'chaudiere gaz standard': 'chaudiere gaz standard',
                        'poele ou insert bois': 'poele ou insert bois',
                        'convecteurs electriques nfc': 'generateurs a effet joule',
                        'chaudiere fioul standard': 'chaudiere fioul standard',
                        'panneaux rayonnants electriques nfc': 'generateurs a effet joule',
                        'chaudiere gaz condensation': 'chaudiere gaz performante(condensation ou basse temperature)',
                        'chaudiere gpl condensation': 'chaudiere gpl performante(condensation ou basse temperature)',
                        'chaudiere gpl standard': 'chaudiere gpl standard',
                        'radiateurs electriques': 'generateurs a effet joule',
                        'reseau de chaleur': 'reseau de chaleur',
                        'chaudiere gaz basse temperature': 'chaudiere gaz performante(condensation ou basse temperature)',
                        'chaudiere gpl basse temperature': 'chaudiere gaz performante(condensation ou basse temperature)',
                        'autres emetteurs a effet joule': 'generateurs a effet joule',
                        'pac air/air': 'pac air/air',
                        'plafonds/planchers rayonnants electriques nfc': 'generateurs a effet joule',
                        'chaudiere bois': 'chaudiere bois',
                        'chaudiere fioul basse temperature': 'chaudiere fioul performante(condensation ou basse temperature)',
                        'chaudiere fioul condensation': 'chaudiere fioul performante(condensation ou basse temperature)',
                        'chaudiere electrique': 'chaudiere electrique',
                        'poele ou insert fioul/gpl': 'poele ou insert fioul/gpl',
                        'convecteurs bi-jonction': 'generateurs a effet joule', }

# GEN CH NORMALIZED DICT
gen_ch_normalized_lib_matching_dict = {"pac air/air": [('pac', 'PAC'), 'air/air', ('electricite', 'electrique')],
                                       "pac air/eau": [('pac', 'PAC'), 'air/eau', ('electricite', 'electrique')],
                                       "pac eau/eau": [('pac', 'PAC'), 'eau/eau', ('electricite', 'electrique')],
                                       "pac geothermique": [('pac', 'PAC'), (
                                           'geothermique', 'géothermique', 'géothermie', 'geothermie'),
                                                            ('electricite', 'electrique')],
                                       'panneaux rayonnants electriques nfc': ['panneau', ('electricite', 'electrique'),
                                                                               'nfc'],
                                       'radiateurs electriques': ['radiateur', ('electricite', 'electrique')],
                                       'plafonds/planchers rayonnants electriques nfc': [('plancher', 'plafond'),
                                                                                         ('electricite', 'electrique')],
                                       "convecteurs electriques nfc": ['convecteur', ('electricite', 'electrique'),
                                                                       'nfc'],
                                       "poele ou insert bois": [('poele', 'insert'), ('bois', 'biomasse')],
                                       "poele ou insert fioul/gpl": [('poele', 'insert'), ('fioul', 'gpl')],

                                       "autres emetteurs a effet joule": [('electricite', 'electrique')],
                                       "reseau de chaleur": ['reseau', 'chaleur'],
                                       "convecteurs bi-jonction": ['bi', 'jonction', ('electricite', 'electrique')],
                                       }

for type_chaudiere, type_chaudiere_keys in zip(['standard', 'basse temperature', 'condensation', 'non déterminee'],
                                               [('standard', 'classique'), 'basse temperature',
                                                ('condensation', 'condenseurs'), None]):
    for energie in ['fioul', 'gaz']:
        if type_chaudiere_keys is not None:
            gen_ch_normalized_lib_matching_dict[f'chaudiere {energie} {type_chaudiere}'] = ['chaudiere', energie,
                                                                                            type_chaudiere_keys]
        else:
            gen_ch_normalized_lib_matching_dict[f'chaudiere {energie} {type_chaudiere}'] = ['chaudiere', energie
                                                                                            ]
    energie_gaz = gen_ch_normalized_lib_matching_dict[f'chaudiere gaz {type_chaudiere}'][1]
    energie_gaz = (energie_gaz, 'gpl', 'butane', 'propane')
    gen_ch_normalized_lib_matching_dict[f'chaudiere gaz {type_chaudiere}'][1] = energie_gaz

gen_ch_normalized_lib_matching_dict['chaudiere bois/biomasse'] = ['chaudiere',
                                                                  ('bois', 'biomasse')]
gen_ch_normalized_lib_matching_dict['chaudiere electrique'] = ['chaudiere',
                                                               ('electricite', 'electrique')]
pac_dict = {2.2: 'pac air/air',
            2.6: 'pac air/eau',
            3.2: "pac eau/eau",
            4.0: "pac géothermique"}

poele_dict = {0.78: 'poele ou insert bois',
              0.66: 'poele ou insert bois',
              0.72: "poele ou insert fioul/gpl"}


def postprocessing_td012(td012):
    table = td012.copy()

    is_rpn = table.rpn > 0
    is_rpint = table.rpint > 0
    is_chaudiere = is_rpint | is_rpn
    # all text description raw concat
    gen_ch_concat_txt_desc = table['tv031_Type de Générateur'].astype('string').replace(np.nan, '') + ' '
    gen_ch_concat_txt_desc.loc[is_chaudiere] += 'chaudiere '
    gen_ch_concat_txt_desc += table['tv036_Type de Chaudière'].astype('string').replace(np.nan, ' ') + ' '
    gen_ch_concat_txt_desc += table["tv030_Type d'installation"].astype('string').replace(np.nan, ' ') + ' '
    gen_ch_concat_txt_desc += table["tv032_Type de Générateur"].astype('string').replace(np.nan, ' ') + ' '
    gen_ch_concat_txt_desc += table['tv035_Type de Chaudière'].astype('string').replace(np.nan, ' ') + ' '
    gen_ch_concat_txt_desc += table['tv036_Type de génération'].astype('string').replace(np.nan, ' ') + ' '
    gen_ch_concat_txt_desc += table["tv030_Type d'installation"].astype('string').replace(np.nan, ' ') + ' '
    gen_ch_concat_txt_desc += table["tr004_description"].astype('string').replace(np.nan, ' ') + ' '
    gen_ch_concat_txt_desc += table["tv045_Energie"].astype('string').replace(np.nan, ' ') + ' '
    gen_ch_concat_txt_desc += table['tv046_Nom du Réseau'].isnull().replace({False: 'réseau de chaleur',
                                                                             True: ""})
    gen_ch_concat_txt_desc = gen_ch_concat_txt_desc.str.lower().apply(lambda x: strip_accents(x))

    table['gen_ch_concat_txt_desc'] = gen_ch_concat_txt_desc

    table['gen_ch_concat_txt_desc'] = table['gen_ch_concat_txt_desc'].apply(lambda x: clean_str(x))

    # calcul gen_ch_lib_infer par matching score text.
    unique_gen_ch = table.gen_ch_concat_txt_desc.unique()
    gen_ch_lib_infer_dict = {k: affect_lib_by_matching_score(k, gen_ch_normalized_lib_matching_dict) for k in
                             unique_gen_ch}
    table['gen_ch_lib_infer'] = table.gen_ch_concat_txt_desc.replace(gen_ch_lib_infer_dict)

    # recup/fix PAC
    is_pac = (table.coefficient_performance > 2) | (table.rendement_generation > 2)
    table.loc[is_pac, 'gen_ch_lib_infer'] = table.loc[is_pac, 'coefficient_performance'].replace(pac_dict)
    is_ind = is_pac & (~table.loc[is_pac, 'gen_ch_lib_infer'].isin(pac_dict.values()))
    table.loc[is_pac, 'gen_ch_lib_infer'] = table.loc[is_pac, 'rendement_generation'].replace(pac_dict)
    is_ind = is_pac & (~table.loc[is_pac, 'gen_ch_lib_infer'].isin(pac_dict.values()))
    table.loc[is_ind, 'gen_ch_lib_infer'] = 'pac indeterminee'

    # recup/fix poele bois
    is_bois = table.gen_ch_concat_txt_desc == 'bois, biomasse bois, biomasse'

    table.loc[is_bois, 'gen_ch_lib_infer'] = table.loc[is_bois, 'rendement_generation'].replace(poele_dict)

    is_ind = is_bois & (~table.loc[is_bois, 'gen_ch_lib_infer'].isin(poele_dict.values()))
    table.loc[is_ind, 'gen_ch_lib_infer'] = 'non affecte'

    # recup reseau chaleur
    non_aff = table.gen_ch_lib_infer == 'non affecte'

    reseau_infer = non_aff & (table.rendement_generation == 0.97) & (table.tr004_description == 'Autres énergies')

    table.loc[reseau_infer, 'gen_ch_lib_infer'] = 'reseau de chaleur'

    table['gen_ch_lib_infer_simp'] = table.gen_ch_lib_infer.replace(gen_ch_lib_simp_dict)

    # fix chaudiere elec

    bool_ej = table.gen_ch_lib_infer == 'autres emetteurs a effet joule'
    bool_ce = table.rendement_generation == 0.77

    table.loc[(bool_ej) & (bool_ce), 'gen_ch_lib_infer'] = 'chaudiere electrique'

    table['type_energie_chauffage'] = table['tr004_description']
    return table