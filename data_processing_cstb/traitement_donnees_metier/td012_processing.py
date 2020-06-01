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

for type_chaudiere, type_chaudiere_keys in zip(['standard', 'basse temperature', 'condensation', 'non déterminée'],
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
pac_dict = {2.2:'pac air/air',
           2.6:'pac air/eau',
           3.2:"pac eau/eau",
           4.0:"pac géothermique"}
