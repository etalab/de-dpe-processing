from .utils import strip_accents,affect_lib_by_matching_score

# ELASTIC SEARCH DICT : ECS
mixte = ('mixte', 'combine', 'chauffage AND ecs','lie ','combine ',"idem")

gen_ecs_normalized_lib_matching_dict_ft = {
    "ECS thermodynamique electrique(PAC ou ballon)": [
        ('pompe AND chaleur', 'pac', 'thermodynamique', 'air extrait', 'air exterieur', 'air ambiant')],

    "ballon a accumulation electrique": [('ballon', 'classique', 'accumulation'),
                                         ('electricite', 'electrique', 'elec.')],
    "ecs electrique indeterminee": [('electricite', 'electrique', 'elec.')],
    "ecs instantanee electrique": ['instantanee', ('electricite', 'electrique', 'elec.')],

}

for type_chaudiere, type_chaudiere_keys in zip(['standard', 'basse temperature', 'condensation', 'non déterminee'],
                                               [('standard', 'classique'), 'basse temperature',
                                                ('condensation', 'condenseurs'), None]):
    for energie in ['fioul', 'gaz', 'autre(gpl/butane/propane)']:
        energie_keywords = energie
        if energie == 'autre(gpl/butane/propane)':
            energie_keywords = ('gpl', 'butane', 'propane')
        if type_chaudiere_keys is not None:
            gen_ecs_normalized_lib_matching_dict_ft[f'chaudiere {energie} {type_chaudiere}'] = ['chaudiere',
                                                                                                energie_keywords,
                                                                                                type_chaudiere_keys]
        else:
            gen_ecs_normalized_lib_matching_dict_ft[f'chaudiere {energie} {type_chaudiere}'] = ['chaudiere',
                                                                                                energie_keywords
                                                                                                ]

gen_ecs_normalized_lib_matching_dict_ft.update({'chaudiere non determinee': ["chaudiere"],
                                                "ecs collective reseau chaleur": ["reseau de chaleur"],
                                                'production mixte gaz': ["gaz", mixte],
                                                'production mixte fioul': ["fioul", mixte],

                                                'production mixte indeterminee': [mixte],
                                                'chauffe-eau gaz independant': [("individuelle AND ballon",
                                                                                 "chauffe-eau", "accumulateur",
                                                                                 "chauffe AND bain"), "gaz"],
                                                'chauffe-eau gpl independant': [("individuelle AND ballon",
                                                                                 "chauffe-eau", "accumulateur",
                                                                                 "chauffe AND bain"), "gpl"],

                                                'chauffe-eau fioul independant': [("individuelle AND ballon",
                                                                                   "chauffe-eau", "accumulateur",
                                                                                   "chauffe AND bain"),
                                                                                  "fioul"],

                                                "chauffe-eau independant indetermine": [("individuelle AND ballon",
                                                                                         "chauffe-eau", "accumulateur",
                                                                                         "chauffe AND bain")]
                                                })




