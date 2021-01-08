# GENERATEURS CHAUFFAGE ET ECS

## elements de recherche communs
# TODO : passer tout en *


energie_chaudiere_mods = ['gaz', 'fioul', 'charbon', 'autre : gpl butane propane']

type_chaudiere_mods = ['condensation', 'basse temperature', 'standard']

# type systemes

# solaire

abscence_ecs_solaire = ('"ecs solaire : non"', '"sans solaire"', '"sans ecs solaire"')

# pour les systemes mixte ECS/CH
mixte = ('mixte', 'combine', 'chauffage AND ecs', 'lie ', 'combine ', "idem")

reseau_chaleur = ('"reseau de chaleur"', '"reseaux de chaleurs"', '"reseaux de chaleur"',
                  '"reseau chaleur"', "urbain", "rcu")

chauffe_bain = ("individu* AND ballon", "chauffe-eau", "accu*", "chauffe AND bain", 'chauffe-bain')

pac = ('pompe* AND chaleur*', 'pac ', 'thermody*')

poele = ('poe*', 'insert', 'cuisin*')

chaudiere = ('chaud.', 'chaud', 'chaudi*', 'chaufferie', 'chauudiere', 'condens*')

# autre char qui peuvent vouloir dir chaudiere dans le cas ou l'on a au moins l'energie de chauffage
chaudieres_plus = tuple(list(('collectif', 'ch.')) + list(chaudiere))

chaudiere_elec = ('"chaudiere elec*"', '"chaudiere individuelle elec*"', '"chaudiere collective elec*"')

chaudiere_bois = ('"chaudiere bois"', '"chaudiere individuelle bois"'
                  , '"chaudiere a bois"'
                  , '"chaudiere bois/biomasse"'
                  , '"chaudiere a bois/biomasse"', "atmospherique")

# types chaudieres

condensation = ('condens*')

basse_temperature = ('"basse temperature"', 'bt ', 'bt.')

# ener

bois = ('bois', "bois//biomasse", 'biomasse', 'flamme AND verte', 'granule*', 'pellet*', 'buche*')

fioul = ('fioul', 'mazout')

elec = ('elec*', 'elec.', 'joule*')

## dictionnaires annexes

installation_dict = {'collectif': [('collecti*', 'coll', 'coll.')],
                     'individuel': [('individu*', 'ind', 'ind.')],
                     }

energie_dict = {
    'gaz': ['gaz'],
    'electricite': [elec],
    'reseau de chaleur': reseau_chaleur,
    'fioul': fioul,
    'bois': [bois],
    'autre : gpl butane propane': [('propane', 'butane', 'gpl')],
    'charbon': ['charbon'],
}

energie_dict_lower = {
    'Gaz naturel': 'gaz',
    'Electricité non renouvelable': 'electricite ',
    'Réseau de chaleurs': 'reseau de chaleur',
    'Fioul domestique': 'fioul',
    'Bois, biomasse': 'bois',
    'Gaz propane ou butane': 'autre : gpl butane propane',
    'Charbon': 'charbon',
}

energie_mods = energie_dict.keys()

## DICTIONNAIRE DE RECHERCHE : GENERATEUR CHAUFFAGE DANS td003_descriptif et td005_fiches_techniques
# les descriptions des systèmes diffèrent en fonction des diagnostiqueurs et logiciels dans ces tables descriptives.
# la configuration de ces dictionnaires de recherches s'éfforce de répertorier la grande majorité des mots utilisés pour décrire un même système de chauffage
# les générateurs sont classés en catégories.

gen_ch_search_dict = dict()

gen_ch_search_dict['pac'] = {"pac geothermique en releve de chaudiere": [pac, "geoth*", 'chaudi*'],
                             "pac eau/eau en releve de chaudiere": [pac, 'eau', 'chaudi*'],
                             "pac air/eau en releve de chaudiere": [pac, 'air AND eau', 'chaudi*'],
                             "pac geothermique": [pac, ('geoth*')],
                             "pac air/eau": [pac, 'air AND eau'],
                             "pac eau/eau": [pac, 'eau'],
                             "pac air/air": [tuple(list(pac) + ['clim*', 'split*']), ('air', 'split*')],
                             "pac indetermine": [pac] + ['clim*', 'split*'], }
gen_ch_search_dict['poele'] = {"poele ou insert bois": [poele, bois],
                               "poele ou insert fioul/gpl/charbon": [poele, ('fioul', 'mazout', 'charbon', 'gpl')],
                               "poele ou insert indetermine": [poele]
                               }

chaudiere_dict_ch = dict()

chaudiere_dict_ch['chaudiere bois exact'] = [chaudiere_bois, bois]
chaudiere_dict_ch['chaudiere bois'] = [chaudiere, 'NOT', poele, bois]

chaudiere_dict_ch['chaudiere electrique'] = [chaudiere_elec]

for type_chaudiere, type_chaudiere_keys in zip(type_chaudiere_mods + ['indetermine'],
                                               [condensation, basse_temperature,
                                                ('standard', 'classique'), None]):

    for energie in energie_chaudiere_mods:
        energie_keywords = energie
        if energie == 'autre : gpl butane propane':
            energie_keywords = ('gpl', 'butane', 'propane')
        if type_chaudiere_keys is not None:
            chaudiere_dict_ch[f'chaudiere {energie} {type_chaudiere}'] = [chaudieres_plus, energie_keywords,
                                                                          type_chaudiere_keys]
        else:
            chaudiere_dict_ch[f'chaudiere {energie} {type_chaudiere}'] = [chaudieres_plus, energie_keywords
                                                                          ]
    # on utilise uniquement le mot chaudiere quand on est indeterminé sur le reste
    chaudiere_dict_ch[f'chaudiere energie indetermine {type_chaudiere}'] = ['chaudiere',
                                                                            type_chaudiere_keys]  # uniquement le mot chaudiere quand on % indeterminé

# les radiateurs gaz sont inclus avec les chaudieres car ces systèmes sont a priori exclusifs les un des autres.
chaudiere_dict_ch['radiateurs gaz'] = [('"radiateur gaz"', '"radiateurs gaz"')]

gen_ch_search_dict['chaudiere'] = chaudiere_dict_ch
gen_ch_search_dict['reseau_chaleur'] = {"reseau de chaleur": [reseau_chaleur], }

gen_ch_search_dict['effet_joule'] = {'radiateurs electriques': [('radiateur', 'radiateurs'), elec],
                                     "convecteurs bi-jonction": [('bi AND jonction', 'bijonction', 'bi-jonction')],
                                     'panneaux rayonnants electriques nfc': [('panneau', 'panneaux'),
                                                                             ('rayonnant', 'rayonnants'),
                                                                             ('nf', 'nfc')],

                                     'plafonds/planchers rayonnants electriques nfc': [('plancher', 'plafond',
                                                                                        'planchers', 'plafonds'),
                                                                                       ('rayonnant', 'rayonnants')],
                                     "convecteurs electriques nfc": [('convecteur', 'convecteurs', 'radiateurs'),
                                                                     ('nf', 'nfc')],
                                     "panneaux rayonnants electriques": [('panneau', 'panneaux'),
                                                                         ('rayonnant', 'rayonnants')],

                                     }
gen_ch_search_dict['chauffage electrique indetermine'] = {"chauffage electrique indetermine": [elec]}
gen_ch_search_dict['chauffage bois indetermine'] = {"chauffage bois indetermine": [bois]}

# version flat du dictionnaire par catégorie
gen_ch_search_dict_flat = dict()
{gen_ch_search_dict_flat.update(v) for k, v in gen_ch_search_dict.items()}

reverse_cat_gen_ch = dict()

for cat, v in gen_ch_search_dict.items():
    for label in v:
        reverse_cat_gen_ch[label] = cat
reverse_cat_gen_ch['indetermine'] = 'indetermine'
solaire_ch_search_dict = dict()
solaire_ch_search_dict['solaire'] = ['solaire']
solaire_ch_search_dict['abscence_solaire'] = [('"solaire : non"', '"sans solaire"', '"ecs solaire"')]

## DICTIONNAIRE DE RECHERCHE : GENERATEUR ECS DANS td003_descriptif et td005_fiches_techniques
# les descriptions des systèmes diffèrent en fonction des diagnostiqueurs et logiciels dans ces tables descriptives.
# la configuration de ces dictionnaires de recherches s'éfforce de répertorier la grande majorité des mots utilisés pour décrire un même système d'ECS
# TODO : ordonner et catégoriser

gen_ecs_search_dict = dict()

gen_ecs_search_dict['solaire'] = {"ecs solaire": ["solaire"], }
gen_ecs_search_dict['abscence_solaire'] = {
    "abscence ecs solaire": [abscence_ecs_solaire], }

gen_ecs_search_dict['ecs_thermodynamique'] = {
    "ecs thermodynamique electrique(pac ou ballon)": [
        ('pompe AND chaleur', 'pac', 'thermodynamique', '"air extrait"', '"air ambiant"')],

}
chaudiere_dict_ecs = dict()
chaudiere_dict_ch['chaudiere bois exacte'] = [chaudiere_bois, bois]
chaudiere_dict_ch['chaudiere bois'] = [chaudiere, 'NOT', poele, bois]
chaudiere_dict_ecs['chaudiere electrique'] = [chaudiere_elec]
for type_chaudiere, type_chaudiere_keys in zip(type_chaudiere_mods + ['indetermine'],
                                               [('condensation', 'condenseurs'), '"basse temperature"',
                                                ('standard', 'classique'), None]):
    for energie in energie_chaudiere_mods:
        energie_keywords = energie
        if energie == 'autre : gpl butane propane':
            energie_keywords = ('gpl', 'butane', 'propane')
        if type_chaudiere_keys is not None:
            chaudiere_dict_ecs[f'chaudiere {energie} {type_chaudiere}'] = [chaudieres_plus, energie_keywords,
                                                                           type_chaudiere_keys]
        else:
            chaudiere_dict_ecs[f'chaudiere {energie} {type_chaudiere}'] = [chaudieres_plus, energie_keywords
                                                                           ]
    # on utilise uniquement le mot chaudiere quand on est indeterminé sur le reste
    chaudiere_dict_ecs[f'chaudiere energie indetermine {type_chaudiere}'] = ['chaudiere', type_chaudiere_keys]

chaudiere_dict_ecs.update({

    'production mixte gaz': ["gaz", mixte],
    'production mixte fioul': ["fioul", mixte],
})

gen_ecs_search_dict['chaudiere'] = chaudiere_dict_ecs
gen_ecs_search_dict['production_mixte_indetermine'] = {'production mixte indetermine': [mixte]}
gen_ecs_search_dict['reseau de chaleur'] = {"reseau de chaleur": [reseau_chaleur], }

gen_ecs_search_dict['effet_joule'] = {
    "ballon a accumulation electrique": [('ballon', 'classique', 'accu*', 'chauffe-eau'), elec],
    "ecs instantanee electrique": ['inst*', elec],
    "ecs electrique indetermine": [elec],
}

gen_ecs_search_dict['chauffe-eau_independant'] = {'chauffe-eau gaz independant': [chauffe_bain, "gaz"],
                                                  'chauffe-eau gpl independant': [chauffe_bain, "gpl"],
                                                  'chauffe-eau fioul independant': [chauffe_bain,
                                                                                    "fioul"],
                                                  "chauffe-eau independant indetermine": [
                                                      ("individuelle AND ballon", "chauffe-eau", "accumulateur",
                                                       "chauffe AND bain")], }

# version flat du dictionnaire par catégorie
gen_ecs_search_dict_flat = dict()
{gen_ecs_search_dict_flat.update(v) for k, v in gen_ecs_search_dict.items()}

reverse_cat_gen_ecs = dict()

for cat, v in gen_ecs_search_dict.items():
    for label in v:
        reverse_cat_gen_ecs[label] = cat
reverse_cat_gen_ecs['indetermine'] = 'indetermine'

## DICTIONNAIRE DE RECHERCHE : CHAUFFAGE DANS TD012.
# ici l'extraction texte est effectuées sur des champs tabulés normés (tables TV) et nécessite de rechercher uniquement les mots présents dans ces tables TV
td012_gen_ch_search_dict = {"pac air/air": ['pac', 'air/air', ('electricite', 'electrique')],
                            "pac air/eau": ['pac', 'air/eau', ('electricite', 'electrique')],
                            "pac eau/eau": ['pac', 'eau/eau', ('electricite', 'electrique')],

                            "pac geothermique": ['pac', (
                                'geothermique', 'geothermie'),

                                                 ('electricite', 'electrique')],
                            "reseau de chaleur": ['reseau', 'chaleur'],

                            'panneaux rayonnants electriques nfc': ['panneau', ('electricite', 'electrique'),
                                                                    'nfc'],
                            'radiateurs electriques': ['radiateur', ('electricite', 'electrique')],
                            'plafonds/planchers rayonnants electriques nfc': [('plancher', 'plafond'),
                                                                              ('electricite', 'electrique')],
                            "convecteurs electriques nfc": ['convecteur', ('electricite', 'electrique'),
                                                            'nfc'],
                            "poele ou insert bois": [('poele', 'insert'), ('bois', 'biomasse')],
                            "poele ou insert fioul/gpl/charbon": [('poele', 'insert'), ('fioul', 'gpl', 'charbon')],
                            "chaudiere bois": ['chaudiere', ('bois', 'biomasse')],
                            "convecteurs bi-jonction": ['bi', 'jonction', ('electricite', 'electrique')],
                            "chauffage bois indetermine": [('bois', 'biomasse')],
                            "chauffage electrique indetermine": [('electricite', 'electrique')],

                            }

for type_chaudiere, type_chaudiere_keys in zip(type_chaudiere_mods + ['indetermine'],
                                               ['condensation', 'basse temperature',
                                                ('standard', 'classique'), None]):
    for energie in energie_chaudiere_mods:
        energie_keywords = energie
        if energie == 'autre : gpl butane propane':
            energie_keywords = ('gpl', 'butane', 'propane')
        if type_chaudiere_keys is not None:
            td012_gen_ch_search_dict[f'chaudiere {energie} {type_chaudiere}'] = ['chaudiere', energie_keywords,
                                                                                 type_chaudiere_keys]
        else:
            td012_gen_ch_search_dict[f'chaudiere {energie} {type_chaudiere}'] = ['chaudiere', energie_keywords
                                                                                 ]
td012_gen_ch_search_dict['chaudiere electrique'] = ['chaudiere',
                                                    ('electricite', 'electrique')]

## DICTIONNAIRE DE RECHERCHE : GENERATEUR ECS DANS TD014.
# ici l'extraction texte est effectuées sur des champs tabulés normés (tables TV) et nécessite de rechercher uniquement les mots présents dans ces tables TV
# TODO : ordonner et catégoriser

td014_gen_ecs_search_dict = {
    "ecs thermodynamique electrique(pompe a chaleur ou ballon)": [
        ('pompe a chaleur', 'pac', 'thermodynamique', 'air extrait', 'air exterieur', 'air ambiant'),
        ('electricite', 'electrique')],
    "ballon a accumulation electrique": [('ballon', 'classique', 'accumulation'), ('electricite', 'electrique')],
    "ecs electrique indetermine": [('electricite', 'electrique')],
    "ecs instantanee electrique": ['instantanee', ('electricite', 'electrique')],

    'chauffe-eau gaz independant': [("individuelle ballon", "chauffe-eau", "accumulateur", "chauffe bain"), "gaz"],
    'chauffe-eau gpl independant': [("individuelle ballon", "chauffe-eau", "accumulateur", "chauffe bain"), "gpl"],
    "chaudiere bois": [('bois', 'biomasse')],
    'chauffe-eau fioul independant': [("individuelle ballon", "chauffe-eau", "accumulateur", "chauffe bain"),
                                      "fioul"],
    "reseau de chaleur": ["reseau", "chaleur"],

}
for type_chaudiere, type_chaudiere_keys in zip(type_chaudiere_mods + ['indetermine'],
                                               [('condensation', 'condenseurs'), 'basse temperature',
                                                ('standard', 'classique'), None]):
    for energie in energie_chaudiere_mods:
        energie_keywords = energie
        if energie == 'autre : gpl butane propane':
            energie_keywords = ('gpl', 'butane', 'propane')
        if type_chaudiere_keys is not None:
            td014_gen_ecs_search_dict[f'chaudiere {energie} {type_chaudiere}'] = ['chaudiere', energie_keywords,
                                                                                  type_chaudiere_keys]
        else:
            td014_gen_ecs_search_dict[f'chaudiere {energie} {type_chaudiere}'] = ['chaudiere', energie_keywords
                                                                                  ]
td014_gen_ecs_search_dict['chaudiere electrique'] = ['chaudiere',
                                                     ('electricite', 'electrique')]

# solaire_dict = dict()
# for k, v in td014_gen_ecs_search_dict.items():
#     k_solaire = 'ecs solaire thermique + ' + k
#     solaire_dict[k_solaire] = v + ['avec solaire']
# td014_gen_ecs_search_dict.update(solaire_dict)
