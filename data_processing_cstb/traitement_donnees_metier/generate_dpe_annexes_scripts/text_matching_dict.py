# GENERATEURS CHAUFFAGE ET ECS

## elements de recherche communs
# TODO : passer tout en *

# energie possible des chaudières qui ne sont pas au bois
energie_chaudiere_mods = ['gaz', 'fioul', 'charbon', 'gpl/butane/propane']

# energie à combustion

energie_combustion_mods = energie_chaudiere_mods + ['bois']

type_chaudiere_mods = ['condensation', 'basse temperature', 'standard']

# type systemes

# solaire

abscence_ecs_solaire = ('"ecs solaire : non"', '"sans solaire"', '"sans ecs solaire"')

# pour les systemes mixte ECS/CH
mixte = ('mixte', 'combine', 'chauffage AND ecs', 'lie ', 'combine ', "idem")

reseau_chaleur = ('"reseau de chaleur"', '"reseaux de chaleurs"', '"reseaux de chaleur"',
                  '"reseau chaleur"', "urbain", "rcu", "(raccordement AND reseau)")

chauffe_bain = ("individu* AND ballon", "chauffe-eau", "accu*", "chauffe AND bain", 'chauffe-bain')

pac = ('pompe* AND chaleur*', 'pac', 'thermody*')

pac_and_thermo = tuple(list(pac) + ['"air extrait"', '"air ambiant"'])

eau_eau = ('"eau eau"', '"eau/eau"', '"eau-eau"', 'eau AND nappe', 'eaueau')
air_eau = ('"air eau"', '"air/eau"', '"air-eau"', 'aireau')
air_air = ('"air air"', '"air/air"', '"air-air"', 'airair', 'air AND NOT eau')

poele = ('poe*', 'insert', 'cuisin*')

chaudiere = ('chaud.', 'chaud', 'chaudi*', 'chaufferie', 'chauudiere', 'condens*')

# autre char qui peuvent vouloir dir chaudiere dans le cas ou l'on a au moins l'energie de chauffage
chaudieres_plus = tuple(list(('collectif', 'ch.')) + list(chaudiere))

chaudiere_elec = ('"chaudiere electrique"', '"chaudiere individuelle electrique"', '"chaudiere collective electrique"')

chaudiere_bois = ('"chaudiere bois"', '"chaudiere individuelle bois"'
                  , '"chaudiere a bois"'
                  , '"chaudiere bois/biomasse"'
                  , '"chaudiere a bois/biomasse"', "atmos*")

# types chaudieres

condensation = ('condens*')

basse_temperature = ('"basse temperature"', 'bt ', 'bt.')

# ener

bois = ('bois', "bois//biomasse", 'bio*', 'flamme AND verte', 'granule*', 'pellet*', 'buche*')

bois_pour_chaudiere_bois = tuple(list(bois) + ['atmos*'])
fioul = ('fioul', 'mazout', 'fuel')

elec = ('elec*', 'elec.', 'joule*')

elec_or_pac = tuple(list(elec) + list(pac))

## dictionnaires annexes

installation_search_dict = {'collectif': [('collecti*', 'coll', 'coll.')],
                            'individuel': [('individu*', 'ind', 'ind.')],
                            }

energie_search_dict = {
    'gaz': ['gaz'],
    'electricite': [elec_or_pac],
    'reseau de chaleur': [reseau_chaleur],
    'fioul': [fioul],
    'bois': [bois],
    'gpl/butane/propane': [('propane', 'butane', 'gpl')],
    'charbon': ['charbon'],
}

energie_dict_lower = {
    'Gaz naturel': 'gaz',
    'Electricité non renouvelable': 'electricite ',
    'Réseau de chaleurs': 'reseau de chaleur',
    'Fioul domestique': 'fioul',
    'Bois, biomasse': 'bois',
    'Gaz propane ou butane': 'gpl/butane/propane',
    'Charbon': 'charbon',
}

energie_mods = energie_search_dict.keys()

## DICTIONNAIRE DE RECHERCHE : GENERATEUR CHAUFFAGE DANS td003_descriptif et td005_fiches_techniques
# les descriptions des systèmes diffèrent en fonction des diagnostiqueurs et logiciels dans ces tables descriptives.
# la configuration de ces dictionnaires de recherches s'éfforce de répertorier la grande majorité des mots utilisés pour décrire un même système de chauffage
# les générateurs sont classés en catégories.

gen_ch_search_dict = dict()

gen_ch_search_dict['pac'] = {"pac geothermique en releve de chaudiere": [pac, "geoth*", 'chaudi*'],
                             "pac air/eau en releve de chaudiere": [pac, ('air AND eau', 'aireau'), 'chaudi*'],
                             "pac eau/eau en releve de chaudiere": [pac, eau_eau, 'chaudi*'],

                             "pac geothermique": [pac, ('geoth*')],
                             "pac air/eau": [pac, air_eau],
                             "pac eau/eau": [pac, eau_eau],
                             "pac air/air": [tuple(list(pac) + ['clim*', 'split*']), air_air],
                             "pac indetermine en releve de chaudiere": [tuple(list(pac) + ['clim*', 'split*']),
                                                                        'chaudi*'],
                             "pac indetermine": [tuple(list(pac) + ['clim*', 'split*'])]}

gen_ch_search_dict['reseau chaleur'] = {"reseau de chaleur": [reseau_chaleur], }

gen_ch_search_dict['chaudiere bois'] = {'chaudiere bois exact': [chaudiere_bois, bois_pour_chaudiere_bois],
                                        'chaudiere bois': [chaudiere, 'NOT', poele, bois_pour_chaudiere_bois]}

chaudiere_dict_ch = dict()

chaudiere_dict_ch['chaudiere electrique exact'] = [chaudiere_elec]

for type_chaudiere, type_chaudiere_keys in zip(type_chaudiere_mods + ['indetermine'],
                                               [condensation, basse_temperature,
                                                ('standard', 'classique'), None]):

    for energie in energie_chaudiere_mods:
        energie_keywords = energie
        if energie == 'gpl/butane/propane':
            energie_keywords = ('gpl', 'butane', 'propane')
        if type_chaudiere_keys is not None:
            chaudiere_dict_ch[f'chaudiere {energie} {type_chaudiere}'] = [chaudieres_plus, energie_keywords,
                                                                          type_chaudiere_keys]
        else:
            chaudiere_dict_ch[f'chaudiere {energie} {type_chaudiere}'] = [chaudieres_plus, energie_keywords
                                                                          ]
    # on utilise uniquement le mot chaudiere quand on est indeterminé sur le reste
    if type_chaudiere_keys is not None:
        chaudiere_dict_ch[f'chaudiere energie indetermine {type_chaudiere}'] = [chaudieres_plus,
                                                                                type_chaudiere_keys]

chaudiere_dict_ch[f'chaudiere electrique'] = [chaudieres_plus, ('elec*', 'elec.')]
chaudiere_dict_ch[f'chaudiere energie indetermine indetermine'] = ['chaudi*']

# les radiateurs gaz sont inclus avec les chaudieres car ces systèmes sont a priori exclusifs les un des autres.
chaudiere_dict_ch['radiateurs gaz'] = [
    ('"radiateur gaz"', '"radiateurs gaz"', '"radiateurs sur conduits fumees"', '"radiateurs a ventouse"')]

# uniquement le mot chaudiere quand on % indeterminé
chaudiere_dict_ch[f'chaudiere energie indetermine indetermine'] = ['chaudiere*']

gen_ch_search_dict['chaudiere'] = chaudiere_dict_ch

gen_ch_search_dict['effet joule'] = {'radiateurs electriques': [('radiateur', 'radiateurs'), elec],
                                     "convecteurs bi-jonction electriques": [
                                         ('bi AND jonction', 'bijonction', 'bi-jonction')],
                                     'panneaux rayonnants electriques nfc': [('panneau', 'panneaux'),
                                                                             ('rayonnant', 'rayonnants'),
                                                                             ('nf', 'nfc')],

                                     'plafonds/planchers rayonnants electriques nfc': [('plancher', 'plafond',
                                                                                        'planchers', 'plafonds'),
                                                                                       ('rayonnant', 'rayonnants')],
                                     "convecteurs electriques nfc": [('convecteur*', 'radiateur*'),
                                                                     ('nf', 'nfc')],
                                     "panneaux rayonnants electriques": [('panneau', 'panneaux'),
                                                                         ('rayonnant', 'rayonnants')],

                                     "convecteurs electriques": [('convecteur*'), elec]

                                     }

gen_ch_search_dict['poele'] = {"poele ou insert bois": [poele, bois],
                               "poele ou insert fioul": [poele, fioul],
                               "poele ou insert gpl/butane/propane": [poele, ('propane', 'butane', 'gpl')],
                               "poele ou insert charbon": [poele, 'charbon'],
                               "poele ou insert indetermine": [poele]
                               }
gen_ch_search_dict['chauffage electrique indetermine'] = {"chauffage electrique indetermine": [elec]}
gen_ch_search_dict['chauffage bois indetermine'] = {"chauffage bois indetermine": [bois]}
gen_ch_search_dict['chauffage fioul indetermine'] = {"chauffage fioul indetermine": [fioul]}
gen_ch_search_dict['chauffage gaz indetermine'] = {"chauffage gaz indetermine": ['gaz']}
gen_ch_search_dict['chauffage gpl/butane/propane indetermine'] = {
    "chauffage gpl/butane/propane indetermine": [('gpl', 'butane', 'propane')]}
gen_ch_search_dict['chauffage charbon indetermine'] = {"chauffage charbon indetermine": ["charbon"]}

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
gen_ecs_search_dict['abscence solaire'] = {
    "abscence ecs solaire": [abscence_ecs_solaire], }

gen_ecs_search_dict['ecs thermodynamique'] = {
    "ecs thermodynamique electrique(pac ou ballon)": [pac_and_thermo],

}
gen_ecs_search_dict['chaudiere bois'] = {'chaudiere bois exact': [chaudiere_bois, bois_pour_chaudiere_bois],
                                         'chaudiere bois': [chaudiere, 'NOT', poele, bois_pour_chaudiere_bois]}
chaudiere_dict_ecs = dict()

chaudiere_dict_ecs['chaudiere electrique exact'] = [chaudiere_elec]

for type_chaudiere, type_chaudiere_keys in zip(type_chaudiere_mods + ['indetermine'],
                                               [condensation, basse_temperature,
                                                ('standard', 'classique'), None]):

    for energie in energie_chaudiere_mods:
        energie_keywords = energie
        if energie == 'gpl/butane/propane':
            energie_keywords = ('gpl', 'butane', 'propane')
        if type_chaudiere_keys is not None:
            chaudiere_dict_ecs[f'chaudiere {energie} {type_chaudiere}'] = [chaudieres_plus, energie_keywords,
                                                                           type_chaudiere_keys]
        else:
            chaudiere_dict_ecs[f'chaudiere {energie} {type_chaudiere}'] = [chaudieres_plus, energie_keywords
                                                                           ]
    # on utilise uniquement le mot chaudiere quand on est indeterminé sur le reste
    if type_chaudiere_keys is not None:
        chaudiere_dict_ecs[f'chaudiere energie indetermine {type_chaudiere}'] = [chaudieres_plus,
                                                                                 type_chaudiere_keys]

chaudiere_dict_ecs[f'chaudiere electrique'] = [chaudieres_plus, ('elec*', 'elec.')]

chaudiere_dict_ecs[f'chaudiere energie indetermine indetermine'] = ['chaudi*']

# uniquement le mot chaudiere quand on % indeterminé
chaudiere_dict_ecs[f'chaudiere energie indetermine indetermine'] = ['chaudiere*']

chaudiere_dict_ecs.update({

    'production mixte gaz': ["gaz", mixte],
    'production mixte fioul': ["fioul", mixte],
})

gen_ecs_search_dict['chaudiere'] = chaudiere_dict_ecs
gen_ecs_search_dict['production mixte indetermine'] = {'production mixte indetermine': [mixte]}
gen_ecs_search_dict['reseau de chaleur'] = {"reseau de chaleur": [reseau_chaleur], }

gen_ecs_search_dict['effet joule'] = {
    "ballon a accumulation electrique": [('ballon', 'classique', 'accu*', 'chauffe-eau', 'vertical', 'horizontal'),
                                         elec],
    "ecs instantanee electrique": ['inst*', elec],
}

gen_ecs_search_dict['chauffe-eau_independant'] = {'chauffe-eau gaz independant': [chauffe_bain, "gaz"],
                                                  'chauffe-eau gpl/butane/propane independant': [chauffe_bain, (
                                                      'gpl', 'butane', 'propane')],
                                                  'poele bouilleur bois': [poele, bois],
                                                  'chauffe-eau fioul independant': [chauffe_bain,
                                                                                    "fioul"],
                                                  "chauffe-eau independant indetermine": [
                                                      chauffe_bain], }

gen_ecs_search_dict['ecs electrique indetermine'] = {"ecs electrique indetermine": [elec]}
gen_ecs_search_dict['ecs bois indetermine'] = {"ecs bois indetermine": [bois]}
gen_ecs_search_dict['ecs fioul indetermine'] = {"ecs fioul indetermine": [fioul]}
gen_ecs_search_dict['ecs gaz indetermine'] = {"ecs gaz indetermine": ['gaz']}
gen_ecs_search_dict['ecs gpl/butane/propane indetermine'] = {
    "ecs gpl/butane/propane indetermine": [('gpl', 'butane', 'propane')]}
gen_ecs_search_dict['ecs charbon indetermine'] = {"ecs charbon indetermine": ["charbon"]}

# version flat du dictionnaire par catégorie
gen_ecs_search_dict_flat = dict()
{gen_ecs_search_dict_flat.update(v) for k, v in gen_ecs_search_dict.items()}

reverse_cat_gen_ecs = dict()

for cat, v in gen_ecs_search_dict.items():
    for label in v:
        reverse_cat_gen_ecs[label] = cat
reverse_cat_gen_ecs['indetermine'] = 'indetermine'

# catégorisation des catégories d'ECS pour un traitement spécifique ordonné (basé sur une hypothèse que lorsque l'on a une
# production d'ECS associé à un système d'ECS on a pas de deuxieme ballon avec production autonome.
priorisation_ecs = {'solaire': "solaire",
                    'abscence solaire': "solaire",
                    'ecs thermodynamique': "principal",
                    'chaudiere bois': "principal",
                    'chaudiere': "principal",
                    'production mixte indetermine': "secondaire",
                    'reseau de chaleur': "principal",
                    'effet joule': "secondaire",
                    'chauffe-eau_independant': "defaut",
                    'ecs electrique indetermine': "secondaire",
                    'ecs bois indetermine': "defaut",
                    'ecs fioul indetermine': "defaut",
                    'ecs gaz indetermine': "defaut",
                    'ecs gpl/butane/propane indetermine': "defaut",
                    'ecs charbon indetermine': "defaut", }

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
                            "poele ou insert fioul": [('poele', 'insert'), ' fioul '],  # les espaces sont importants
                            "poele ou insert gpl/propane/butane": [('poele', 'insert'), ' gpl/butane/propane '],
                            # les espaces sont importants
                            "poele ou insert charbon": [('poele', 'insert'), ' charbon '],
                            # les espaces sont importants
                            "chaudiere bois": ['chaudiere', ('bois', 'biomasse')],
                            "convecteurs bi-jonction electriques": ['bi', 'jonction', ('electricite', 'electrique')],

                            }

for type_chaudiere, type_chaudiere_keys in zip(type_chaudiere_mods + ['indetermine'],
                                               ['condensation', 'basse temperature',
                                                ('standard', 'classique'), None]):
    for energie in energie_chaudiere_mods:
        energie_keywords = energie
        if energie == 'gpl/butane/propane':
            energie_keywords = ('gpl', 'butane', 'propane')
        if type_chaudiere_keys is not None:
            td012_gen_ch_search_dict[f'chaudiere {energie} {type_chaudiere}'] = ['chaudiere', energie_keywords,
                                                                                 type_chaudiere_keys]
        else:
            td012_gen_ch_search_dict[f'chaudiere {energie} {type_chaudiere}'] = ['chaudiere', energie_keywords
                                                                                 ]
td012_gen_ch_search_dict['chaudiere electrique'] = ['chaudiere',
                                                    ('electricite', 'electrique')]
td012_gen_ch_search_dict.update({"chauffage bois indetermine": [('bois', 'biomasse')],
                                 "chauffage electrique indetermine": [('electricite', 'electrique')],
                                 "chauffage fioul indetermine": [' fioul '],  # les espaces sont importants
                                 "chauffage gaz indetermine": ['gaz'],
                                 "chauffage gpl/butane/propane indetermine": [' gpl/butane/propane '],
                                 # les espaces sont importants
                                 "chauffage charbon indetermine": [' charbon '], })  # les espaces sont importants

## DICTIONNAIRE DE RECHERCHE : GENERATEUR ECS DANS TD014.
# ici l'extraction texte est effectuées sur des champs tabulés normés (tables TV) et nécessite de rechercher uniquement les mots présents dans ces tables TV
# TODO : ordonner et catégoriser

td014_gen_ecs_search_dict = {
    "ecs thermodynamique electrique(pac ou ballon)": [
        ('pompe a chaleur', 'pac', 'thermodynamique', 'air extrait', 'air exterieur', 'air ambiant'),
        ('electricite', 'electrique')],
    "ballon a accumulation electrique": [('ballon', 'classique', 'accumulation'), ('electricite', 'electrique')],
    "ecs electrique indetermine": [('electricite', 'electrique')],
    "ecs instantanee electrique": ['instantanee', ('electricite', 'electrique')],

    'chauffe-eau gaz independant': [("individuelle ballon", "chauffe-eau", "accumulateur", "chauffe bain"), "gaz"],
    'chauffe-eau gpl/butane/propane independant': [
        ("individuelle ballon", "chauffe-eau", "accumulateur", "chauffe bain"), ('gpl', 'butane', 'propane')],
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
        if energie == 'gpl/butane/propane':
            energie_keywords = ('gpl', 'butane', 'propane')
        if type_chaudiere_keys is not None:
            td014_gen_ecs_search_dict[f'chaudiere {energie} {type_chaudiere}'] = ['chaudiere', energie_keywords,
                                                                                  type_chaudiere_keys]
        else:
            td014_gen_ecs_search_dict[f'chaudiere {energie} {type_chaudiere}'] = ['chaudiere', energie_keywords
                                                                                  ]
td014_gen_ecs_search_dict['chaudiere electrique'] = ['chaudiere',
                                                     ('electricite', 'electrique')]
td014_gen_ecs_search_dict.update({
    "ecs electrique indetermine": [('electricite', 'electrique')],
    "ecs fioul indetermine": ['fioul'],
    "ecs gaz indetermine": ['gaz'],
    "ecs gpl/butane/propane indetermine": ['butane'],
    "ecs charbon indetermine": ['charbon'], })
# solaire_dict = dict()
# for k, v in td014_gen_ecs_search_dict.items():
#     k_solaire = 'ecs solaire thermique + ' + k
#     solaire_dict[k_solaire] = v + ['avec solaire']
# td014_gen_ecs_search_dict.update(solaire_dict)


tr003_desc_to_gen = {'Installation de chauffage avec insert ou poêle bois en appoint': "poele ou insert indetermine",
                     'Installation de chauffage par insert, poêle bois (ou biomasse) avec un chauffage électrique dans la salle de bain': "poele ou insert indetermine",
                     'Installation de chauffage avec en appoint un insert ou poêle bois et un chauffage électrique dans la salle de bain (différent du chauffage principal)': "poele ou insert indetermine",
                     'Installation de chauffage avec chaudière en relève de PAC': "pac indetermine en releve de chaudiere",
                     'Convecteurs bi-jonction': "convecteurs bi-jonction electriques",
                     'Installation de chauffage avec chaudière en relève de PAC avec insert ou poêle bois en appoint': "pac indetermine en releve de chaudiere",
                     'Installation de chauffage avec chaudière  gaz ou fioul en  relève  d’une chaudière bois': "chaudiere bois",
                     }
ordered_ch_labels = ['chauffage solaire'] + list(gen_ch_search_dict_flat.keys()) + ['chauffage autre indetermine',
                                                                                    'indetermine']
ordered_ecs_labels = list(gen_ecs_search_dict_flat.keys()) + ['ecs autre indetermine', 'indetermine']

priorisation_ecs = {'solaire': "solaire",
                    'abscence solaire': "solaire",
                    'ecs thermodynamique': "principal",
                    'chaudiere bois': "principal",
                    'chaudiere': "principal",
                    'production mixte indetermine': "secondaire",
                    'reseau de chaleur': "principal",
                    'effet joule': "secondaire",
                    'chauffe-eau_independant': "defaut",
                    'ecs electrique indetermine': "secondaire",
                    'ecs bois indetermine': "defaut",
                    'ecs fioul indetermine': "defaut",
                    'ecs gaz indetermine': "defaut",
                    'ecs gpl/butane/propane indetermine': "defaut",
                    'ecs charbon indetermine': "defaut", }
