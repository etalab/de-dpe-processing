
# GENERATEURS CHAUFFAGE ET ECS


## elements de recherche communs

mixte = ('mixte', 'combine', 'chauffage ecs','lie ','combine ',"idem")
elec =('electricite', 'electrique','electriques','elec.','joule','joules')
reseau_chaleur = ('"reseau de chaleur"','"reseaux de chaleurs"','"reseaux de chaleur"',
                  '"reseau chaleur"','"chauffage urbain"','"sous-station"')
pac = ('pompe AND chaleur', 'pac ', 'thermodynamique')
type_chaudiere = dict(zip(['standard', 'basse temperature', 'condensation'],
                                               [('standard', 'classique'), '"basse temperature"',
                                                ('condensation', 'condenseurs')]))

chauffe_bain=("individuelle AND ballon", "chauffe-eau", "accumulateur", "chauffe AND bain",'chauffe-bain')

abscence_ecs_solaire = ('"ecs solaire : non"','sans solaire')

## dictionnaires annexes

installation_dict={'Chauffage Collectif':[('collective','collectif','coll','coll.')],
                                 'Chauffage Individuel':[('individuelle','individuel','ind','ind.')],
                                }

energie_dict = {
               'Gaz naturel':['gaz'],
               'Electricité non renouvelable':[elec],
                'Réseau de chaleurs':reseau_chaleur,
                'Fioul domestique':['fioul'],
    'Bois, biomasse':[('bois','biomasse')],
    'Gaz propane ou butane':[('propane','butane','gpl')],
    'Charbon':['charbon'],
               }


energie_dict_lower = {
               'Gaz naturel':'gaz',
               'Electricité non renouvelable':'fioul',
                'Réseau de chaleurs':'reseau de chaleur',
                'Fioul domestique':'fioul',
    'Bois, biomasse':'bois',
    'Gaz propane ou butane':'autre : gpl butane propane',
    'Charbon':'charbon',
               }

energie_mods = energie_dict.keys()
energie_chaudiere_mods = ['gaz','fioul','charbon','autre : gpl butane propane']
type_chaudiere_mods = ['condensation','basse temperature','standard']

bois = ('bois', 'biomasse','flamme verte')


## DICTIONNAIRE DE RECHERCHE : GENERATEUR CHAUFFAGE DANS td003_descriptif et td005_fiches_techniques
# les descriptions des systèmes diffèrent en fonction des diagnostiqueurs et logiciels dans ces tables descriptives.
# la configuration de ces dictionnaires de recherches s'éfforce de répertorier la grande majorité des mots utilisés pour décrire un même système de chauffage
# les générateurs sont classés en catégories.

gen_ch_search_dict = dict()

gen_ch_search_dict['pac'] = {"pac geothermique en releve de chaudiere": [pac, (
    'geothermique', 'geothermie'), 'chaudiere'],
                             "pac eau/eau en releve de chaudiere": [pac, 'eau', 'chaudiere'],
                             "pac air/eau en releve de chaudiere": [pac, 'air AND eau', 'chaudiere'],
                             "pac geothermique": [pac, (
                                 'geothermique', 'geothermie')],
                             "pac air/eau": [pac, 'air AND eau'],
                             "pac eau/eau": [pac, 'eau'],
                             "pac air/air": [pac, ('air', 'split')], }

gen_ch_search_dict['poele'] = {"poele ou insert bois": [('poele', 'insert'), bois],
                               "poele ou insert fioul/gpl": [('poele', 'insert'), ('fioul', 'gpl')], }

chaudiere_dict_ch = dict()

chaudiere_dict_ch['chaudiere bois'] = [('"chaudiere bois"'
                                        , '"chaudiere a bois"'
                                        , '"chaudiere bois/biomasse"'
                                        , '"chaudiere a bois/biomasse"')]
chaudiere_dict_ch['chaudiere electrique'] = ['"chaudiere electrique"']

for type_chaudiere, type_chaudiere_keys in zip(type_chaudiere_mods + ['indetermine'],
                                               [('condensation', 'condenseurs'), '"basse temperature"',
                                                ('standard', 'classique'), None]):

    for energie in energie_chaudiere_mods:
        energie_keywords = energie
        if energie == 'autre : gpl butane propane':
            energie_keywords = ('gpl', 'butane', 'propane')
        if type_chaudiere_keys is not None:
            chaudiere_dict_ch[f'chaudiere {energie} {type_chaudiere}'] = ['chaudiere', energie_keywords,
                                                                          type_chaudiere_keys]
        else:
            chaudiere_dict_ch[f'chaudiere {energie} {type_chaudiere}'] = [('chaudiere', 'collectif'), energie_keywords
                                                                          ]
    chaudiere_dict_ch[f'chaudiere energie indetermine {type_chaudiere}'] = ['chaudiere', type_chaudiere_keys]

# les radiateurs gaz sont inclus avec les chaudieres car ces systèmes sont a priori exclusifs les un des autres.
chaudiere_dict_ch['radiateurs gaz'] = [('radiateur', 'radiateurs'), 'gaz']

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
                                     "convecteurs electriques nfc": [('convecteur', 'convecteurs'),
                                                                     ('nf', 'nfc')],

                                     "autres emetteurs a effet joule": [elec], }
# version flat du dictionnaire par catégorie
gen_ch_search_dict_flat = dict()
{gen_ch_search_dict_flat.update(v) for k, v in gen_ch_search_dict.items()}

reverse_cat_gen_ch = dict()

for cat, v in gen_ch_search_dict.items():
    for label in v:
        reverse_cat_gen_ch[label] = cat



## DICTIONNAIRE DE RECHERCHE : GENERATEUR ECS DANS td003_descriptif et td005_fiches_techniques
# les descriptions des systèmes diffèrent en fonction des diagnostiqueurs et logiciels dans ces tables descriptives.
# la configuration de ces dictionnaires de recherches s'éfforce de répertorier la grande majorité des mots utilisés pour décrire un même système d'ECS
# TODO : ordonner et catégoriser

gen_ecs_search_dict = dict()

gen_ecs_search_dict['solaire']={"ecs solaire" : ["solaire"],}
gen_ecs_search_dict['abscence_solaire']= {
    "abscence ecs solaire": [('"ecs solaire : non"', '"sans solaire"', '"sans ecs solaire"')], }

gen_ecs_search_dict['ecs_thermodynamique']= {
    "ecs thermodynamique electrique(PAC ou ballon)": [
        ('pompe AND chaleur', 'pac', 'thermodynamique', '"air extrait"', '"air exterieur"', '"air ambiant"')],

}
chaudiere_dict_ecs =dict()
chaudiere_dict_ecs['chaudiere bois'] = [('"chaudiere bois"'
                                        , '"chaudiere a bois"'
                                        , '"chaudiere bois/biomasse"'
                                        , '"chaudiere a bois/biomasse"')]
chaudiere_dict_ecs['chaudiere electrique'] = ['"chaudiere electrique"']
for type_chaudiere, type_chaudiere_keys in zip(type_chaudiere_mods + ['indetermine'],
                                               [('condensation', 'condenseurs'), '"basse temperature"',
                                                ('standard', 'classique'), None]):
    for energie in energie_chaudiere_mods:
        energie_keywords = energie
        if energie == 'autre : gpl butane propane':
            energie_keywords = ('gpl', 'butane', 'propane')
        if type_chaudiere_keys is not None:
            chaudiere_dict_ecs[f'chaudiere {energie} {type_chaudiere}'] = ['chaudiere', energie_keywords,
                                                                            type_chaudiere_keys]
        else:
            chaudiere_dict_ecs[f'chaudiere {energie} {type_chaudiere}'] = ['chaudiere', energie_keywords
                                                                            ]
    chaudiere_dict_ecs[f'chaudiere energie indetermine {type_chaudiere}'] = ['chaudiere', type_chaudiere_keys]

chaudiere_dict_ecs.update({

    'production mixte gaz': ["gaz", mixte],
    'production mixte fioul': ["fioul", mixte],
    'production mixte indetermine': [mixte]})

gen_ecs_search_dict['chaudiere']=chaudiere_dict_ecs
gen_ecs_search_dict['reseau de chaleur']= {"reseau de chaleur": [reseau_chaleur], }

gen_ecs_search_dict['effet_joule']= {"ballon a accumulation electrique": [('ballon', 'classique', 'accumulation'), elec],
                                     'chaudiere electrique': ["chaudiere", elec],
                                     "ecs instantanee electrique": ['instantanee', elec],
                                     "ecs electrique indetermine": [elec],
                                     }


gen_ecs_search_dict['chauffe-eau_independant']={    'chauffe-eau gaz independant': [chauffe_bain, "gaz"],
    'chauffe-eau gpl independant': [chauffe_bain, "gpl"],
    'chauffe-eau fioul independant': [chauffe_bain,
                                      "fioul"],
    "chauffe-eau independant indetermine": [
        ("individuelle AND ballon", "chauffe-eau", "accumulateur", "chauffe AND bain")],}

# version flat du dictionnaire par catégorie
gen_ecs_search_dict_flat = dict()
{gen_ecs_search_dict_flat.update(v) for k, v in gen_ecs_search_dict.items()}

reverse_cat_gen_ecs = dict()

for cat, v in gen_ecs_search_dict.items():
    for label in v:
        reverse_cat_gen_ecs[label] = cat

## DICTIONNAIRE DE RECHERCHE : CHAUFFAGE DANS TD012.
# ici l'extraction texte est effectuées sur des champs tabulés normés (tables TV) et nécessite de rechercher uniquement les mots présents dans ces tables TV
# TODO : ordonner et catégoriser
td012_gen_ch_search_dict = {"pac air/air": ['pac', 'air/air', ('electricite', 'electrique')],
                                       "pac air/eau": ['pac', 'air/eau', ('electricite', 'electrique')],
                                       "pac eau/eau": ['pac', 'eau/eau', ('electricite', 'electrique')],
                                       "pac geothermique": ['pac', (
                                           'geothermique', 'geothermie'),
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
                            "chaudiere bois": ['chaudiere',('bois','biomasse')],
                            "autres emetteurs a effet joule": [('electricite', 'electrique')],
                            "reseau de chaleur": ['reseau', 'chaleur'],
                            "convecteurs bi-jonction": ['bi', 'jonction', ('electricite', 'electrique')],
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