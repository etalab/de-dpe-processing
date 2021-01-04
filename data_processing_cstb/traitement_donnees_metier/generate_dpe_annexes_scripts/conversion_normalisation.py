# TYPE ENERGIE

energie_normalise_ordered = ["autre", "charbon", "electricite renouvelable", "autre : gpl butane propane",
                             'bois', 'reseau de chaleur', 'fioul',
                             'gaz', 'electricite'
                             ]
energie_normalise_ordered = list(reversed(energie_normalise_ordered))

energy_cols = ['tv042_type_energie', 'tr004_description', 'tv045_energie', 'tv044_type_energie']

ener_conv_dict = dict()

ener_conv_dict['tv045_energie'] = {"Electricité (hors électricité d'origine renouvelab": "electricite",
                                   'Gaz naturel': "gaz",
                                   'Réseau de chaleurs': "reseau de chaleur",
                                   'Fioul domestique': "fioul",
                                   'Bois, biomasse': "bois",
                                   'Gaz propane ou butane': "autre : gpl butane propane",
                                   "Electricité d'origine renouvelable utilisée dans l": "electricite renouvelable",
                                   "Charbon": "charbon"}

ener_conv_dict['tv044_type_energie'] = {'Bois de chauffage': 'bois',
                                        'Gaz naturel': 'gaz',
                                        'Gaz propane ou butane': 'autre : gpl butane propane',
                                        'Fioul domestique': 'fioul',
                                        'Charbon': 'charbon',
                                        'Electricité': 'electricite',
                                        'Réseau urbain': 'reseau de chaleur', }

ener_conv_dict['tr004_description'] = {'Bois, Biomasse': 'bois',
                                       'Electricité': 'electricite',
                                       'Gaz': 'gaz',
                                       'Autres énergies': 'autre',
                                       "Production d'électricité": "electricite renouvelable"}

ener_conv_dict['tv042_type_energie'] = {'Electricité': "electricite",
                                        'Fioul': "fioul",
                                        'Chauffage urbain': "reseau de chaleur",
                                        'Propane': "autre : gpl butane propane",
                                        'Charbon': "charbon",
                                        'Bois': "bois",
                                        'Gaz': "gaz", }

for k, v in ener_conv_dict.items():

    assert (len(set(v.values()) - set(energie_normalise_ordered)) == 0)

type_installation_conv_dict = dict()

type_installation_conv_dict['tv027_type_installation'] = {
    'Individuelle': 'individuel',
    'Collective': 'collectif',

}
type_installation_conv_dict['tv040_type_installation'] = {
    'Individuelle': 'individuel',
    'Collective': 'collectif',

}

type_installation_conv_dict['tv025_type_installation'] = {
    'Chauffage Individuel': 'individuel',
    'Chauffage Collectif': 'collectif',

}
