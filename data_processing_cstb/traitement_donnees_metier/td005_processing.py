from utils import strip_accents,affect_lib_by_matching_score
mixte = ('mixte', 'combine', 'chauffage + ecs','chauffage et ecs','lie ','combine ')
gen_ecs_normalized_lib_matching_dict_ft = {
    "ballon thermodynamique electrique": [('thermodynamique', 'air extrait', 'air exterieur'),
                                          ('thermodynamique', 'air extrait', 'air exterieur'),

                                          ],
    "pompe a chaleur combinee ecs/chauffage (indeterminee)": [
        ('pac', 'pompe a chaleur'),
        mixte
    ],
    "pompe a chaleur combinee ecs/chauffage (pac air/eau)": [
        ('pac', 'pompe a chaleur'),
        mixte, 'air/eau'
    ],
    "pompe a chaleur combinee ecs/chauffage (pac geothermique)": [
        ('pac', 'pompe a chaleur'),
        mixte, 'geothermique'
    ],

    "pompe a chaleur combinee ecs/chauffage (pac air/air)": [
        ('pac', 'pompe a chaleur'),
        ('mixte', 'combine'), ('air/air', 'split')
    ],
    "pompe a chaleur combinee ecs/chauffage (pac eau/eau)": [
        ('pac', 'pompe a chaleur'),
        ('mixte', 'combine'), 'eau/eau'
    ],
    "ballon electrique": [('electricite', 'electrique','elec')],
    "ecs instantanee electrique": [('instantanee', 'instantane'), ('electricite', 'electrique')],

    'chaudiere mixte gaz': ["chaudiere", mixte, ("gaz", "gpl")],
    'chaudiere mixte fioul': ["chaudiere", mixte, "fioul"],
    'chaudiere mixte bois': ["chaudiere", mixte, "classe",
                             ("bois", "biomasse")],

    'chaudiere mixte indeterminee': ["chaudiere", mixte],
    'production mixte indeterminee': [ mixte],
    'chauffe-eau gaz independant': [("ballon", "chauffe-eau", "accumulateur", "chauffe bain"), "gaz"],
    'chauffe-eau gpl independant': [("ballon", "chauffe-eau", "accumulateur", "chauffe bain"), "gpl", 'gpl'],

    'chauffe-eau fioul independant': [("ballon", "chauffe-eau", "accumulateur", "bain"), "fioul", 'fioul'],
    "ecs collective reseau de chaleur": ["reseau de chaleur"],

    'chaudiere gaz': ["chaudiere", "gaz"],
    'chaudiere gpl': ["chaudiere", "gpl", 'gpl'],
    'chaudiere fioul': ["chaudiere", "fioul", 'fioul'],
    'chaudiere indeterminee': ["chaudiere"],

}

def postprocessing_ecs_ft(td005):
    td005_ecs = td005.loc[td005.tr011_sous_categorie_fiche_technique_id == '17']
    vr_ecs = td005_ecs.valeur_renseignee.str.lower().apply(lambda x: strip_accents(x))
    sys_ecs_lib_infer_ft=vr_ecs.apply(lambda x:affect_lib_by_matching_score(x,gen_ecs_normalized_lib_matching_dict_ft))