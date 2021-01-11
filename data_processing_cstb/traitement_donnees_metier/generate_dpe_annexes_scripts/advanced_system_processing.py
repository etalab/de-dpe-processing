import pandas as pd
from .text_matching_dict import gen_ch_search_dict_flat, reverse_cat_gen_ch, gen_ch_search_dict, reverse_cat_gen_ecs, \
    gen_ecs_search_dict, gen_ecs_search_dict_flat
from .conversion_normalisation import energie_normalise_ordered


def main_advanced_system_processing(td001_sys_ch, td001,
                                    td001_sys_ecs, td003, td005, td012_p, td014_p):
    td001_sys = td001.rename(columns={'id': 'td001_dpe_id'})[['td001_dpe_id', 'tr002_type_batiment_id']].merge(
        td001_sys_ch[['td001_dpe_id',
                      'gen_ch_lib_infer_concat',
                      ]], on='td001_dpe_id',
        how='left')
    td001_sys = td001_sys.merge(td001_sys_ecs[['td001_dpe_id', 'gen_ecs_lib_infer_concat']], on='td001_dpe_id',
                                how='left')


def concat_and_sort_gen_ch_lib(gen_ch_lib_desc, gen_ch_lib_ft, td012_p):
    """

    assemblage des libéllés de chauffage extraits des fiches techniques/descriptifs et extrait du modèle de donnée affilié 3CL.
    chaque libéllé a une catégorie parente affectée.

    Règle de simplification n°1 :On ne peut avoir deux systèmes de la même catégorie dans la description des générateurs de chauffage
    i.e chaudiere gaz condensation et chaudiere fioul standard sont dans la même catégorie "chaudiere". Un seul libéllé sera retenu.

    Règle n°2  : les libéllés sont ordonnées selon 3 critères :

    * determination :  si un libéllé dans une catégorie est mieux déterminé qu'un autre alors il est classé plus haut (chaudiere gaz standard > chaudiere gaz indetermine)

    * probabilité d'être le système principal :  chaudiere gaz standard > chaudiere fioul standard. Si on a fioul et gaz dans un logement la probabilité est bien plus grande d'avoir chaudière gaz + appoint fioul que l'inverse

    * identification certaine : si certains libéllés sont identifiables avec

    Règle n°3  : les données ne sont pour l'instant pas priorisées si elles viennent du modèle de données 3CL vs description texte.

    Returns
    -------

    """

    # libéllés des generateurs chauffage issues des données textes
    gen_ch_lib_from_txt = pd.concat([gen_ch_lib_desc[['label', 'category', 'td001_dpe_id']],
                                     gen_ch_lib_ft[['label', 'category', 'td001_dpe_id']]], axis=0)

    gen_ch_lib_from_txt = gen_ch_lib_from_txt.sort_values(by=['td001_dpe_id', 'category', 'label'])

    gen_ch_lib_from_txt = gen_ch_lib_from_txt.drop_duplicates(subset=['category', 'td001_dpe_id'], keep='first')

    # suppression de la dénomination exact qui ne correpond qu'a une méthode de recherche plus rigide
    gen_ch_lib_from_txt.label = gen_ch_lib_from_txt.label.str.replace(' exact', '')

    gen_ch_lib_from_txt['source'] = 'txt'

    # libéllés des générateurs chauffage issus de la table td012
    gen_ch_lib_from_data = td012_p[['td001_dpe_id', 'gen_ch_lib_infer']].copy()

    gen_ch_lib_from_data.columns = ['td001_dpe_id', 'label']

    # on affecte les mêmes catégories aux données issue du modèle 3CL (from_data)
    gen_ch_lib_from_data['category'] = gen_ch_lib_from_data.label.replace(reverse_cat_gen_ch)

    gen_ch_lib_from_data['label'] = pd.Categorical(gen_ch_lib_from_data.label,
                                                   categories=list(gen_ch_search_dict_flat.keys()) + ['indetermine'],
                                                   ordered=True)
    gen_ch_lib_from_data['source'] = 'data'

    # concat.
    gen_ch_lib = pd.concat([gen_ch_lib_from_data, gen_ch_lib_from_txt], axis=0)

    gen_ch_lib['source'] = pd.Categorical(gen_ch_lib.source, categories=['data', 'txt'], ordered=True)
    gen_ch_lib['category'] = pd.Categorical(gen_ch_lib.category, categories=list(gen_ch_search_dict.keys()),
                                            ordered=True)
    return gen_ch_lib


def concat_and_sort_type_installation_ch_lib(type_installation_ch_desc, type_installation_ch_ft, td011_p):
    """

    assemblage des libéllés de type installation de chauffage extraits des fiches techniques/descriptifs et extrait du modèle de donnée affilié 3CL.
    on fait un tri collectif>individuel (si on trouve la mention de termes collectifs on les priorise par rapports aux termes individuels

    -------

    """

    # libéllés des generateurs chauffage issues des données textes
    type_installation_ch_from_txt = pd.concat([type_installation_ch_desc[['label', 'td001_dpe_id']],
                                               type_installation_ch_ft[['label', 'td001_dpe_id']]], axis=0)

    type_installation_ch_from_txt['label'] = pd.Categorical(type_installation_ch_from_txt['label'],
                                                            categories=['collectif', 'individuel', 'indetermine'],
                                                            ordered=True)

    type_installation_ch_from_txt = type_installation_ch_from_txt.sort_values(by=['td001_dpe_id', 'label'])

    type_installation_ch_from_txt = type_installation_ch_from_txt.drop_duplicates(subset=['td001_dpe_id'], keep='first')

    type_installation_ch_from_txt['source'] = 'txt'

    type_installation_ch_from_data = td011_p[['td001_dpe_id', 'type_installation_ch']].copy()
    type_installation_ch_from_data.columns = ['td001_dpe_id', 'label']
    type_installation_ch_from_data['label'] = type_installation_ch_from_data['label'].fillna('indetermine')
    type_installation_ch_from_data['label'] = pd.Categorical(type_installation_ch_from_data['label'],
                                                             categories=['collectif', 'individuel', 'indetermine'],
                                                             ordered=True)
    type_installation_ch_from_data['source'] = 'data'
    type_installation_ch = pd.concat([type_installation_ch_from_data, type_installation_ch_from_txt], axis=0)

    type_installation_ch['is_indetermine'] = type_installation_ch.label == 'indetermine'

    return type_installation_ch


def concat_and_sort_energie_ch_lib(energie_ch_desc, energie_ch_ft, td012_p):
    """

    assemblage des libéllés de l'energie extraits des fiches techniques/descriptifs et extrait du modèle de donnée affilié 3CL.

    -------

    """

    # libéllés des generateurs chauffage issues des données textes
    energie_ch_from_txt = pd.concat([energie_ch_desc[['label', 'td001_dpe_id']],
                                     energie_ch_ft[['label', 'td001_dpe_id']]], axis=0)

    energie_ch_from_txt['label'] = pd.Categorical(energie_ch_from_txt['label'],
                                                  categories=energie_normalise_ordered, ordered=True)

    energie_ch_from_txt = energie_ch_from_txt.sort_values(by=['td001_dpe_id', 'label'])

    energie_ch_from_txt = energie_ch_from_txt.drop_duplicates(subset=['td001_dpe_id'], keep='first')

    energie_ch_from_txt['source'] = 'txt'

    energie_ch_from_data = td012_p[['td001_dpe_id', 'type_energie_ch']].copy()
    energie_ch_from_data.columns = ['td001_dpe_id', 'label']
    energie_ch_from_data['label'] = energie_ch_from_data['label'].fillna('indetermine')
    energie_ch_from_data['label'] = pd.Categorical(energie_ch_from_data['label'],
                                                   categories=energie_normalise_ordered, ordered=True)
    energie_ch_from_data['source'] = 'data'
    energie_ch = pd.concat([energie_ch_from_data, energie_ch_from_txt], axis=0)

    energie_ch['is_indetermine'] = energie_ch.label == 'indetermine'

    return energie_ch


def concat_and_sort_gen_ecs_lib(gen_ecs_lib_desc, gen_ecs_lib_ft, td014_p):
    """

    assemblage des libéllés de chauffage extraits des fiches techniques/descriptifs et extrait du modèle de donnée affilié 3CL.
    chaque libéllé a une catégorie parente affectée.

    Règle de simplification n°1 :On ne peut avoir deux systèmes de la même catégorie dans la description des générateurs de chauffage
    i.e chaudiere gaz condensation et chaudiere fioul standard sont dans la même catégorie "chaudiere". Un seul libéllé sera retenu.

    Règle n°2  : les libéllés sont ordonnées selon 3 critères :

    * determination :  si un libéllé dans une catégorie est mieux déterminé qu'un autre alors il est classé plus haut (chaudiere gaz standard > chaudiere gaz indetermine)

    * probabilité d'être le système principal :  chaudiere gaz standard > chaudiere fioul standard. Si on a fioul et gaz dans un logement la probabilité est bien plus grande d'avoir chaudière gaz + appoint fioul que l'inverse

    * identification certaine : si certains libéllés sont identifiables avec

    Règle n°3  : les données ne sont pour l'instant pas priorisées si elles viennent du modèle de données 3CL vs description texte.

    Returns
    -------

    """

    # libéllés des generateurs chauffage issues des données textes
    gen_ecs_lib_from_txt = pd.concat([gen_ecs_lib_desc[['label', 'category', 'td001_dpe_id']],
                                      gen_ecs_lib_ft[['label', 'category', 'td001_dpe_id']]], axis=0)

    gen_ecs_lib_from_txt = gen_ecs_lib_from_txt.sort_values(by=['td001_dpe_id', 'category', 'label'])

    gen_ecs_lib_from_txt = gen_ecs_lib_from_txt.drop_duplicates(subset=['category', 'td001_dpe_id'], keep='first')

    # suppression de la dénomination exact qui ne correpond qu'a une méthode de recherche plus rigide
    gen_ecs_lib_from_txt.label = gen_ecs_lib_from_txt.label.str.replace(' exact', '')

    gen_ecs_lib_from_txt['source'] = 'txt'

    # libéllés des générateurs chauffage issus de la table td012
    gen_ecs_lib_from_data = td014_p[['td001_dpe_id', 'gen_ecs_lib_infer']].copy()

    gen_ecs_lib_from_data.columns = ['td001_dpe_id', 'label']
    gen_ecs_lib_from_data['label'] = gen_ecs_lib_from_data['label'].fillna('indetermine')
    gen_ecs_lib_from_data['category'] = gen_ecs_lib_from_data.label.replace(reverse_cat_gen_ecs)

    gen_ecs_lib_from_data['label'] = pd.Categorical(gen_ecs_lib_from_data.label,
                                                    categories=list(gen_ecs_search_dict_flat.keys()) + ['indetermine'],
                                                    ordered=True)
    gen_ecs_lib_from_data['label'] = gen_ecs_lib_from_data['label'].fillna('indetermine')

    gen_ecs_lib_from_data['source'] = 'data'

    # concat.
    gen_ecs_lib = pd.concat([gen_ecs_lib_from_data, gen_ecs_lib_from_txt], axis=0)

    gen_ecs_lib['source'] = pd.Categorical(gen_ecs_lib.source, categories=['data', 'txt'], ordered=True)
    gen_ecs_lib['category'] = pd.Categorical(gen_ecs_lib.category, categories=list(gen_ecs_search_dict.keys()),
                                             ordered=True)
    return gen_ecs_lib
