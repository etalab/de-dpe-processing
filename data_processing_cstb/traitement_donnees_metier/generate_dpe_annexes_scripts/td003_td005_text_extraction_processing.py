from .text_matching_dict import gen_ch_search_dict, gen_ch_search_dict_flat, gen_ecs_search_dict_flat, \
    gen_ecs_search_dict, reverse_cat_gen_ecs, reverse_cat_gen_ch, installation_dict, energie_dict, \
    solaire_ch_search_dict
from .utils_elasticsearch import search_and_affect, categorize_search_res
from .utils import strip_accents, clean_desc_txt


# EXTRACT CHAUFFAGE DATA

def extract_td005_ch_variables(td005):
    """
    extract generateur,type installation et energie des systèmes de chauffage depuis les donnees textes td005 + présence de chauffage solaire

    Parameters
    ----------
    td005

    Returns
    -------

    """
    td005_ch = td005.loc[td005.tr011_sous_categorie_fiche_technique_id == '16']
    td005_ch.descriptif = td005_ch.valeur_renseignee.fillna('indetermine')

    td005_ch.valeur_renseignee = td005_ch.valeur_renseignee.str.lower().apply(lambda x: strip_accents(x))
    td005_ch.valeur_renseignee = td005_ch.valeur_renseignee.apply(lambda x: clean_desc_txt(x))

    m = search_and_affect(td005_ch, id_col='id', val_col='valeur_renseignee',
                          search_dict=gen_ch_search_dict_flat)

    m = categorize_search_res(m, label_cat=list(gen_ch_search_dict_flat.keys()) + ['indetermine'],
                              category_dict=reverse_cat_gen_ch)

    gen_ch_lib_ft = m.merge(td005_ch[['id', 'td001_dpe_id']], how='left')

    m = search_and_affect(td005_ch, id_col='id', val_col='valeur_renseignee',
                          search_dict=installation_dict)

    type_installation_ch_ft = m.merge(td005_ch[['id', 'td001_dpe_id']], how='left')

    m = search_and_affect(td005_ch, id_col='id', val_col='valeur_renseignee',
                          search_dict=energie_dict)

    energie_ch_ft = m.merge(td005_ch[['id', 'td001_dpe_id']], how='left')

    m = search_and_affect(td005_ch, id_col='id', val_col='valeur_renseignee',
                          search_dict=solaire_ch_search_dict)
    solaire_ch_ft = m.merge(td005_ch[['id', 'td001_dpe_id']], how='left')

    return gen_ch_lib_ft, type_installation_ch_ft, energie_ch_ft, solaire_ch_ft


def extract_td003_ch_variables(td003):
    """
    extract generateur,type installation et energie des systèmes de chauffage depuis les donnees textes td003 + présence de chauffage solaire

    Parameters
    ----------
    td003

    Returns
    -------

    """

    td003_ch = td003.loc[td003.tr007_type_descriptif_id == '11']
    td003_ch.descriptif = td003_ch.descriptif.fillna('indetermine')
    td003_ch.descriptif = td003_ch.descriptif.str.lower().apply(lambda x: strip_accents(x))
    td003_ch.descriptif = td003_ch.descriptif.apply(lambda x: clean_desc_txt(x))

    m = search_and_affect(td003_ch, id_col='id', val_col='descriptif',
                          search_dict=gen_ch_search_dict_flat)

    m = categorize_search_res(m, label_cat=list(gen_ch_search_dict_flat.keys()) + ['indetermine'],
                              category_dict=reverse_cat_gen_ch)
    gen_ch_lib_desc = m.merge(td003_ch[['id', 'td001_dpe_id']], how='left')

    m = search_and_affect(td003_ch, id_col='id', val_col='descriptif',
                          search_dict=installation_dict)

    type_installation_ch_desc = m.merge(td003_ch[['id', 'td001_dpe_id']], how='left')

    m = search_and_affect(td003_ch, id_col='id', val_col='descriptif',
                          search_dict=energie_dict)

    energie_ch_desc = m.merge(td003_ch[['id', 'td001_dpe_id']], how='left')

    m = search_and_affect(td003_ch, id_col='id', val_col='descriptif',
                          search_dict=solaire_ch_search_dict)

    solaire_ch_desc = m.merge(td003_ch[['id', 'td001_dpe_id']], how='left')

    return gen_ch_lib_desc, type_installation_ch_desc, energie_ch_desc, solaire_ch_desc


# EXTRACT ECS DATA

def extract_td005_ecs_variables(td005):
    """
    extract generateur,type installation et energie des systèmes d'ecs depuis les donnees textes td005

    Parameters
    ----------
    td005

    Returns
    -------

    """

    td005_ecs = td005.loc[td005.tr011_sous_categorie_fiche_technique_id == '17']
    td005_ecs.valeur_renseignee = td005_ecs.valeur_renseignee.fillna('indetermine')

    td005_ecs.valeur_renseignee = td005_ecs.valeur_renseignee.str.lower().apply(lambda x: strip_accents(x))
    td005_ecs.valeur_renseignee = td005_ecs.valeur_renseignee.apply(lambda x: clean_desc_txt(x))

    m = search_and_affect(td005_ecs, id_col='id', val_col='valeur_renseignee',
                          search_dict=gen_ecs_search_dict_flat)
    m = categorize_search_res(m, label_cat=list(gen_ecs_search_dict_flat.keys()) + ['indetermine'],
                              category_dict=reverse_cat_gen_ecs)
    gen_ecs_lib_ft = m.merge(td005_ecs[['id', 'td001_dpe_id']], how='left')

    m = search_and_affect(td005_ecs, id_col='id', val_col='valeur_renseignee',
                          search_dict=installation_dict)

    type_installation_ecs_ft = m.merge(td005_ecs[['id', 'td001_dpe_id']], how='left')

    m = search_and_affect(td005_ecs, id_col='id', val_col='valeur_renseignee',
                          search_dict=energie_dict)

    energie_ecs_ft = m.merge(td005_ecs[['id', 'td001_dpe_id']], how='left')

    return gen_ecs_lib_ft, type_installation_ecs_ft, energie_ecs_ft


def extract_td003_ecs_variables(td003):
    """
    extract generateur,type installation et energie des systèmes d'ecs depuis les donnees textes td003

    Parameters
    ----------
    td003

    Returns
    -------

    """
    td003_ecs = td003.loc[td003.tr007_type_descriptif_id == '10']
    td003_ecs.descriptif = td003_ecs.descriptif.fillna('indetermine')
    td003_ecs.descriptif = td003_ecs.descriptif.str.lower().apply(lambda x: strip_accents(x))
    td003_ecs.descriptif = td003_ecs.descriptif.apply(lambda x: clean_desc_txt(x))

    m = search_and_affect(td003_ecs, id_col='id', val_col='descriptif',
                          search_dict=gen_ecs_search_dict_flat)
    m = categorize_search_res(m, label_cat=list(gen_ecs_search_dict_flat.keys()) + ['indetermine'],
                              category_dict=reverse_cat_gen_ecs)
    gen_ecs_lib_desc = m.merge(td003_ecs[['id', 'td001_dpe_id']], how='left')

    m = search_and_affect(td003_ecs, id_col='id', val_col='descriptif',
                          search_dict=installation_dict)

    type_installation_ecs_desc = m.merge(td003_ecs[['id', 'td001_dpe_id']], how='left')

    m = search_and_affect(td003_ecs, id_col='id', val_col='descriptif',
                          search_dict=energie_dict)

    energie_ecs_desc = m.merge(td003_ecs[['id', 'td001_dpe_id']], how='left')

    return gen_ecs_lib_desc, type_installation_ecs_desc, energie_ecs_desc
