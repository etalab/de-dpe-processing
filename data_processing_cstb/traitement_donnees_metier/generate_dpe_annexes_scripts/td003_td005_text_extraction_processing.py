from .text_matching_dict import gen_ch_search_dict_flat, gen_ecs_search_dict_flat, \
    reverse_cat_gen_ecs, reverse_cat_gen_ch, installation_search_dict, energie_search_dict, \
    solaire_ch_search_dict, ph_materiau_search_dict, pb_materiau_search_dict, \
    materiau_baie_search_dict, isolation_search_dict, type_vitrage_search_dict, type_remplissage_search_dict, \
    orientation_baie_search_dict
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
    td005_ch.valeur_renseignee = td005_ch.valeur_renseignee.fillna('indetermine')

    td005_ch.valeur_renseignee = td005_ch.valeur_renseignee.str.lower().apply(lambda x: strip_accents(x))
    td005_ch.valeur_renseignee = td005_ch.valeur_renseignee.apply(lambda x: clean_desc_txt(x))

    m = search_and_affect(td005_ch, id_col='id', val_col='valeur_renseignee',
                          search_dict=gen_ch_search_dict_flat)

    m = categorize_search_res(m, label_cat=list(gen_ch_search_dict_flat.keys()) + ['indetermine'],
                              category_dict=reverse_cat_gen_ch)

    gen_ch_lib_ft = m.merge(td005_ch[['id', 'td001_dpe_id']], how='left')

    m = search_and_affect(td005_ch, id_col='id', val_col='valeur_renseignee',
                          search_dict=installation_search_dict)

    type_installation_ch_ft = m.merge(td005_ch[['id', 'td001_dpe_id']], how='left')

    m = search_and_affect(td005_ch, id_col='id', val_col='valeur_renseignee',
                          search_dict=energie_search_dict)

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
                          search_dict=installation_search_dict)

    type_installation_ch_desc = m.merge(td003_ch[['id', 'td001_dpe_id']], how='left')

    m = search_and_affect(td003_ch, id_col='id', val_col='descriptif',
                          search_dict=energie_search_dict)

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
                          search_dict=installation_search_dict)

    type_installation_ecs_ft = m.merge(td005_ecs[['id', 'td001_dpe_id']], how='left')

    m = search_and_affect(td005_ecs, id_col='id', val_col='valeur_renseignee',
                          search_dict=energie_search_dict)

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
                          search_dict=installation_search_dict)

    type_installation_ecs_desc = m.merge(td003_ecs[['id', 'td001_dpe_id']], how='left')

    m = search_and_affect(td003_ecs, id_col='id', val_col='descriptif',
                          search_dict=energie_search_dict)

    energie_ecs_desc = m.merge(td003_ecs[['id', 'td001_dpe_id']], how='left')

    return gen_ecs_lib_desc, type_installation_ecs_desc, energie_ecs_desc


def extract_td003_murs_variables(td003):
    """
    extract materiau and isolation des murs depuis td003

    Parameters
    ----------
    td003

    Returns
    -------

    """

    td003_mur = td003.loc[td003.tr007_type_descriptif_id == '6']
    td003_mur.descriptif = td003_mur.descriptif.fillna('indetermine')

    td003_mur.descriptif = td003_mur.descriptif.str.lower().apply(lambda x: strip_accents(x))
    td003_mur.descriptif = td003_mur.descriptif.apply(lambda x: clean_desc_txt(x))

    m = search_and_affect(td003_mur, id_col='id', val_col='descriptif',
                          search_dict=ph_materiau_search_dict)

    materiau_mur_desc = m.merge(td003_mur[['id', 'td001_dpe_id', 'descriptif']], how='left')

    m = search_and_affect(td003_mur, id_col='id', val_col='descriptif',
                          search_dict=isolation_search_dict)

    isolation_mur_desc = m.merge(td003_mur[['id', 'td001_dpe_id']], how='left')

    return materiau_mur_desc, isolation_mur_desc


def extract_td005_murs_variables(td005):
    """
    extract materiau and isolation des murs depuis td005

    Parameters
    ----------
    td003

    Returns
    -------

    """

    td005_mur = td005.loc[td005.tr011_sous_categorie_fiche_technique_id == '9']
    td005_mur.valeur_renseignee = td005_mur.valeur_renseignee.fillna('indetermine')

    td005_mur.valeur_renseignee = td005_mur.valeur_renseignee.str.lower().apply(lambda x: strip_accents(x))
    td005_mur.valeur_renseignee = td005_mur.valeur_renseignee.apply(lambda x: clean_desc_txt(x))

    m = search_and_affect(td005_mur, id_col='id', val_col='valeur_renseignee',
                          search_dict=ph_materiau_search_dict)

    materiau_mur_ft = m.merge(td005_mur[['id', 'td001_dpe_id', 'valeur_renseignee']], how='left')

    m = search_and_affect(td005_mur, id_col='id', val_col='valeur_renseignee',
                          search_dict=isolation_search_dict)

    isolation_mur_ft = m.merge(td005_mur[['id', 'td001_dpe_id']], how='left')

    return materiau_mur_ft, isolation_mur_ft


def extract_td003_ph_variables(td003):
    """
    extract materiau and isolation des planchers hauts depuis td003

    Parameters
    ----------
    td003

    Returns
    -------

    """

    td003_ph = td003.loc[td003.tr007_type_descriptif_id == '16']
    td003_ph.descriptif = td003_ph.descriptif.fillna('indetermine')

    td003_ph.descriptif = td003_ph.descriptif.str.lower().apply(lambda x: strip_accents(x))
    td003_ph.descriptif = td003_ph.descriptif.apply(lambda x: clean_desc_txt(x))

    m = search_and_affect(td003_ph, id_col='id', val_col='descriptif',
                          search_dict=ph_materiau_search_dict)

    materiau_ph_desc = m.merge(td003_ph[['id', 'td001_dpe_id', 'descriptif']], how='left')

    m = search_and_affect(td003_ph, id_col='id', val_col='descriptif',
                          search_dict=isolation_search_dict)

    isolation_ph_desc = m.merge(td003_ph[['id', 'td001_dpe_id']], how='left')

    return materiau_ph_desc, isolation_ph_desc


def extract_td005_ph_variables(td005):
    """
    extract materiau and isolation des planchers hauts depuis td005

    Parameters
    ----------
    td003

    Returns
    -------

    """

    td005_ph = td005.loc[td005.tr011_sous_categorie_fiche_technique_id == '11']
    td005_ph.valeur_renseignee = td005_ph.valeur_renseignee.fillna('indetermine')

    td005_ph.valeur_renseignee = td005_ph.valeur_renseignee.str.lower().apply(lambda x: strip_accents(x))
    td005_ph.valeur_renseignee = td005_ph.valeur_renseignee.apply(lambda x: clean_desc_txt(x))

    m = search_and_affect(td005_ph, id_col='id', val_col='valeur_renseignee',
                          search_dict=ph_materiau_search_dict)

    materiau_ph_ft = m.merge(td005_ph[['id', 'td001_dpe_id', 'valeur_renseignee']], how='left')

    m = search_and_affect(td005_ph, id_col='id', val_col='valeur_renseignee',
                          search_dict=isolation_search_dict)

    isolation_ph_ft = m.merge(td005_ph[['id', 'td001_dpe_id']], how='left')

    return materiau_ph_ft, isolation_ph_ft


def extract_td003_pb_variables(td003):
    """
    extract materiau and isolation des planchers bas depuis td003

    Parameters
    ----------
    td003

    Returns
    -------

    """

    td003_pb = td003.loc[td003.tr007_type_descriptif_id == '8']
    td003_pb.descriptif = td003_pb.descriptif.fillna('indetermine')

    td003_pb.descriptif = td003_pb.descriptif.str.lower().apply(lambda x: strip_accents(x))
    td003_pb.descriptif = td003_pb.descriptif.apply(lambda x: clean_desc_txt(x))

    m = search_and_affect(td003_pb, id_col='id', val_col='descriptif',
                          search_dict=pb_materiau_search_dict)

    materiau_pb_desc = m.merge(td003_pb[['id', 'td001_dpe_id', 'descriptif']], how='left')

    m = search_and_affect(td003_pb, id_col='id', val_col='descriptif',
                          search_dict=isolation_search_dict)

    isolation_pb_desc = m.merge(td003_pb[['id', 'td001_dpe_id']], how='left')

    return materiau_pb_desc, isolation_pb_desc


def extract_td005_pb_variables(td005):
    """
    extract materiau and isolation des planchers bas depuis td005

    Parameters
    ----------
    td003

    Returns
    -------

    """

    td005_pb = td005.loc[td005.tr011_sous_categorie_fiche_technique_id == '10']
    td005_pb.valeur_renseignee = td005_pb.valeur_renseignee.fillna('indetermine')

    td005_pb.valeur_renseignee = td005_pb.valeur_renseignee.str.lower().apply(lambda x: strip_accents(x))
    td005_pb.valeur_renseignee = td005_pb.valeur_renseignee.apply(lambda x: clean_desc_txt(x))

    m = search_and_affect(td005_pb, id_col='id', val_col='valeur_renseignee',
                          search_dict=pb_materiau_search_dict)

    materiau_pb_ft = m.merge(td005_pb[['id', 'td001_dpe_id', 'valeur_renseignee']], how='left')

    m = search_and_affect(td005_pb, id_col='id', val_col='valeur_renseignee',
                          search_dict=isolation_search_dict)

    isolation_pb_ft = m.merge(td005_pb[['id', 'td001_dpe_id']], how='left')

    return materiau_pb_ft, isolation_pb_ft


def extract_td005_baie_variables(td005):
    """
    extract materiau type,remplissage orientation des baies

    Parameters
    ----------
    td003

    Returns
    -------

    """

    td005_fen = td005.loc[td005.tr011_sous_categorie_fiche_technique_id == '12']
    td005_fen.valeur_renseignee = td005_fen.valeur_renseignee.fillna('indetermine')

    td005_fen.valeur_renseignee = td005_fen.valeur_renseignee.str.lower().apply(lambda x: strip_accents(x))
    td005_fen.valeur_renseignee = td005_fen.valeur_renseignee.apply(lambda x: clean_desc_txt(x))

    m = search_and_affect(td005_fen, id_col='id', val_col='valeur_renseignee',
                          search_dict=type_vitrage_search_dict)

    type_vitrage_ft = m.merge(td005_fen[['id', 'td001_dpe_id', 'valeur_renseignee']], how='left')

    m = search_and_affect(td005_fen, id_col='id', val_col='valeur_renseignee',
                          search_dict=type_remplissage_search_dict)

    type_remplissage_ft = m.merge(td005_fen[['id', 'td001_dpe_id', 'valeur_renseignee']], how='left')

    m = search_and_affect(td005_fen, id_col='id', val_col='valeur_renseignee',
                          search_dict=materiau_baie_search_dict)

    materiau_baie_ft = m.merge(td005_fen[['id', 'td001_dpe_id', 'valeur_renseignee']], how='left')

    m = search_and_affect(td005_fen, id_col='id', val_col='valeur_renseignee',
                          search_dict=orientation_baie_search_dict)

    orientation_baie_ft = m.merge(td005_fen[['id', 'td001_dpe_id', 'valeur_renseignee']], how='left')

    return type_vitrage_ft,type_remplissage_ft,materiau_baie_ft,orientation_baie_ft


def extract_td003_baie_variables(td003):
    """
    extract materiau type,remplissage orientation des baies

    Parameters
    ----------
    td003

    Returns
    -------

    """

    td003_fen = td003.loc[td003.tr007_type_descriptif_id == '5']
    td003_fen.descriptif = td003_fen.descriptif.fillna('indetermine')

    td003_fen.descriptif = td003_fen.descriptif.str.lower().apply(lambda x: strip_accents(x))
    td003_fen.descriptif = td003_fen.descriptif.apply(lambda x: clean_desc_txt(x))

    m = search_and_affect(td003_fen, id_col='id', val_col='valeur_renseignee',
                          search_dict=type_vitrage_search_dict)

    type_vitrage_desc = m.merge(td003_fen[['id', 'td001_dpe_id', 'valeur_renseignee']], how='ledesc')

    m = search_and_affect(td003_fen, id_col='id', val_col='valeur_renseignee',
                          search_dict=type_remplissage_search_dict)

    type_remplissage_desc = m.merge(td003_fen[['id', 'td001_dpe_id', 'valeur_renseignee']], how='ledesc')

    m = search_and_affect(td003_fen, id_col='id', val_col='valeur_renseignee',
                          search_dict=materiau_baie_search_dict)

    materiau_baie_desc = m.merge(td003_fen[['id', 'td001_dpe_id', 'valeur_renseignee']], how='ledesc')

    m = search_and_affect(td003_fen, id_col='id', val_col='valeur_renseignee',
                          search_dict=orientation_baie_search_dict)

    orientation_baie_desc = m.merge(td003_fen[['id', 'td001_dpe_id', 'valeur_renseignee']], how='ledesc')

    return type_vitrage_desc, type_remplissage_desc, materiau_baie_desc, orientation_baie_desc
