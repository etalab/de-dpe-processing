import pandas as pd
import numpy as np
from utils import agg_pond_top_freq, agg_pond_avg

td008_types = {'id': 'str',
               'td007_paroi_opaque_id': 'str',
               'reference': 'str',
               'td008_baie_id': 'str',
               'deperdition': 'float',
               'tv009_coefficient_transmission_thermique_vitrage_id': 'category',
               'presence_survitrage': 'str',
               'coefficient_transmission_thermique_baie': 'float',
               'tv010_coefficient_transmission_thermique_baie_id': 'category',
               'tv011_resistance_additionnelle_id': 'category',
               'tv012_coef_transmission_thermique_baie_protection_solaire_id': 'category',
               'surface': 'float',
               'perimetre': 'float',
               'tv013_valeur_pont_thermique_id': 'category',
               'facteur_solaire': 'float',
               'tv021_facteur_solaire_id': 'category',
               'tv022_coefficient_masques_proches_id': 'category',
               'coefficient_masques_lointains_non_homogenes': 'category',
               'tv023_coefficient_masques_lointains_homogenes_id': 'category',
               'tv020_coefficient_orientation_id': 'category'}


def merge_td008_tr_tv(td008):
    from trtvtables import DPETrTvTables
    meta = DPETrTvTables()
    table = td008.copy()
    table = table.rename(columns={
        'tv012_coef_transmission_thermique_baie_protection_solaire_id': 'tv012_coefficient_transmission_thermique_baie_protection_solaire_id'})
    table = meta.merge_all_tr_tables(table)

    table = meta.merge_all_tv_tables(table)

    table = table.astype({k: v for k, v in td008_types.items() if k in table})
    table = table.loc[:, ~table.columns.duplicated()]

    return table


def postprocessing_td008(td008):
    from utils import intervals_to_category

    td008 = td008.copy()

    # orientation processing avec tv020 et reference.
    td008['orientation_infer'] = td008['tv020_orientation_paroi'].astype('string').fillna('NONDEF')
    nondef = td008.orientation_infer == 'NONDEF'
    horiz = td008['tv020_coefficient_orientation_id'] == "13"
    td008.loc[horiz, 'orientation_infer'] = 'Horizontale'
    ouest = td008.reference.str.lower().str.contains('ouest')
    nord = td008.reference.str.lower().str.contains('nord')
    sud = td008.reference.str.lower().str.contains('sud')
    est = td008.reference.str.lower().str.contains('est')
    td008.loc[nord & nondef, 'orientation_infer'] = "Nord"
    td008.loc[sud & nondef, 'orientation_infer'] = "Sud"
    td008.loc[ouest & nondef, 'orientation_infer'] = "Ouest"
    td008.loc[est & nondef, 'orientation_infer'] = "Est"
    td008.loc[(ouest & est & nondef), 'orientation_infer'] = "Est ou Ouest"
    td008.orientation_infer = td008.orientation_infer.astype('category')

    # type vitrage processing avec tv009, tv010, tv021 et reference
    td008['fen_lib_from_tv009'] = td008['tv009_type_vitrage'].astype('string') + ' ' + td008[
        'tv009_remplissage'].astype('string').fillna('') + ' '
    td008['fen_lib_from_tv009'] += td008['tv009_epaisseur_lame'].fillna('0').astype(int).astype(str).replace('0',
                                                                                                             '').apply(
        lambda x: x + ' mm ' if x != '' else x) + td008['tv010_type_materiaux'].astype('string').fillna('') + ' ' + \
                                   td008[
                                       'tv009_traitement_vitrage'].astype('string').fillna('')
    td008['fen_lib_from_tv009'] = td008['fen_lib_from_tv009'].fillna('NONDEF')

    td008['fen_lib_from_tv021'] = td008['tv021_type_baie'].astype('string') + ' ' + td008[
        'tv021_type_vitrage'].astype('string').fillna('') + ' '
    td008['fen_lib_from_tv021'] += td008['tv021_materiaux'].astype('string').fillna('')
    td008['fen_lib_from_tv021'] = td008['fen_lib_from_tv021'].fillna('NONDEF')

    double_vitrage = td008.fen_lib_from_tv009.str.lower().str.contains(
        'double') | td008.fen_lib_from_tv021.str.lower().str.contains('double')

    triple_vitrage = td008.fen_lib_from_tv009.str.lower().str.contains(
        'triple') | td008.fen_lib_from_tv021.str.lower().str.contains('triple')

    simple_vitrage = td008.fen_lib_from_tv009.str.lower().str.contains(
        'simple') | td008.fen_lib_from_tv021.str.lower().str.contains('simple')

    porte = td008['tv010_type_materiaux'].astype(str).fillna('').str.lower().str.contains(
        'portes ')  # l'espace à la fin est important sinon confusion portes-fenetres
    porte = porte | td008['tv010_type_baie'].astype(str).fillna('').str.lower().str.contains('porte ')
    porte = porte | td008['reference'].fillna('').str.lower().str.contains('porte ')
    porte = porte | td008['reference'].fillna('').str.lower().str.contains('portes ')
    porte = porte & (~td008['reference'].fillna('').str.lower().str.contains('fen'))

    td008['type_vitrage_simple_infer'] = 'NONDEF'

    td008.loc[double_vitrage, 'type_vitrage_simple_infer'] = 'double vitrage'
    td008.loc[triple_vitrage, 'type_vitrage_simple_infer'] = 'triple vitrage'
    td008.loc[simple_vitrage, 'type_vitrage_simple_infer'] = 'simple vitrage'

    td008.loc[simple_vitrage & double_vitrage, 'type_vitrage_simple_infer'] = "INCOHERENT"
    td008.loc[simple_vitrage & triple_vitrage, 'type_vitrage_simple_infer'] = "INCOHERENT"
    td008.loc[triple_vitrage & double_vitrage, 'type_vitrage_simple_infer'] = "INCOHERENT"
    td008.loc[porte, 'type_vitrage_simple_infer'] = "porte"

    # distinction brique de verre

    brique = td008['tv010_type_materiaux'].astype(str).fillna('').str.lower().str.contains('brique')

    brique = brique | td008['tv010_type_materiaux'].astype(str).fillna('').str.lower().str.contains('polycarb')

    brique = brique | td008.reference.str.lower().str.contains('brique')

    brique = brique | td008.reference.str.lower().str.contains('polycarb')

    td008.loc[brique, 'type_vitrage_simple_infer'] = "brique de verre ou polycarbonate"

    td008.type_vitrage_simple_infer = td008.type_vitrage_simple_infer.astype('category')

    # traitement avancé en utilisant les valeurs.
    # s_type_from_value = intervals_to_category(table.coefficient_transmission_thermique_baie,infer_type_by_value)

    # infer_type_by_value = {'simple vitrage':[3.7,7],
    #                       'double vitrage':[2,3.69],
    #                       'triple vitrage':[1,2],
    #                       'INCOHERENT':[0,0.99]}

    # inc=table.type_vitrage_simple_infer=='INCOHERENT'
    # nondef=table.type_vitrage_simple_infer=='NONDEF'
    # inc_or_nondef=inc|nondef

    # table.loc[inc_or_nondef,'type_vitrage_simple_infer'] = s_type_from_value[inc_or_nondef]

    # quantitatifs (EXPERIMENTAL)
    td008['nb_baie_calc'] = (
            td008.deperdition / (td008.surface * td008.coefficient_transmission_thermique_baie)).round(0)
    null = (td008.surface == 0) | (td008.coefficient_transmission_thermique_baie == 0) | (td008.deperdition == 0)
    td008.loc[null, 'nb_baie_calc'] = np.nan
    zeros = td008.nb_baie_calc == 0
    td008.loc[zeros, 'nb_baie_calc'] = np.nan

    td008['surfacexnb_baie_calc'] = td008.surface * td008.nb_baie_calc
    # MATERIAU
    td008['materiaux'] = td008.tv021_materiaux.astype('string').fillna('NONDEF')

    mat_tv010 = td008.tv010_type_materiaux

    td008['baie_mat_tv010'] = mat_tv010
    baie_mat_tv010 = td008.baie_mat_tv010.astype('string')

    bois_ou_PVC = mat_tv010.str.contains('bois ou PVC').fillna(False)
    bois = mat_tv010.str.contains('bois').fillna(False)

    metal = mat_tv010.str.contains('métal').fillna(False)
    polycarb = mat_tv010.str.contains('Polycarbonate').fillna(False)
    autres = mat_tv010.str.contains('Autres').fillna(False)

    baie_mat_tv010.loc[bois] = 'Bois'
    baie_mat_tv010.loc[bois_ou_PVC] = 'Bois ou PVC'
    baie_mat_tv010.loc[metal] = 'Métal'
    baie_mat_tv010.loc[polycarb] = 'Polycarbonate'
    baie_mat_tv010.loc[autres] = 'Autres'

    baie_mat_tv010 = baie_mat_tv010.fillna('NONDEF')

    mat = td008.materiaux
    nondef = mat == 'NONDEF'

    td008.loc[nondef, 'materiaux'] = baie_mat_tv010.loc[nondef]

    # TYPE MENUISERIE
    ## type menuiserie en fonction des caractéristiques déjà inférée
    baie = td008.type_vitrage_simple_infer.str.contains('vitrage')
    porte = td008.type_vitrage_simple_infer.str.contains('porte')
    brique = td008.type_vitrage_simple_infer.str.contains('brique')

    td008['cat_baie_simple_infer'] = 'NONDEF'
    td008.loc[baie, 'cat_baie_simple_infer'] = 'baie vitrée'
    td008.loc[porte, 'cat_baie_simple_infer'] = 'porte'
    td008.loc[brique, 'cat_baie_simple_infer'] = 'paroi en brique de verre ou polycarbonate'

    nondef = td008.cat_baie_simple_infer == "NONDEF"
    ## pour les non def on va chercher dans le string de description
    # type menuiserie en fonction des caractéristiques déjà inférée
    baie = td008.type_vitrage_simple_infer.str.contains('vitrage')
    porte = td008.type_vitrage_simple_infer.str.contains('porte')
    brique = td008.type_vitrage_simple_infer.str.contains('brique')

    td008['cat_baie_simple_infer'] = 'NONDEF'
    td008.loc[baie, 'cat_baie_simple_infer'] = 'baie_vitree'
    td008.loc[porte, 'cat_baie_simple_infer'] = 'porte'

    nondef = td008.cat_baie_simple_infer == "NONDEF"
    # pour les non def on va chercher dans le string de description
    baie = td008.reference.str.lower().str.contains('fen')
    ref = td008.reference.str.lower()
    baie = baie | ref.str.contains('baie')
    baie = baie | ref.str.startswith('f')
    baie = baie | ref.str.startswith('pf')
    baie = baie | ref.str.startswith('sv')
    baie = baie | ref.str.contains('velux')
    baie = baie | (~td008.tv009_coefficient_transmission_thermique_vitrage_id.isnull())
    baie = baie | ref.str.contains('velux')
    baie = baie | (td008.tv009_coefficient_transmission_thermique_vitrage_id.isnull())
    baie = baie | td008['tv010_type_baie'].str.lower().str.contains('fen')
    baie = baie | td008.reference.str.lower().str.contains('vitr')
    porte = td008.reference.str.lower().str.contains('porte') & (~baie)
    td008.loc[nondef & baie, 'cat_baie_simple_infer'] = 'baie_vitree'
    td008.loc[nondef & porte, 'cat_baie_simple_infer'] = 'porte'
    td008.loc[brique, 'cat_baie_simple_infer'] = 'brique_ou_poly'
    td008.cat_baie_simple_infer = td008.cat_baie_simple_infer.astype('category')

    # type baie distinguée fenetre/porte fenetre

    pf = td008.tv021_type_baie.str.contains('ortes-fenêtres').fillna(False)
    pf = pf | td008.tv010_type_menuiserie.str.contains('ortes-fenêtres').fillna(False)
    pf = pf | td008.reference.str.startswith('pf')

    f = td008.tv021_type_baie.str.contains('enêtre').fillna(False)
    f = f | td008.tv010_type_menuiserie.str.contains('enêtre').fillna(False)

    td008['cat_baie_infer'] = td008.cat_baie_simple_infer.astype('string')
    cat_baie = td008.cat_baie_infer

    cat_baie.loc[f] = 'fenetre'
    cat_baie.loc[pf] = 'porte_fenetre'
    is_baie = cat_baie == 'baie_vitree'
    cat_baie.loc[is_baie] = 'fenetre'
    p_simple = td008.cat_baie_simple_infer == 'porte'
    cat_baie.loc[p_simple] = 'porte'
    td008['cat_baie_infer'] = cat_baie

    # METHODE SAISIE U

    not_vitrage = td008.tv009_code.isnull()
    not_baie = td008.tv010_code.isnull()

    td008.loc[not_baie, 'meth_calc_U'] = 'Uw saisi'
    td008.loc[~not_baie, 'meth_calc_U'] = 'Uw defaut'

    not_fs = td008.tv021_code.isnull()

    td008.loc[not_fs, 'meth_calc_Fs'] = 'Fs saisi'
    td008.loc[~not_fs, 'meth_calc_Fs'] = 'Fs defaut'

    # MASQUES ET BALCONS
    td008['avancee_masque'] = td008.tv022_avance
    td008['type_occultation'] = td008.tv011_fermetures
    td008['type_masque'] = td008.tv022_type_masque
    td008['avancee_masque'] = pd.Categorical(td008['avancee_masque'],
                                             categories=['< 1 m', '1 <= … < 2', '2 <= … < 3', '3 <='], ordered=True)
    td008['presence_balcon'] = td008.tv022_type_masque.str.contains('balcon').replace(False, np.nan)

    # MAX SURFACE
    td008['max_surface'] = td008[['surfacexnb_baie_calc', 'surface']].max(axis=1)

    return td008


def agg_td008_to_td001_essential(td008):
    # AGG orientation

    td008_vitree = td008.loc[td008.cat_baie_simple_infer.isin(['baie_vitree',
                                                               'brique_ou_poly'])]
    orientation_agg = td008_vitree.groupby('td001_dpe_id')['orientation_infer'].apply(
        lambda x: ' + '.join(list(set(x.tolist()))))

    est_double = orientation_agg.str.count('Est') > 1
    ouest_double = orientation_agg.str.count('Ouest') > 1
    est_and_ouest_double = (est_double & ouest_double)

    orientation_agg.loc[est_and_ouest_double] = orientation_agg.loc[est_and_ouest_double].str.replace('Est ou Ouest + ',
                                                                                                      '').str.replace(
        '\+ Est ou Ouest ', '')

    # AGG top freq type vitrage

    type_vitrage_agg = agg_pond_top_freq(td008_vitree, 'type_vitrage_simple_infer', 'max_surface', 'td001_dpe_id')

    # AGG Ujn avg

    td008_sel = td008_vitree.loc[td008_vitree.coefficient_transmission_thermique_baie > 0]
    Ubaie_avg = agg_pond_avg(td008_sel, 'coefficient_transmission_thermique_baie', 'max_surface',
                             'td001_dpe_id').to_frame('Ubaie_avg')

    agg = pd.concat([Ubaie_avg, type_vitrage_agg, orientation_agg], axis=1)
    agg.columns = ['Ubaie_avg', 'type_vitrage_simple_top', 'orientation_concat']

    return agg


def agg_td008_to_td001(td008):
    # AGG orientation
    concat = list()

    td008_vitree = td008.loc[td008.cat_baie_simple_infer.isin(['baie_vitree',
                                                               'brique_ou_poly'])]
    orientation_agg = td008_vitree.groupby('td001_dpe_id')['orientation_infer'].apply(
        lambda x: ' + '.join(list(set(x.tolist()))))

    est_double = orientation_agg.str.count('Est') > 1
    ouest_double = orientation_agg.str.count('Ouest') > 1
    est_and_ouest_double = (est_double & ouest_double)

    orientation_agg.loc[est_and_ouest_double] = orientation_agg.loc[est_and_ouest_double].str.replace('Est ou Ouest + ',
                                                                                                      '').str.replace(
        '\+ Est ou Ouest ', '')
    concat.append(orientation_agg)

    # AGG SURFS
    surfs = td008.pivot_table(index='td001_dpe_id', columns='cat_baie_infer', values='surfacexnb_baie_calc',
                              aggfunc='sum')
    surfs.columns = [f'surface_{col}' for col in surfs]
    concat.append(surfs)

    td008 = td008.rename(columns={'coefficient_transmission_thermique_baie': 'Ubaie',
                                  'type_vitrage_simple_infer': 'type_vitrage',
                                  'tv010_uw': 'Uw',
                                  'tv010_ug': 'Ug'})

    td008.Uw = td008.Uw.astype('string').fillna('NONDEF')
    td008.Ug = td008.Ug.astype('string').fillna('NONDEF')

    #

    avancee_masque_max = td008.groupby('td001_dpe_id').avancee_masque.apply(
        lambda x: x.sort_values(ascending=False).iloc[0] if x.isnull().sum() > 0 else np.nan)
    concat.append(avancee_masque_max.to_frame('avancee_masque_max'))

    concat.append((td008.groupby('td001_dpe_id').presence_balcon.sum() > 0).to_frame('presence_balcon'))

    # AGG parois vitrees
    td008_vit = td008.loc[td008.cat_baie_simple_infer != 'porte']

    for col in ['Ubaie', 'Uw', 'Ug', 'type_occultation', 'materiaux', 'type_vitrage', 'meth_calc_U', 'meth_calc_Fs']:
        var_agg = agg_pond_top_freq(td008_vit, col, 'surfacexnb_baie_calc',
                                    'td001_dpe_id').to_frame(col + '_baie_vitree_top')
        concat.append(var_agg)

    # AGG parois opaques

    td008_opaque = td008.loc[td008.cat_baie_simple_infer == 'porte']

    for col in ['Ubaie', 'materiaux', 'meth_calc_U', 'meth_calc_Fs']:
        var_agg = agg_pond_top_freq(td008_opaque, col, 'surfacexnb_baie_calc',
                                    'td001_dpe_id').to_frame(col + '_porte_top')
        concat.append(var_agg)
    # AGG fenetre/porte fenetre distinct

    for type_baie in ['fenetre', 'porte_fenetre']:

        sel = td008_vit.loc[td008_vit.cat_baie_infer == type_baie]
        for col in ['Ubaie', 'Uw', 'Ug', 'type_occultation', 'materiaux', 'type_vitrage', 'meth_calc_U',
                    'meth_calc_Fs']:
            var_agg = agg_pond_top_freq(sel, col, 'surfacexnb_baie_calc',
                                        'td001_dpe_id').to_frame(col + f'_{type_baie}_top')
            concat.append(var_agg)

    td008_baie_agg = pd.concat(concat, axis=1)
    td008_baie_agg.index.name = 'td001_dpe_id'

    return td008_baie_agg
