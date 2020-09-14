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

    table = td008.copy()

    # orientation processing avec tv020 et reference.
    table['orientation_infer'] = table['tv020_orientation_paroi'].astype('string').fillna('NONDEF')
    nondef = table.orientation_infer == 'NONDEF'
    horiz = table['tv020_coefficient_orientation_id'] == "13"
    table.loc[horiz, 'orientation_infer'] = 'Horizontale'
    ouest = table.reference.str.lower().str.contains('ouest')
    nord = table.reference.str.lower().str.contains('nord')
    sud = table.reference.str.lower().str.contains('sud')
    est = table.reference.str.lower().str.contains('est')
    table.loc[nord & nondef, 'orientation_infer'] = "Nord"
    table.loc[sud & nondef, 'orientation_infer'] = "Sud"
    table.loc[ouest & nondef, 'orientation_infer'] = "Ouest"
    table.loc[est & nondef, 'orientation_infer'] = "Est"
    table.loc[(ouest & est & nondef), 'orientation_infer'] = "Est ou Ouest"
    table.orientation_infer = table.orientation_infer.astype('category')

    # type vitrage processing avec tv009, tv010, tv021 et reference
    table['fen_lib_from_tv009'] = table['tv009_type_vitrage'].astype('string') + ' ' + table[
        'tv009_remplissage'].astype('string').fillna('') + ' '
    table['fen_lib_from_tv009'] += table['tv009_epaisseur_lame'].fillna('0').astype(int).astype(str).replace('0', '').apply(
        lambda x: x + ' mm ' if x != '' else x) + table['tv010_type_materiaux'].astype('string').fillna('') + ' ' + \
                                   table[
                                       'tv009_traitement_vitrage'].astype('string').fillna('')
    table['fen_lib_from_tv009'] = table['fen_lib_from_tv009'].fillna('NONDEF')

    table['fen_lib_from_tv021'] = table['tv021_type_baie'].astype('string') + ' ' + table[
        'tv021_type_vitrage'].astype('string').fillna('') + ' '
    table['fen_lib_from_tv021'] += table['tv021_materiaux'].astype('string').fillna('')
    table['fen_lib_from_tv021'] = table['fen_lib_from_tv021'].fillna('NONDEF')

    double_vitrage = table.fen_lib_from_tv009.str.lower().str.contains(
        'double') | table.fen_lib_from_tv021.str.lower().str.contains('double')

    triple_vitrage = table.fen_lib_from_tv009.str.lower().str.contains(
        'triple') | table.fen_lib_from_tv021.str.lower().str.contains('triple')

    simple_vitrage = table.fen_lib_from_tv009.str.lower().str.contains(
        'simple') | table.fen_lib_from_tv021.str.lower().str.contains('simple')

    porte = table['tv010_type_materiaux'].astype(str).fillna('').str.lower().str.contains(
        'portes ')  # l'espace à la fin est important sinon confusion portes-fenetres
    porte = porte | table['tv010_type_baie'].astype(str).fillna('').str.lower().str.contains('porte ')
    porte = porte | table['reference'].fillna('').str.lower().str.contains('porte ')
    porte = porte | table['reference'].fillna('').str.lower().str.contains('portes ')
    porte = porte & (~table['reference'].fillna('').str.lower().str.contains('fen'))


    table['type_vitrage_simple_infer'] = 'NONDEF'

    table.loc[double_vitrage, 'type_vitrage_simple_infer'] = 'double vitrage'
    table.loc[triple_vitrage, 'type_vitrage_simple_infer'] = 'triple vitrage'
    table.loc[simple_vitrage, 'type_vitrage_simple_infer'] = 'simple vitrage'

    table.loc[simple_vitrage & double_vitrage, 'type_vitrage_simple_infer'] = "INCOHERENT"
    table.loc[simple_vitrage & triple_vitrage, 'type_vitrage_simple_infer'] = "INCOHERENT"
    table.loc[triple_vitrage & double_vitrage, 'type_vitrage_simple_infer'] = "INCOHERENT"
    table.loc[porte, 'type_vitrage_simple_infer'] = "porte"

    # distinction brique de verre

    brique = table['tv010_type_materiaux'].astype(str).fillna('').str.lower().str.contains('brique')

    brique = brique | table['tv010_type_materiaux'].astype(str).fillna('').str.lower().str.contains('polycarb')

    brique = brique | table.reference.str.lower().str.contains('brique')

    brique = brique | table.reference.str.lower().str.contains('polycarb')

    table.loc[brique, 'type_vitrage_simple_infer'] = "brique de verre ou polycarbonate"

    table.type_vitrage_simple_infer = table.type_vitrage_simple_infer.astype('category')

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
    table['nb_baie_calc'] = (
            table.deperdition / (table.surface * table.coefficient_transmission_thermique_baie)).round(0)
    null = (table.surface == 0) | (table.coefficient_transmission_thermique_baie == 0) | (table.deperdition == 0)
    table.loc[null, 'nb_baie_calc'] = np.nan
    zeros = table.nb_baie_calc == 0
    table.loc[zeros, 'nb_baie_calc'] = np.nan

    table['surfacexnb_baie_calc'] = table.surface * table.nb_baie_calc

    # TYPE MENUISERIE
    ## type menuiserie en fonction des caractéristiques déjà inférée
    baie = table.type_vitrage_simple_infer.str.contains('vitrage')
    porte = table.type_vitrage_simple_infer.str.contains('porte')
    brique = table.type_vitrage_simple_infer.str.contains('brique')

    table['cat_baie_simple_infer'] = 'NONDEF'
    table.loc[baie, 'cat_baie_simple_infer'] = 'baie vitrée'
    table.loc[porte, 'cat_baie_simple_infer'] = 'porte'
    table.loc[brique, 'cat_baie_simple_infer'] = 'paroi en brique de verre ou polycarbonate'

    nondef = table.cat_baie_simple_infer == "NONDEF"
    ## pour les non def on va chercher dans le string de description
    # type menuiserie en fonction des caractéristiques déjà inférée
    baie = table.type_vitrage_simple_infer.str.contains('vitrage')
    porte = table.type_vitrage_simple_infer.str.contains('porte')
    brique = table.type_vitrage_simple_infer.str.contains('brique')

    table['cat_baie_simple_infer'] = 'NONDEF'
    table.loc[baie, 'cat_baie_simple_infer'] = 'baie_vitree'
    table.loc[porte, 'cat_baie_simple_infer'] = 'porte'

    nondef = table.cat_baie_simple_infer == "NONDEF"
    # pour les non def on va chercher dans le string de description
    baie = table.reference.str.lower().str.contains('fen')
    ref = table.reference.str.lower()
    baie = baie | ref.str.contains('baie')
    baie = baie | ref.str.startswith('f')
    baie = baie | ref.str.startswith('pf')
    baie = baie | ref.str.startswith('sv')
    baie = baie | ref.str.contains('velux')
    baie = baie | (~table.tv009_coefficient_transmission_thermique_vitrage_id.isnull())
    baie = baie | ref.str.contains('velux')
    baie = baie | (table.tv009_coefficient_transmission_thermique_vitrage_id.isnull())
    baie = baie | table['tv010_type_baie'].str.lower().str.contains('fen')
    baie = baie | table.reference.str.lower().str.contains('vitr')
    porte = table.reference.str.lower().str.contains('porte') & (~baie)
    table.loc[nondef & baie, 'cat_baie_simple_infer'] = 'baie_vitree'
    table.loc[nondef & porte, 'cat_baie_simple_infer'] = 'porte'
    table.loc[brique, 'cat_baie_simple_infer'] = 'brique_ou_poly'
    table.cat_baie_simple_infer = table.cat_baie_simple_infer.astype('category')

    return table


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
                                                                                                      '').str.replace('\+ Est ou Ouest ', '')

    # AGG top freq type vitrage

    td008_vitree['max_surface'] = td008_vitree[['surfacexnb_baie_calc', 'surface']].max(axis=1)
    type_vitrage_agg = agg_pond_top_freq(td008_vitree, 'type_vitrage_simple_infer', 'max_surface', 'td001_dpe_id')

    # AGG Ujn avg

    td008_sel = td008_vitree.loc[td008_vitree.coefficient_transmission_thermique_baie > 0]
    Ubaie_avg = agg_pond_avg(td008_sel, 'coefficient_transmission_thermique_baie', 'max_surface',
                             'td001_dpe_id').to_frame('Ubaie_avg')

    agg = pd.concat([Ubaie_avg, type_vitrage_agg, orientation_agg], axis=1)
    agg.columns = ['Ubaie_avg', 'type_vitrage_simple_top', 'orientation_concat']

    return agg
