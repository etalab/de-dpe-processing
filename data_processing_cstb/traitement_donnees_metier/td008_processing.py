td008_types = {'id': 'object',
               'td007_paroi_opaque_id': 'object',
               'reference': 'object',
               'td008_baie_id': 'object',
               'deperdition': 'float',
               'tv009_coefficient_transmission_thermique_vitrage_id': 'category',
               'presence_survitrage': 'bool',
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
               'coefficient_masques_lointains_non_homogenes': 'float',
               'tv023_coefficient_masques_lointains_homogenes_id': 'category',
               'tv020_coefficient_orientation_id': 'category',
               'tv009_Type de vitrage': 'category',
               'tv009_Orientation': 'category',
               'tv009_Remplissage': 'category',
               'tv009_Epaisseur Lame': 'category',
               'tv009_Traitement du vitrage': 'category',
               'tv009_Ug': 'category',
               'tv010_Type de matériaux': 'category',
               'tv010_Type de Baie': 'category',
               'tv010_Ug': 'category',
               'tv010_Uw': 'category',
               'tv011_Fermetures': 'category',
               'tv011_∆R': 'category',
               'tv012_Uw': 'category',
               'tv012_∆R': 'category',
               'tv012_Ujn': 'category',
               'tv013_Type de liaison': 'category',
               'tv013_Isolation Mur': 'category',
               'tv013_Isolation Plancher bas': 'category',
               'tv013_Largeur du dormant': 'category',
               'tv013_Type de pose': 'category',
               "tv013_Retour d'isolation": 'category',
               'tv013_K': 'category',
               'tv021_Type de Pose': 'category',
               'tv021_Matériaux': 'category',
               'tv021_Type de Baie': 'category',
               'tv021_Type de Vitrage': 'category',
               'tv021_Fts': 'category',
               'tv022_Type de maque': 'category',
               'tv022_Avancé L': 'category',
               'tv022_Orientation': 'category',
               'tv022_Rapport L1/L2': 'category',
               'tv022_β & γ': 'category',
               'tv022_Fe1': 'category',
               'tv023_Hauteur α (°)': 'category',
               'tv023_Orientation': 'category',
               'tv023_Fe2': 'category',
               'tv020_Inclinaison de la paroi': 'category',
               'tv020_Orientation de la paroi': 'category',
               'tv020_Type de baie': 'category',
               'tv020_C1': 'category'}


def post_processing_td008(td008):
    from assets_orm import DPEMetaData
    meta = DPEMetaData()
    table = td008
    table = meta.merge_all_tr_table(table)

    table = meta.merge_all_tv_table(table)
    table = table.astype(td008_types)

    # orientation processing from tv020 and name.
    table['orientation_infer'] = table['tv020_Orientation de la paroi'].astype('string').fillna('NONDEF')
    nondef = table.orientation_infer == 'NONDEF'
    horiz = table['tv020_coefficient_orientation_id'] == "TV020_013"
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


    # fen lib

    table['fen_lib_from_tv009'] = table['tv009_Type de vitrage'].astype('string') + ' ' + table[
        'tv009_Remplissage'].astype('string').fillna('') + ' '
    table['fen_lib_from_tv009'] += table[
                                       'tv009_Epaisseur Lame'].astype('Int32').fillna(0).astype(int).astype(
        str).replace('0', '').apply(
        lambda x: x + ' mm ' if x != '' else x) + table['tv010_Type de matériaux'].astype('string').fillna('') + ' ' + \
                                   table[
                                       'tv009_Traitement du vitrage'].astype('string').fillna('')
    table['fen_lib_from_tv009'] = table['fen_lib_from_tv009'].fillna('NONDEF')

    table['fen_lib_from_tv021'] = table['tv021_Type de Baie'].astype('string') + ' ' + table[
        'tv021_Type de Vitrage'].astype('string').fillna('') + ' '
    table['fen_lib_from_tv021'] += table['tv021_Matériaux'].astype('string').fillna('')
    table['fen_lib_from_tv021'] = table['fen_lib_from_tv021'].fillna('NONDEF')

    return table