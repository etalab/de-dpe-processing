import pandas as pd
import numpy as np
from .utils import concat_string_cols, strip_accents, affect_lib_by_matching_score, clean_str
from .trtvtables import DPETrTvTables
from .text_matching_dict import td014_gen_ecs_search_dict
from .conversion_normalisation import ener_conv_dict

td013_types = {
    'td006_batiment_id': 'str',
    'tr005_type_installation_ecs_id': 'category',
    'nombre_appartements_echantillon': 'float',
    'surface_habitable_echantillon': 'float',
    'becs': 'float',
    'tv039_formule_becs_id': 'category',
    'surface_alimentee': 'float'}

td014_types = {'td014_generateur_ecs_id': 'str',
               'td013_installation_ecs_id': 'str',
               'tr004_type_energie_id': 'str',
               'tv045_conversion_kwh_co2_id': 'category',
               'tv046_evaluation_contenu_co2_reseaux_id': 'category',
               'coefficient_performance': 'float',
               'tv032_coefficient_performance_id': 'category',
               'rpn': 'float',
               'qp0': 'float',
               'puissance_veilleuse': 'float',
               'tv036_puissance_veilleuse_id': 'category',
               'tv037_puissance_necessaire_production_ecs_id': 'category',
               'puissance_nominale': 'float',
               'tv038_puissance_nominale_id': 'category',
               'tv040_rendement_distribution_ecs_id': 'category',
               'tv047_rendement_generation_ecs_id': 'category',
               'volume_stockage': 'float',
               'tv048_rendement_stockage_ecs_id': 'category',
               'tv049_perte_stockage_ecs_id': 'category',
               'tv041_coefficient_emplacement_fonctionnement_id': 'category',
               'tv043_pertes_stockage_id': 'category',
               'tv019_fecs_id': 'category',
               'tv027_pertes_recuperees_ecs_id': 'category',
               'td001_dpe_id': 'str'}

replace_elec_tv045_ener = {"Electricité (hors électricité d'origine renouvelab": 'Electricité non renouvelable',
                           "Electricité d'origine renouvelable utilisée dans l": "Electricité d'origine renouvelable", }

gen_ecs_lib_simp_dict = {'ecs electrique indetermine': 'ecs a effet joule electrique',
                         'ballon a accumulation electrique': 'ecs a effet joule electrique',
                         'ecs instantanee electrique': 'ecs a effet joule electrique',
                         }
# solaire_dict = dict()
# for k, v in gen_ecs_lib_simp_dict.items():
#     k_solaire = 'ecs solaire thermique + ' + k
#     v_solaire = 'ecs solaire thermique + ' + v
#     solaire_dict[k_solaire] = v_solaire
#gen_ecs_lib_simp_dict.update(solaire_dict)

gen_to_installation_infer_dict = {"ecs a effet joule electrique": "Individuelle",
                                  "ECS thermodynamique electrique(PAC ou ballon)": "indetermine",
                                  "indetermine": "indetermine",
                                  "chaudiere bois": "Individuelle",
                                  "reseau de chaleur": "Collective",
                                  "chauffe-eau gaz independant": "Individuelle",
                                  'chauffe-eau gpl independant': "Individuelle",
                                  'chauffe-eau fioul independant': "Individuelle",
                                  }
for type_chaudiere in ['standard', 'basse temperature', 'condensation', 'indetermine']:
    gen_to_installation_infer_dict[f'chaudiere fioul {type_chaudiere}'] = 'Individuelle'
    gen_to_installation_infer_dict[f'chaudiere gaz {type_chaudiere}'] = 'indetermine'
    gen_to_installation_infer_dict[f'chaudiere autre(gpl/butane/propane) {type_chaudiere}'] = 'Individuelle'

# solaire_dict = dict()
# for k, v in gen_to_installation_infer_dict.items():
#     k_solaire = 'ecs thermique solaire + ' + k
#     v_solaire = v
#     solaire_dict[k_solaire] = v_solaire
# gen_ecs_lib_simp_dict.update(solaire_dict)

sys_princ_scores = {'thermodynamique': 5,
                    'solaire': 4,
                    'chaudiere': 3,
                    'ballon a accumulation': 2,
                    'electrique indetermine': 1,
                    'indépendant': 0, }

sys_princ_score_lib = dict()
for k in list(td014_gen_ecs_search_dict.keys()):
    sys_princ_score_lib[k] = 0
    for term, score in sys_princ_scores.items():
        if term in k:
            sys_princ_score_lib[k] += score
sys_princ_score_lib['indetermine'] = -1


def merge_td013_tr_tv(td013):
    meta = DPETrTvTables()
    table = td013.copy()
    table = meta.merge_all_tr_tables(table)
    table = meta.merge_all_tv_tables(table)
    table = table.astype(td013_types)
    table = table.rename(columns={'id': 'td013_installation_ecs_id'})

    return table


def merge_td014_tr_tv(td014):
    meta = DPETrTvTables()
    table = td014.copy()
    table = meta.merge_all_tr_tables(table)
    table = meta.merge_all_tv_tables(table)
    table = table.astype(td014_types)
    table = table.rename(columns={'id': 'td013_installation_ecs_id'})

    return table


def postprocessing_td014(td013, td014, td001, td001_sys_ch_agg):
    table = td014.copy()
    table['type_energie'] = table["tv045_energie"].astype('string').fillna('indetermine').replace(
        ener_conv_dict['tv045_energie'])
    table['tr004_description'] = table["tr004_description"].astype('string').fillna('indetermine').replace(
        ener_conv_dict['tr004_description'])

    table = table.merge(
        td013[['tr005_code', 'tr005_description', 'td013_installation_ecs_id', 'surface_habitable_echantillon']],
        on='td013_installation_ecs_id')

    is_chaudiere = table.rpn > 0

    gen_ecs_concat_txt_desc = table["tv027_type_installation"].astype('string').replace(np.nan, '') + ' '
    gen_ecs_concat_txt_desc.loc[is_chaudiere] += 'chaudiere '

    gen_ecs_concat_txt_desc += table['tv027_type_systeme'].astype('string').replace(np.nan, '') + ' '
    gen_ecs_concat_txt_desc += table['tv027_type_installation'].astype('string').replace(np.nan, '') + ' '
    gen_ecs_concat_txt_desc += table["tv032_type_generateur"].astype('string').replace(np.nan, '') + ' '
    gen_ecs_concat_txt_desc += table['tv036_type_generation'].astype('string').replace(np.nan, '') + ' '
    gen_ecs_concat_txt_desc += table['tv037_type_production'].astype('string').replace(np.nan, '') + ' '
    gen_ecs_concat_txt_desc += table['tv040_type_generateur'].astype('string').replace(np.nan, '') + ' '
    gen_ecs_concat_txt_desc += table["tv040_type_installation"].astype('string').replace(np.nan, '') + ' '
    gen_ecs_concat_txt_desc += table["tr004_description"].astype('string').replace(np.nan, '') + ' '
    gen_ecs_concat_txt_desc += table["type_energie"].astype('string').replace(np.nan, '') + ' '
    gen_ecs_concat_txt_desc += table['tv047_type_generateur'].astype('string').replace(np.nan, '') + ' '
    gen_ecs_concat_txt_desc += table['tr005_description'].astype('string').replace(np.nan, '') + ' '

    gen_ecs_concat_txt_desc = gen_ecs_concat_txt_desc.str.lower().apply(lambda x: strip_accents(x))

    table['gen_ecs_concat_txt_desc'] = gen_ecs_concat_txt_desc

    table['gen_ecs_concat_txt_desc'] = table['gen_ecs_concat_txt_desc'].apply(lambda x: clean_str(x))

    # calcul gen_ecs_lib_infer par matching score text.
    unique_gen_ecs = table.gen_ecs_concat_txt_desc.unique()
    gen_ecs_lib_infer_dict = {k: affect_lib_by_matching_score(k, td014_gen_ecs_search_dict) for k in
                              unique_gen_ecs}
    table['gen_ecs_lib_infer'] = table.gen_ecs_concat_txt_desc.replace(gen_ecs_lib_infer_dict)
    is_pac = table.coefficient_performance > 2
    table.loc[is_pac, 'gen_ecs_lib_infer'] = "ecs thermodynamique electrique(pompe a chaleur ou ballon)"
    ecs_ind = table.gen_ecs_lib_infer == "ecs electrique indetermine"
    stockage = table.volume_stockage > 20
    table.loc[ecs_ind & stockage, 'gen_ecs_lib_infer'] = 'ballon a accumulation electrique'
    table.loc[ecs_ind & (~stockage), 'gen_ecs_lib_infer'] = 'ballon a accumulation electrique'
    table['gen_ecs_lib_infer_simp'] = table.gen_ecs_lib_infer.replace(gen_ecs_lib_simp_dict)

    # recupération fioul
    non_aff = table['gen_ecs_lib_infer'] == 'indetermine'
    fioul = table['tv045_energie'] == 'Fioul domestique'
    table.loc[fioul & non_aff, 'gen_ecs_lib_infer'] = 'chaudiere fioul standard'

    table['type_energie_ecs'] = table['tv045_energie'].replace(replace_elec_tv045_ener)

    table['score_gen_ecs_lib_infer'] = table['gen_ecs_lib_infer'].replace(sys_princ_score_lib).astype(float)

    # Type installation ECS (collective ou individuelle)
    table['type_installation_ecs'] = table.tv027_type_installation.astype('string')
    null = table.type_installation_ecs.isnull()
    table.loc[null, 'type_installation_ecs'] = table.tv040_type_installation.loc[null].astype('string')
    null = table.type_installation_ecs.isnull()
    from_gen = table.gen_ecs_lib_infer_simp.replace(gen_to_installation_infer_dict)
    null = table.type_installation_ecs.isnull()

    table.loc[null, 'type_installation_ecs'] = from_gen.loc[null]

    table = table.merge(td001.rename(columns={'id': 'td001_dpe_id'})[['tr002_type_batiment_id', 'td001_dpe_id']])

    is_house = table.tr002_type_batiment_id == '1'
    table.loc[null, 'type_installation_ecs'] = is_house.loc[null].replace(
        {True: 'Individuelle', False: 'indetermine'})

    del table['tr002_type_batiment_id']

    table = table.merge(td001_sys_ch_agg[['type_installation_ch_concat', 'td001_dpe_id']], on='td001_dpe_id')

    null = table.type_installation_ecs == 'indetermine'

    ch_ind = table.type_installation_ch_concat == 'Chauffage Individuel'

    table.loc[null, 'type_installation_ecs'] = ch_ind.loc[null].replace({True: 'Individuelle', False: 'indetermine'})

    del table['type_installation_ch_concat']

    # présence d'ECS solaire
    table['is_ecs_solaire'] = table.tr005_code == 'TR005_002'

    return table


def agg_systeme_ecs_essential(td001, td013, td014):
    sys_ecs_princ_rename = {
        'td001_dpe_id': 'td001_dpe_id',
        'gen_ecs_lib_infer': 'sys_ecs_princ_gen_ecs_lib_infer',
        'gen_ecs_lib_infer_simp': 'sys_ecs_princ_gen_ecs_lib_infer_simp',
        'type_energie_ecs': 'sys_ecs_princ_type_energie_ecs',
        "type_installation_ecs": 'sys_ecs_princ_type_installation_ecs',
        'nb_generateurs': 'sys_ecs_princ_nb_generateur'
    }

    sys_ecs_sec_rename = {
        'td001_dpe_id': 'td001_dpe_id',
        'gen_ecs_lib_infer': 'sys_ecs_sec_gen_ecs_lib_infer',
        'gen_ecs_lib_infer_simp': 'sys_ecs_sec_gen_ecs_lib_infer_simp',
        'type_energie_ecs': 'sys_ecs_sec_type_energie_ecs',
        "type_installation_ecs": 'sys_ecs_sec_type_installation_ecs',
        'nb_generateurs': 'sys_ecs_sec_nb_generateur'
    }

    sys_ecs_tert_concat_rename = {
        'td001_dpe_id': 'td001_dpe_id',
        'gen_ecs_lib_infer': 'sys_ecs_tert_gen_ecs_lib_infer_concat',
        'gen_ecs_lib_infer_simp': 'sys_ecs_tert_gen_ecs_lib_infer_simp_concat',
        'type_energie_ecs': 'sys_ecs_tert_type_energie_ecs_concat',
        "type_installation_ecs": 'sys_ecs_tert_type_installation_ecs_concat',
        'nb_generateurs': 'sys_ecs_tert_nb_generateurs'
    }

    table = td014.copy()
    is_solaire = table.is_ecs_solaire
    table['nb_generateurs'] = 1
    table.loc[is_solaire, 'nb_generateurs'] = 2

    cols = ['td001_dpe_id', 'gen_ecs_lib_infer_simp', 'gen_ecs_lib_infer', 'type_energie_ecs',
            'score_gen_ecs_lib_infer']
    cols += ['type_installation_ecs', 'surface_habitable_echantillon', 'nb_generateurs', 'id_unique']

    agg_cols = ['td001_dpe_id', 'gen_ecs_lib_infer', 'type_installation_ecs']

    table['id_unique'] = table.td001_dpe_id + table.gen_ecs_lib_infer.astype(
        'string') + table.type_installation_ecs.astype(
        'string')

    is_unique = table.groupby('td001_dpe_id').td001_dpe_id.count() == 1
    ist_unique = is_unique[is_unique].index

    table_gen_unique = table.loc[table.td001_dpe_id.isin(ist_unique)][cols]

    table_gen_multiple = table.loc[~table.td001_dpe_id.isin(ist_unique)][cols]

    table_gen_multiple[agg_cols] = table_gen_multiple[agg_cols].astype(str)

    agg = table_gen_multiple.groupby(agg_cols).agg({
        'gen_ecs_lib_infer_simp': 'first',
        'type_energie_ecs': 'first',
        'surface_habitable_echantillon': 'sum',
        "nb_generateurs": 'sum',
        'score_gen_ecs_lib_infer': 'mean'

    }).reset_index()

    agg['id_unique'] = agg.td001_dpe_id + agg.gen_ecs_lib_infer + agg.type_installation_ecs

    sys_princ = agg.sort_values(['surface_habitable_echantillon', 'score_gen_ecs_lib_infer'],
                                ascending=False).drop_duplicates(subset='td001_dpe_id')

    id_sys_princ = sys_princ.id_unique.unique().tolist()

    sys_secs = agg.loc[~agg.id_unique.isin(id_sys_princ)]

    sys_sec = sys_secs.sort_values(['surface_habitable_echantillon', 'score_gen_ecs_lib_infer'],
                                   ascending=False).drop_duplicates(
        'td001_dpe_id')

    id_sys_sec = sys_sec.id_unique.unique().tolist()

    sys_terts = agg.loc[~agg.id_unique.isin(id_sys_princ + id_sys_sec)]

    sys_tert_concat = sys_terts.groupby('td001_dpe_id').agg({
        'gen_ecs_lib_infer': lambda x: ' + '.join(sorted(list(set(x)))),

        'gen_ecs_lib_infer_simp': lambda x: ' + '.join(sorted(list(set(x)))),
        'type_energie_ecs': lambda x: ' + '.join(sorted(list(set(x)))),
        'surface_habitable_echantillon': 'sum',
        "nb_generateurs": 'sum',
        'type_installation_ecs': lambda x: ' + '.join(sorted(list(set(x)))),

    }).reset_index()

    sys_princ = sys_princ.append(table_gen_unique[sys_princ.columns])

    sys_princ = sys_princ.rename(columns=sys_ecs_princ_rename)[sys_ecs_princ_rename.values()]

    sys_sec = sys_sec.rename(columns=sys_ecs_sec_rename)[sys_ecs_sec_rename.values()]

    sys_tert_concat = sys_tert_concat.rename(columns=sys_ecs_tert_concat_rename)[
        sys_ecs_tert_concat_rename.values()]

    td001_sys_ecs = td001[['td001_dpe_id']].merge(sys_princ, on='td001_dpe_id', how='left')
    td001_sys_ecs = td001_sys_ecs.merge(sys_sec, on='td001_dpe_id', how='left')
    td001_sys_ecs = td001_sys_ecs.merge(sys_tert_concat, on='td001_dpe_id', how='left')
    nb_installation = td013.groupby('td001_dpe_id').td013_installation_ecs_id.count().to_frame(
        'nb_installations_ecs_total')

    td001_sys_ecs = td001_sys_ecs.merge(nb_installation, on='td001_dpe_id', how='left')

    is_ecs_solaire = (td014.groupby('td001_dpe_id').is_ecs_solaire.sum() > 0).to_frame(
        'is_solaire')
    td001_sys_ecs = td001_sys_ecs.merge(is_ecs_solaire, on='td001_dpe_id', how='left')

    cols_end = sys_princ.columns.tolist() + sys_sec.columns.tolist() + sys_tert_concat.columns.tolist()
    cols_end = np.unique(cols_end).tolist()
    cols_end.remove('td001_dpe_id')
    td001_sys_ecs['nb_gen_ecs_total'] = td001_sys_ecs.sys_ecs_princ_nb_generateur
    td001_sys_ecs['nb_gen_ecs_total'] += td001_sys_ecs.sys_ecs_sec_nb_generateur.fillna(0)
    td001_sys_ecs['nb_gen_ecs_total'] += td001_sys_ecs.sys_ecs_tert_nb_generateurs.fillna(0)

    cols = ['sys_ecs_princ_type_energie_ecs',
            'sys_ecs_sec_type_energie_ecs',
            'sys_ecs_tert_type_energie_ecs_concat']

    td001_sys_ecs['mix_energetique_ecs'] = concat_string_cols(td001_sys_ecs, cols=cols, join_string=' + ',
                                                              is_unique=True, is_sorted=True)

    cols = ['sys_ecs_princ_type_installation_ecs',
            'sys_ecs_sec_type_installation_ecs',
            'sys_ecs_tert_type_installation_ecs_concat']

    td001_sys_ecs['type_installation_ecs_concat'] = concat_string_cols(td001_sys_ecs, cols=cols, join_string=' + ',
                                                                       is_unique=True, is_sorted=True)

    cols = ['sys_ecs_princ_gen_ecs_lib_infer',
            'sys_ecs_sec_gen_ecs_lib_infer',
            'sys_ecs_tert_gen_ecs_lib_infer_concat']

    td001_sys_ecs['gen_ecs_lib_infer_concat'] = concat_string_cols(td001_sys_ecs, cols=cols, join_string=' + ',
                                                                   is_unique=True, is_sorted=True)

    cols = ['sys_ecs_princ_gen_ecs_lib_infer_simp',
            'sys_ecs_sec_gen_ecs_lib_infer_simp',
            'sys_ecs_tert_gen_ecs_lib_infer_simp_concat']

    td001_sys_ecs['gen_ecs_lib_infer_simp_concat'] = concat_string_cols(td001_sys_ecs, cols=cols, join_string=' + ',
                                                                        is_unique=True, is_sorted=True)

    isnull = td001_sys_ecs.sys_ecs_princ_nb_generateur.isnull()
    is_multiple_install = td001_sys_ecs.nb_installations_ecs_total > 1
    td001_sys_ecs.loc[isnull, 'cfg_sys_ecs'] = pd.NA
    td001_sys_ecs.loc[~isnull, 'cfg_sys_ecs'] = 'type de générateur unique/installation unique'
    is_solaire = td001_sys_ecs.sys_ecs_princ_gen_ecs_lib_infer.str.contains('ecs solaire thermique')
    isnull = td001_sys_ecs.sys_ecs_sec_nb_generateur.isnull()
    td001_sys_ecs.loc[
        ~isnull | is_solaire, 'cfg_sys_ecs'] = 'types de générateur multiples/installation unique'
    td001_sys_ecs.loc[(~isnull | is_solaire) & (
        is_multiple_install), 'cfg_sys_ecs'] = 'types de générateur multiples/installations multiples'

    cols_first = [el for el in td001_sys_ecs.columns.tolist() if el not in cols_end]

    cols = cols_first + cols_end

    return td001_sys_ecs[cols]
