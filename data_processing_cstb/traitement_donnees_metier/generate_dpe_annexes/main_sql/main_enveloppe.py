
import json
from multiprocessing import Pool
from generate_dpe_annexes.td007_processing import merge_td007_tr_tv, postprocessing_td007, generate_pb_table, \
    generate_ph_table, generate_mur_table, agg_td007_mur_to_td001, agg_td007_ph_to_td001, agg_td007_pb_to_td001

from generate_dpe_annexes.td008_processing import merge_td008_tr_tv, postprocessing_td008
from generate_dpe_annexes.td007_processing import agg_surf_envelope
from generate_dpe_annexes.td008_processing import agg_td008_to_td001
from generate_dpe_annexes.td010_processing import merge_td010_tr_tv, postprocessing_td010, agg_td010_td001
from generate_dpe_annexes.doc_annexe import td001_annexe_enveloppe_agg_desc, td001_sys_ch_agg_desc, \
    td001_sys_ecs_agg_desc, \
    td007_annexe_desc, td008_annexe_desc, td012_annexe_desc, td014_annexe_desc, enums_cstb, \
    td001_annexe_generale_desc
from generate_dpe_annexes.td006_processing import merge_td006_tr_tv
from generate_dpe_annexes.advanced_enveloppe_processing import main_advanced_enveloppe_processing
from generate_dpe_annexes.sql_queries import *
from generate_dpe_annexes.config import config
from generate_dpe_annexes.sql_config import engine, sql_config
from generate_dpe_annexes.utils import select_only_new_cols,remerge_td001_columns
import multiprocessing
import sys
import traceback as tb
#@timeit
def run_enveloppe_processing(dept):
    try:
        function_name = "run_enveloppe_processing"
        logger = config['logger']
        add_cols = ['tv016_departement_id', 'td001_dpe_id', 'annee_construction']
        logger.debug(f'{function_name} -------------- init for department {dept}')

        logger.debug(f'{function_name} -------------- load tables')
        td001_raw = get_td001(dept=dept)
        td006_raw = get_td006(dept=dept)
        td007_raw = get_td007(dept=dept)
        td008_raw = get_td008(dept=dept)
        td010_raw = get_td010(dept=dept)
        td003_raw = get_td003(dept=dept)
        td005_raw = get_td005(dept=dept)

        logger.debug(f'{function_name} -------------- postprotables')

        # POSTPRO DES TABLES
        td006 = merge_td006_tr_tv(td006_raw)

        td008 = merge_td008_tr_tv(td008_raw)
        td008 = postprocessing_td008(td008)

        td007 = merge_td007_tr_tv(td007_raw)
        td007 = postprocessing_td007(td007, td008)

        td010 = merge_td010_tr_tv(td010_raw)
        td010 = postprocessing_td010(td010)

        # TABLES PAR TYPE COMPOSANT
        td007_pb = generate_pb_table(td007)
        td007_ph = generate_ph_table(td007)
        td007_mur = generate_mur_table(td007)

        logger.debug(f'{function_name} -------------- aggregation td001')

        # # TABLES SYNTHETIQUES TOUTES THEMATIQUES
        #
        # td007_agg_essential = agg_td007_to_td001_essential(td007)
        # td008_agg_essential = agg_td008_to_td001_essential(td008)
        surfaces_agg_essential = agg_surf_envelope(td007, td008)
        #
        # td001_enveloppe_agg = pd.concat([td007_agg_essential, td008_agg_essential, surfaces_agg_essential], axis=1)

        # td001_enveloppe_agg.index.name = 'td001_dpe_id'

        # TABLES AGGREGEES PAR TYPE COMPOSANT
        td007_murs_agg = agg_td007_mur_to_td001(td007_mur)
        td007_ph_agg = agg_td007_ph_to_td001(td007_ph)
        td007_pb_agg = agg_td007_pb_to_td001(td007_pb)
        td008_agg = agg_td008_to_td001(td008)
        td010_agg = agg_td010_td001(td010)

        env_compo_dict = dict(
            # td007_paroi_opaque=select_only_new_cols(td007_raw,td007,'td007_paroi_opaque_id',add_cols=add_cols)
            td007_ph_annexe=select_only_new_cols(td007_raw, td007_ph, 'td007_paroi_opaque_id', add_cols=add_cols),
            td007_pb_annexe=select_only_new_cols(td007_raw, td007_pb, 'td007_paroi_opaque_id', add_cols=add_cols),
            td007_mur_annexe=select_only_new_cols(td007_raw, td007_mur, 'td007_paroi_opaque_id', add_cols=add_cols),
            td008_baie_annexe=select_only_new_cols(td008_raw, td008, 'td008_baie_id', add_cols=add_cols),
            td010_pont_thermique_annexe=select_only_new_cols(td010_raw, td010, 'td010_pont_thermique_id', add_cols=add_cols)
        )

        env_compo_agg_dict = dict(td007_mur_agg_annexe=td007_murs_agg,
                                  td001_surfaces_agg_essential_annexe=surfaces_agg_essential,
                                  td007_ph_agg_annexe=td007_ph_agg,
                                  td007_pb_agg_annexe=td007_pb_agg,
                                  td008_baie_agg_annexe=td008_agg,
                                  td010_pont_thermique_agg_annexe=td010_agg)

        for k, v in env_compo_agg_dict.items():
            env_compo_agg_dict[k] = remerge_td001_columns(v, td001_raw, ['tv016_departement_id'])

        logger.debug(f'{function_name} -------------- dump table data')

        for k, v in env_compo_dict.items():
            dump_sql(table=v, table_name=k, dept=dept)

        for k, v in env_compo_agg_dict.items():
            dump_sql(table=v, table_name=k, dept=dept)

        logger.debug(f'{function_name} -------------- traitement avancé + traitement texte')

        td001_env_adv_agg = main_advanced_enveloppe_processing(td001=td001_raw, env_compo_agg_dict=env_compo_agg_dict,
                                                               td003=td003_raw, td005=td005_raw)

        td001_env_adv_agg = remerge_td001_columns(td001_env_adv_agg, td001_raw, ['tv016_departement_id'])
        dump_sql(table=td001_env_adv_agg, table_name="td001_env_adv_agg_annexe", dept=dept)
    except Exception as e:
        with open(f'{function_name}_error_log','w') as f:
            f.write(tb.format_exc())
        raise e
def build_doc(annexe_dir):
    doc_annexe = dict()
    doc_annexe['td001_annexe_generale'] = td001_annexe_generale_desc
    doc_annexe['td001_annexe_enveloppe_agg'] = td001_annexe_enveloppe_agg_desc
    doc_annexe['td001_sys_ch_agg'] = td001_sys_ch_agg_desc
    doc_annexe['td001_sys_ecs_agg'] = td001_sys_ecs_agg_desc
    doc_annexe['td007_annexe'] = td007_annexe_desc
    doc_annexe['td008_annexe'] = td008_annexe_desc
    doc_annexe['td012_annexe'] = td012_annexe_desc
    doc_annexe['td014_annexe'] = td014_annexe_desc

    with open(annexe_dir / 'doc_table_annexes_cstb.json', 'w', encoding='utf-8') as f:
        json.dump(doc_annexe, f, indent=4)

    with open(annexe_dir / 'enum_table_annexes_cstb.json', 'w', encoding='utf-8') as f:
        json.dump(enums_cstb, f, indent=4)


if __name__ == '__main__':
    nb_args = len(sys.argv) - 1
    if nb_args > 0:
        config['multiprocessing']['is_multiprocessing'] = sys.argv[1]
    all_depts = get_raw_departements()
    already_processed_depts = get_annexe_departements('td001_env_adv_agg_annexe')
    depts_to_be_processed = [dept for dept in all_depts if dept not in already_processed_depts]
    if config['multiprocessing']['is_multiprocessing'] == True:

        with multiprocessing.get_context('spawn').Pool(processes=config['multiprocessing']['nb_proc']) as pool:
            pool.starmap(run_enveloppe_processing, [(dept,) for dept in depts_to_be_processed])
    else:
        for dept in depts_to_be_processed:
            run_enveloppe_processing(dept)
