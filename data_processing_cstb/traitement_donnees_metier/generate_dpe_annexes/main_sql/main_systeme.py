import pandas as pd
from pathlib import Path
import json
from generate_dpe_annexes.utils import round_float_cols, unique_ordered
from multiprocessing import Pool

from generate_dpe_annexes.td011_td012_processing import merge_td012_tr_tv, postprocessing_td011_td012, \
    merge_td011_tr_tv, \
    agg_systeme_ch_essential
from generate_dpe_annexes.td013_td014_processing import merge_td013_tr_tv, postprocessing_td014, \
    merge_td014_tr_tv, \
    agg_systeme_ecs_essential
from generate_dpe_annexes.td001_merge import merge_td001_dpe_id_system
from generate_dpe_annexes.doc_annexe import td001_annexe_enveloppe_agg_desc, td001_sys_ch_agg_desc, \
    td001_sys_ecs_agg_desc, \
    td007_annexe_desc, td008_annexe_desc, td012_annexe_desc, td014_annexe_desc, enums_cstb, \
    td001_annexe_generale_desc
from generate_dpe_annexes.advanced_system_processing import main_advanced_system_processing
import subprocess
from generate_dpe_annexes.sql_queries import *
from generate_dpe_annexes.config import config
from generate_dpe_annexes.utils import select_only_new_cols,remerge_td001_columns,timeit

@timeit
def run_systeme_processing(dept):

    function_name = "run_systeme_processing"
    logger = config['logger']
    add_cols = ['tv016_departement_id', 'td001_dpe_id', 'annee_construction']
    logger.debug(f'{function_name} -------------- load tables')
    td001_raw = get_td001(dept=dept)
    td002_raw = get_td002(dept=dept)
    td003_raw = get_td003(dept=dept)
    #td006_raw = get_td006(dept=dept)
    td005_raw = get_td005(dept=dept)
    td011_raw = get_td011(dept=dept)
    td012_raw = get_td012(dept=dept)
    td013_raw = get_td013(dept=dept)
    td014_raw = get_td014(dept=dept)
    td016_raw = get_td016(dept=dept)

    # POSTPRO DES TABLES
    logger.debug(f'{function_name} -------------- postprotables')

    td011 = merge_td011_tr_tv(td011_raw)
    td012 = merge_td012_tr_tv(td012_raw)
    td013 = merge_td013_tr_tv(td013_raw)
    td014 = merge_td014_tr_tv(td014_raw)

    td011, td012 = postprocessing_td011_td012(td011, td012)

    td001_sys_ch_agg = agg_systeme_ch_essential(td001_raw, td011, td012)

    td014 = postprocessing_td014(td013, td014, td001_raw, td001_sys_ch_agg)

    td001_sys_ecs_agg = agg_systeme_ecs_essential(td001_raw, td013, td014)

    td001_sys_adv_agg = main_advanced_system_processing(td001_sys_ch_agg=td001_sys_ch_agg, td001_sys_ecs_agg=td001_sys_ecs_agg, td001=td001_raw,
                                                        td002=td002_raw, td016=td016_raw,
                                                        td003=td003_raw, td005=td005_raw, td011_p=td011, td012_p=td012, td014_p=td014)

    sys_dict = dict(
        # td007_paroi_opaque=select_only_new_cols(td007_raw,td007,'td007_paroi_opaque_id',add_cols=add_cols)
        td012_annexe=select_only_new_cols(td012_raw, td012, 'td012_generateur_chauffage_id', add_cols=add_cols),
        td014_annexe=select_only_new_cols(td014_raw, td014, 'td014_generateur_ecs_id', add_cols=add_cols),

    )

    td001_sys_table_dict = dict(
        td001_sys_ch_from_data_annexe=td001_sys_ch_agg,
        td001_sys_ecs_from_data_annexe=td001_sys_ecs_agg,
        td001_sys_adv_annexe=td001_sys_adv_agg
    )

    for k, v in sys_dict.items():
        td001_sys_table_dict[k] = remerge_td001_columns(v, td001_raw, ['tv016_departement_id'])

    for k, v in td001_sys_table_dict.items():
        td001_sys_table_dict[k] = remerge_td001_columns(v, td001_raw, ['tv016_departement_id'])

    logger.debug(f'{function_name} -------------- dump table data')

    for k, v in sys_dict.items():
        dump_sql(table=v, table_name=k, dept=dept)

    for k, v in td001_sys_table_dict.items():
        dump_sql(table=v, table_name=k, dept=dept)



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

    all_depts = get_raw_departements()
    already_processed_depts = get_annexe_departements('td001_sys_adv_annexe')
    depts_to_be_processed = [dept for dept in all_depts if dept not in already_processed_depts]
    if config['multiprocessing']['is_multiprocessing'] is True:

        with Pool(processes=config['multiprocessing']['nb_proc']) as pool:
            pool.starmap(run_systeme_processing, [(dept,) for dept in depts_to_be_processed])
    else:
        for dept in depts_to_be_processed:
            run_systeme_processing(dept)
