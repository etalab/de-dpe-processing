import pandas as pd
from pathlib import Path
import os
from generate_dpe_annexes.td001_processing import postprocessing_td001
from generate_dpe_annexes.utils import round_float_cols
from multiprocessing import Pool
from generate_dpe_annexes.sql_queries import *
from generate_dpe_annexes.config import config
from generate_dpe_annexes.td006_processing import agg_td006_td001, merge_td006_tr_tv
from generate_dpe_annexes.advanced_general_processing import main_advanced_general_processing
from generate_dpe_annexes.utils import remerge_td001_columns
import subprocess
from generate_dpe_annexes.utils import select_only_new_cols,remerge_td001_columns

#@timeit
def run_general_processing(dept):
    function_name = "run_general_processing"
    logger = config['logger']
    add_cols = ['tv016_departement_id', 'td001_dpe_id', 'annee_construction']
    logger.debug(f'{function_name} -------------- load tables')
    td001_raw = get_td001(dept=dept)
    td003_raw = get_td003(dept=dept)
    td006_raw = get_td006(dept=dept)
    td005_raw = get_td005(dept=dept)
    td016_raw = get_td016(dept=dept)
    td002_raw = get_td002(dept=dept)
    td007_raw = get_td007(dept=dept)
    td012_raw = get_td012(dept=dept)
    td017_raw = get_td017(dept=dept)

    logger.debug(f'{function_name} -------------- postprocessing')

    td001 = postprocessing_td001(td001=td001_raw, td002=td002_raw, td007=td007_raw, td012=td012_raw, td016=td016_raw, td017=td017_raw)
    td006 = merge_td006_tr_tv(td006_raw)
    td001_td006 = agg_td006_td001(td006=td006)

    logger.debug(f'{function_name} -------------- advanced')

    td001 = select_only_new_cols(td001_raw, td001, 'td001_dpe_id')
    td001_gen_agg_adv = main_advanced_general_processing(td001=td001, td003=td003_raw, td005=td005_raw, td001_td006=td001_td006)
    td001_general_table_dict = dict(
        td001_gen_agg_adv_annexe=td001_gen_agg_adv)

    logger.debug(f'{function_name} -------------- dump sql')

    for k, v in td001_general_table_dict.items():
        td001_general_table_dict[k] = remerge_td001_columns(v, td001_raw, ['tv016_departement_id'])

    for k, v in td001_general_table_dict.items():
        dump_sql(table=v, table_name=k, dept=dept)


if __name__ == '__main__':

    all_depts = get_raw_departements()
    already_processed_depts = get_annexe_departements('td001_gen_agg_adv_annexe')
    depts_to_be_processed = [dept for dept in all_depts if dept not in already_processed_depts]
    if config['multiprocessing']['is_multiprocessing'] is True:

        with Pool(processes=config['multiprocessing']['nb_proc']) as pool:
            pool.starmap(run_general_processing, [(dept,) for dept in depts_to_be_processed])
    else:
        for dept in depts_to_be_processed:
            run_general_processing(dept)
