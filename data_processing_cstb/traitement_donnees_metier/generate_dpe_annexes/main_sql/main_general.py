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
import subprocess




def run_general_processing(dept):

    function_name = "run_general_processing"
    logger = config['logger']
    add_cols = ['tv016_departement_id', 'td001_dpe_id', 'annee_construction']
    logger.debug(f'{function_name} -------------- load tables')
    td001_raw = get_td001(dept=dept)
    td003_raw = get_td003(dept=dept)
    td006_raw = get_td006(dept=dept)
    td005_raw = get_td005(dept=dept)



    td001 = postprocessing_td001(td001_raw)
    td006 = merge_td006_tr_tv(td006_raw)
    td001_td006 = agg_td006_td001(td006=td006)

    round_float_cols(td001_td006).to_csv(annexe_dept_dir / 'td001_td006_agg_annexe.csv')
    td001_gen = main_advanced_general_processing(td001=td001, td003=td003, td005=td005, td001_td006=td001_td006)
    round_float_cols(td001_gen).to_csv(annexe_dept_dir / 'td001_gen_agg_adv.csv')

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

def run_postprocessing_by_depts(dept_dir):
    print(dept_dir)
    annexe_dept_dir = annexe_dir / dept_dir.name
    annexe_dept_dir.mkdir(exist_ok=True, parents=True)
    # LOAD TABLES
    td003 = pd.read_csv(dept_dir / 'td003_descriptif.csv', dtype=str)
    td005 = pd.read_csv(dept_dir / 'td005_fiche_technique.csv', dtype=str)
    td006 = pd.read_csv(dept_dir / 'td006_batiment.csv', dtype=str)
    td001 = pd.read_csv(dept_dir / 'td001_dpe.csv', dtype=str)
    td001 = td001.rename(columns={'id': 'td001_dpe_id'})
    td001 = postprocessing_td001(td001)
    td006 = merge_td006_tr_tv(td006)

    td001_td006 = agg_td006_td001(td006=td006)
    round_float_cols(td001_td006).to_csv(annexe_dept_dir / 'td001_td006_agg_annexe.csv')
    td001_gen = main_advanced_general_processing(td001=td001, td003=td003, td005=td005, td001_td006=td001_td006)
    round_float_cols(td001_gen).to_csv(annexe_dept_dir / 'td001_gen_agg_adv.csv')


if __name__ == '__main__':
    list_dir = list(Path(data_dir).iterdir())
    firsts = [a_dir for a_dir in list_dir if
              not (annexe_dir / a_dir.name / 'td001_gen_agg_adv.csv').is_file()]
    lasts = [a_dir for a_dir in list_dir if (annexe_dir / a_dir.name / 'td001_gen_agg_adv.csv').is_file()]
    lasts = list(reversed(sorted(lasts, key=os.path.getmtime)))
    print(len(firsts), len(lasts))
    list_dir = firsts + lasts
    list_dir = firsts
    # list_dir = [el for el in list_dir if '94' in el.name]
    # list_dir.reverse()
    # for dept_dir in list_dir:
    #     if dept_dir.name == '94':
    #         print(dept_dir)
    #         run_postprocessing_by_depts(dept_dir)
    p_es = subprocess.Popen(str(es_server_path.absolute()))

    with Pool(processes=nb_proc) as pool:
        pool.starmap(run_postprocessing_by_depts, [(dept_dir,) for dept_dir in list_dir])

    p_es.terminate()