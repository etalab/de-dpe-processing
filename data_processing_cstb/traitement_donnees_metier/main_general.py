import pandas as pd
from pathlib import Path
import os
import json
from generate_dpe_annexes_scripts import td001_processing
from generate_dpe_annexes_scripts.td001_processing import postprocessing_td001
from generate_dpe_annexes_scripts.utils import round_float_cols, unique_ordered
from config import paths,nb_proc
from multiprocessing import Pool

from generate_dpe_annexes_scripts.td006_processing import agg_td006_td001, merge_td006_tr_tv
from generate_dpe_annexes_scripts.gorenove_scripts import concat_td001_gorenove
from generate_dpe_annexes_scripts.advanced_general_processing import main_advanced_general_processing
import subprocess

data_dir = paths['DPE_DEPT_PATH']
annexe_dir = paths['DPE_DEPT_ANNEXE_PATH']
annexe_dir = Path(annexe_dir)
annexe_dir.mkdir(exist_ok=True, parents=True)
es_server_path = paths['ES_SERVER_PATH']

def run_postprocessing_by_depts(dept_dir):
    print(dept_dir)
    annexe_dept_dir = annexe_dir / dept_dir.name
    annexe_dept_dir.mkdir(exist_ok=True, parents=True)
    # LOAD TABLES
    td007 = pd.read_csv(dept_dir / 'td007_paroi_opaque.csv', dtype=str)
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