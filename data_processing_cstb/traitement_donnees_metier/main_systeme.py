import pandas as pd
from pathlib import Path
import os
import json
from generate_dpe_annexes_scripts import td001_processing
from generate_dpe_annexes_scripts.td001_processing import postprocessing_td001
from generate_dpe_annexes_scripts.utils import round_float_cols, unique_ordered
from config import paths,nb_proc
from multiprocessing import Pool
from generate_dpe_annexes_scripts.td007_processing import merge_td007_tr_tv, postprocessing_td007, generate_pb_table, \
    generate_ph_table, generate_mur_table, agg_td007_mur_to_td001, agg_td007_ph_to_td001, agg_td007_pb_to_td001

from generate_dpe_annexes_scripts.td008_processing import merge_td008_tr_tv, postprocessing_td008
from generate_dpe_annexes_scripts.td001_merge import merge_td001_dpe_id_envelope
from generate_dpe_annexes_scripts.td007_processing import agg_td007_to_td001_essential, agg_surf_envelope
from generate_dpe_annexes_scripts.td010_processing import merge_td010_tr_tv, postprocessing_td010, agg_td010_td001
from generate_dpe_annexes_scripts.td011_td012_processing import merge_td012_tr_tv, postprocessing_td011_td012, \
    merge_td011_tr_tv, \
    agg_systeme_ch_essential
from generate_dpe_annexes_scripts.td013_td014_processing import merge_td013_tr_tv, postprocessing_td014, \
    merge_td014_tr_tv, \
    agg_systeme_ecs_essential
from generate_dpe_annexes_scripts.td001_merge import merge_td001_dpe_id_system
from generate_dpe_annexes_scripts.doc_annexe import td001_annexe_enveloppe_agg_desc, td001_sys_ch_agg_desc, \
    td001_sys_ecs_agg_desc, \
    td007_annexe_desc, td008_annexe_desc, td012_annexe_desc, td014_annexe_desc, enums_cstb, \
    td001_annexe_generale_desc
from generate_dpe_annexes_scripts.td006_processing import agg_td006_td001, merge_td006_tr_tv
from generate_dpe_annexes_scripts.gorenove_scripts import concat_td001_gorenove
from generate_dpe_annexes_scripts.advanced_system_processing import main_advanced_system_processing
import subprocess


def run_system_processing(td001, td006, td011, td012, td013, td014):
    td011_raw_cols = td011.columns.tolist()
    td012_raw_cols = td012.columns.tolist()
    td013_raw_cols = td013.columns.tolist()
    td014_raw_cols = td014.columns.tolist()
    td001, td006, td011, td012, td013, td014 = merge_td001_dpe_id_system(td001, td006, td011, td012, td013, td014)
    td011 = merge_td011_tr_tv(td011)
    td012 = merge_td012_tr_tv(td012)
    td013 = merge_td013_tr_tv(td013)
    td014 = merge_td014_tr_tv(td014)

    td011, td012 = postprocessing_td011_td012(td011, td012)

    cols = [el for el in td011.columns if el not in td011_raw_cols]
    cols.append('td011_installation_chauffage_id')
    cols = unique_ordered(cols)
    td011_p = td011[cols]

    cols = [el for el in td012.columns if
            el not in td012_raw_cols + ['besoin_chauffage_infer', 'gen_ch_concat_txt_desc']]
    cols.append('td012_generateur_chauffage_id')
    cols = unique_ordered(cols)
    td012_p = td012[cols]

    td001_sys_ch_agg = agg_systeme_ch_essential(td001, td011, td012)

    td014 = postprocessing_td014(td013, td014, td001, td001_sys_ch_agg)

    cols = [el for el in td013.columns if el not in td013_raw_cols]
    cols.append('td013_installation_ecs_id')
    cols = unique_ordered(cols)
    td013_p = td013[cols]

    cols = [el for el in td014.columns if
            el not in td014_raw_cols + ['score_gen_ecs_lib_infer', 'gen_ecs_concat_txt_desc']]
    cols.append('td014_generateur_ecs_id')
    cols = unique_ordered(cols)
    td014_p = td014[cols]

    td001_sys_ecs_agg = agg_systeme_ecs_essential(td001, td013, td014)

    return td011_p, td012_p, td001_sys_ch_agg, td013_p, td014_p, td001_sys_ecs_agg


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


data_dir = paths['DPE_DEPT_PATH']
annexe_dir = paths['DPE_DEPT_ANNEXE_PATH']
annexe_dir = Path(annexe_dir)
annexe_dir.mkdir(exist_ok=True, parents=True)


def run_postprocessing_by_depts(dept_dir):
    print(dept_dir)
    annexe_dept_dir = annexe_dir / dept_dir.name
    annexe_dept_dir.mkdir(exist_ok=True, parents=True)
    # LOAD TABLES
    td001 = pd.read_csv(dept_dir / 'td001_dpe.csv', dtype=str)
    td002 = pd.read_csv(dept_dir / 'td002_consommation.csv', dtype=str)
    td003 = pd.read_csv(dept_dir / 'td003_descriptif.csv', dtype=str)
    td005 = pd.read_csv(dept_dir / 'td005_fiche_technique.csv', dtype=str)
    td006 = pd.read_csv(dept_dir / 'td006_batiment.csv', dtype=str)
    td016 = pd.read_csv(dept_dir / 'td016_facture.csv', dtype=str)

    # SYSTEM PROCESSING

    td011 = pd.read_csv(dept_dir / 'td011_installation_chauffage.csv', dtype=str)
    td012 = pd.read_csv(dept_dir / 'td012_generateur_chauffage.csv', dtype=str)
    td013 = pd.read_csv(dept_dir / 'td013_installation_ecs.csv', dtype=str)
    td014 = pd.read_csv(dept_dir / 'td014_generateur_ecs.csv', dtype=str)

    td011_p, td012_p, td001_sys_ch_agg, td013_p, td014_p, td001_sys_ecs_agg = run_system_processing(td001, td006,
                                                                                                    td011, td012,
                                                                                                    td013, td014)
    round_float_cols(td001_sys_ch_agg).to_csv(annexe_dept_dir / 'td001_sys_ch_agg_annexe.csv')
    round_float_cols(td001_sys_ecs_agg).to_csv(annexe_dept_dir / 'td001_sys_ecs_agg_annexe.csv')
    round_float_cols(td011_p).to_csv(annexe_dept_dir / 'td011_installation_chauffage_annexe.csv')
    round_float_cols(td012_p).to_csv(annexe_dept_dir / 'td012_generateur_chauffage_annexe.csv')
    round_float_cols(td013_p).to_csv(annexe_dept_dir / 'td013_installation_ecs_annexe.csv')
    round_float_cols(td014_p).to_csv(annexe_dept_dir / 'td014_generateur_ecs_annexe.csv')

    td001_sys_adv_agg = main_advanced_system_processing(td001_sys_ch_agg=td001_sys_ch_agg, td001_sys_ecs_agg=td001_sys_ecs_agg, td001=td001,
                                    td002=td002, td016=td016,
                                    td003=td003, td005=td005, td011_p=td011_p, td012_p=td012_p, td014_p=td014_p)

    round_float_cols(td001_sys_adv_agg).to_csv(annexe_dept_dir / 'td001_sys_agg_adv.csv')


if __name__ == '__main__':
    build_doc(annexe_dir)
    list_dir = list(Path(data_dir).iterdir())
    firsts = [a_dir for a_dir in list_dir if
              not (annexe_dir / a_dir.name / 'td001_sys_agg_adv.csv').is_file()]
    lasts = [a_dir for a_dir in list_dir if (annexe_dir / a_dir.name / 'td001_sys_agg_adv.csv').is_file()]
    print(len(firsts), len(lasts))
    list_dir = firsts + lasts
    list_dir = firsts


    # list_dir = [el for el in list_dir if '94' in el.name]
    # list_dir.reverse()

    p_es = subprocess.Popen(str(es_server_path.absolute()))

    with Pool(processes=nb_proc) as pool:
        pool.starmap(run_postprocessing_by_depts, [(dept_dir,) for dept_dir in list_dir])

    p_es.terminate()